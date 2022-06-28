use crate::prelude::*;

use {
    async_trait::async_trait,
    net2::TcpStreamExt, // for set_keepalive
    serde::{Serialize, Serializer},
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

#[derive(PartialEq, Debug, Clone)]
pub enum ChannelData {
    Disconnect(Serial), // strictly speaking, only ever goes inverter->coordinator, but eh.
    Packet(Packet),     // this one goes both ways through the channel.
    Shutdown,
}
pub type Sender = broadcast::Sender<ChannelData>;
pub type Receiver = broadcast::Receiver<ChannelData>;

// WaitForReply {{{
#[async_trait]
pub trait WaitForReply {
    #[cfg(not(feature = "mocks"))]
    const TIMEOUT: u64 = 10;

    #[cfg(feature = "mocks")]
    const TIMEOUT: u64 = 0; // fail immediately in tests

    async fn wait_for_reply(&mut self, packet: &Packet) -> Result<Packet>;
}
#[async_trait]
impl WaitForReply for Receiver {
    async fn wait_for_reply(&mut self, packet: &Packet) -> Result<Packet> {
        let start = std::time::Instant::now();

        loop {
            match (packet, self.try_recv()) {
                (
                    Packet::TranslatedData(td),
                    Ok(ChannelData::Packet(Packet::TranslatedData(reply))),
                ) => {
                    if td.datalog == reply.datalog
                        && td.register == reply.register
                        && td.device_function == reply.device_function
                    {
                        return Ok(Packet::TranslatedData(reply));
                    }
                }
                (_, Ok(ChannelData::Packet(_))) => {} // TODO ReadParam and WriteParam
                (_, Ok(ChannelData::Disconnect(inverter_datalog))) => {
                    if inverter_datalog == packet.datalog() {
                        bail!("inverter disconnect?");
                    }
                }
                (_, Ok(ChannelData::Shutdown)) => bail!("shutting down"),
                (_, Err(broadcast::error::TryRecvError::Empty)) => {} // ignore and loop
                (_, Err(err)) => bail!("try_recv error: {:?}", err),
            }
            if start.elapsed().as_secs() > Self::TIMEOUT {
                bail!("wait_for_reply {:?} - timeout", packet);
            }

            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
} // }}}

// Serial {{{
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Serial([u8; 10]);

impl Serial {
    pub fn new(input: &[u8]) -> Result<Self> {
        Ok(Self(input.try_into()?))
    }

    pub fn default() -> Self {
        Self([0; 10])
    }

    pub fn data(&self) -> [u8; 10] {
        self.0
    }
}

impl Serialize for Serial {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl std::str::FromStr for Serial {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 10 {
            return Err(anyhow!("{} must be exactly 10 characters", s));
        }

        let mut r: [u8; 10] = Default::default();
        r.copy_from_slice(s.as_bytes());
        Ok(Self(r))
    }
}

impl std::fmt::Display for Serial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(&self.0))
    }
}

impl std::fmt::Debug for Serial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from_utf8_lossy(&self.0))
    }
} // }}}

pub struct Inverter {
    idx: usize,
    config: RefCell<Config>,
    channels: Channels,
}

impl Inverter {
    pub fn new(idx: usize, config: Config, channels: Channels) -> Self {
        let config = RefCell::new(config);
        Self {
            idx,
            config,
            channels,
        }
    }

    fn enabled(&self) -> bool {
        let i = &self.config.borrow().inverters[self.idx];

        if !i.enabled {
            info!("inverter {} is disabled, not connecting", i.datalog);
        }

        i.enabled
    }

    pub async fn start(&self) -> Result<()> {
        if !self.enabled() {
            return Ok(());
        }

        while let Err(e) = self.connect().await {
            let datalog = self.config.borrow().inverters[self.idx].datalog;
            error!("inverter {}: {}", datalog, e);
            info!("inverter {}: reconnecting in 5s", datalog);
            self.channels
                .from_inverter
                .send(ChannelData::Disconnect(datalog))?; // kill any waiting readers
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }

        Ok(())
    }

    pub fn stop(&self) {
        let _ = self.channels.to_inverter.send(ChannelData::Shutdown);
    }

    async fn connect(&self) -> Result<()> {
        let inverter_hp = {
            let config = &self.config.borrow().inverters[self.idx];

            info!(
                "connecting to inverter {} at {}:{}",
                &config.datalog, &config.host, config.port
            );

            (config.host.to_string(), config.port)
        };

        let stream = tokio::net::TcpStream::connect(inverter_hp).await?;
        let std_stream = stream.into_std()?;
        std_stream.set_keepalive(Some(std::time::Duration::new(60, 0)))?;
        let (reader, writer) = tokio::net::TcpStream::from_std(std_stream)?.into_split();

        info!(
            "inverter {}: connected!",
            self.config.borrow().inverters[self.idx].datalog
        );

        futures::try_join!(
            self.config_updater(),
            self.sender(writer),
            self.receiver(reader)
        )?;

        Ok(())
    }

    async fn config_updater(&self) -> Result<()> {
        let mut receiver = self.channels.config_updates.subscribe();

        use config::ChannelData::*;

        loop {
            match receiver.recv().await? {
                ConfigUpdated(new) => *self.config.borrow_mut() = new,
                Shutdown => break,
                _ => continue,
            }
        }

        Ok(())
    }

    // inverter -> coordinator
    async fn receiver(&self, mut socket: tokio::net::tcp::OwnedReadHalf) -> Result<()> {
        use {bytes::BytesMut, tokio_util::codec::Decoder};

        let mut buf = BytesMut::new();
        let mut decoder = lxp::packet_decoder::PacketDecoder::new();

        loop {
            // read_buf appends to buf rather than overwrite existing data
            let len = socket.read_buf(&mut buf).await?;

            // TODO: reconnect if nothing for 5 minutes?
            // or maybe send our own heartbeats?

            if len == 0 {
                while let Some(packet) = decoder.decode_eof(&mut buf)? {
                    // bytes received are logged in packet_decoder, no need here
                    //debug!("inverter {}: RX {:?}", self.config.datalog, packet);

                    self.channels
                        .from_inverter
                        .send(ChannelData::Packet(packet))?;
                }
                break;
            }

            while let Some(packet) = decoder.decode(&mut buf)? {
                // bytes received are logged in packet_decoder, no need here
                //debug!("inverter {}: RX {:?}", self.config.datalog, packet);

                self.channels
                    .from_inverter
                    .send(ChannelData::Packet(packet.clone()))?;

                self.compare_datalog(packet.datalog()); // all packets have datalog serial
                if let Packet::TranslatedData(td) = packet {
                    // only TranslatedData has inverter serial
                    self.compare_serial(td.inverter);
                };
            }
        }

        Err(anyhow!("lost connection"))
    }

    // coordinator -> inverter
    async fn sender(&self, mut socket: tokio::net::tcp::OwnedWriteHalf) -> Result<()> {
        let mut receiver = self.channels.to_inverter.subscribe();

        use ChannelData::*;

        loop {
            match receiver.recv().await? {
                Shutdown => break,
                // this doesn't actually happen yet; Disconnect is never sent to this channel
                Disconnect(_) => bail!("sender exiting due to ChannelData::Disconnect"),
                Packet(packet) => {
                    let bytes = {
                        let config = &self.config.borrow().inverters[self.idx];
                        if packet.datalog() != config.datalog {
                            continue;
                        }

                        //debug!("inverter {}: TX {:?}", config.datalog, packet);
                        let bytes = lxp::packet::TcpFrameFactory::build(&packet);
                        debug!("inverter {}: TX {:?}", config.datalog, bytes);
                        bytes
                    };
                    socket.write_all(&bytes).await?
                }
            }
        }

        let config = &self.config.borrow().inverters[self.idx];
        info!("inverter {}: sender exiting", config.datalog);

        Ok(())
    }

    fn compare_datalog(&self, packet: Serial) {
        let config = &self.config.borrow().inverters[self.idx];
        if packet == config.datalog {
            return;
        }

        warn!(
            "datalog serial mismatch found; packet={}, config={} - please check config!",
            packet, config.datalog
        );

        //construct a new config::Inverter object with the correct datalog
        let config = config.clone(); // copy of the old config
        let mut new = config.clone();
        new.datalog = packet;

        self.channels
            .config_updates
            .send(config::ChannelData::UpdateInverter(config, new))
            .unwrap();
    }

    fn compare_serial(&self, packet: Serial) {
        let config = &self.config.borrow().inverters[self.idx];
        if packet == config.serial {
            return;
        }

        warn!(
            "inverter serial mismatch found; packet={}, config={} - please check config!",
            packet, config.serial
        );

        //construct a new config::Inverter object with the correct datalog
        let config = config.clone(); // copy of the old config
        let mut new = config.clone();
        new.serial = packet;

        self.channels
            .config_updates
            .send(config::ChannelData::UpdateInverter(config, new))
            .unwrap();
    }

    /*
    fn compare_inverter(&self, packet: Serial) {
        let b_inverter = self.config().serial;

        if packet != b_inverter {
            warn!(
                "inverter serial mismatch found; packet={}, config={} - please check config!",
                packet, b_inverter
            );

            //construct a new config::Inverter object with the correct serial
            let mut new = self.config().clone();
            new.serial = packet;

            self.channels
                .config_updates
                .send(config::ChannelData::UpdateInverter(
                    self.config().clone(),
                    new,
                ))
                .unwrap();
        }
    }
    */
}
