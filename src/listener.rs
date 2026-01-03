use crate::connection_action::ConnectionAction;
use crate::flv;
use crate::state::State;
use access_unit::aac::ensure_adts_header;
use access_unit::AccessUnit;
use bytes::{Bytes, BytesMut};
use futures::future::FutureExt;
use gatekeeper::Gatekeeper;
use rml_rtmp::chunk_io::Packet;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult, StreamMetadata,
};
use std::collections::VecDeque;
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{error, info};

const METRICS_INTERVAL: u32 = 400;
const RTP_NEW: &str = "RTP:NEW";
const RTP_UP: &str = "RTP:UP ";
const RTP_DOWN: &str = "RTP:DOWN";

pub struct Connection {
    id: i32,
    session: Option<ServerSession>,
    state: State,
    pub metadata: Option<StreamMetadata>,
    api_addr: Option<String>,
    sps_pps: Option<Bytes>,
    tx: mpsc::Sender<AccessUnit>,
    tx_shutdown: watch::Sender<()>,
    gatekeeper: Arc<Gatekeeper>,
    authed_id: Option<u64>,
    authed_key: Option<String>,
    up_counter: u32,
}

impl Connection {
    pub fn new(
        id: i32,
        tx: mpsc::Sender<AccessUnit>,
        tx_shutdown: watch::Sender<()>,
        gatekeeper: Arc<Gatekeeper>,
    ) -> Self {
        Connection {
            id,
            session: None,
            state: State::Waiting,
            metadata: None,
            api_addr: None,
            sps_pps: None,
            tx,
            tx_shutdown,
            gatekeeper,
            authed_id: None,
            authed_key: None,
            up_counter: 0,
        }
    }

    pub async fn start_handshake(
        self,
        mut stream: TcpStream,
        tx_key: oneshot::Sender<String>,
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
        let mut handshake = Handshake::new(PeerType::Server);
        let server_p0_and_1 = handshake.generate_outbound_p0_and_p1()?;
        stream.write_all(&server_p0_and_1).await?;

        let mut buffer = [0; 4096];
        loop {
            let bytes_read = stream.read(&mut buffer).await?;
            if bytes_read == 0 {
                return Ok(());
            }

            match handshake.process_bytes(&buffer[0..bytes_read])? {
                HandshakeProcessResult::InProgress { response_bytes } => {
                    stream.write_all(&response_bytes).await?;
                }
                HandshakeProcessResult::Completed {
                    response_bytes,
                    remaining_bytes,
                } => {
                    stream.write_all(&response_bytes).await?;
                    spawn(self.start_connection_manager(stream, remaining_bytes, tx_key));
                    return Ok(());
                }
            }
        }
    }

    async fn start_connection_manager(
        mut self,
        stream: TcpStream,
        received_bytes: Vec<u8>,
        tx_key: oneshot::Sender<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (stream_reader, stream_writer) = tokio::io::split(stream);
        let (read_bytes_sender, mut read_bytes_receiver) = mpsc::unbounded_channel();
        let (mut write_bytes_sender, write_bytes_receiver) = mpsc::unbounded_channel();

        spawn(connection_reader(self.id, stream_reader, read_bytes_sender));
        spawn(connection_writer(
            self.id,
            stream_writer,
            write_bytes_receiver,
        ));

        let config = ServerSessionConfig::new();
        let (session, mut results) = ServerSession::new(config)?;
        self.session = Some(session);

        let remaining_bytes_results = self
            .session
            .as_mut()
            .unwrap()
            .handle_input(&received_bytes)?;
        results.extend(remaining_bytes_results);

        let mut key_sender = Some(tx_key);

        loop {
            let action = self.handle_session_results(&mut results, &mut write_bytes_sender)?;
            if action == ConnectionAction::New {
                match self.state {
                    State::Publishing {
                        ref app_name,
                        ref stream_key,
                    } => {
                        if let Some(sender) = key_sender.take() {
                            sender.send(stream_key.clone());
                        }
                    }
                    _ => {}
                }
            }
            if action == ConnectionAction::Disconnect {
                break;
            }

            tokio::select! {
                message = read_bytes_receiver.recv() => {
                    match message {
                        None => break,
                        Some(bytes) => {
                           results = self.session.as_mut()
                                .unwrap()
                                .handle_input(&bytes)?;
                        }
                    }
                }
            }
        }

        if let (Some(k), Some(id)) = (self.authed_key.as_deref(), self.authed_id) {
            info!("{} key={} id={}", RTP_DOWN, k, id);
        }

        let _ = self.tx_shutdown.send(());
        Ok(())
    }

    fn handle_session_results(
        &mut self,
        results: &mut Vec<ServerSessionResult>,
        byte_writer: &mut UnboundedSender<Packet>,
    ) -> Result<ConnectionAction, Box<dyn std::error::Error + Sync + Send>> {
        if results.is_empty() {
            return Ok(ConnectionAction::None);
        }

        let mut ret = ConnectionAction::None;
        let mut new_results = Vec::new();
        for result in results.drain(..) {
            match result {
                ServerSessionResult::OutboundResponse(packet) => {
                    if !send(&byte_writer, packet) {
                        break;
                    }
                }

                ServerSessionResult::RaisedEvent(event) => {
                    let action =
                        self.handle_raised_event(event, &mut new_results, byte_writer.clone())?;
                    if action == ConnectionAction::Disconnect {
                        return Ok(ConnectionAction::Disconnect);
                    }
                    if action == ConnectionAction::New {
                        ret = ConnectionAction::New;
                    }
                }

                ServerSessionResult::UnhandleableMessageReceived(_) => {}
            }
        }

        self.handle_session_results(&mut new_results, byte_writer)?;
        Ok(ret)
    }

    fn handle_raised_event(
        &mut self,
        event: ServerSessionEvent,
        new_results: &mut Vec<ServerSessionResult>,
        byte_writer: UnboundedSender<Packet>,
    ) -> Result<ConnectionAction, Box<dyn std::error::Error + Sync + Send>> {
        match event {
            ServerSessionEvent::ConnectionRequested {
                request_id,
                app_name,
            } => {
                new_results.extend(self.session.as_mut().unwrap().accept_request(request_id)?);
                self.state = State::Connected { app_name };
            }

            ServerSessionEvent::PublishStreamRequested {
                request_id,
                app_name,
                mode,
                stream_key,
            } => {
                let sk = match self.gatekeeper.streamkey(&stream_key) {
                    Ok(v) => v,
                    Err(_) => {
                        if let Ok(results) = self.session.as_mut().unwrap().reject_request(
                            request_id.clone(),
                            "NetStream.Publish.Denied",
                            "Unauthorized or invalid stream key",
                        ) {
                            for r in results {
                                if let ServerSessionResult::OutboundResponse(packet) = r {
                                    if !send(&byte_writer, packet) {
                                        break;
                                    }
                                }
                            }
                        }
                        return Ok(ConnectionAction::Disconnect);
                    }
                };

                self.state = State::PublishRequested {
                    request_id: request_id.clone(),
                    app_name: app_name.clone(),
                    stream_key: stream_key.clone(),
                };

                self.state = State::Publishing {
                    app_name: app_name.clone(),
                    stream_key: stream_key.clone(),
                };

                self.authed_id = Some(sk.id());
                self.authed_key = Some(sk.key().to_string());
                info!("{} {} {}", RTP_NEW, sk.key(), sk.id());

                for r in self.session.as_mut().unwrap().accept_request(request_id)? {
                    if let ServerSessionResult::OutboundResponse(packet) = r {
                        if !send(&byte_writer, packet) {
                            break;
                        }
                    }
                }

                return Ok(ConnectionAction::None);
            }

            ServerSessionEvent::StreamMetadataChanged {
                stream_key: _,
                app_name: _,
                metadata,
            } => match &self.state {
                State::Publishing { .. } => {
                    self.metadata = Some(metadata);
                    return Ok(ConnectionAction::New);
                }
                _ => return Ok(ConnectionAction::Disconnect),
            },

            ServerSessionEvent::VideoDataReceived {
                timestamp, data, ..
            } => {
                let is_key_frame = is_video_keyframe(&data);
                let sps_pps = self.sps_pps.as_ref();

                if let Ok((mut au, is_avcc)) =
                    flv::extract_au(data, timestamp.value as i64, sps_pps)
                {
                    if is_avcc {
                        self.sps_pps = Some(au.data.clone());
                    }
                    if is_key_frame {
                        au.key = true;
                    }

                    if let (Some(k), Some(id)) = (self.authed_key.as_deref(), self.authed_id) {
                        self.up_counter = self.up_counter.wrapping_add(1);
                        if self.up_counter % METRICS_INTERVAL == 0 {
                            info!("{} key={} id={}", RTP_UP, k, id);
                        }
                    }

                    match self.tx.try_send(au) {
                        Ok(_) => return Ok(ConnectionAction::None),
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            return Ok(ConnectionAction::None)
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            return Ok(ConnectionAction::Disconnect)
                        }
                    }
                }
            }

            ServerSessionEvent::AudioDataReceived {
                timestamp, data, ..
            } => {
                if let Some(m) = &self.metadata {
                    if let (Some(channels), Some(sample_rate)) =
                        (m.audio_channels, m.audio_sample_rate)
                    {
                        let data = ensure_adts_header(data, channels as u8, sample_rate);

                        if let (Some(k), Some(id)) = (self.authed_key.as_deref(), self.authed_id) {
                            self.up_counter = self.up_counter.wrapping_add(1);
                            if self.up_counter % METRICS_INTERVAL == 0 {
                                info!("{} key={} id={}", RTP_UP, k, id);
                            }
                        }

                        match self.tx.try_send(AccessUnit {
                            data,
                            dts: timestamp.value as u64,
                            pts: timestamp.value as u64,
                            key: false,
                            stream_type: access_unit::PSI_STREAM_AAC,
                            id: 0,
                        }) {
                            Ok(_) => return Ok(ConnectionAction::None),
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                return Ok(ConnectionAction::None)
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                return Ok(ConnectionAction::Disconnect)
                            }
                        }
                    }
                }
            }

            ServerSessionEvent::PublishStreamFinished { .. } => {
                return Ok(ConnectionAction::Disconnect);
            }

            _ => {}
        }

        Ok(ConnectionAction::None)
    }
}

async fn connection_reader(
    connection_id: i32,
    mut stream: ReadHalf<TcpStream>,
    manager: mpsc::UnboundedSender<Bytes>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        let bytes_read = stream.read_buf(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }

        let bytes = buffer.split_off(bytes_read);
        if !send(&manager, buffer.freeze()) {
            break;
        }

        buffer = bytes;
    }

    info!("Connection {}: Reader disconnected", connection_id);
    Ok(())
}

async fn connection_writer(
    connection_id: i32,
    mut stream: WriteHalf<TcpStream>,
    mut packets_to_send: mpsc::UnboundedReceiver<Packet>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const BACKLOG_THRESHOLD: usize = 100;
    let mut send_queue = VecDeque::new();

    loop {
        let packet = packets_to_send.recv().await;
        if packet.is_none() {
            break;
        }

        let packet = packet.unwrap();

        send_queue.push_back(packet);
        while let Some(Some(packet)) = packets_to_send.recv().now_or_never() {
            send_queue.push_back(packet);
        }

        let mut send_optional_packets = true;
        if send_queue.len() > BACKLOG_THRESHOLD {
            send_optional_packets = false;
        }

        for packet in send_queue.drain(..) {
            if send_optional_packets || !packet.can_be_dropped {
                stream.write_all(packet.bytes.as_ref()).await?;
            }
        }
    }

    info!("Connection {}: Writer disconnected", connection_id);
    Ok(())
}

fn is_video_keyframe(data: &Bytes) -> bool {
    data.len() >= 2 && data[0] == 0x17 && data[1] != 0x00
}

fn spawn<F, E>(future: F)
where
    F: Future<Output = Result<(), E>> + Send + 'static,
    E: Display,
{
    tokio::task::spawn(async {
        if let Err(error) = future.await {
            error!("{}", error);
        }
    });
}

fn send<T>(sender: &UnboundedSender<T>, message: T) -> bool {
    match sender.send(message) {
        Ok(_) => true,
        Err(_) => false,
    }
}
