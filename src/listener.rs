use crate::aac::ensure_adts_header;
use crate::connection_action::ConnectionAction;
use crate::flv;
use crate::state::State;
use bytes::{Bytes, BytesMut};
use chrono::Duration;
use futures::future::FutureExt;
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
use ts::AccessUnit;

pub struct Connection {
    id: i32,
    session: Option<ServerSession>,
    state: State,
    pub metadata: Option<StreamMetadata>,
    api_addr: Option<String>,
    sps_pps: Option<Bytes>,
    tx: mpsc::Sender<AccessUnit>,
    tx_shutdown: watch::Sender<()>,
}

impl Connection {
    pub fn new(id: i32, tx: mpsc::Sender<AccessUnit>, tx_shutdown: watch::Sender<()>) -> Self {
        Connection {
            id,
            session: None,
            state: State::Waiting,
            metadata: None,
            api_addr: None,
            sps_pps: None,
            tx,
            tx_shutdown,
        }
    }

    pub async fn start_handshake(
        self,
        mut stream: TcpStream,
        tx_key: oneshot::Sender<String>,
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
        let mut handshake = Handshake::new(PeerType::Server);

        let server_p0_and_1 = handshake
            .generate_outbound_p0_and_p1()
            .map_err(|x| format!("Failed to generate p0 and p1: {:?}", x))?;

        stream.write_all(&server_p0_and_1).await?;

        let mut buffer = [0; 4096];
        loop {
            let bytes_read = stream.read(&mut buffer).await?;
            if bytes_read == 0 {
                return Ok(());
            }

            match handshake
                .process_bytes(&buffer[0..bytes_read])
                .map_err(|x| format!("Connection {}: Failed to process bytes: {:?}", self.id, x))?
            {
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
        let (session, mut results) = ServerSession::new(config)
            .map_err(|x| format!("Server session error occurred: {:?}", x))?;

        self.session = Some(session);

        let remaining_bytes_results = self
            .session
            .as_mut()
            .unwrap()
            .handle_input(&received_bytes)
            .map_err(|x| format!("Failed to handle input: {:?}", x))?;

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
                                .handle_input(&bytes)
                                .map_err(|x| format!("Error handling input: {:?}", x))?;
                        }
                    }
                }
            }
        }

        self.tx_shutdown.send(());
        info!("Connection {}: Client disconnected", self.id);

        Ok(())
    }

    fn handle_session_results(
        &mut self,
        results: &mut Vec<ServerSessionResult>,
        byte_writer: &mut UnboundedSender<Packet>,
    ) -> Result<ConnectionAction, Box<dyn std::error::Error + Sync + Send>> {
        if results.len() == 0 {
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

                ServerSessionResult::UnhandleableMessageReceived(payload) => {
                    info!(
                        "Connection {}: Unhandleable message received: {:?}",
                        self.id, payload
                    );
                }
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
                info!(
                    "Connection {}: Client requested connection to app {:?}",
                    self.id, app_name
                );

                if self.state != State::Waiting {
                    error!(
                        "Connection {}: Client was not in the waiting state, but was in {:?}",
                        self.id, self.state
                    );
                    return Ok(ConnectionAction::Disconnect);
                }

                new_results.extend(
                    self.session
                        .as_mut()
                        .unwrap()
                        .accept_request(request_id)
                        .map_err(|x| {
                            format!(
                                "Connection {}: Error occurred accepting request: {:?}",
                                self.id, x
                            )
                        })?,
                );

                self.state = State::Connected { app_name };
            }

            ServerSessionEvent::PublishStreamRequested {
                request_id,
                app_name,
                mode,
                stream_key,
            } => {
                info!(
                    "Connection {}: Client requesting publishing on {}/{} in mode {:?}",
                    self.id, app_name, stream_key, mode
                );

                self.state = State::PublishRequested {
                    request_id: request_id.clone(),
                    app_name: app_name.clone(),
                    stream_key: stream_key.clone(),
                };

                self.state = State::Publishing {
                    app_name: app_name.clone(),
                    stream_key: stream_key.clone(),
                };

                match self
                    .session
                    .as_mut()
                    .unwrap()
                    .accept_request(request_id.clone())
                {
                    Ok(results) => {
                        for result in results {
                            match result {
                                ServerSessionResult::OutboundResponse(packet) => {
                                    if !send(&byte_writer, packet) {
                                        break;
                                    }
                                }
                                _ => {}
                            }
                        }

                        return Ok(ConnectionAction::None);
                    }
                    Err(_) => return Ok(ConnectionAction::Disconnect),
                }
            }

            ServerSessionEvent::StreamMetadataChanged {
                stream_key,
                app_name: _,
                metadata,
            } => {
                info!(
                    "Connection {}: New metadata published for stream key '{}': {:?}",
                    self.id, stream_key, metadata
                );

                match &self.state {
                    State::Publishing { .. } => {
                        self.metadata = Some(metadata);
                        return Ok(ConnectionAction::New);
                    }

                    _ => {
                        error!(
                            "Connection {}: expected client to be in publishing state, was in {:?}",
                            self.id, self.state
                        );
                        return Ok(ConnectionAction::Disconnect);
                    }
                }
            }

            ServerSessionEvent::VideoDataReceived {
                app_name: _app,
                stream_key: _key,
                timestamp,
                data,
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

                    match self.tx.try_send(au) {
                        Ok(_) => {
                            return Ok(ConnectionAction::None);
                        }
                        Err(e) => {
                            match e {
                                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                                    error!("Channel is full");
                                    // skip this packet
                                    return Ok(ConnectionAction::None);
                                }
                                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                    error!("Failed to send AccessUnit: Receiver has been dropped");
                                    return Ok(ConnectionAction::Disconnect);
                                }
                            }
                        }
                    }
                }
            }

            ServerSessionEvent::AudioDataReceived {
                timestamp, data, ..
            } => {
                if let Some(m) = &self.metadata {
                    if let Some(channels) = m.audio_channels {
                        if let Some(sample_rate) = m.audio_sample_rate {
                            let data = ensure_adts_header(data, channels as u8, sample_rate);
                            match self.tx.try_send(AccessUnit {
                                data,
                                dts: timestamp.value as u64,
                                pts: timestamp.value as u64,
                                key: false,
                                avc: false,
                            }) {
                                Ok(_) => {
                                    return Ok(ConnectionAction::None);
                                }
                                Err(e) => {
                                    match e {
                                        tokio::sync::mpsc::error::TrySendError::Full(_) => {
                                            error!("Channel is full");
                                            // skip this packet
                                            return Ok(ConnectionAction::None);
                                        }
                                        tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                            error!("Failed to send AccessUnit: Receiver has been dropped");
                                            return Ok(ConnectionAction::Disconnect);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            ServerSessionEvent::PublishStreamFinished { .. } => match &self.state {
                State::Publishing { .. } => {
                    return Ok(ConnectionAction::Disconnect);
                }

                _ => {
                    error!(
                        "Connection {}: Expected client to be in publishing state, was in {:?}",
                        self.id, self.state
                    );
                    return Ok(ConnectionAction::Disconnect);
                }
            },

            x => info!("Connection {}: Unknown event raised: {:?}", self.id, x),
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
            break; // connection closed
        }

        let packet = packet.unwrap();

        // Since RTMP is TCP based, if bandwidth is low between the server and the client then
        // we will end up backlogging the mpsc receiver.  However, mpsc does not have a good
        // way to know how many items are pending.  So we need to receive all pending packets
        // in a non-blocking manner, put them in a queue, and if the queue is too large ignore
        // optional packets.
        send_queue.push_back(packet);
        while let Some(Some(packet)) = packets_to_send.recv().now_or_never() {
            send_queue.push_back(packet);
        }

        let mut send_optional_packets = true;
        if send_queue.len() > BACKLOG_THRESHOLD {
            info!(
                "Connection {}: Too many pending packets, dropping optional ones",
                connection_id
            );
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
    // assumings h264
    return data.len() >= 2 && data[0] == 0x17 && data[1] != 0x00; // 0x00 is the sequence header, don't count that for now
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

/// Sends a message over an unbounded receiver and returns true if the message was sent
/// or false if the channel has been closed.
fn send<T>(sender: &UnboundedSender<T>, message: T) -> bool {
    match sender.send(message) {
        Ok(_) => true,
        Err(_) => false,
    }
}
