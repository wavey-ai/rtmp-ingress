//! Upload-response integration for RTMP ingest
//!
//! This module provides integration with the upload-response service,
//! allowing RTMP streams to be processed through the shared-memory cache.

use access_unit::AccessUnit;
use bytes::{BufMut, Bytes, BytesMut};
use http_pack::stream::{StreamHeaders, StreamRequestHeaders};
use http_pack::{HeaderField, HttpVersion};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{debug, error, info};
use upload_response::UploadResponseService;

/// Auth callback for RTMP connections
pub trait RtmpAuth: Send + Sync + 'static {
    fn authenticate(&self, app_name: &str, stream_key: &str) -> bool;
}

/// Default auth that allows all connections
pub struct AllowAll;

impl RtmpAuth for AllowAll {
    fn authenticate(&self, _app_name: &str, _stream_key: &str) -> bool {
        true
    }
}

/// Serialize an AccessUnit to bytes
/// Format: [stream_type:1][key:1][id:8][dts:8][pts:8][data_len:4][data:N]
pub fn serialize_access_unit(au: &AccessUnit) -> Bytes {
    let mut buf = BytesMut::with_capacity(30 + au.data.len());
    buf.put_u8(au.stream_type);
    buf.put_u8(if au.key { 1 } else { 0 });
    buf.put_u64(au.id);
    buf.put_u64(au.dts);
    buf.put_u64(au.pts);
    buf.put_u32(au.data.len() as u32);
    buf.extend_from_slice(&au.data);
    buf.freeze()
}

/// Deserialize an AccessUnit from bytes
pub fn deserialize_access_unit(data: &[u8]) -> Option<(AccessUnit, usize)> {
    if data.len() < 30 {
        return None;
    }
    let stream_type = data[0];
    let key = data[1] != 0;
    let id = u64::from_be_bytes(data[2..10].try_into().ok()?);
    let dts = u64::from_be_bytes(data[10..18].try_into().ok()?);
    let pts = u64::from_be_bytes(data[18..26].try_into().ok()?);
    let data_len = u32::from_be_bytes(data[26..30].try_into().ok()?) as usize;

    if data.len() < 30 + data_len {
        return None;
    }

    let au = AccessUnit {
        stream_type,
        key,
        id,
        dts,
        pts,
        data: Bytes::copy_from_slice(&data[30..30 + data_len]),
    };

    Some((au, 30 + data_len))
}

/// RTMP ingest server that feeds into UploadResponseService
pub struct RtmpUploadIngest<A: RtmpAuth = AllowAll> {
    service: Arc<UploadResponseService>,
    auth: Arc<A>,
}

impl RtmpUploadIngest<AllowAll> {
    pub fn new(service: Arc<UploadResponseService>) -> Self {
        Self {
            service,
            auth: Arc::new(AllowAll),
        }
    }
}

impl<A: RtmpAuth> RtmpUploadIngest<A> {
    pub fn with_auth(service: Arc<UploadResponseService>, auth: A) -> Self {
        Self {
            service,
            auth: Arc::new(auth),
        }
    }

    /// Start the RTMP listener on the given address
    pub async fn start(
        self,
        addr: SocketAddr,
    ) -> Result<watch::Sender<()>, Box<dyn std::error::Error + Send + Sync>> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(());
        let service = self.service;
        let auth = self.auth;

        let listener = TcpListener::bind(addr).await?;
        info!("RTMP upload-response server listening on {}", addr);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("RTMP upload-response server shutting down");
                        break;
                    }
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, peer_addr)) => {
                                let service = Arc::clone(&service);
                                let auth = Arc::clone(&auth);
                                tokio::spawn(async move {
                                    if let Err(e) = handle_connection(stream, peer_addr, service, auth).await {
                                        error!("RTMP connection error: {}", e);
                                    }
                                });
                            }
                            Err(e) => {
                                error!("RTMP accept error: {}", e);
                            }
                        }
                    }
                }
            }
        });

        Ok(shutdown_tx)
    }
}

use crate::flv;
use access_unit::aac::ensure_adts_header;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult, StreamMetadata,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

const READ_TIMEOUT_SECS: u64 = 30;

async fn handle_connection<A: RtmpAuth>(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    service: Arc<UploadResponseService>,
    auth: Arc<A>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Acquire stream slot
    let _permit = service
        .acquire_stream()
        .await
        .map_err(|e| format!("Failed to acquire stream: {}", e))?;

    let stream_id = service.next_id();

    debug!(
        stream_id,
        peer = %peer_addr,
        "RTMP connection received"
    );

    // Perform RTMP handshake
    let mut handshake = Handshake::new(PeerType::Server);
    let server_p0_and_p1 = handshake.generate_outbound_p0_and_p1()?;
    stream.write_all(&server_p0_and_p1).await?;

    let mut buffer = [0u8; 4096];
    let remaining_bytes = loop {
        let bytes_read = stream.read(&mut buffer).await?;
        if bytes_read == 0 {
            return Ok(());
        }

        match handshake.process_bytes(&buffer[..bytes_read])? {
            HandshakeProcessResult::InProgress { response_bytes } => {
                stream.write_all(&response_bytes).await?;
            }
            HandshakeProcessResult::Completed {
                response_bytes,
                remaining_bytes,
            } => {
                stream.write_all(&response_bytes).await?;
                break remaining_bytes;
            }
        }
    };

    debug!(stream_id, "RTMP handshake complete");

    // Create RTMP session
    let config = ServerSessionConfig::new();
    let (mut session, mut results) = ServerSession::new(config)?;

    // Process any remaining bytes from handshake
    if !remaining_bytes.is_empty() {
        let more_results = session.handle_input(&remaining_bytes)?;
        results.extend(more_results);
    }

    let mut app_name = String::new();
    let mut stream_key = String::new();
    let mut publishing = false;
    let mut headers_written = false;
    let mut metadata: Option<StreamMetadata> = None;
    let mut sps_pps: Option<Bytes> = None;
    let mut au_count = 0u64;

    let slot_bytes = service.config().slot_bytes();
    let mut pending = Vec::new();

    // Main loop
    loop {
        // Handle session results
        for result in results.drain(..) {
            match result {
                ServerSessionResult::OutboundResponse(packet) => {
                    stream.write_all(&packet.bytes).await?;
                }
                ServerSessionResult::RaisedEvent(event) => {
                    match event {
                        ServerSessionEvent::ConnectionRequested {
                            request_id,
                            app_name: app,
                        } => {
                            app_name = app;
                            let response_results = session.accept_request(request_id)?;
                            for r in response_results {
                                if let ServerSessionResult::OutboundResponse(packet) = r {
                                    stream.write_all(&packet.bytes).await?;
                                }
                            }
                        }
                        ServerSessionEvent::PublishStreamRequested {
                            request_id,
                            app_name: _,
                            stream_key: key,
                            mode: _,
                        } => {
                            stream_key = key;

                            // Auth check
                            if !auth.authenticate(&app_name, &stream_key) {
                                error!(stream_id, "RTMP auth failed");
                                return Ok(());
                            }

                            // Write headers to cache
                            let headers = StreamHeaders::Request(StreamRequestHeaders {
                                stream_id,
                                version: HttpVersion::Http11,
                                method: b"POST".to_vec(),
                                scheme: None,
                                authority: None,
                                path: format!("/rtmp/{}/{}", app_name, stream_key).into_bytes(),
                                headers: vec![
                                    HeaderField {
                                        name: b"x-rtmp-app".to_vec(),
                                        value: app_name.clone().into_bytes(),
                                    },
                                    HeaderField {
                                        name: b"x-rtmp-stream-key".to_vec(),
                                        value: stream_key.clone().into_bytes(),
                                    },
                                    HeaderField {
                                        name: b"x-peer-addr".to_vec(),
                                        value: peer_addr.to_string().into_bytes(),
                                    },
                                ],
                            });

                            service
                                .write_request_headers(stream_id, headers)
                                .await
                                .map_err(|e| format!("Failed to write headers: {}", e))?;
                            headers_written = true;

                            let response_results = session.accept_request(request_id)?;
                            for r in response_results {
                                if let ServerSessionResult::OutboundResponse(packet) = r {
                                    stream.write_all(&packet.bytes).await?;
                                }
                            }
                            publishing = true;
                            debug!(
                                stream_id,
                                app = %app_name,
                                key = %stream_key,
                                "RTMP publishing started"
                            );
                        }
                        ServerSessionEvent::StreamMetadataChanged { metadata: meta, .. } => {
                            metadata = Some(meta);
                        }
                        ServerSessionEvent::VideoDataReceived { data, timestamp, .. } => {
                            if publishing && headers_written {
                                let is_keyframe =
                                    data.len() >= 2 && data[0] == 0x17 && data[1] != 0x00;

                                // Extract AU and process synchronously to avoid Send issues
                                let au_result: Option<(AccessUnit, bool)> = flv::extract_au(
                                    data,
                                    timestamp.value as i64,
                                    sps_pps.as_ref(),
                                )
                                .ok();

                                if let Some((mut au, is_avcc)) = au_result {
                                    if is_avcc {
                                        sps_pps = Some(au.data.clone());
                                    } else {
                                        au.key = is_keyframe;
                                        au.id = au_count;
                                        au_count += 1;

                                        let serialized = serialize_access_unit(&au);
                                        pending.extend_from_slice(&serialized);
                                    }
                                }

                                // Write full slots - awaits are outside the match
                                while pending.len() >= slot_bytes {
                                    let chunk: Vec<u8> = pending.drain(..slot_bytes).collect();
                                    service
                                        .append_request_body(stream_id, Bytes::from(chunk))
                                        .await
                                        .map_err(|e| format!("Failed to write body: {}", e))?;
                                }
                            }
                        }
                        ServerSessionEvent::AudioDataReceived { data, timestamp, .. } => {
                            if publishing && headers_written {
                                if let Some(ref meta) = metadata {
                                    if let (Some(channels), Some(sample_rate)) =
                                        (meta.audio_channels, meta.audio_sample_rate)
                                    {
                                        // Skip AAC sequence header (packet type 0)
                                        if data.len() >= 2 && data[1] == 0 {
                                            continue;
                                        }

                                        let aac_data = if data.len() > 2 {
                                            ensure_adts_header(
                                                data.slice(2..),
                                                channels as u8,
                                                sample_rate as u32,
                                            )
                                        } else {
                                            continue;
                                        };

                                        let au = AccessUnit {
                                            stream_type: access_unit::PSI_STREAM_AAC,
                                            key: false,
                                            id: au_count,
                                            dts: timestamp.value as u64,
                                            pts: timestamp.value as u64,
                                            data: aac_data,
                                        };
                                        au_count += 1;

                                        let serialized = serialize_access_unit(&au);
                                        pending.extend_from_slice(&serialized);

                                        // Write full slots
                                        while pending.len() >= slot_bytes {
                                            let chunk: Vec<u8> =
                                                pending.drain(..slot_bytes).collect();
                                            service
                                                .append_request_body(stream_id, Bytes::from(chunk))
                                                .await
                                                .map_err(|e| format!("Failed to write body: {}", e))?;
                                        }
                                    }
                                }
                            }
                        }
                        ServerSessionEvent::PublishStreamFinished { .. } => {
                            debug!(stream_id, "RTMP publish finished");
                        }
                        _ => {}
                    }
                }
                ServerSessionResult::UnhandleableMessageReceived(_) => {}
            }
        }

        // Read more data
        match timeout(
            Duration::from_secs(READ_TIMEOUT_SECS),
            stream.read(&mut buffer),
        )
        .await
        {
            Ok(Ok(0)) => {
                debug!(stream_id, "RTMP connection closed by peer");
                break;
            }
            Ok(Ok(n)) => {
                results = session.handle_input(&buffer[..n])?;
            }
            Ok(Err(e)) => {
                error!(stream_id, error = %e, "RTMP read error");
                break;
            }
            Err(_) => {
                debug!(stream_id, "RTMP read timeout");
                break;
            }
        }
    }

    // Flush remaining data
    if !pending.is_empty() && headers_written {
        service
            .append_request_body(stream_id, Bytes::from(pending))
            .await
            .map_err(|e| format!("Failed to write final body: {}", e))?;
    }

    // End marker
    if headers_written {
        service
            .end_request(stream_id)
            .await
            .map_err(|e| format!("Failed to end request: {}", e))?;

        debug!(
            stream_id,
            au_count, "RTMP request complete, waiting for response"
        );

        // Wait for response
        let rx = service.register_response(stream_id).await;
        let timeout_duration = Duration::from_millis(service.config().response_timeout_ms);

        match timeout(timeout_duration, rx).await {
            Ok(Ok(Ok((status, body)))) => {
                debug!(stream_id, ?status, len = body.len(), "Got RTMP response");
            }
            Ok(Ok(Err(e))) => {
                error!(stream_id, error = %e, "Response error");
                service.drop_response_channel(stream_id).await;
            }
            Ok(Err(_)) => {
                error!(stream_id, "Response channel closed");
                service.drop_response_channel(stream_id).await;
            }
            Err(_) => {
                error!(stream_id, "Response timeout");
                service.drop_response_channel(stream_id).await;
            }
        }
    }

    Ok(())
}
