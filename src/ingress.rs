use crate::listener::Connection;
use xmpegts::define::epsi_stream_type;
use xmpegts::ts::TsMuxer;
use access_unit::{AccessUnit, detect_audio, AudioType};
use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use srt_tokio::{options::*, SrtSocket};
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};

const RTP_NEW: &str = "SRT:NEW";
const RTP_UP: &str = "SRT:UP";
const RTP_DOWN: &str = "SRT:DOWN";

pub async fn start_rtmp_listener(
    rtp_addr: SocketAddr,
    srt_addr: SocketAddr,
) -> Result<
    (
        oneshot::Receiver<()>,
        oneshot::Receiver<()>,
        watch::Sender<()>,
    ),
    Box<dyn Error + Send + Sync>,
> {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(());
    let (up_tx, up_rx) = oneshot::channel();
    let (fin_tx, fin_rx) = oneshot::channel();

    let listener = TcpListener::bind(rtp_addr).await?;
    let srv = async move {
        let mut current_id = 0usize;
        let _ = up_tx.send(());

        loop {
            tokio::select! {
                accept_res = listener.accept() => {
                    let (tcp, addr) = match accept_res {
                        Ok(v) => v,
                        Err(e) => {
                            error!(error=%e, "TCP accept failed");
                            continue;
                        }
                    };

                    current_id = current_id.wrapping_add(1);
                    tokio::spawn(async move {
                        let (tx, rx) = mpsc::channel::<AccessUnit>(16);
                        let (ts_tx, ts_rx) = mpsc::channel::<bytes::Bytes>(16);
                        let (close_tx, _close_rx) = watch::channel(());
                        let (tx_key, rx_key) = oneshot::channel::<String>();
                        let connection = Connection::new(current_id.try_into().unwrap(), tx, close_tx);

                        info!("Connection {}: Connection received from {}", current_id, addr.ip());

                        if let Err(e) = connection.start_handshake(tcp, tx_key).await {
                            error!(error=%e, "Handshake failed");
                            return;
                        }

                        tokio::spawn(async move {
                            mux_stream_with_ts_muxer(rx, ts_tx).await;
                        });

                        match rx_key.await {
                            Ok(stream_key) => {
                                match SrtSocket::builder().call(srt_addr, Some(&stream_key)).await {
                                    Ok(mut socket) => {
                                        let mut stream = ReceiverStream::new(ts_rx)
                                            .map(|bytes| {
                                                Ok((Instant::now(), bytes)) as Result<(Instant, Bytes), io::Error>
                                            });

                                        if let Err(e) = socket.send_all(&mut stream).await {
                                            error!(error=%e, "SRT send_all failed");
                                        }
                                    }
                                    Err(e) => {
                                        error!(error=%e, "Failed to open SRT socket");
                                    }
                                }
                            }
                            Err(e) => {
                                error!(error=%e, "Stream key handshake channel closed");
                            }
                        }
                    });
                }
                _ = shutdown_rx.changed() => {
                    break;
                }
            }
        }

        let _ = fin_tx.send(());
    };

    tokio::spawn(srv);

    Ok((up_rx, fin_rx, shutdown_tx))
}

async fn mux_stream_with_ts_muxer(mut rx: mpsc::Receiver<AccessUnit>, ts_tx: mpsc::Sender<Bytes>) {
    let mut muxer = TsMuxer::new();
    let video_pid = match muxer.add_stream(epsi_stream_type::PSI_STREAM_H264, BytesMut::new()) {
        Ok(pid) => pid,
        Err(e) => {
            error!(error=%e, "Failed to add H264 stream");
            return;
        }
    };
    let audio_pid = match muxer.add_stream(epsi_stream_type::PSI_STREAM_AAC, BytesMut::new()) {
        Ok(pid) => pid,
        Err(e) => {
            error!(error=%e, "Failed to add AAC stream");
            return;
        }
    };

    while let Some(au) = rx.recv().await {
        let pts: i64 = match au.pts.try_into() {
            Ok(v) => v,
            Err(e) => {
                error!(error=%e, "Invalid PTS");
                continue;
            }
        };
        let dts: i64 = match au.dts.try_into() {
            Ok(v) => v,
            Err(e) => {
                error!(error=%e, "Invalid DTS");
                continue;
            }
        };

        let payload = BytesMut::from(au.data.clone());
        let pid = match detect_audio(&au.data) {
            AudioType::AAC => audio_pid,
            AudioType::FLAC => audio_pid,
            _ => video_pid,
        };

        if let Err(e) = muxer.write(pid, pts, dts, 0, payload) {
            error!(error=%e, "TsMuxer write failed");
            continue;
        }

        let encoded = muxer.get_data();
        for chunk in encoded.chunks(188 * 6) {
            let out = Bytes::copy_from_slice(chunk);
            if let Err(e) = ts_tx.send(out).await {
                debug!(error=%e, "TS out channel closed");
                return;
            }
        }
    }
}

