use crate::listener::Connection;
use bytes::Bytes;
use futures::SinkExt;
use srt_tokio::{options::*, SrtListener, SrtSocket};
use std::error::Error;
use std::io;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};
use ts::muxer::mux_stream;
use ts::AccessUnit;

const RTP_NEW: &str = "SRT:NEW";
const RTP_UP: &str = "SRT:UP";
const RTP_DOWN: &str = "SRT:DOWN";

pub async fn start_rtmp_listener() -> Result<
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

    let mut current_id = 0;

    let srv = async move {
        let listener = TcpListener::bind("0.0.0.0:1935").await.unwrap();
        up_tx.send(());
        loop {
            tokio::select! {
                Ok(conn) = listener.accept() => {
                    tokio::spawn(async move {
                        let (tx, rx) = mpsc::channel::<AccessUnit>(16);
                        let (ts_tx, ts_rx) = mpsc::channel::<Bytes>(16);
                        let (close_tx, close_rx) = watch::channel(());
                        let (tx_key, rx_key) = oneshot::channel::<String>();

                        current_id += 1;
                        let connection = Connection::new(current_id, tx, close_tx);

                        info!(
                            "Connection {}: Connection received from {}",
                            current_id,
                            conn.1.ip(),
                        );

                        connection.start_handshake(conn.0, tx_key).await;

                        tokio::spawn(async move {
                            mux_stream(rx, ts_tx).await;
                        });


                        // block waiting for stream key
                        let stream_key = rx_key.await;

                        match SrtSocket::builder().call("127.0.0.1:8000", None).await {
                            Ok(mut socket) => {
                                let mut stream = ReceiverStream::new(ts_rx)
                                    .map(|bytes| {
                                        Ok((Instant::now(), bytes)) as Result<(Instant, Bytes), io::Error>
                                    });

                                match socket.send_all(&mut stream).await {
                                    Ok(_) => {
                                        dbg!("hmmmm");
                                    },
                                    Err(err ) => {
                                        dbg!(err);
                                    }
                                }
                            }
                            Err(e) => {
                                dbg!(e);
                            }
                        }
                    });
                }
                _ = shutdown_rx.changed() => {
                    break;
                }
            }
        }

        fin_tx.send(());
    };

    tokio::spawn(srv);

    Ok((up_rx, fin_rx, shutdown_tx))
}
