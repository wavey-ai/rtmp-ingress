use crate::listener::Connection;
use access_unit::AccessUnit;
use gatekeeper::{Gatekeeper, Streamkey};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, error, info};

pub async fn start_rtmp_listener(
    base64_encoded_pem_key: String,
    rtp_addr: SocketAddr,
) -> Result<
    (
        oneshot::Receiver<()>,
        oneshot::Receiver<()>,
        watch::Sender<()>,
        mpsc::Receiver<(String, AccessUnit)>,
    ),
    Box<dyn Error + Send + Sync>,
> {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(());
    let (up_tx, up_rx) = oneshot::channel();
    let (fin_tx, fin_rx) = oneshot::channel();
    let (out_tx, out_rx) = mpsc::channel::<(String, AccessUnit)>(1024);

    let gatekeeper = Arc::new(Gatekeeper::new(&base64_encoded_pem_key)?);

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
                    let out_tx = out_tx.clone();

                    let gatekeeper = gatekeeper.clone();
                    tokio::spawn(async move {
                        let (tx, mut rx) = mpsc::channel::<AccessUnit>(16);
                        let (close_tx, _close_rx) = watch::channel(());
                        let (tx_key, rx_key) = oneshot::channel::<String>();

                        let gatekeeper_inner = gatekeeper.clone();

                        let connection = Connection::new(current_id.try_into().unwrap(), tx, close_tx, gatekeeper_inner);

                        info!("connection {}: eonnection received from {}", current_id, addr.ip());

                        if let Err(e) = connection.start_handshake(tcp, tx_key).await {
                            error!(error=%e, "Handshake failed");
                            return;
                        }

                        tokio::spawn(async move {
                            let key = match rx_key.await {
                                Ok(k) => k,
                                Err(_) => return,
                            };
                            while let Some(au) = rx.recv().await {
                                if out_tx.send((key.clone(), au)).await.is_err() {
                                    break;
                                }
                            }
                        });
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

    Ok((up_rx, fin_rx, shutdown_tx, out_rx))
}
