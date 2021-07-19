use std::net::SocketAddr;

use anyhow::Context;
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::{error::Error, Message};

use crate::get_env;
use tokio::sync::mpsc::Sender;

pub async fn run(tx: Sender<crate::message::Message>) -> anyhow::Result<()> {
    let addr = get_env("WS_HOST")?;
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("Can't listen on {}", &addr))?;
    info!("Listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        let peer = match stream.peer_addr() {
            Ok(addr) => addr,
            Err(e) => {
                error!("connected streams should have a peer address. {}", e);
                continue;
            }
        };
        info!("Peer address: {}", peer);

        tokio::spawn(accept_connection(peer, stream, tx.clone()));
    }
    Ok(())
}

async fn accept_connection(
    peer: SocketAddr,
    stream: TcpStream,
    tx: Sender<crate::message::Message>,
) {
    if let Err(e) = handle_connection(peer, stream, tx).await {
        match e {
            Error::ConnectionClosed => info!("connection closed: {}", &peer),
            Error::Protocol(err) => warn!("protocol error: {}. by: {}", err, &peer),
            err @ Error::Utf8 => warn!("utf8 encoding error: {}. by: {}", err, &peer),
            err => error!("Error processing ws connection: {}", err),
        }
    }
}

async fn handle_connection(
    peer: SocketAddr,
    stream: TcpStream,
    tx: Sender<crate::message::Message>,
) -> tokio_tungstenite::tungstenite::error::Result<()> {
    let ws_stream = accept_async(stream).await?;
    info!("New WebSocket connection: {}", peer);

    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    loop {
        match ws_receiver.next().await {
            Some(msg) => {
                let msg = msg?;
                if msg.is_ping() {
                    ws_sender.send(Message::Pong(vec![0x04])).await?;
                } else if msg.is_binary() {
                    if let Err(e) = tx
                        .send({
                            let mut msg = match crate::message::Message::deserialize(
                                &msg.into_data(),
                            ) {
                                Ok(msg) => msg,
                                Err(err) => {
                                    warn!(
                                        "消息格式错误. 从{}收到的消息无法反序列化. 连接关闭. {}",
                                        &peer, err
                                    );
                                    break;
                                }
                            };
                            msg.source = Some(peer.to_string());
                            msg
                        })
                        .await
                    {
                        error!("向处理队列发送消息时发生错误: {}", e);
                    }
                } else if msg.is_text() {
                    warn!("未知的ws消息: {:?}", msg.into_text())
                } else if msg.is_close() {
                    break;
                }
            }
            None => break,
        }
    }

    Ok(())
}
