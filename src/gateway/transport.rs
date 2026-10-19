//! Bounded frontend negotiation and decrypted disconnect observation.

use crate::{tls::ServerTls, transport::Socket};
use std::{io, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::watch,
    task::JoinHandle,
};

pub(super) enum Observer {
    Plain(TcpStream),
    Secure(watch::Receiver<bool>),
}

impl Observer {
    pub async fn disconnected(&mut self) {
        match self {
            Self::Secure(closed) => {
                if !*closed.borrow() {
                    let _ = closed.changed().await;
                }
            }
            Self::Plain(socket) => {
                let mut byte = [0];
                match socket.peek(&mut byte).await {
                    Ok(0) | Err(_) => {}
                    Ok(_) if byte[0] == b'X' => {}
                    // Never consume pipelined protocol messages.
                    Ok(_) => std::future::pending::<()>().await,
                }
            }
        }
    }
}

pub(super) struct Bridge(JoinHandle<()>);
impl Bridge {
    pub async fn finish(&mut self) -> io::Result<()> {
        (&mut self.0)
            .await
            .map_err(|_| io::Error::other("frontend TLS bridge failed"))
    }
}
impl Drop for Bridge {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn negotiate(
    mut socket: TcpStream,
    tls: Option<&Arc<ServerTls>>,
    max_frame_bytes: usize,
) -> io::Result<(Socket, Observer, Option<Bridge>)> {
    socket.set_nodelay(true)?;
    let Some(tls) = tls else {
        let raw = socket.into_std()?;
        let observer = TcpStream::from_std(raw.try_clone()?)?;
        return Ok((
            Box::new(TcpStream::from_std(raw)?),
            Observer::Plain(observer),
            None,
        ));
    };
    // Only fixed-size encryption requests are read before TLS. Plaintext startup
    // and cancellation are rejected without decoding or allocating their payloads.
    let mut header = [0; 8];
    socket.read_exact(&mut header).await?;
    if header == [0, 0, 0, 8, 4, 210, 22, 48] {
        socket.write_all(b"N").await?;
        socket.read_exact(&mut header).await?;
    }
    if header != [0, 0, 0, 8, 4, 210, 22, 47] {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "TLS required",
        ));
    }
    socket.write_all(b"S").await?;
    let secure = tls
        .acceptor
        .accept(socket)
        .await
        .map_err(|_| io::Error::other("frontend TLS handshake failed"))?;
    // Fixed buffers keep the bridge bounded. Reading the decrypted stream also
    // observes TLS close_notify and EOF while execution owns the protocol writer.
    let (session, bridge) = tokio::io::duplex(16 * 1024);
    let (closed, observer) = watch::channel(false);
    let secure = crate::transport::FrameGuard::frontend(Box::new(secure), max_frame_bytes)
        .observe_termination(closed.clone());
    let task = tokio::spawn(async move {
        let (mut input, mut output) = tokio::io::split(secure);
        let (mut from_session, mut to_session) = tokio::io::split(bridge);
        tokio::select! {
            _ = tokio::io::copy(&mut input, &mut to_session) => {},
            _ = tokio::io::copy(&mut from_session, &mut output) => {},
        }
        closed.send_replace(true);
    });
    Ok((
        Box::new(session),
        Observer::Secure(observer),
        Some(Bridge(task)),
    ))
}
