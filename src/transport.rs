//! Bounded PostgreSQL frames, shared by the frontend and backend adapters.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Check the length before delivering a frame header to pgwire. Never read ahead
/// into another frame: pipelined requests each pass through this same bound.
pub(crate) struct FrameGuard {
    socket: TcpStream,
    startup: bool,
    header: [u8; 8],
    read: usize,
    sent: usize,
    header_len: usize,
    remaining: usize,
}

impl FrameGuard {
    pub(crate) fn frontend(socket: TcpStream) -> Self {
        Self {
            socket,
            startup: true,
            header: [0; 8],
            read: 0,
            sent: 0,
            header_len: 8,
            remaining: 0,
        }
    }
}

impl FrameGuard {
    pub(crate) fn backend(socket: TcpStream) -> Self {
        let mut guard = Self::frontend(socket);
        guard.startup = false;
        guard.header_len = 5;
        guard
    }
}

impl AsyncRead for FrameGuard {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        while this.read < this.header_len {
            // Validate startup length after four bytes, before reading the code.
            let end = if this.startup && this.read < 4 {
                4
            } else {
                this.header_len
            };
            let mut header = ReadBuf::new(&mut this.header[this.read..end]);
            match Pin::new(&mut this.socket).poll_read(cx, &mut header) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
            let n = header.filled().len();
            if n == 0 {
                return Poll::Ready(if this.read == 0 {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated frame header",
                    ))
                });
            }
            this.read += n;
            if (this.startup && this.read >= 4) || (!this.startup && this.read == 5) {
                let offset = usize::from(!this.startup);
                let length = u32::from_be_bytes(this.header[offset..offset + 4].try_into().unwrap())
                    as usize;
                let minimum = if this.startup { 8 } else { 4 };
                if length < minimum || length > MAX_FRAME_BYTES - offset {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid frame length",
                    )));
                }
                this.remaining = length + offset - this.header_len;
            }
        }
        if this.sent < this.header_len {
            let n = (this.header_len - this.sent).min(buf.remaining());
            buf.put_slice(&this.header[this.sent..this.sent + n]);
            this.sent += n;
        } else if this.remaining > 0 {
            let n = this.remaining.min(buf.remaining());
            let mut payload = ReadBuf::new(buf.initialize_unfilled_to(n));
            match Pin::new(&mut this.socket).poll_read(cx, &mut payload) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {}
            }
            let n = payload.filled().len();
            if n == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated frame",
                )));
            }
            buf.advance(n);
            this.remaining -= n;
        }
        if this.sent == this.header_len && this.remaining == 0 {
            if this.startup {
                let code = u32::from_be_bytes(this.header[4..8].try_into().unwrap());
                this.startup = matches!(code, 80877103 | 80877104);
            }
            this.header_len = if this.startup { 8 } else { 5 };
            this.read = 0;
            this.sent = 0;
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for FrameGuard {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().socket).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_shutdown(cx)
    }
}
