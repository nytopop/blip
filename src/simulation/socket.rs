// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
use futures::{ready, stream::Stream};
use hyper::client::connect::{Connected as HyperConnected, Connection as HyperConnection};
use std::{
    io::{self, ErrorKind::BrokenPipe},
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tonic::transport::{server::Connected, Certificate};

type IO<T> = io::Result<T>;

/// A simulated network socket powered by an in-memory pipe.
pub struct Socket {
    rx: mpsc::Receiver<Vec<u8>>,
    tx: mpsc::Sender<Vec<u8>>,
    buf: Vec<u8>,
}

impl AsyncRead for Socket {
    fn poll_read(mut self: Pin<&mut Self>, c: &mut Context<'_>, buf: &mut [u8]) -> Poll<IO<usize>> {
        if !self.buf.is_empty() {
            let n = self.read_into(buf);
            return Poll::Ready(Ok(n));
        }

        let rx = Pin::new(&mut self.rx);

        if let Some(mut rx) = ready!(rx.poll_next(c)) {
            self.buf.append(&mut rx);
            let n = self.read_into(buf);
            Poll::Ready(Ok(n))
        } else {
            Poll::Ready(Err(BrokenPipe.into()))
        }
    }
}

impl AsyncWrite for Socket {
    fn poll_write(mut self: Pin<&mut Self>, c: &mut Context<'_>, buf: &[u8]) -> Poll<IO<usize>> {
        let mut tx = Pin::new(&mut self.tx);

        if ready!(tx.poll_ready(c)).is_err() {
            return Poll::Ready(Err(BrokenPipe.into()));
        }

        tx.try_send(buf.into()).unwrap();

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<IO<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<IO<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Connected for Socket {
    // TODO
    fn remote_addr(&self) -> Option<SocketAddr> {
        None
    }

    // TODO
    fn peer_certs(&self) -> Option<Vec<Certificate>> {
        None
    }
}

impl HyperConnection for Socket {
    fn connected(&self) -> HyperConnected {
        HyperConnected::new()
    }
}

impl Socket {
    pub fn new() -> (Self, Self) {
        let (a_tx, a_rx) = mpsc::channel(32);
        let (b_tx, b_rx) = mpsc::channel(32);

        let src = Self {
            rx: a_rx,
            tx: b_tx,
            buf: vec![],
        };

        let dst = Self {
            rx: b_rx,
            tx: a_tx,
            buf: vec![],
        };

        (src, dst)
    }

    fn read_into(&mut self, buf: &mut [u8]) -> usize {
        if buf.is_empty() {
            return 0;
        }

        if buf.len() <= self.buf.len() {
            let n = buf.len();
            buf.copy_from_slice(&self.buf[..n]);
            self.buf = self.buf.split_off(n);
            n
        } else {
            let n = self.buf.len();
            (&mut buf[..n]).copy_from_slice(&self.buf);
            self.buf.clear();
            n
        }
    }
}
