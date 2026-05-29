// SPDX-FileCopyrightText: Copyright (c) 2017-2026 slowtec GmbH <post@slowtec.de>
// SPDX-License-Identifier: MIT OR Apache-2.0

#[cfg(feature = "rtu")]
pub(crate) mod rtu;

#[cfg(feature = "tcp")]
pub(crate) mod tcp;

#[cfg(any(feature = "rtu", feature = "tcp"))]
mod actor {
    //! Transport-independent connection actor and client handle.
    //!
    //! Each connection is owned by a dedicated background task (the *actor*).
    //! The public client is a cheap, cloneable [`Handle`] that forwards
    //! requests to the actor over a channel and awaits the response. Because
    //! the actor processes one command at a time it naturally serializes the
    //! request/response exchange of a single _Modbus_ connection, so no lock
    //! is required and the connection always has a single owner.

    use std::{
        io,
        sync::{
            Arc,
            atomic::{AtomicU8, Ordering},
        },
    };

    use async_trait::async_trait;
    use tokio::sync::{mpsc, oneshot};

    use crate::{Request, Response, Result, Slave, slave::SlaveContext};

    /// Bound of the command channel between a [`Handle`] and its actor.
    ///
    /// Acts as backpressure: at most this many requests can be queued ahead of
    /// the one currently being processed before [`Handle::call`] starts to wait.
    pub(crate) const REQUEST_CHANNEL_BOUND: usize = 32;

    /// A command sent from a [`Handle`] to its connection actor.
    pub(crate) enum Command {
        /// Invoke a _Modbus_ function against `slave` and reply on `response_tx`.
        Call {
            request: Request<'static>,
            slave: Slave,
            response_tx: oneshot::Sender<Result<Response>>,
        },
        /// Gracefully shut down the connection; the actor exits afterwards.
        Disconnect {
            response_tx: oneshot::Sender<io::Result<()>>,
        },
    }

    /// Transport-independent, cloneable client handle backed by a connection actor.
    #[derive(Debug, Clone)]
    pub(crate) struct Handle {
        command_tx: mpsc::Sender<Command>,
        // Per-handle slave selection, snapshotted into each `Call` command. Shared
        // between clones so `set_slave` on one is visible to the others.
        slave: Arc<AtomicU8>,
    }

    impl Handle {
        pub(crate) fn new(command_tx: mpsc::Sender<Command>, slave: Slave) -> Self {
            Self {
                command_tx,
                slave: Arc::new(AtomicU8::new(slave.0)),
            }
        }
    }

    /// Error returned when the actor is gone (channel closed) or never replied.
    fn disconnected() -> crate::Error {
        io::Error::new(io::ErrorKind::NotConnected, "disconnected").into()
    }

    #[async_trait]
    impl crate::client::Client for Handle {
        async fn call(&self, request: Request<'_>) -> Result<Response> {
            let slave = Slave(self.slave.load(Ordering::Relaxed));
            let (response_tx, response_rx) = oneshot::channel();
            let command = Command::Call {
                request: request.into_owned(),
                slave,
                response_tx,
            };
            self.command_tx
                .send(command)
                .await
                .map_err(|_| disconnected())?;
            response_rx.await.map_err(|_| disconnected())?
        }

        async fn disconnect(&self) -> io::Result<()> {
            let (response_tx, response_rx) = oneshot::channel();
            if self
                .command_tx
                .send(Command::Disconnect { response_tx })
                .await
                .is_err()
            {
                // Actor already gone => already disconnected.
                return Ok(());
            }
            // If the actor drops the sender without replying, treat it as done.
            response_rx.await.unwrap_or(Ok(()))
        }
    }

    impl SlaveContext for Handle {
        fn set_slave(&self, slave: Slave) {
            self.slave.store(slave.0, Ordering::Relaxed);
        }
    }
}

#[cfg(any(feature = "rtu", feature = "tcp"))]
pub(crate) use actor::{Command, Handle, REQUEST_CHANNEL_BOUND};

#[cfg(any(feature = "rtu", feature = "tcp"))]
async fn disconnect<T, C>(framed: tokio_util::codec::Framed<T, C>) -> std::io::Result<()>
where
    T: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt as _;

    framed
        .into_inner()
        .shutdown()
        .await
        .or_else(|err| match err.kind() {
            std::io::ErrorKind::NotConnected | std::io::ErrorKind::BrokenPipe => {
                // Already disconnected.
                Ok(())
            }
            _ => Err(err),
        })
}

/// Check that `req_hdr` is the same `Header` as `rsp_hdr`.
///
/// # Errors
///
/// If the 2 headers are different, an error message with the details will be returned.
#[cfg(any(feature = "rtu", feature = "tcp"))]
fn verify_response_header<H: Eq + std::fmt::Debug>(req_hdr: &H, rsp_hdr: &H) -> Result<(), String> {
    if req_hdr != rsp_hdr {
        return Err(format!(
            "expected/request = {req_hdr:?}, actual/response = {rsp_hdr:?}"
        ));
    }
    Ok(())
}
