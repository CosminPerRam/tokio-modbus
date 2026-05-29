// SPDX-FileCopyrightText: Copyright (c) 2017-2026 slowtec GmbH <post@slowtec.de>
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::io;

use futures_util::{SinkExt as _, StreamExt as _};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tokio_util::codec::Framed;

use crate::{
    ExceptionResponse, ProtocolError, Request, Response, Result, Slave, codec,
    frame::{rtu::*, *},
    slave::SlaveId,
};

use super::{Command, Handle, REQUEST_CHANNEL_BOUND, disconnect, verify_response_header};

/// Spawn a connection actor for `transport` and return a handle to it.
///
/// Must be called from within a _Tokio_ runtime, as it spawns a background task.
pub(crate) fn new<T>(transport: T, slave: Slave) -> Handle
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let framed = Framed::new(transport, codec::rtu::ClientCodec::default());
    let (command_tx, command_rx) = mpsc::channel(REQUEST_CHANNEL_BOUND);
    tokio::spawn(run_actor(framed, command_rx));
    Handle::new(command_tx, slave)
}

/// Connection actor: owns the framed transport and serves commands until all
/// handles are dropped or an explicit disconnect is requested.
async fn run_actor<T>(
    mut framed: Framed<T, codec::rtu::ClientCodec>,
    mut command_rx: mpsc::Receiver<Command>,
) where
    T: AsyncRead + AsyncWrite + Unpin,
{
    while let Some(command) = command_rx.recv().await {
        match command {
            Command::Call {
                request,
                slave,
                response_tx,
            } => {
                let result = call(&mut framed, slave, request).await;
                // Ignore send errors: the caller may have gone away (e.g. timed out).
                drop(response_tx.send(result));
            }
            Command::Disconnect { response_tx } => {
                drop(response_tx.send(disconnect(framed).await));
                return;
            }
        }
    }
    // All handles dropped: drop the connection (mirrors dropping the old client).
}

async fn call<T>(
    framed: &mut Framed<T, codec::rtu::ClientCodec>,
    slave: Slave,
    req: Request<'_>,
) -> Result<Response>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    log::debug!("Call {req:?}");

    let req_function_code = req.function_code();
    let slave_id: SlaveId = slave.into();
    let req_adu = RequestAdu {
        hdr: Header { slave_id },
        pdu: RequestPdu::from(req),
    };
    let req_hdr = req_adu.hdr;

    framed.read_buffer_mut().clear();
    framed.send(req_adu).await?;

    let res_adu = framed
        .next()
        .await
        .unwrap_or_else(|| Err(io::Error::from(io::ErrorKind::BrokenPipe)))?;
    let ResponseAdu {
        hdr: res_hdr,
        pdu: res_pdu,
    } = res_adu;
    let ResponsePdu(result) = res_pdu;

    // Match headers of request and response.
    if let Err(message) = verify_response_header(&req_hdr, &res_hdr) {
        return Err(ProtocolError::HeaderMismatch { message, result }.into());
    }

    // Match function codes of request and response.
    let rsp_function_code = match &result {
        Ok(response) => response.function_code(),
        Err(ExceptionResponse { function, .. }) => *function,
    };
    if req_function_code != rsp_function_code {
        return Err(ProtocolError::FunctionCodeMismatch {
            request: req_function_code,
            result,
        }
        .into());
    }

    Ok(result.map_err(
        |ExceptionResponse {
             function: _,
             exception,
         }| exception,
    ))
}

#[cfg(test)]
mod tests {

    use core::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, Result};

    use crate::{
        Error,
        client::Client as _,
        service::{rtu::Header, verify_response_header},
    };

    #[test]
    fn validate_same_headers() {
        // Given
        let req_hdr = Header { slave_id: 0 };
        let rsp_hdr = Header { slave_id: 0 };

        // When
        let result = verify_response_header(&req_hdr, &rsp_hdr);

        // Then
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_validate_not_same_slave_id() {
        // Given
        let req_hdr = Header { slave_id: 0 };
        let rsp_hdr = Header { slave_id: 5 };

        // When
        let result = verify_response_header(&req_hdr, &rsp_hdr);

        // Then
        assert!(result.is_err());
    }

    #[derive(Debug)]
    struct MockTransport;

    impl Unpin for MockTransport {}

    impl AsyncRead for MockTransport {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for MockTransport {
        fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<Result<usize>> {
            Poll::Ready(Ok(2))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<()>> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn handle_broken_pipe() {
        let transport = MockTransport;
        let client =
            crate::service::rtu::new(transport, crate::service::rtu::Slave::broadcast());
        let res = client
            .call(crate::service::rtu::Request::ReadCoils(0x00, 5))
            .await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(
            matches!(err, Error::Transport(err) if err.kind() == std::io::ErrorKind::BrokenPipe)
        );
    }
}
