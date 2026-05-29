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
    frame::{
        RequestPdu, ResponsePdu,
        tcp::{Header, RequestAdu, ResponseAdu, TransactionId, UnitId},
    },
    service::verify_response_header,
};

use super::{Command, Handle, REQUEST_CHANNEL_BOUND, disconnect};

const INITIAL_TRANSACTION_ID: TransactionId = 0;

/// Generates the per-request transaction ids.
///
/// Owned exclusively by the connection actor, so a plain counter suffices.
#[derive(Debug)]
struct TransactionIdGenerator {
    next_transaction_id: TransactionId,
}

impl TransactionIdGenerator {
    const fn new() -> Self {
        Self {
            next_transaction_id: INITIAL_TRANSACTION_ID,
        }
    }

    fn next(&mut self) -> TransactionId {
        let next_transaction_id = self.next_transaction_id;
        self.next_transaction_id = next_transaction_id.wrapping_add(1);
        next_transaction_id
    }
}

/// Spawn a connection actor for `transport` and return a handle to it.
///
/// Must be called from within a _Tokio_ runtime, as it spawns a background task.
pub(crate) fn new<T>(transport: T, slave: Slave) -> Handle
where
    T: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let framed = Framed::new(transport, codec::tcp::ClientCodec::new());
    let (command_tx, command_rx) = mpsc::channel(REQUEST_CHANNEL_BOUND);
    tokio::spawn(run_actor(framed, command_rx));
    Handle::new(command_tx, slave)
}

/// Connection actor: owns the framed transport and serves commands until all
/// handles are dropped or an explicit disconnect is requested.
async fn run_actor<T>(
    mut framed: Framed<T, codec::tcp::ClientCodec>,
    mut command_rx: mpsc::Receiver<Command>,
) where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut transaction_id_generator = TransactionIdGenerator::new();
    while let Some(command) = command_rx.recv().await {
        match command {
            Command::Call {
                request,
                slave,
                response_tx,
            } => {
                let result = call(&mut framed, &mut transaction_id_generator, slave, request).await;
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
    framed: &mut Framed<T, codec::tcp::ClientCodec>,
    transaction_id_generator: &mut TransactionIdGenerator,
    slave: Slave,
    req: Request<'_>,
) -> Result<Response>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    log::debug!("Call {req:?}");

    let req_function_code = req.function_code();
    let unit_id: UnitId = slave.into();
    let req_adu = RequestAdu {
        hdr: Header {
            transaction_id: transaction_id_generator.next(),
            unit_id,
        },
        pdu: RequestPdu::from(req),
    };
    let req_hdr = req_adu.hdr;

    framed.read_buffer_mut().clear();
    framed.send(req_adu).await?;

    let res_adu = framed.next().await.ok_or_else(io::Error::last_os_error)??;
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
    use super::*;

    #[test]
    fn validate_same_headers() {
        // Given
        let req_hdr = Header {
            unit_id: 0,
            transaction_id: 42,
        };
        let rsp_hdr = Header {
            unit_id: 0,
            transaction_id: 42,
        };

        // When
        let result = verify_response_header(&req_hdr, &rsp_hdr);

        // Then
        assert!(result.is_ok());
    }

    #[test]
    fn invalid_validate_not_same_unit_id() {
        // Given
        let req_hdr = Header {
            unit_id: 0,
            transaction_id: 42,
        };
        let rsp_hdr = Header {
            unit_id: 5,
            transaction_id: 42,
        };

        // When
        let result = verify_response_header(&req_hdr, &rsp_hdr);

        // Then
        assert!(result.is_err());
    }

    #[test]
    fn invalid_validate_not_same_transaction_id() {
        // Given
        let req_hdr = Header {
            unit_id: 0,
            transaction_id: 42,
        };
        let rsp_hdr = Header {
            unit_id: 0,
            transaction_id: 86,
        };

        // When
        let result = verify_response_header(&req_hdr, &rsp_hdr);

        // Then
        assert!(result.is_err());
    }
}
