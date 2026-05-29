// SPDX-FileCopyrightText: Copyright (c) 2017-2026 slowtec GmbH <post@slowtec.de>
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Round-trip test for the synchronous TCP client.
//!
//! The synchronous client builds its own current-thread runtime and spawns the
//! connection actor onto it; this test makes sure requests actually round-trip
//! (i.e. the actor makes progress while the runtime is driven by `block_on`).

#![cfg(all(feature = "tcp-sync", feature = "tcp-server"))]

#[allow(unused)]
mod exception;

use std::{net::SocketAddr, sync::mpsc, thread, time::Duration};

use tokio::net::TcpListener;
use tokio_modbus::{
    ExceptionCode,
    client::sync::{Reader as _, Writer as _},
    server::tcp::{Server, accept_tcp_connection},
};

use crate::exception::TestService;

#[test]
fn sync_tcp_client_roundtrip() {
    // Run the server on a dedicated thread with its own runtime.
    let (addr_tx, addr_rx) = mpsc::channel::<SocketAddr>();
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addr_tx.send(listener.local_addr().unwrap()).unwrap();
            let server = Server::new(listener);
            let new_service = |_socket_addr| Ok(Some(TestService {}));
            let on_connected = |stream, socket_addr| async move {
                accept_tcp_connection(stream, socket_addr, new_service)
            };
            let _ = server.serve(&on_connected, |err| eprintln!("{err}")).await;
        });
    });

    let server_addr = addr_rx.recv().unwrap();
    // Give the server a moment to start accepting.
    thread::sleep(Duration::from_millis(200));

    // Synchronous client: builds its own current-thread runtime and spawns the
    // connection actor onto it. This exercises the sync path end-to-end.
    let mut ctx = tokio_modbus::client::sync::tcp::connect(server_addr).unwrap();

    // `TestService` maps these requests to deterministic exceptions; round-tripping
    // the exception responses proves the sync actor plumbing works.
    let response = ctx
        .read_holding_registers(0x00, 2)
        .expect("communication failed");
    assert!(matches!(response, Err(ExceptionCode::IllegalFunction)));

    // A second call proves the actor keeps serving across separate blocking calls.
    let response = ctx
        .write_single_register(0x00, 42)
        .expect("communication failed");
    assert!(matches!(response, Err(ExceptionCode::MemoryParityError)));
}
