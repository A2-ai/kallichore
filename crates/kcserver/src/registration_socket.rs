//
// registration_socket.rs
//
// Copyright (C) 2025 Posit Software, PBC. All rights reserved.
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//
//

use crate::{
    jupyter_messages::JupyterMsg, kernel_connection::KernelConnection, wire_message::WireMessage,
};
use anyhow::{Context, Result};
use kcshared::jupyter_message::JupyterChannel;
use kcshared::{
    handshake_protocol::{HandshakeReply, HandshakeRequest, HandshakeStatus},
    jupyter_message::{JupyterMessage, JupyterMessageHeader},
};
use log::{info, warn};
use std::collections::HashMap;
use zeromq::{RepSocket, Socket, SocketRecv, SocketSend};

/// Manages a registration socket for JEP 66 handshaking protocol
pub struct RegistrationSocket {
    /// The ZeroMQ Reply socket for the registration socket
    socket: RepSocket,

    /// The port that the `socket` is bound to
    port: u16,
}

impl RegistrationSocket {
    /// Create a new registration socket
    ///
    /// It is bound to a port immediately on creation. Uses an initial port of `0` to
    /// allow the OS to pick the port, avoiding race conditions.
    pub async fn new() -> Result<Self> {
        let address = "tcp://127.0.0.1:0";

        let mut socket = RepSocket::new();

        // Save the port that the OS binds to
        let port = match socket.bind(address).await? {
            zeromq::Endpoint::Tcp(_, port) => port,
            _ => unreachable!("Address is always TCP"),
        };

        Ok(Self { socket, port })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Wait for the kernel's handshake request and answer it.
    ///
    /// Returns the request once the kernel has been sent its reply, and not
    /// before: until then the kernel is still waiting to be acknowledged. An
    /// error means the socket failed, or the reply could not be sent.
    ///
    /// A message that is not a handshake request this session can read (it
    /// does not parse, or is not signed with the session's key) is answered
    /// with an error, and the wait goes on. The port is open to every process
    /// on the machine; a stray message must not use up the one registration.
    ///
    /// This future can be dropped at any point. The caller ends the wait that
    /// way when the kernel exits or takes too long, and then calls `close`.
    pub async fn handshake(&mut self, connection: &KernelConnection) -> Result<HandshakeRequest> {
        loop {
            let request_data = self.socket.recv().await?;
            info!(
                "[session {}] Received a message on the registration socket",
                connection.session_id
            );
            let wire_message = WireMessage::from_zmq(
                "registration".to_string(),
                JupyterChannel::Registration,
                request_data,
            );
            let refusal = match wire_message
                .to_jupyter(JupyterChannel::Registration, connection.hmac_key.clone())
            {
                Ok(jupyter_message) => match JupyterMsg::from(jupyter_message.clone()) {
                    JupyterMsg::HandshakeRequest(request) => {
                        self.send_successful_handshake(jupyter_message, connection)
                            .await?;
                        return Ok(request);
                    }
                    _ => {
                        warn!(
                            "[session {}] Received non-handshake request on the registration socket: {:?}",
                            connection.session_id, jupyter_message
                        );
                        String::from("Not a handshake request")
                    }
                },
                Err(e) => {
                    warn!(
                        "[session {}] Failed to parse JupyterMessage: {}",
                        connection.session_id, e
                    );
                    format!("Failed to parse JupyterMessage: {}", e)
                }
            };

            // A reply socket has to answer one request before it can receive
            // the next
            let reply = HandshakeReply {
                status: HandshakeStatus::Error,
                error: Some(refusal),
                capabilities: HashMap::new(),
            };
            let reply_data =
                serde_json::to_vec(&reply).expect("Failed to serialize handshake reply");
            if let Err(e) = self.socket.send(reply_data.into()).await {
                warn!(
                    "[session {}] Failed to send handshake reply: {}",
                    connection.session_id, e
                );
            }
        }
    }

    /// Send the kernel the reply to a handshake request that was accepted.
    async fn send_successful_handshake(
        &mut self,
        message: JupyterMessage,
        connection: &KernelConnection,
    ) -> Result<()> {
        // Create a successful handshake reply
        let reply = HandshakeReply {
            status: HandshakeStatus::Ok,
            error: None,
            capabilities: HashMap::new(),
        };

        // Create a Jupyter message containing the handshake reply
        // Generate a message id
        let jupyter_msg = JupyterMessage {
            header: JupyterMessageHeader {
                msg_type: "handshake_reply".to_string(),
                msg_id: uuid::Uuid::new_v4().to_string(),
            },
            parent_header: Some(message.header),
            channel: JupyterChannel::Registration,
            content: serde_json::to_value(reply).unwrap(),
            metadata: serde_json::Value::Null,
            buffers: vec![],
        };

        // Convert to a wire message for sending
        let wire_message = WireMessage::from_jupyter(jupyter_msg, connection.clone())
            .context("Failed to create wire message for handshake reply")?;
        info!(
            "[session {}] Sending successful handshake reply to kernel",
            connection.session_id
        );
        // Convert wire message to ZMQ format for sending
        self.socket
            .send(wire_message.into())
            .await
            .context("Failed to send handshake reply to kernel")?;
        Ok(())
    }

    /// Close the socket and release its port.
    ///
    /// Consumes `self`: a kernel registers once, and whether it did or not
    /// there is nothing more to listen for.
    pub async fn close(self) {
        self.socket.close().await;
    }
}
