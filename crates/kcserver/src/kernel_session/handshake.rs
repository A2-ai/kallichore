//
// handshake.rs
//
// Copyright (C) 2024-2025 Posit Software, PBC. All rights reserved.
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//
//

//! JEP 66 handshaking logic for kernel connections.

use async_channel::{Receiver, Sender};
use kallichore_api::models::{ConnectionInfo, StartupError};
use kcshared::{
    handshake_protocol::HandshakeRequest, kernel_message::KernelMessage,
    websocket_message::WebsocketMessage,
};

use crate::{
    connection_file::ConnectionFile,
    error::KSError,
    kernel_connection::KernelConnection,
    kernel_session::utils::strip_startup_markers,
    registration_file::RegistrationFile,
    registration_socket::RegistrationSocket,
    startup_status::StartupStatus,
};

/// Coordinates JEP 66 handshaking between kernel and server.
pub struct HandshakeCoordinator {
    /// Session ID for logging
    session_id: String,

    /// Connection information
    connection: KernelConnection,

    /// Channel to send messages to the WebSocket
    ws_json_tx: Sender<WebsocketMessage>,

    /// The shell used for startup (for error reporting)
    shell_used: Option<String>,

    /// The startup command/script executed (for error reporting)
    startup_command: Option<String>,
}

impl HandshakeCoordinator {
    /// Create a new handshake coordinator.
    pub fn new(
        session_id: String,
        connection: KernelConnection,
        ws_json_tx: Sender<WebsocketMessage>,
        shell_used: Option<String>,
        startup_command: Option<String>,
    ) -> Self {
        Self {
            session_id,
            connection,
            ws_json_tx,
            shell_used,
            startup_command,
        }
    }

    /// Set up a registration socket for JEP 66 handshaking.
    ///
    /// Creates a registration file and socket that the kernel will connect to
    /// during the handshake process.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - The path to the registration file
    /// - The registration socket
    pub async fn setup_registration_socket(
        &self,
    ) -> Result<(std::path::PathBuf, RegistrationSocket), StartupError> {
        // Create a registration socket that immediately binds to an OS-selected port
        let registration_socket = match RegistrationSocket::new().await {
            Ok(socket) => socket,
            Err(e) => {
                return Err(StartupError {
                    exit_code: None,
                    output: None,
                    error: KSError::HandshakeFailed(
                        self.session_id.clone(),
                        anyhow::anyhow!("Failed to create registration socket: {}", e),
                    )
                    .to_json(None),
                });
            }
        };

        let registration_port = registration_socket.port();

        log::debug!(
            "Started registration socket for session {} with port {}",
            self.session_id,
            registration_port
        );

        // Create the registration file name and path
        let mut registration_file_name = std::ffi::OsString::from("registration_");
        registration_file_name.push(&self.session_id);
        registration_file_name.push(".json");
        let registration_path = std::env::temp_dir().join(registration_file_name);

        // Create the registration file
        let registration_file = RegistrationFile::new(
            "127.0.0.1".to_string(),
            registration_port,
            self.connection.key.clone(),
        );
        registration_file
            .to_file(registration_path.clone())
            .map_err(|e| StartupError {
                exit_code: None,
                output: None,
                error: KSError::ProcessStartFailed(anyhow::anyhow!(
                    "Failed to write registration file: {}",
                    e
                ))
                .to_json(None),
            })?;

        log::debug!(
            "Wrote registration file for session {} at {:?} with port {}",
            self.session_id,
            registration_path,
            registration_port
        );

        Ok((registration_path, registration_socket))
    }

    /// Wait for a handshake to be completed.
    ///
    /// This is used when starting a kernel that supports JEP 66 handshaking.
    /// Whatever the outcome, the registration socket is closed and its port
    /// released before this returns.
    ///
    /// # Arguments
    ///
    /// * `registration_socket` - The socket to use for the handshake procedure
    /// * `startup_rx` - The channel to receive startup status messages (including kernel exits)
    /// * `timeout_secs` - Time to wait for the handshake in seconds
    pub async fn wait_for_handshake(
        &self,
        mut registration_socket: RegistrationSocket,
        startup_rx: Receiver<StartupStatus>,
        timeout_secs: u64,
    ) -> Result<ConnectionFile, StartupError> {
        // Wait for the handshake to complete, kernel exit, or timeout
        let result = tokio::select! {
            handshake = registration_socket.handshake(&self.connection) => {
                self.handle_handshake_result(handshake).await
            }
            startup_status = startup_rx.recv() => {
                self.handle_startup_during_handshake(startup_status)
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(timeout_secs)) => {
                self.handle_handshake_timeout()
            }
        };

        registration_socket.close().await;

        result
    }

    /// Handle the outcome of the handshake on the registration socket.
    async fn handle_handshake_result(
        &self,
        handshake: Result<HandshakeRequest, anyhow::Error>,
    ) -> Result<ConnectionFile, StartupError> {
        match handshake {
            Ok(request) => {
                // Handshake completed successfully
                log::info!(
                    "[session {}] Handshake completed successfully",
                    self.session_id
                );

                // Create a connection file from the ports the kernel
                // registered, with the key included
                let connection_file = ConnectionFile::from_info(ConnectionInfo {
                    shell_port: request.shell_port as i32,
                    iopub_port: request.iopub_port as i32,
                    stdin_port: request.stdin_port as i32,
                    control_port: request.control_port as i32,
                    hb_port: request.hb_port as i32,
                    transport: "tcp".to_string(),
                    signature_scheme: "hmac-sha256".to_string(),
                    key: match &self.connection.key {
                        Some(key) => key.clone(),
                        None => String::new(),
                    },
                    ip: "127.0.0.1".to_string(),
                });

                // Create an event message to send through the websocket
                let msg = WebsocketMessage::Kernel(KernelMessage::HandshakeCompleted(
                    self.session_id.clone(),
                    connection_file.info.clone(),
                ));

                // Send the event through the websocket channel
                if let Err(e) = self.ws_json_tx.send(msg).await {
                    log::warn!(
                        "[session {}] Failed to send handshake completed message: {}",
                        self.session_id,
                        e
                    );
                }

                Ok(connection_file)
            }
            Err(e) => {
                log::error!(
                    "[session {}] The handshake failed: {:#}",
                    self.session_id,
                    e
                );
                Err(self.ks_error_to_startup_error(KSError::HandshakeFailed(
                    self.session_id.clone(),
                    e,
                )))
            }
        }
    }

    /// Handle startup status events that occur during handshake.
    fn handle_startup_during_handshake(
        &self,
        startup_status: Result<StartupStatus, async_channel::RecvError>,
    ) -> Result<ConnectionFile, StartupError> {
        match startup_status {
            Ok(StartupStatus::AbnormalExit(exit_code, output, error)) => {
                // The kernel exited before the handshake could complete

                // Strip internal markers from output
                let clean_output = strip_startup_markers(&output);

                // Build context for the error message
                let mut error_context = String::new();
                if let Some(shell) = &self.shell_used {
                    error_context.push_str(&format!("Shell: {}\n", shell));
                }
                if let Some(cmd) = &self.startup_command {
                    error_context.push_str(&format!("Startup command: {}\n", cmd));
                }
                error_context
                    .push_str("The kernel exited before a connection could be established");

                log::error!(
                    "[session {}] Kernel exited during handshake with code {}: {}",
                    self.session_id,
                    exit_code,
                    error
                );
                log::error!(
                    "[session {}] Output before exit: \n{}",
                    self.session_id,
                    clean_output
                );

                Err(StartupError {
                    exit_code: Some(exit_code),
                    output: Some(clean_output),
                    error: KSError::ExitedBeforeConnection.to_json(Some(error_context)),
                })
            }
            Ok(status) => {
                // Other startup status (shouldn't happen during handshake)
                log::warn!(
                    "[session {}] Unexpected startup status during handshake: {:?}",
                    self.session_id,
                    status
                );
                Err(self.ks_error_to_startup_error(KSError::HandshakeFailed(
                    self.session_id.clone(),
                    anyhow::anyhow!("Unexpected startup status"),
                )))
            }
            Err(e) => {
                // Error receiving from startup channel
                log::error!(
                    "[session {}] Error receiving startup status during handshake: {}",
                    self.session_id,
                    e
                );
                Err(self.ks_error_to_startup_error(KSError::HandshakeFailed(
                    self.session_id.clone(),
                    anyhow::anyhow!("Startup channel error: {}", e),
                )))
            }
        }
    }

    /// Handle a handshake timeout.
    fn handle_handshake_timeout(&self) -> Result<ConnectionFile, StartupError> {
        log::error!(
            "[session {}] Timeout waiting for handshake",
            self.session_id
        );
        Err(self.ks_error_to_startup_error(KSError::HandshakeFailed(
            self.session_id.clone(),
            anyhow::anyhow!("Timeout waiting for handshake"),
        )))
    }

    /// Helper method to convert a KSError to a StartupError.
    fn ks_error_to_startup_error(&self, error: KSError) -> StartupError {
        StartupError {
            exit_code: None,
            output: None,
            error: error.to_json(None),
        }
    }
}
