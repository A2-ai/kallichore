//
// process.rs
//
// Copyright (C) 2024-2025 Posit Software, PBC. All rights reserved.
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//
//

//! Child process management for kernel sessions.

use async_channel::{Receiver, Sender};
use event_listener::Event;
use kallichore_api::models;
use kcshared::{
    kernel_message::{KernelMessage, OutputStream},
    websocket_message::WebsocketMessage,
};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead};
use tokio::sync::RwLock;

use crate::{error::KSError, kernel_state::KernelState, startup_status::StartupStatus};

/// Monitors a kernel child process and handles its lifecycle.
pub struct ProcessMonitor {
    /// Session ID for logging
    session_id: String,

    /// Shared kernel state
    state: Arc<RwLock<KernelState>>,

    /// Event that fires when the process exits
    exit_event: Arc<Event>,

    /// Channel to send JSON messages to the WebSocket
    ws_json_tx: Sender<WebsocketMessage>,
}

impl ProcessMonitor {
    /// Create a new process monitor.
    pub fn new(
        session_id: String,
        state: Arc<RwLock<KernelState>>,
        exit_event: Arc<Event>,
        ws_json_tx: Sender<WebsocketMessage>,
    ) -> Self {
        Self {
            session_id,
            state,
            exit_event,
            ws_json_tx,
        }
    }

    /// Capture stdout and stderr from a child process and forward them to the WebSocket.
    pub fn capture_output_streams(&self, child: &mut tokio::process::Child) {
        // Capture stdout
        if let Some(stdout) = child.stdout.take() {
            Self::stream_output(stdout, OutputStream::Stdout, self.ws_json_tx.clone());
        }

        // Capture stderr
        if let Some(stderr) = child.stderr.take() {
            Self::stream_output(stderr, OutputStream::Stderr, self.ws_json_tx.clone());
        }
    }

    /// Monitor a child process, waiting for it to exit.
    ///
    /// This method blocks until the child process exits, then updates the kernel
    /// state and notifies listeners.
    ///
    /// A message on `stop_rx` stops the process. The start that launched the
    /// process sends one when it fails while the process is still running.
    pub async fn run_child<F>(
        &self,
        mut child: tokio::process::Child,
        startup_tx: Sender<StartupStatus>,
        stop_rx: Receiver<()>,
        consume_output: F,
    ) where
        F: Fn() -> String,
    {
        // Actually run the kernel! This will block until the kernel exits.
        //
        // Once the start has succeeded its end of the stop channel is dropped;
        // the second branch is then disabled and this waits on the process
        // alone.
        let status = tokio::select! {
            status = child.wait() => status,
            Ok(()) = stop_rx.recv() => {
                log::info!(
                    "Stopping the child process for session {}; its start failed",
                    self.session_id
                );
                if let Err(e) = child.start_kill() {
                    log::warn!(
                        "Failed to stop the child process for session {}: {}",
                        self.session_id,
                        e
                    );
                }
                child.wait().await
            }
        }
        .expect("Failed to wait on child process");
        let code = status.code().unwrap_or(-1);

        log::info!(
            "Child process for session {} exited with status: {}",
            self.session_id,
            status
        );

        // The kernel's accounting job has nothing left in it now.
        super::job_object::release_session_job(&self.session_id);

        // If the start that launched this process is still waiting for the
        // kernel, that's bad; tell it the process has exited. This goes by
        // the channel rather than by the session's status: the status can
        // have moved on from Starting while the start still waits (a kernel
        // with no heartbeat is marked offline), and it can still be Starting
        // after the start has failed and stopped waiting.
        if !startup_tx.is_closed() {
            let output = consume_output();
            let _ = startup_tx
                .send(StartupStatus::AbnormalExit(
                    code,
                    output,
                    KSError::ProcessAbnormalExit(status),
                ))
                .await;
        }

        // We are now exited; mark the kernel as such
        {
            let mut state = self.state.write().await;
            state
                .set_status(
                    models::Status::Exited,
                    Some(String::from("child process exited")),
                )
                .await;
            // Before anyone hears of the exit: a restart waiting for it then
            // writes its own files, which this must not remove
            state.remove_launch_files();
        }

        // Notify anyone listening that the kernel has exited
        self.exit_event.notify(usize::MAX);

        let event = WebsocketMessage::Kernel(KernelMessage::Exited(code));
        self.ws_json_tx
            .send(event)
            .await
            .expect("Failed to send exit event to client");
    }

    /// Stream output from a child process to the WebSocket.
    ///
    /// This function reads lines from a stream and sends them to the WebSocket. It's used to forward
    /// the stdout and stderr of a child process to the client.
    ///
    /// # Arguments
    ///
    /// - `stream`: The stream to read from
    /// - `kind`: The kind of output (stdout or stderr)
    /// - `ws_json_tx`: The channel to send JSON messages to the WebSocket
    fn stream_output<T: AsyncRead + Unpin + Send + 'static>(
        stream: T,
        kind: OutputStream,
        ws_json_tx: Sender<WebsocketMessage>,
    ) {
        tokio::spawn(async move {
            let mut reader = tokio::io::BufReader::new(Box::pin(stream));
            let mut buffer = String::new();
            loop {
                buffer.clear();
                match reader.read_line(&mut buffer).await {
                    Ok(0) => {
                        log::debug!("End of output stream (kind: {:?})", kind);
                        break;
                    }
                    Ok(_) => {
                        let message = WebsocketMessage::Kernel(KernelMessage::Output(
                            kind.clone(),
                            buffer.to_string(),
                        ));
                        ws_json_tx
                            .send(message)
                            .await
                            .expect("Failed to send standard stream message to client");
                    }
                    Err(e) => {
                        log::error!("Failed to read from standard stream: {}", e);
                        break;
                    }
                }
            }
        });
    }
}
