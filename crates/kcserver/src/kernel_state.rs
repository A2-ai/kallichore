//
// kernel_state.rs
//
// Copyright (C) 2024-2025 Posit Software, PBC. All rights reserved.
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//
//

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use async_channel::Sender;
use kallichore_api::models;
use kcshared::{
    jupyter_message::JupyterMessage,
    kernel_message::{KernelMessage, StatusUpdate},
    websocket_message::WebsocketMessage,
};
use tokio::sync::mpsc;

use crate::connection_file::{ConnectionFile, PortReservation};
use crate::execution_history::ExecutionHistory;
use crate::execution_queue::ExecutionQueue;

#[cfg(not(target_os = "windows"))]
use crate::working_dir::get_process_cwd;

/// The mutable state of the kernel.
///
/// Does not implement the Clone trait; only one instance of the kernel state
/// should exist at a time.
#[derive(Debug)]
pub struct KernelState {
    /// The session ID for this kernel instance.
    pub session_id: String,

    /// The kernel's current status.
    pub status: models::Status,

    /// Whether the kernel is connected to a client.
    pub connected: bool,

    /// The restart of the kernel that is in progress, if one is: the kernel
    /// has been asked to shut down and is started again when it has exited.
    /// It is over when its `RestartClaim` is dropped, which is also what
    /// happens to a restart whose client stops waiting for the kernel to
    /// exit.
    restart: Weak<()>,

    /// The current working directory of the kernel.
    pub working_directory: String,

    /// The set of environment variable actions for startup.
    pub env_vars: Vec<models::VarAction>,

    /// The resolved environment variables for the kernel.
    pub resolved_env: HashMap<String, String>,

    /// The current process ID of the kernel, or None if the kernel is not running.
    pub process_id: Option<u32>,

    /// The execution queue for the kernel.
    pub execution_queue: ExecutionQueue,

    /// The code the kernel has run and what it produced.
    pub history: ExecutionHistory,

    /// The current input prompt.
    pub input_prompt: String,

    /// The current continuation prompt.
    pub continuation_prompt: String,

    /// The time at which the kernel last became idle.
    pub idle_since: Option<std::time::Instant>,

    /// The time at which the kernel last became busy.
    pub busy_since: Option<std::time::Instant>,

    /// A channel on which to send idle nudges
    pub idle_nudge_tx: mpsc::Sender<Option<u32>>,

    /// A channel to publish status updates to the websocket
    ws_json_tx: Sender<WebsocketMessage>,

    /// The connection file for the kernel, or None if not set.
    pub connection_file: Option<ConnectionFile>,

    /// The reservation of the ports in a connection file that was generated
    /// before the kernel was started, because a client asked for the
    /// connection information early. The ports are released when that
    /// connection file is replaced, or when the session is dropped.
    pub connection_file_ports: Option<PortReservation>,

    /// The Unix domain socket path for the current client, if any.
    pub client_socket_path: Option<String>,

    /// The ID of the client the session is connected to, if any. A client
    /// that another has taken the place of is no longer it, and does not
    /// mark the session disconnected when its own connection ends.
    connected_client: Option<String>,

    /// The kernel information, as returned by the kernel_info_request message.
    pub kernel_info: Option<serde_json::Value>,

    /// The most recent resource usage measurement for the kernel.
    pub resource_usage: Option<models::ResourceUsage>,

    /// RPC listeners: maps a parent msg_id to a channel that receives
    /// Jupyter messages matching that parent. Used by the execute_code RPC
    /// to collect iopub output and the shell reply without going through
    /// the WebSocket.
    pub rpc_listeners: HashMap<String, mpsc::UnboundedSender<JupyterMessage>>,

    /// The status the kernel had when it was marked offline, while it is
    /// offline. It is the status the kernel is given back when its heartbeat
    /// returns.
    status_before_offline: Option<models::Status>,
}

/// A restart of a kernel that is in progress. The session counts as
/// restarting until this is dropped.
#[derive(Debug)]
pub struct RestartClaim(#[allow(dead_code)] Arc<()>);

impl KernelState {
    /// Whether a restart of the kernel is in progress.
    pub fn restarting(&self) -> bool {
        self.restart.strong_count() > 0
    }

    /// Begins a restart of the kernel, which lasts until the claim that is
    /// returned is dropped. Returns `None` if a restart is in progress.
    pub fn begin_restart(&mut self) -> Option<RestartClaim> {
        if self.restarting() {
            return None;
        }
        let claim = Arc::new(());
        self.restart = Arc::downgrade(&claim);
        Some(RestartClaim(claim))
    }

    /// Create a new kernel state.
    pub fn new(
        session: models::NewSession,
        working_directory: String,
        idle_nudge_tx: mpsc::Sender<Option<u32>>,
        ws_json_tx: Sender<WebsocketMessage>,
    ) -> Self {
        KernelState {
            session_id: session.session_id.clone(),
            status: models::Status::Idle,
            working_directory,
            connected: false,
            restart: Weak::new(),
            process_id: None,
            execution_queue: ExecutionQueue::new(),
            history: ExecutionHistory::default(),
            env_vars: session.env.clone(),
            resolved_env: HashMap::new(),
            input_prompt: session.input_prompt.clone(),
            continuation_prompt: session.continuation_prompt.clone(),
            ws_json_tx,
            idle_nudge_tx,
            idle_since: Some(std::time::Instant::now()),
            busy_since: None,
            connection_file: None,
            connection_file_ports: None,
            client_socket_path: None,
            connected_client: None,
            kernel_info: None,
            resource_usage: None,
            rpc_listeners: HashMap::new(),
            status_before_offline: None,
        }
    }

    async fn nudge_idle(&mut self) {
        if let Err(err) = self.idle_nudge_tx.send(None).await {
            log::error!(
                "[session {}] Failed to send idle nudge: {}",
                self.session_id,
                err
            );
        }
    }

    pub async fn set_connected(&mut self, connected: bool) {
        self.connected = connected;
        self.nudge_idle().await;
    }

    /// Records that the client `client_id` is now the session's client, in
    /// place of any client before it.
    pub async fn client_connected(&mut self, client_id: &str, socket_path: Option<String>) {
        self.connected_client = Some(client_id.to_string());
        self.client_socket_path = socket_path;
        self.set_connected(true).await;
    }

    /// Records that the connection of the client `client_id` has ended. If
    /// another client has taken its place since, the session is still
    /// connected, to that one, and nothing changes.
    pub fn client_disconnected(&mut self, client_id: &str) {
        if self.connected_client.as_deref() != Some(client_id) {
            return;
        }
        self.connected_client = None;
        self.connected = false;
        self.client_socket_path = None;
    }

    /// Set the Unix domain socket path for the current client. Only the
    /// handler of a named pipe's raw protocol sets it by itself.
    #[cfg(windows)]
    pub fn set_client_socket_path(&mut self, socket_path: Option<String>) {
        self.client_socket_path = socket_path;
    }

    /// Set the kernel information
    pub fn set_kernel_info(&mut self, kernel_info: serde_json::Value) {
        self.kernel_info = Some(kernel_info);
    }

    /// Polls the working directory to see if it's changed.
    ///
    /// If it has, updates state and sends a message to the client. Only supported
    /// on non-Windows platforms.
    #[cfg(not(target_os = "windows"))]
    pub async fn poll_working_dir(&mut self) {
        if self.process_id.is_none() {
            return;
        }

        let working_dir = match get_process_cwd(self.process_id.unwrap()) {
            Ok(dir) => dir,
            Err(err) => {
                log::error!(
                    "[session {}] Failed to get working directory: {}",
                    self.session_id,
                    err
                );
                return;
            }
        };

        let working_dir = working_dir.to_string_lossy().to_string();

        if working_dir != self.working_directory {
            log::debug!(
                "[session {}] Working directory changed: '{}' => '{}'",
                self.session_id,
                self.working_directory,
                working_dir
            );
            let msg =
                WebsocketMessage::Kernel(KernelMessage::WorkingDirChanged(working_dir.clone()));
            self.ws_json_tx.send(msg).await.unwrap();
            self.working_directory = working_dir;
        }
    }

    /// The kernel answers its heartbeat again after it was marked offline:
    /// give it back the status it had before.
    ///
    /// Does nothing if the status has changed since the kernel was marked
    /// offline. A status the kernel has reported itself meanwhile, or its
    /// exit, is newer than what is remembered here.
    pub async fn restore_status_after_offline(&mut self) {
        if self.status != models::Status::Offline {
            return;
        }
        let status = self
            .status_before_offline
            .take()
            .unwrap_or(models::Status::Idle);
        self.set_status(
            status,
            Some(String::from("heartbeat detected after offline")),
        )
        .await;
    }

    /// The kernel has given its first answer while it is marked offline.
    ///
    /// If its heartbeat was lost while it was still starting, the status to
    /// come back to is no longer a starting one: the answer is what ends the
    /// start, and nothing else would move the session on from there.
    pub fn first_answer_while_offline(&mut self) {
        if matches!(
            self.status_before_offline,
            Some(models::Status::Uninitialized | models::Status::Starting | models::Status::Ready)
        ) {
            self.status_before_offline = Some(models::Status::Idle);
        }
    }

    /// Set the kernel's status.
    pub async fn set_status(&mut self, status: models::Status, reason: Option<String>) {
        log::debug!(
            "[session {}] status '{}' => '{}' {}",
            self.session_id,
            self.status,
            status,
            match reason {
                Some(ref r) => format!("({})", r),
                None => "".to_string(),
            }
        );

        // If the status didn't change, don't perform any side effects or notify the client.
        if self.status == status {
            return;
        }

        // Once the kernel has exited, don't let a late/buffered iopub status
        // message (e.g. the trailing 'idle' a kernel emits while handling a
        // shutdown_request) resurrect it into a running state. Coming back to
        // life only happens via an explicit restart, which moves the status
        // through 'starting' first.
        //
        // The same goes for a heartbeat that timed out just as the kernel
        // exited: a kernel that has exited is not offline.
        if self.status == models::Status::Exited
            && matches!(
                status,
                models::Status::Idle | models::Status::Busy | models::Status::Offline
            )
        {
            log::debug!(
                "[session {}] Ignoring status '{}' => '{}'; kernel has already exited",
                self.session_id,
                self.status,
                status
            );
            return;
        }

        // Going offline, remember the status to come back to. Any other
        // change of status says more about the kernel than that does.
        self.status_before_offline = if status == models::Status::Offline {
            Some(self.status)
        } else {
            None
        };

        self.status = status;

        // When exiting ...
        if status == models::Status::Exited {
            // ... clear the execution queue
            self.execution_queue.clear();
            // ... clear the process ID (no longer running)
            self.process_id = None;
        }

        // When idle, record the time at which the kernel became idle
        if status == models::Status::Idle
            || status == models::Status::Ready
            || status == models::Status::Exited
        {
            self.idle_since = Some(std::time::Instant::now());
            self.nudge_idle().await;
        } else {
            self.idle_since = None;
        }

        // When busy, record the time at which the kernel became busy
        if status == models::Status::Busy {
            self.busy_since = Some(std::time::Instant::now());
        } else {
            self.busy_since = None;
        }

        // Publish the new status to the status stream (for internal use)
        let update = StatusUpdate {
            status: self.status.clone(),
            reason,
        };

        let status_message = WebsocketMessage::Kernel(KernelMessage::Status(update.clone()));
        self.ws_json_tx.send(status_message).await.unwrap();

        // When the kernel becomes idle after executing code, poll the working
        // directory to see if it's changed.
        #[cfg(not(target_os = "windows"))]
        if status == models::Status::Idle {
            if let Some(reason) = update.reason {
                if reason == "execute_request" {
                    self.poll_working_dir().await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use models::Status;

    /// A kernel state in the given status, with the receiving ends of its
    /// channels (which have to outlive it).
    async fn state_in(
        status: Status,
    ) -> (
        KernelState,
        mpsc::Receiver<Option<u32>>,
        async_channel::Receiver<WebsocketMessage>,
    ) {
        let session = models::NewSession {
            session_id: "kernel-state-test".to_string(),
            display_name: "Test".to_string(),
            language: "test".to_string(),
            username: "test".to_string(),
            input_prompt: ">".to_string(),
            continuation_prompt: "+".to_string(),
            notebook_uri: None,
            workspace_id: None,
            session_mode: models::SessionMode::Console,
            argv: vec![],
            working_directory: String::new(),
            env: vec![],
            connection_timeout: None,
            interrupt_mode: models::InterruptMode::Message,
            protocol_version: None,
            startup_environment: models::StartupEnvironment::None,
            startup_environment_arg: None,
        };
        let (idle_nudge_tx, idle_nudge_rx) = mpsc::channel(64);
        let (ws_json_tx, ws_json_rx) = async_channel::unbounded();
        let mut state = KernelState::new(session, String::new(), idle_nudge_tx, ws_json_tx);
        state.set_status(status, None).await;
        (state, idle_nudge_rx, ws_json_rx)
    }

    #[tokio::test]
    async fn a_restart_is_in_progress_until_its_claim_is_dropped() {
        let (mut state, _nudges, _messages) = state_in(Status::Idle).await;
        assert!(!state.restarting());

        let claim = state.begin_restart();
        assert!(claim.is_some());
        assert!(state.restarting());
        assert!(
            state.begin_restart().is_none(),
            "a second restart cannot begin while the first is in progress"
        );

        // The restart ends, or its client stops waiting for it
        drop(claim);
        assert!(!state.restarting());
        assert!(state.begin_restart().is_some());
    }

    #[tokio::test]
    async fn a_client_whose_connection_ends_leaves_the_session_disconnected() {
        let (mut state, _nudges, _messages) = state_in(Status::Idle).await;
        state
            .client_connected("first", Some("/a/socket".to_string()))
            .await;
        assert!(state.connected);
        assert_eq!(state.client_socket_path.as_deref(), Some("/a/socket"));

        state.client_disconnected("first");
        assert!(!state.connected);
        assert_eq!(state.client_socket_path, None);
    }

    #[tokio::test]
    async fn a_replaced_client_whose_connection_ends_leaves_the_session_connected() {
        let (mut state, _nudges, _messages) = state_in(Status::Idle).await;
        state.client_connected("first", None).await;
        state
            .client_connected("second", Some("/a/socket".to_string()))
            .await;

        // The first client's connection ends after the second took its place
        state.client_disconnected("first");
        assert!(state.connected);
        assert_eq!(state.client_socket_path.as_deref(), Some("/a/socket"));

        state.client_disconnected("second");
        assert!(!state.connected);
        assert_eq!(state.client_socket_path, None);
    }

    #[tokio::test]
    async fn a_kernel_back_from_offline_has_the_status_it_had() {
        let (mut state, _nudges, _messages) = state_in(Status::Busy).await;
        state.set_status(Status::Offline, None).await;
        state.restore_status_after_offline().await;
        assert_eq!(state.status, Status::Busy);
    }

    #[tokio::test]
    async fn a_status_reported_while_offline_is_kept() {
        let (mut state, _nudges, _messages) = state_in(Status::Busy).await;
        state.set_status(Status::Offline, None).await;
        state.set_status(Status::Idle, None).await;
        state.restore_status_after_offline().await;
        assert_eq!(state.status, Status::Idle);

        // ... and what was remembered for that outage is not used for the next
        state.set_status(Status::Starting, None).await;
        state.set_status(Status::Offline, None).await;
        state.restore_status_after_offline().await;
        assert_eq!(state.status, Status::Starting);
    }

    #[tokio::test]
    async fn a_first_answer_while_offline_ends_the_start() {
        let (mut state, _nudges, _messages) = state_in(Status::Starting).await;
        state.set_status(Status::Offline, None).await;
        state.first_answer_while_offline();
        assert_eq!(state.status, Status::Offline);
        state.restore_status_after_offline().await;
        assert_eq!(state.status, Status::Idle);
    }

    #[tokio::test]
    async fn a_first_answer_while_offline_leaves_a_busy_kernel_busy() {
        let (mut state, _nudges, _messages) = state_in(Status::Busy).await;
        state.set_status(Status::Offline, None).await;
        state.first_answer_while_offline();
        state.restore_status_after_offline().await;
        assert_eq!(state.status, Status::Busy);
    }

    #[tokio::test]
    async fn a_kernel_that_has_exited_is_not_marked_offline() {
        let (mut state, _nudges, _messages) = state_in(Status::Exited).await;
        state.set_status(Status::Offline, None).await;
        assert_eq!(state.status, Status::Exited);
        state.restore_status_after_offline().await;
        assert_eq!(state.status, Status::Exited);
    }
}
