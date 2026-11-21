//
// mod.rs
//
// Copyright (C) 2024-2025 Posit Software, PBC. All rights reserved.
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//
//

//! Wraps Jupyter kernel sessions.

mod connection;
mod environment;
mod execute;
mod handshake;
pub(crate) mod job_object;
mod lifecycle;
mod process;
mod shell_wrapper;
mod startup;
mod utils;

use std::sync::{Arc, Weak};

use async_channel::{Receiver, Sender};
use chrono::{DateTime, Utc};
use event_listener::Event;
use kallichore_api::models::{self, StartupError};
use kcshared::{jupyter_message::JupyterMessage, websocket_message::WebsocketMessage};
use tokio::sync::RwLock;

use crate::{
    connection_file::{ConnectionFile, PortReservation},
    execution_history::RECENT_ENTRIES,
    kernel_connection::KernelConnection,
    kernel_state::KernelState,
    mcp::McpState,
    registration_socket::RegistrationSocket,
    startup_status::StartupStatus,
};

use connection::ConnectionManager;
#[cfg(windows)]
use environment::create_interrupt_event;
use handshake::HandshakeCoordinator;
use lifecycle::LifecycleManager;
use process::ProcessMonitor;
use startup::StartupCoordinator;

// Re-export utility functions for external use
pub use execute::{ExecuteError, ExecuteOptions};
pub use utils::make_message_id;

/// How long a start that has failed waits for the process it launched to be
/// stopped before it reports the failure anyway.
const FAILED_START_STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// A Jupyter kernel session.
///
/// This object represents an instance of Jupyter kernel. It consists of only
/// immutable state so that it can safely be cloned; all mutable kernel state is
/// stored in the `KernelState` object.
#[derive(Debug, Clone)]
pub struct KernelSession {
    /// Metadata about the session
    pub connection: KernelConnection,

    /// The session model that was used to create this session
    pub model: models::NewSession,

    /// The interrupt event handle, if we have one. Only used on Windows.
    #[cfg(windows)]
    pub interrupt_event_handle: Option<isize>,

    /// The command line arguments used to start the kernel. The first is the
    /// path to the kernel itself.
    pub argv: Vec<String>,

    /// The current state of the kernel
    pub state: Arc<RwLock<KernelState>>,

    /// The current set of reserved ports for all kernels
    pub reserved_ports: Arc<std::sync::RwLock<Vec<i32>>>,

    /// The date and time the kernel was started
    pub started: DateTime<Utc>,

    /// The channel to send JSON messages to the WebSocket
    pub ws_json_tx: Sender<WebsocketMessage>,

    /// The channel to receive JSON messages from the WebSocket
    pub ws_json_rx: Receiver<WebsocketMessage>,

    /// The channel to send ZMQ messages to the kernel
    pub ws_zmq_tx: Sender<JupyterMessage>,

    /// The channel to receive ZMQ messages from the kernel
    pub ws_zmq_rx: Receiver<JupyterMessage>,

    /// The exit event; fires when the kernel process exits
    pub exit_event: Arc<Event>,

    /// The MCP server, whose endpoint the kernel is given when the session
    /// belongs to a registered workspace. Weak because the MCP server holds
    /// the sessions.
    pub mcp: Weak<McpState>,
}

impl KernelSession {
    /// Create a new kernel session.
    pub async fn new(
        session: models::NewSession,
        key: String,
        idle_nudge_tx: tokio::sync::mpsc::Sender<Option<u32>>,
        reserved_ports: Arc<std::sync::RwLock<Vec<i32>>>,
        mcp: Weak<McpState>,
    ) -> Result<Self, anyhow::Error> {
        let (zmq_tx, zmq_rx) = async_channel::unbounded::<JupyterMessage>();
        let (json_tx, json_rx) = async_channel::unbounded::<WebsocketMessage>();
        let kernel_state = Arc::new(RwLock::new(KernelState::new(
            session.clone(),
            session.working_directory.clone(),
            idle_nudge_tx,
            json_tx.clone(),
        )));

        let connection = KernelConnection::from_session(&session, key.clone())?;
        let started = Utc::now();

        // On Windows, if the interrupt mode is Signal, create an event for
        // interruptions since Windows doesn't have signals.
        #[cfg(windows)]
        let interrupt_event_handle = match session.interrupt_mode {
            models::InterruptMode::Signal => match create_interrupt_event() {
                Ok(event) => Some(event),
                Err(e) => {
                    log::error!("Failed to create interrupt event: {}", e);
                    None
                }
            },
            models::InterruptMode::Message => None,
        };

        let kernel_session = KernelSession {
            argv: session.argv.clone(),
            state: kernel_state.clone(),
            ws_json_tx: json_tx.clone(),
            model: session,
            ws_json_rx: json_rx,
            ws_zmq_tx: zmq_tx,
            ws_zmq_rx: zmq_rx,
            connection,
            started,
            #[cfg(windows)]
            interrupt_event_handle,
            exit_event: Arc::new(Event::new()),
            reserved_ports,
            mcp,
        };
        Ok(kernel_session)
    }

    /// Start the kernel.
    ///
    /// # Returns
    ///
    /// The kernel info, as a JSON object.
    pub async fn start(&self) -> Result<serde_json::Value, StartupError> {
        self.start_once(false).await
    }

    /// Start the kernel, once: the session is claimed first (`claim_start`),
    /// and a start that is refused there has launched nothing and stops
    /// nothing; the kernel that is running, or on its way, is left alone.
    async fn start_once(&self, for_restart: bool) -> Result<serde_json::Value, StartupError> {
        let claim = {
            let mut state = self.state.write().await;
            state.claim_start("start API called", for_restart).await
        };
        if let Err(reason) = claim {
            let err = crate::error::KSError::StartFailed(anyhow::anyhow!("{}", reason));
            err.log();
            return Err(StartupError {
                exit_code: None,
                output: None,
                error: err.to_json(None),
            });
        }

        // The task that watches the kernel's process holds the receiving end,
        // so that a start that fails can have the process it launched stopped.
        let (stop_tx, stop_rx) = async_channel::bounded::<()>(1);

        let result = self.launch(stop_rx).await;
        if result.is_err() {
            self.stop_failed_start(stop_tx).await;
        }
        result
    }

    /// Stop the process of a kernel whose start has failed, and wait for the
    /// session to be marked as exited.
    ///
    /// A start can fail while the process it launched is still running: the
    /// kernel did not connect, or did not complete its handshake, in time.
    /// Nothing is connected to that process, so it is stopped before the
    /// failure is reported, and the session is left as one that has exited.
    async fn stop_failed_start(&self, stop_tx: Sender<()>) {
        // Listen before reading the status, so an exit between the two is seen
        let exited = self.exit_event.listen();

        // Already exited: the process ended by itself, or the start failed
        // before it launched one
        if self.state.read().await.status == models::Status::Exited {
            return;
        }

        // Nobody is listening if the start was refused before it began
        if stop_tx.send(()).await.is_err() {
            return;
        }

        if tokio::time::timeout(FAILED_START_STOP_TIMEOUT, exited)
            .await
            .is_err()
        {
            log::warn!(
                "[session {}] The kernel's process was still running {} seconds after its failed start asked for it to be stopped",
                self.connection.session_id,
                FAILED_START_STOP_TIMEOUT.as_secs()
            );
        }
    }

    /// Launch the kernel's process and connect to it.
    async fn launch(&self, stop_rx: Receiver<()>) -> Result<serde_json::Value, StartupError> {
        // Create the startup coordinator
        let mut coordinator = StartupCoordinator {
            session_id: self.connection.session_id.clone(),
            model: self.model.clone(),
            state: self.state.clone(),
            mcp: self.mcp.clone(),
            argv: self.argv.clone(),
            #[cfg(windows)]
            interrupt_event_handle: self.interrupt_event_handle,
            shell_used: None,
            startup_command: None,
        };

        // Validate that the session can be started. The session is claimed
        // (`Starting`) and has no process: one that cannot be started is
        // left as one that has exited, which can be started again or deleted.
        if let Err(e) = coordinator.validate_startup() {
            let mut state = self.state.write().await;
            state
                .set_status(
                    models::Status::Exited,
                    Some(String::from(
                        "the start failed before the kernel was launched",
                    )),
                )
                .await;
            return Err(e);
        }

        let working_directory = self.state.read().await.working_directory.clone();

        // Check if we expect JEP 66 handshaking based on protocol version
        let jep66_enabled =
            ConnectionFile::requires_handshaking(&self.connection.protocol_version.clone());

        // Launch the kernel's process. When that fails there is no process
        // whose exit would take the session out of the starting state, so
        // this does: a session left as starting can not be deleted, and reads
        // as one whose kernel is on its way.
        let (child, registration, port_reservation) = match self
            .launch_process(&mut coordinator, &working_directory, jep66_enabled)
            .await
        {
            Ok(launched) => launched,
            Err(e) => {
                let mut state = self.state.write().await;
                if state.process_id.is_none() {
                    state
                        .set_status(
                            models::Status::Exited,
                            Some(String::from(
                                "the start failed before the kernel was launched",
                            )),
                        )
                        .await;
                }
                return Err(e);
            }
        };

        // Create a channel to receive startup status
        let (startup_tx, startup_rx) = async_channel::unbounded::<StartupStatus>();

        // Spawn a task to monitor the child process
        let kernel = self.clone();
        let startup_child_tx = startup_tx.clone();
        tokio::spawn(async move {
            let monitor = ProcessMonitor::new(
                kernel.connection.session_id.clone(),
                kernel.state.clone(),
                kernel.exit_event.clone(),
                kernel.ws_json_tx.clone(),
            );
            let consume_output = || kernel.consume_output_streams();
            monitor
                .run_child(child, startup_child_tx, stop_rx, consume_output)
                .await;
        });

        // If JEP 66 is enabled, wait for the handshake
        if let Some(registration_socket) = registration {
            log::info!(
                "[session {}] Waiting for JEP 66 handshake",
                self.connection.session_id
            );

            let connection_timeout = self.model.connection_timeout.unwrap_or(30) as u64;

            let handshake_coord = HandshakeCoordinator::new(
                self.connection.session_id.clone(),
                self.connection.clone(),
                self.ws_json_tx.clone(),
                coordinator.shell_used.clone(),
                coordinator.startup_command.clone(),
            );

            match handshake_coord
                .wait_for_handshake(registration_socket, startup_rx.clone(), connection_timeout)
                .await
            {
                Ok(connection_file) => {
                    log::info!(
                        "[session {}] JEP 66 handshake completed successfully",
                        self.connection.session_id
                    );
                    let conn_mgr = self.connection_manager();
                    conn_mgr.update_connection_file(connection_file).await;
                }
                Err(startup_error) => {
                    log::error!(
                        "[session {}] JEP 66 handshake failed: {:?}",
                        self.connection.session_id,
                        startup_error
                    );
                    return Err(startup_error);
                }
            }
        }

        // Start the ZeroMQ proxy
        let kernel = self.clone();
        let conn_mgr = self.connection_manager();
        let connection_file = match conn_mgr.get_connection_file().await {
            Some(cf) => cf,
            None => {
                log::error!(
                    "[session {}] Failed to get connection information!",
                    self.connection.session_id
                );
                return Err(StartupError {
                    exit_code: None,
                    output: None,
                    error: crate::error::KSError::ProcessStartFailed(anyhow::anyhow!(
                        "Failed to get connection file for ZeroMQ proxy"
                    ))
                    .to_json(None),
                });
            }
        };

        // A kernel that was given its ports in a connection file may not
        // have bound them yet; one that did the handshake reported ports it
        // had bound
        let wait_for_ports = port_reservation.is_some();

        let startup_proxy_tx = startup_tx.clone();
        tokio::spawn(async move {
            let connection = kernel.connection.clone();
            kernel
                .start_zmq_proxy(
                    connection_file,
                    connection,
                    startup_proxy_tx,
                    None,
                    wait_for_ports,
                )
                .await;

            // The proxy for this kernel has ended, whichever way it ended:
            // the ports picked for the kernel are no longer kept from others
            drop(port_reservation);
        });

        // Wait for the kernel to connect
        log::trace!(
            "[session {}] Waiting for kernel sockets to connect",
            self.connection.session_id
        );
        let startup_result = startup_rx.recv().await;
        log::trace!("[session {}] Waiting complete", self.connection.session_id);

        let result = coordinator.process_startup_result(startup_result)?;

        // Handle kernel info
        coordinator.handle_kernel_info(&result).await;

        Ok(result)
    }

    /// Everything a start does up to and including the launch of the
    /// kernel's process: the connection or registration file, the
    /// environment, the command.
    ///
    /// Returns the process, and one of two things: for a kernel that
    /// performs the JEP 66 handshake, the socket it will register on; for
    /// any other, the reservation of the ports picked for it. When this
    /// fails after the ports were picked, they are released.
    async fn launch_process(
        &self,
        coordinator: &mut StartupCoordinator,
        working_directory: &str,
        jep66_enabled: bool,
    ) -> Result<
        (
            tokio::process::Child,
            Option<RegistrationSocket>,
            Option<PortReservation>,
        ),
        StartupError,
    > {
        // Set up connection or registration file
        let (connection_file_path, registration, port_reservation) = if jep66_enabled {
            log::info!(
                "[session {}] Kernel supports JEP 66 (protocol version {}) - creating registration socket for handshake",
                self.connection.session_id,
                self.connection.protocol_version
            );

            // Set up JEP 66 handshaking
            let handshake_coord = HandshakeCoordinator::new(
                self.connection.session_id.clone(),
                self.connection.clone(),
                self.ws_json_tx.clone(),
                None, // shell_used not yet determined
                None, // startup_command not yet determined
            );

            let (path, socket) = handshake_coord.setup_registration_socket().await?;
            (path, Some(socket), None)
        } else {
            // Traditional kernel - generate connection file
            let (connection_file, port_reservation) = ConnectionFile::generate(
                "127.0.0.1".to_string(),
                self.reserved_ports.clone(),
                self.connection.key.clone(),
            )
            .map_err(|e| StartupError {
                exit_code: None,
                output: None,
                error: crate::error::KSError::ProcessStartFailed(anyhow::anyhow!(
                    "Failed to generate connection file: {}",
                    e
                ))
                .to_json(None),
            })?;

            // Store the connection file
            let conn_mgr = self.connection_manager();
            conn_mgr
                .update_connection_file(connection_file.clone())
                .await;

            // Write to disk
            let path = coordinator.write_connection_file(&connection_file, "connection")?;
            (path, None, Some(port_reservation))
        };

        // Substitute connection file path in arguments
        let argv = coordinator.substitute_connection_file(&self.argv, &connection_file_path);

        log::debug!(
            "Starting kernel for session {}: {:?}",
            self.model.session_id,
            argv
        );

        // Resolve environment variables
        let resolved_env = coordinator.resolve_environment().await?;

        // Build the command
        let mut cmd = coordinator
            .build_command(&argv, &resolved_env, working_directory)
            .await?;

        // Create the process monitor
        let process_monitor = ProcessMonitor::new(
            self.connection.session_id.clone(),
            self.state.clone(),
            self.exit_event.clone(),
            self.ws_json_tx.clone(),
        );

        // Spawn the kernel process
        let child = coordinator
            .spawn_kernel_process(&mut cmd, &resolved_env, &process_monitor)
            .await?;

        Ok((child, registration, port_reservation))
    }

    /// Shutdown the kernel.
    pub async fn shutdown(&self) -> Result<(), anyhow::Error> {
        let lifecycle = self.lifecycle_manager();
        lifecycle.shutdown().await
    }

    /// Restart the kernel.
    pub async fn restart(
        &self,
        working_directory: Option<String>,
        env: Option<Vec<models::VarAction>>,
    ) -> Result<(), StartupError> {
        let lifecycle = self.lifecycle_manager();

        // Listen for the kernel's exit before anything is asked of it: an
        // exit that came before the listener was there would not be heard,
        // and the restart would wait for good.
        let exited = self.exit_event.listen();
        let claim = lifecycle.restart(working_directory, env).await?;

        // Wait for the kernel to exit. This wait belongs to the request: if
        // the client stops waiting, the restart is over with it (the claim
        // is dropped), and the kernel, which has been asked to shut down, is
        // not started again behind the client's back whenever it does exit.
        exited.await;

        // Start the kernel again, in a task of its own: a start is not left
        // half done if the client stops waiting now. The session counts as
        // restarting until the start is over.
        let session = self.clone();
        let start = tokio::spawn(async move {
            let _claim = claim;
            session.start_once(true).await
        });
        match start.await {
            Ok(Ok(_)) => {
                log::debug!(
                    "[session {}] Kernel restarted successfully",
                    self.connection.session_id
                );
                Ok(())
            }
            Ok(Err(e)) => {
                log::error!(
                    "[session {}] Failed to restart kernel: {}",
                    self.connection.session_id,
                    e.error.message
                );
                Err(e)
            }
            Err(e) => {
                let err = crate::error::KSError::RestartFailed(anyhow::anyhow!(
                    "The start of the kernel did not run to its end: {}",
                    e
                ));
                err.log();
                Err(StartupError {
                    exit_code: None,
                    output: None,
                    error: err.to_json(None),
                })
            }
        }
    }

    /// Interrupt the kernel.
    pub async fn interrupt(&self) -> Result<(), anyhow::Error> {
        let lifecycle = self.lifecycle_manager();
        lifecycle
            .interrupt(
                self.model.interrupt_mode.clone(),
                #[cfg(windows)]
                self.interrupt_event_handle,
            )
            .await
    }

    /// Connect to an existing kernel.
    ///
    /// This is used when adopting a kernel that is already running.
    pub async fn connect(
        &self,
        connection_file: ConnectionFile,
    ) -> Result<serde_json::Value, crate::error::KSError> {
        use crate::error::KSError;

        // Claim the session before anything of it is changed: an adoption
        // into a session that has a kernel, or one on its way, is refused
        let claim = {
            let mut state = self.state.write().await;
            state.claim_start("adopting a running kernel", false).await
        };
        if let Err(reason) = claim {
            return Err(KSError::SessionConnectionFailed(anyhow::anyhow!(
                "{}", reason
            )));
        }

        // Store the connection file
        let conn_mgr = self.connection_manager();
        conn_mgr
            .update_connection_file(connection_file.clone())
            .await;

        // The kernel we're adopting is already running, signing and
        // validating messages with its own key -- the one in
        // `connection_file.info.key`, taken from the `ConnectionInfo` the
        // adopting client supplied. That's not the same key this session was
        // created with (`self.connection`'s key is generated locally when a
        // new session is created, before an adopted kernel's real key is
        // known), so it must be used in place of the session's own key for
        // all further communication with this kernel.
        let connection = match self.connection.with_key(&connection_file.info.key) {
            Ok(connection) => connection,
            Err(e) => {
                // Claimed and connected to nothing: a session that can be
                // adopted again, started, or deleted
                let mut state = self.state.write().await;
                state
                    .set_status(
                        models::Status::Exited,
                        Some(String::from("the adoption failed before it connected")),
                    )
                    .await;
                return Err(KSError::SessionConnectionFailed(e));
            }
        };

        // Create a channel to receive startup status
        let (startup_tx, startup_rx) = async_channel::unbounded::<StartupStatus>();

        // Held for as long as this call waits for the adoption. A kernel that
        // is busy gives its first answer only when it is done, which can be
        // long after the caller has stopped waiting; when this call is
        // dropped so is the sender, and the proxy that is still waiting for
        // that answer gives the adoption up (see `start_zmq_proxy`).
        let (waiting_tx, waiting_rx) = tokio::sync::oneshot::channel::<()>();

        // Start the ZeroMQ proxy
        let kernel = self.clone();
        tokio::spawn(async move {
            log::debug!(
                "[session {}] Starting ZeroMQ proxy for adopted kernel",
                kernel.connection.session_id
            );

            kernel
                .start_zmq_proxy(
                    connection_file,
                    connection,
                    startup_tx,
                    Some(waiting_rx),
                    false,
                )
                .await;

            log::debug!(
                "[session {}] ZeroMQ proxy for adopted kernel has exited",
                kernel.connection.session_id
            );

            // Mark kernel as exited
            {
                let mut state = kernel.state.write().await;
                kernel.exit_event.notify(usize::MAX);
                state
                    .set_status(
                        models::Status::Exited,
                        Some(String::from(
                            "all sockets disconnected from an adopted kernel",
                        )),
                    )
                    .await;
            }

            // Fire the exit event
            let event = kcshared::kernel_message::KernelMessage::Exited(0);
            let _ = kernel
                .ws_json_tx
                .send(WebsocketMessage::Kernel(event))
                .await;
        });

        // Wait for the proxy to connect
        let startup_result = startup_rx.recv().await;
        drop(waiting_tx);
        match startup_result {
            Ok(StartupStatus::Connected(kernel_info)) => {
                log::trace!(
                    "[session {}] Kernel sockets connected successfully; kernel successfully adopted",
                    self.connection.session_id
                );
                // Save the kernel info. The adopted kernel has answered: the
                // session goes from `Starting` (the adoption's claim) to idle
                {
                    let mut state = self.state.write().await;
                    state.set_kernel_info(kernel_info.clone());
                    state.first_answer("adopted kernel answered").await;
                }
                Ok(kernel_info)
            }
            Ok(StartupStatus::ConnectionFailed(_, e)) => {
                log::error!(
                    "[session {}] Failed to connect to adopted kernel: {}",
                    self.connection.session_id,
                    e
                );
                Err(e)
            }
            Ok(StartupStatus::AbnormalExit(_, _, e)) => {
                log::error!(
                    "[session {}] Unexpected exit from adopted kernel: {}",
                    self.connection.session_id,
                    e
                );
                Err(e)
            }
            Err(e) => {
                log::error!(
                    "[session {}] Failed to connect to adopted kernel: {}",
                    self.connection.session_id,
                    e
                );
                Err(KSError::SessionConnectionFailed(anyhow::anyhow!("{}", e)))
            }
        }
    }

    /// Format this session as an active session.
    pub async fn as_active_session(&self) -> models::ActiveSession {
        let state = self.state.read().await;

        // Compute idle and busy times
        let idle_seconds = match state.idle_since {
            Some(instant) => instant.elapsed().as_secs() as i32,
            None => 0,
        };

        let busy_seconds = match state.busy_since {
            Some(instant) => instant.elapsed().as_secs() as i32,
            None => 0,
        };

        models::ActiveSession {
            session_id: self.connection.session_id.clone(),
            session_mode: self.model.session_mode.clone(),
            username: self.connection.username.clone(),
            display_name: self.model.display_name.clone(),
            language: self.model.language.clone(),
            interrupt_mode: self.model.interrupt_mode.clone(),
            initial_env: Some(state.resolved_env.clone()),
            argv: self.argv.clone(),
            process_id: state.process_id.map(|pid| pid as i32),
            input_prompt: state.input_prompt.clone(),
            resource_usage: state.resource_usage.clone(),
            idle_seconds,
            busy_seconds,
            continuation_prompt: state.continuation_prompt.clone(),
            connected: state.connected,
            working_directory: state.working_directory.clone(),
            notebook_uri: self.model.notebook_uri.clone(),
            started: self.started,
            status: state.status,
            execution_queue: state.execution_queue.to_json(),
            history: Some(state.history.recent(RECENT_ENTRIES)),
            socket_path: state.client_socket_path.clone(),
            kernel_info: state.kernel_info.clone().unwrap_or(serde_json::json!({})),
        }
    }

    /// Ensure a connection file exists, generating one if necessary.
    pub async fn ensure_connection_file(&self) -> Result<ConnectionFile, anyhow::Error> {
        let conn_mgr = self.connection_manager();
        conn_mgr.ensure_connection_file().await
    }

    // Helper methods to create managers

    fn connection_manager(&self) -> ConnectionManager {
        ConnectionManager::new(
            self.connection.session_id.clone(),
            self.state.clone(),
            self.reserved_ports.clone(),
            self.connection.key.clone(),
        )
    }

    fn lifecycle_manager(&self) -> LifecycleManager {
        LifecycleManager::new(
            self.connection.session_id.clone(),
            self.state.clone(),
            self.ws_zmq_tx.clone(),
            self.model.working_directory.clone(),
        )
    }

    /// Start the ZeroMQ proxy for this kernel session.
    ///
    /// `connection` carries the signing key to use for this proxy's
    /// lifetime. It's usually `self.connection`, but adopted kernels (see
    /// `connect`) sign with a key that isn't known until the adopting client
    /// supplies it, so callers pass a corrected connection in that case.
    ///
    /// `adoption_waiter` is given for a kernel that is being adopted, and is
    /// `None` for one this session launched. It resolves when nobody waits
    /// for the adoption any more; a proxy that has not had the kernel's first
    /// answer by then ends, and closes its sockets, without anything being
    /// sent to the kernel. A proxy left waiting would go on to forward
    /// messages for an adoption its caller has written off.
    ///
    /// `wait_for_ports` is passed to `ZmqWsProxy::connect`: whether the
    /// kernel may not have bound its ports yet.
    async fn start_zmq_proxy(
        &self,
        connection_file: ConnectionFile,
        connection: KernelConnection,
        status_tx: Sender<StartupStatus>,
        adoption_waiter: Option<tokio::sync::oneshot::Receiver<()>>,
        wait_for_ports: bool,
    ) {
        use crate::{error::KSError, zmq_ws_proxy::ZmqWsProxy};

        let launched = adoption_waiter.is_none();

        let mut proxy = ZmqWsProxy::new(
            connection_file,
            connection,
            self.state.clone(),
            self.ws_json_tx.clone(),
            self.ws_zmq_rx.clone(),
            self.exit_event.clone(),
        );

        // Listen for the kernel's exit, and for the loss of its heartbeat,
        // from before anything is connected, and keep these same listeners
        // until the proxy ends. A listener made later, or made anew for each
        // wait, is not there in between and misses an event that comes then.
        let mut exited = self.exit_event.listen();
        let mut disconnected = proxy.disconnected_event.listen();

        // The kernel this start launched may have exited before the listener
        // above was there. (Not asked of an adoption: the session a kernel is
        // adopted into has no process of its own to have exited.)
        if launched && self.state.read().await.status == models::Status::Exited {
            return;
        }

        let abandoned = async {
            match adoption_waiter {
                Some(waiter) => {
                    let _ = waiter.await;
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(abandoned);

        // Wait for either the proxy to connect or for the session to exit
        let connect_or_exit = async {
            tokio::select! {
                result = proxy.connect(wait_for_ports) => {
                    match result {
                        Ok(_) => Ok(()),
                        Err(e) => Err(KSError::SessionConnectionFailed(e)),
                    }
                },
                _ = &mut exited => {
                    Err(KSError::ExitedBeforeConnection)
                }
            }
        };

        let connection_timeout = self.model.connection_timeout.unwrap_or(30) as u64;

        let connected = tokio::select! {
            connected = tokio::time::timeout(
                std::time::Duration::new(connection_timeout, 0),
                connect_or_exit,
            ) => connected,
            _ = &mut abandoned => {
                log::info!(
                    "[session {}] Nobody is waiting for this adoption any more; giving it up",
                    self.connection.session_id
                );
                return;
            }
        };

        match connected {
            Ok(Ok(())) => {
                // Get kernel info. The wait for it has no time limit, so it
                // has to end when there is nothing left to wait for: the
                // kernel has exited (the task that watches its process tells
                // the start), its heartbeat is lost, or the adoption has been
                // abandoned.
                let kernel_info = tokio::select! {
                    info = proxy.get_kernel_info() => info,
                    _ = &mut exited => {
                        log::debug!(
                            "[session {}] The kernel exited before its first answer; ending its proxy",
                            self.connection.session_id
                        );
                        return;
                    }
                    _ = &mut disconnected => Err(anyhow::anyhow!(
                        "The kernel's heartbeat was lost before the kernel gave its first answer"
                    )),
                    _ = &mut abandoned => {
                        log::info!(
                            "[session {}] Nobody is waiting for this adoption any more; giving it up",
                            self.connection.session_id
                        );
                        return;
                    }
                };
                match kernel_info {
                    Ok(info) => {
                        let taken = status_tx.send(StartupStatus::Connected(info)).await;
                        if taken.is_err() && !launched {
                            // The caller went away as the answer came
                            return;
                        }
                    }
                    Err(e) => {
                        let error = KSError::SessionConnectionFailed(e);
                        error.log();
                        let output = self.consume_output_streams();
                        let _ = status_tx
                            .send(StartupStatus::ConnectionFailed(output, error))
                            .await;
                        return;
                    }
                }
            }
            Ok(Err(e)) => {
                e.log();
                let output = self.consume_output_streams();
                if matches!(e, KSError::SessionConnectionFailed(_)) {
                    let _ = status_tx
                        .send(StartupStatus::ConnectionFailed(output, e))
                        .await;
                }
                return;
            }
            Err(_) => {
                let error = KSError::SessionConnectionTimeout(connection_timeout as u32);
                error.log();
                let output = self.consume_output_streams();
                let _ = status_tx
                    .send(StartupStatus::ConnectionFailed(output, error))
                    .await;
                return;
            }
        }

        // Listen for messages
        match proxy.listen(exited, disconnected).await {
            Ok(_) => (),
            Err(e) => {
                let error = KSError::ZmqProxyError(e);
                error.log();
            }
        }
    }

    /// Collect any standard out and standard error messages that were sent
    /// to the websocket during startup but haven't been delivered to the
    /// client.
    fn consume_output_streams(&self) -> String {
        use kcshared::kernel_message::KernelMessage;

        let mut output = String::new();
        while let Ok(msg) = self.ws_json_rx.try_recv() {
            if let WebsocketMessage::Kernel(KernelMessage::Output(_, text)) = msg {
                output.push_str(&text);
            }
        }
        output
    }
}
