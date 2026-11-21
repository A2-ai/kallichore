//
// lifecycle.rs
//
// Copyright (C) 2024-2025 Posit Software, PBC. All rights reserved.
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//
//

//! Kernel lifecycle management (restart, shutdown, interrupt).

use async_channel::{SendError, Sender};
use kallichore_api::models::{self, StartupError, VarAction};
use kcshared::jupyter_message::{JupyterChannel, JupyterMessage, JupyterMessageHeader};
use std::{fs, sync::Arc};
use tokio::sync::RwLock;

use crate::{
    error::KSError,
    kernel_state::{KernelState, RestartClaim},
    working_dir::expand_path,
};

use super::utils::make_message_id;

/// The error for a restart that is refused before anything is asked of the
/// kernel.
fn refused(reason: &str) -> StartupError {
    let err = KSError::RestartFailed(anyhow::anyhow!("{}", reason));
    err.log();
    StartupError {
        exit_code: None,
        output: None,
        error: err.to_json(None),
    }
}

/// Manages kernel lifecycle operations.
pub struct LifecycleManager {
    /// Session ID for logging
    session_id: String,

    /// Shared kernel state
    state: Arc<RwLock<KernelState>>,

    /// Channel to send ZMQ messages to the kernel
    ws_zmq_tx: Sender<JupyterMessage>,

    /// Original working directory from the model
    #[allow(dead_code)]
    original_working_directory: String,
}

impl LifecycleManager {
    /// Create a new lifecycle manager.
    pub fn new(
        session_id: String,
        state: Arc<RwLock<KernelState>>,
        ws_zmq_tx: Sender<JupyterMessage>,
        original_working_directory: String,
    ) -> Self {
        Self {
            session_id,
            state,
            ws_zmq_tx,
            original_working_directory,
        }
    }

    /// Shutdown the kernel.
    pub async fn shutdown(&self) -> Result<(), anyhow::Error> {
        self.shutdown_request(false).await?;
        Ok(())
    }

    /// Send a shutdown request to the kernel.
    async fn shutdown_request(&self, restart: bool) -> Result<(), SendError<JupyterMessage>> {
        let msg = JupyterMessage {
            header: JupyterMessageHeader {
                msg_id: make_message_id(),
                msg_type: "shutdown_request".to_string(),
            },
            parent_header: None,
            metadata: serde_json::json!({}),
            content: serde_json::json!({
                "restart": restart,
            }),
            channel: JupyterChannel::Control,
            buffers: vec![],
        };

        self.ws_zmq_tx.send(msg).await
    }

    /// Restart the kernel.
    ///
    /// # Arguments
    ///
    /// * `working_directory` - The working directory to use after restart.
    /// Optional; if not supplied, the working directory supplied when the
    /// kernel was started will be used (Windows) or the kernel's current
    /// working directory will be used (non-Windows).
    ///
    /// * `env` - The new set of environment variable actions to apply to
    /// the kernel's environment.
    ///
    /// # Returns
    ///
    /// The claim of the restart that has begun, once the kernel has been
    /// asked to shut down; the caller keeps it until the restart is over. An
    /// error if the kernel cannot be restarted: a restart is in progress
    /// already, or the kernel has exited, so that there is nothing to shut
    /// down and no exit to wait for.
    pub async fn restart(
        &self,
        working_directory: Option<String>,
        env: Option<Vec<VarAction>>,
    ) -> Result<RestartClaim, StartupError> {
        // Expand the working directory if it was supplied
        let working_directory = match working_directory {
            Some(dir) => match expand_path(dir.clone()) {
                Ok(dir) => Some(dir.to_string_lossy().to_string()),
                Err(e) => {
                    log::warn!(
                        "[session {}] Requested working directory '{}' could not be expanded: {} (ignoring)",
                        self.session_id,
                        dir,
                        e
                    );
                    None
                }
            },
            None => None,
        };

        // Validate the working directory if it was supplied.
        let working_directory = match working_directory {
            Some(dir) => {
                // Test the working directory to see if it exists.
                match fs::metadata(dir.clone()) {
                    Ok(metadata) => {
                        if !metadata.is_dir() {
                            log::warn!(
                                "[session {}] Requested working directory '{}' is not a directory; ignoring",
                                self.session_id,
                                dir
                            );
                            None
                        } else {
                            Some(dir)
                        }
                    }
                    Err(e) => {
                        log::warn!(
                            "[session {}] Requested working directory '{}' could not be read: {} (ignoring)",
                            self.session_id,
                            dir,
                            e
                        );
                        None
                    }
                }
            }
            None => None,
        };

        // Enter the restarting state.
        let claim = {
            let mut state = self.state.write().await;
            if state.restarting() {
                return Err(refused("Kernel is already restarting"));
            }

            // A kernel that has exited cannot be asked to shut down, and will
            // not exit again: a restart that waited for that would never end.
            if state.status == models::Status::Exited {
                return Err(refused(
                    "The kernel has exited, so there is nothing to restart; start the session to run it again",
                ));
            }

            // Set the working directory.
            match working_directory {
                Some(dir) => {
                    log::debug!(
                        "[session {}] Will restart in working directory '{}' (supplied by client)",
                        self.session_id,
                        dir
                    );
                    state.working_directory = dir
                }
                None => {
                    #[cfg(not(target_os = "windows"))]
                    {
                        state.poll_working_dir().await;
                        log::debug!(
                            "[session {}] Will restart in working directory '{}' (read from OS)",
                            self.session_id,
                            state.working_directory
                        );
                    }

                    #[cfg(target_os = "windows")]
                    {
                        state.working_directory = self.original_working_directory.clone();
                        log::debug!(
                            "[session {}] Will restart in working directory '{}' (original)",
                            self.session_id,
                            state.working_directory
                        );
                    }
                }
            }

            // Set the environment variables, if supplied
            if let Some(env) = env {
                log::debug!(
                    "[session {}] Will restart with environment variables: {:?}",
                    self.session_id,
                    env
                );
                state.env_vars = env;
            }

            match state.begin_restart() {
                Some(claim) => claim,
                None => return Err(refused("Kernel is already restarting")),
            }
        };

        match self.shutdown_request(true).await {
            Ok(_) => {
                log::debug!("Preparing for restart; sent shutdown request to kernel");
            }
            Err(e) => {
                // The restarting state is left as the claim is dropped,
                // since we failed to send the shutdown request.
                let err = KSError::RestartFailed(anyhow::anyhow!(
                    "Failed to send shutdown request to kernel"
                ));
                return Err(StartupError {
                    exit_code: None,
                    output: None,
                    error: err.to_json(Some(format!("{}", e))),
                });
            }
        }

        // The caller waits for the kernel to exit and then starts it again
        log::debug!(
            "[session {}] Waiting for kernel to exit before restarting",
            self.session_id
        );
        Ok(claim)
    }

    /// Interrupt the kernel.
    ///
    /// Sends an interrupt signal to the kernel, either via a system signal
    /// (Unix) or an event handle (Windows), or via a Jupyter message.
    pub async fn interrupt(
        &self,
        interrupt_mode: models::InterruptMode,
        #[cfg(windows)] interrupt_event_handle: Option<isize>,
    ) -> Result<(), anyhow::Error> {
        match interrupt_mode {
            models::InterruptMode::Signal => {
                // On Windows, interrupts are signaled by setting a kernel event.
                #[cfg(windows)]
                {
                    use windows::Win32::Foundation::HANDLE;
                    use windows::Win32::System::Threading::SetEvent;

                    if let Some(handle_value) = interrupt_event_handle {
                        #[allow(unsafe_code)]
                        unsafe {
                            log::debug!(
                                "Setting interrupt event {} for session {}",
                                handle_value,
                                self.session_id
                            );
                            // Convert the stored isize back to a HANDLE
                            let handle = HANDLE(handle_value as *mut std::ffi::c_void);
                            return match SetEvent(handle) {
                                Ok(_) => {
                                    log::debug!(
                                        "Successfully set interrupt event for session {}",
                                        self.session_id
                                    );
                                    Ok(())
                                }
                                Err(e) => Err(anyhow::anyhow!(
                                    "Failed to set interrupt event: {}, process not interrupted",
                                    e
                                )),
                            };
                        }
                    } else {
                        return Err(anyhow::anyhow!("No interrupt event to set"));
                    }
                }

                // On Unix-alikes, interrupts are signaled by sending a SIGINT signal
                #[cfg(unix)]
                {
                    let pid = self.state.read().await.process_id.unwrap_or(0);
                    if pid == 0 {
                        return Err(anyhow::anyhow!("No process ID to interrupt"));
                    }
                    if !crate::process_control::interrupt(pid) {
                        return Err(anyhow::anyhow!("Failed to interrupt process {}", pid));
                    }
                }
            }
            models::InterruptMode::Message => {
                let msg = JupyterMessage {
                    header: JupyterMessageHeader {
                        msg_id: make_message_id(),
                        msg_type: "interrupt_request".to_string(),
                    },
                    parent_header: None,
                    metadata: serde_json::json!({}),
                    content: serde_json::json!({}),
                    channel: JupyterChannel::Control,
                    buffers: vec![],
                };
                self.ws_zmq_tx.send(msg).await?;
            }
        }
        Ok(())
    }
}
