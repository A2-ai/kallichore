//
// heartbeat.rs
//
// Copyright (C) 2024-2025 Posit Software, PBC. All rights reserved.
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//
//

use std::sync::Arc;

use kallichore_api::models::Status;
use tokio::sync::RwLock;
use zeromq::ReqSocket;
use zeromq::Socket;
use zeromq::SocketRecv;
use zeromq::SocketSend;
use zeromq::ZmqMessage;
use zeromq::ZmqResult;

use crate::kernel_state::KernelState;
use event_listener::{Event, EventListener};
use tokio::time::Duration;

pub struct HeartbeatMonitor {
    state: Arc<RwLock<KernelState>>,
    session_id: String,
    address: String,
    exit_event: Arc<Event>,
    disconnected_event: Arc<Event>,
}

const HB_PAYLOAD: &str = "kallichore-heartbeat";

/// How long the kernel has to answer a heartbeat before it is marked offline.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long after one heartbeat has been answered the next is sent.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// A heartbeat monitor for a kernel session.
impl HeartbeatMonitor {
    /// Create a new heartbeat monitor.
    ///
    /// # Arguments
    ///
    /// - `state`: The kernel state to monitor.
    /// - `session_id`: The ID of the session to monitor.
    /// - `address`: The address of the heartbeat socket.
    pub fn new(
        state: Arc<RwLock<KernelState>>,
        session_id: String,
        address: String,
        exit_event: Arc<Event>,
        disconnected_event: Arc<Event>,
    ) -> Self {
        Self {
            state,
            session_id,
            address,
            exit_event,
            disconnected_event,
        }
    }

    /// Monitor the kernel's heartbeat. Returns immediately and runs the monitor
    /// job in the background.
    pub fn monitor(&self) {
        let addr = self.address.clone();
        let state = self.state.clone();
        let session_id = self.session_id.clone();
        let exit_event = self.exit_event.clone();
        let disconnected_event = self.disconnected_event.clone();

        tokio::spawn(async move {
            // One listener for the life of the monitor. Made anew for each
            // wait, it would miss an exit that came between two waits.
            let mut exited = exit_event.listen();

            let mut hb_socket = match Self::connect_heartbeat_socket(&addr, &session_id).await {
                Some(socket) => socket,
                None => return,
            };

            let mut initial = true;

            loop {
                // Check if we should stop the monitor
                if Self::should_stop_monitor(&state, &session_id).await {
                    return;
                }

                // Send heartbeat
                log::trace!("[session {}] Sending heartbeat to kernel.", session_id);
                if let Err(e) = hb_socket.send(HB_PAYLOAD.into()).await {
                    Self::handle_heartbeat_error(e, &state, &disconnected_event, &session_id).await;
                    hb_socket.close().await;
                    return;
                }

                // Wait for the kernel's answer, or for the kernel to exit
                match Self::wait_for_answer(&mut hb_socket, &mut exited, &state, &session_id).await
                {
                    Some(Ok(response)) => {
                        log::trace!(
                            "[session {}] Got heartbeat response: {:?}",
                            session_id,
                            response
                        );
                        if initial {
                            initial = false;
                            log::info!(
                                "[session {}] Received initial heartbeat from kernel",
                                session_id
                            );
                        }
                    }
                    Some(Err(e)) => {
                        Self::handle_heartbeat_error(e, &state, &disconnected_event, &session_id)
                            .await;
                        hb_socket.close().await;
                        return;
                    }
                    None => {
                        log::debug!(
                            "[session {}] Stopping heartbeat monitor (exit event signaled).",
                            session_id
                        );
                        hb_socket.close().await;
                        return;
                    }
                }

                // Wait before next heartbeat or exit if signaled
                tokio::select! {
                    _ = &mut exited => {
                        log::debug!(
                            "[session {}] Stopping heartbeat monitor (exit event signaled).",
                            session_id
                        );
                        return;
                    }
                    _ = tokio::time::sleep(HEARTBEAT_INTERVAL) => {}
                }
            }
        });
    }

    /// Connect to the heartbeat socket
    async fn connect_heartbeat_socket(addr: &str, session_id: &str) -> Option<ReqSocket> {
        let mut hb_socket = ReqSocket::new();
        match hb_socket.connect(addr).await {
            Err(err) => {
                log::error!(
                    "[session {}] Failed to connect to heartbeat socket: {}.",
                    session_id,
                    err
                );
                None
            }
            Ok(_) => {
                log::info!(
                    "[session {}] Connected to heartbeat socket at {}.",
                    session_id,
                    addr
                );
                Some(hb_socket)
            }
        }
    }

    /// Check if the monitor should stop based on kernel state
    async fn should_stop_monitor(state: &Arc<RwLock<KernelState>>, session_id: &str) -> bool {
        let current_state = state.read().await;
        if current_state.status == Status::Exited {
            log::debug!(
                "[session {}] Stopping heartbeat monitor (kernel exited).",
                session_id
            );
            true
        } else {
            false
        }
    }

    /// Wait for the kernel's answer to the heartbeat that has just been sent.
    ///
    /// The one receive is kept until it completes. A kernel can be unable to
    /// answer for a while (its process is stopped, or the machine is short of
    /// memory) and answer later. That late answer is the reply to this
    /// request, and the request socket takes no other request until it has
    /// had it. So when `HEARTBEAT_TIMEOUT` has passed the kernel is marked
    /// offline and the wait goes on; it is not given up and started again.
    ///
    /// When the answer does come after the kernel was marked offline, the
    /// session is given back the status it had before.
    ///
    /// Returns `None` if the kernel exited first.
    async fn wait_for_answer(
        hb_socket: &mut ReqSocket,
        exited: &mut EventListener,
        state: &Arc<RwLock<KernelState>>,
        session_id: &str,
    ) -> Option<ZmqResult<ZmqMessage>> {
        let mut answer = hb_socket.recv();
        let overdue = tokio::time::sleep(HEARTBEAT_TIMEOUT);
        tokio::pin!(overdue);
        let mut offline = false;

        let result = loop {
            tokio::select! {
                _ = &mut *exited => return None,
                result = &mut answer => break result,
                _ = &mut overdue, if !offline => {
                    offline = true;
                    log::error!(
                        "[session {}] No heartbeat response received after {}s, marking kernel as offline.",
                        session_id,
                        HEARTBEAT_TIMEOUT.as_secs()
                    );
                    let mut state = state.write().await;
                    state
                        .set_status(Status::Offline, Some(String::from("lost heartbeat")))
                        .await;
                }
            }
        };

        if offline && result.is_ok() {
            log::trace!(
                "[session {}] Kernel was offline; marking it online.",
                session_id
            );
            let mut state = state.write().await;
            state.restore_status_after_offline().await;
        }

        Some(result)
    }

    /// Handle heartbeat error
    async fn handle_heartbeat_error(
        e: zeromq::ZmqError,
        state: &Arc<RwLock<KernelState>>,
        disconnected_event: &Arc<Event>,
        session_id: &str,
    ) {
        let current = state.read().await;

        if current.status == Status::Exited {
            log::debug!(
                "[session {}] Stopping heartbeat monitor (kernel exited).",
                session_id
            );
        } else {
            log::info!(
                "[session {}] Error receiving heartbeat response: {:?} (kernel is {}). Marking kernel disconnected.",
                session_id,
                e,
                current.status
            );
            disconnected_event.notify(usize::MAX);
        }
    }
}
