//
// failed_start_test.rs
//
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//

//! Tests for what a start, or an adoption, that does not succeed leaves
//! behind: no kernel process of the server's making, and a session that has
//! exited and can be deleted.

#![allow(unused_imports)]

#[path = "common/mod.rs"]
mod common;

use common::test_utils::{create_session_with_client, create_test_session};
use common::TestServer;
use kallichore_api::{models, ApiNoContext};
use std::time::Duration;
use uuid::Uuid;
use zeromq::{Socket, SocketRecv, SocketSend};

type Client = Box<dyn ApiNoContext<common::test_utils::ClientContext> + Send + Sync>;

/// Whether a process is still running.
#[cfg(unix)]
fn process_is_running(pid: i32) -> bool {
    // Signal 0 performs kill()'s existence check without sending anything.
    #[allow(unsafe_code)]
    let alive = unsafe { libc::kill(pid, 0) } == 0;
    alive
}

/// Creates a session whose "kernel" never binds a port and never performs a
/// handshake, with a connection timeout of two seconds.
///
/// Returns the session's ID and the file the process writes its own ID to
/// before it becomes `sleep`: the session no longer reports a process ID once
/// it has exited.
#[cfg(unix)]
async fn create_session_that_never_connects(
    client: &Client,
    protocol_version: &str,
) -> (String, std::path::PathBuf) {
    let session_id = format!("failed-start-test-{}", Uuid::new_v4());
    let pid_file = std::env::temp_dir().join(format!("{}.pid", session_id));
    let mut session = create_test_session(session_id.clone(), "/bin/sh");
    session.argv = vec![
        "/bin/sh".to_string(),
        "-c".to_string(),
        "echo $$ > \"$0\"; exec sleep 60".to_string(),
        pid_file.to_string_lossy().to_string(),
        "{connection_file}".to_string(),
    ];
    session.connection_timeout = Some(2);
    session.protocol_version = Some(protocol_version.to_string());
    create_session_with_client(client, session).await;
    (session_id, pid_file)
}

/// Waits for a session to report that it has exited.
async fn wait_until_exited(client: &Client, session_id: &str, within: Duration) {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let kallichore_api::GetSessionResponse::SessionDetails(details) = client
            .get_session(session_id.to_string())
            .await
            .expect("failed to get session")
        else {
            panic!("expected the session's details");
        };
        if details.status == models::Status::Exited {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the session should have exited within {:?}; it is {}",
            within,
            details.status
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Checks that the process a session launched is gone and that the session
/// can be deleted.
#[cfg(unix)]
async fn check_nothing_is_left(client: &Client, session_id: &str, pid_file: &std::path::Path) {
    let pid: i32 = std::fs::read_to_string(pid_file)
        .expect("the process should have written its ID")
        .trim()
        .parse()
        .expect("the process ID should be a number");
    let _ = std::fs::remove_file(pid_file);
    assert!(
        !process_is_running(pid),
        "process {} should not outlive its failed start",
        pid
    );
    check_session_can_be_deleted(client, session_id).await;
}

async fn check_session_can_be_deleted(client: &Client, session_id: &str) {
    let response = client
        .delete_session(session_id.to_string())
        .await
        .expect("delete request failed");
    assert!(
        matches!(
            response,
            kallichore_api::DeleteSessionResponse::SessionDeleted(_)
        ),
        "expected the session to be deleted, got {:?}",
        response
    );
}

/// Starts a session whose "kernel" never connects. The start must fail, leave
/// no process running, and leave a session that has exited and can be
/// deleted.
#[cfg(unix)]
async fn check_failed_start_leaves_nothing_running(protocol_version: &str) {
    let server = TestServer::start().await;
    let client = server.create_client().await;
    let (session_id, pid_file) =
        create_session_that_never_connects(&client, protocol_version).await;

    let response = client
        .start_session(session_id.clone())
        .await
        .expect("start request failed");
    assert!(
        matches!(
            response,
            kallichore_api::StartSessionResponse::StartFailed(_)
        ),
        "expected the start to fail, got {:?}",
        response
    );

    // The session has exited by the time the failure is reported
    wait_until_exited(&client, &session_id, Duration::ZERO).await;
    check_nothing_is_left(&client, &session_id, &pid_file).await;

    drop(server);
}

/// Starts a session whose "kernel" never connects, and stops waiting for the
/// start long before the session's connection timeout. The start must still
/// be carried through: once the kernel has failed to connect its process is
/// stopped, and the session has exited and can be deleted.
#[cfg(unix)]
async fn check_abandoned_start_is_carried_through(protocol_version: &str) {
    let server = TestServer::start().await;
    let client = server.create_client().await;
    let (session_id, pid_file) =
        create_session_that_never_connects(&client, protocol_version).await;

    let start = tokio::time::timeout(
        Duration::from_millis(500),
        client.start_session(session_id.clone()),
    )
    .await;
    assert!(
        start.is_err(),
        "the start should still have been in progress, got {:?}",
        start
    );

    wait_until_exited(&client, &session_id, Duration::from_secs(10)).await;
    check_nothing_is_left(&client, &session_id, &pid_file).await;

    drop(server);
}

/// A start that times out waiting to connect to the kernel's sockets stops the
/// process it launched.
#[cfg(unix)]
#[tokio::test]
async fn test_failed_start_stops_process_connection_timeout() {
    tokio::time::timeout(
        Duration::from_secs(30),
        check_failed_start_leaves_nothing_running("5.3"),
    )
    .await
    .expect("test timed out");
}

/// A start that times out waiting for the kernel's handshake stops the process
/// it launched.
#[cfg(unix)]
#[tokio::test]
async fn test_failed_start_stops_process_handshake_timeout() {
    tokio::time::timeout(
        Duration::from_secs(30),
        check_failed_start_leaves_nothing_running("5.5"),
    )
    .await
    .expect("test timed out");
}

/// A start whose client stops waiting is carried through (connection file).
#[cfg(unix)]
#[tokio::test]
async fn test_abandoned_start_is_carried_through_connection_timeout() {
    tokio::time::timeout(
        Duration::from_secs(30),
        check_abandoned_start_is_carried_through("5.3"),
    )
    .await
    .expect("test timed out");
}

/// A start whose client stops waiting is carried through (handshake).
#[cfg(unix)]
#[tokio::test]
async fn test_abandoned_start_is_carried_through_handshake_timeout() {
    tokio::time::timeout(
        Duration::from_secs(30),
        check_abandoned_start_is_carried_through("5.5"),
    )
    .await
    .expect("test timed out");
}

/// A start that fails before it has launched the kernel's process (here the
/// session's startup script does not exist) leaves a session that has exited
/// and can be deleted, not one that is starting for good.
#[cfg(unix)]
#[tokio::test]
async fn test_start_that_fails_before_launch_leaves_an_exited_session() {
    let server = TestServer::start().await;
    let client = server.create_client().await;

    let session_id = format!("failed-start-test-{}", Uuid::new_v4());
    let mut session = create_test_session(session_id.clone(), "/bin/sh");
    session.argv = vec![
        "/bin/sh".to_string(),
        "-c".to_string(),
        "exec sleep 60".to_string(),
        "{connection_file}".to_string(),
    ];
    session.startup_environment = models::StartupEnvironment::Script;
    session.startup_environment_arg = Some(format!("/nonexistent/{}.sh", session_id));
    create_session_with_client(&client, session).await;

    let response = client
        .start_session(session_id.clone())
        .await
        .expect("start request failed");
    assert!(
        matches!(
            response,
            kallichore_api::StartSessionResponse::StartFailed(_)
        ),
        "expected the start to fail, got {:?}",
        response
    );

    wait_until_exited(&client, &session_id, Duration::ZERO).await;
    check_session_can_be_deleted(&client, &session_id).await;

    drop(server);
}

/// Binds a socket to a port of the system's choosing and returns the port.
async fn bind_to_free_port<S: Socket>(socket: &mut S) -> i32 {
    match socket
        .bind("tcp://127.0.0.1:0")
        .await
        .expect("failed to bind a socket")
    {
        zeromq::Endpoint::Tcp(_, port) => port as i32,
        endpoint => panic!("expected a TCP endpoint, got {:?}", endpoint),
    }
}

/// An adoption waits for the kernel's first answer, and a kernel that is busy
/// gives none until it is done. When the client stops waiting, the adoption
/// is given up: the session has exited and can be deleted, and is not left
/// connected to a kernel on behalf of a caller that has gone.
#[tokio::test]
async fn test_abandoned_adoption_is_given_up() {
    let server = TestServer::start().await;
    let client = server.create_client().await;

    // Stands in for a kernel that is busy: its sockets are bound, its
    // heartbeat answers, and no request is ever answered.
    let mut shell = zeromq::RouterSocket::new();
    let mut control = zeromq::RouterSocket::new();
    let mut stdin = zeromq::RouterSocket::new();
    let mut iopub = zeromq::PubSocket::new();
    let mut heartbeat = zeromq::RepSocket::new();
    let connection_info = models::ConnectionInfo {
        shell_port: bind_to_free_port(&mut shell).await,
        control_port: bind_to_free_port(&mut control).await,
        stdin_port: bind_to_free_port(&mut stdin).await,
        iopub_port: bind_to_free_port(&mut iopub).await,
        hb_port: bind_to_free_port(&mut heartbeat).await,
        signature_scheme: "hmac-sha256".to_string(),
        key: Uuid::new_v4().simple().to_string(),
        transport: "tcp".to_string(),
        ip: "127.0.0.1".to_string(),
    };
    let heartbeats = tokio::spawn(async move {
        while let Ok(message) = heartbeat.recv().await {
            if heartbeat.send(message).await.is_err() {
                break;
            }
        }
    });

    let session_id = format!("abandoned-adoption-test-{}", Uuid::new_v4());
    let session = create_test_session(session_id.clone(), "/bin/sh");
    create_session_with_client(&client, session).await;

    let adoption = tokio::time::timeout(
        Duration::from_secs(2),
        client.adopt_session(session_id.clone(), connection_info),
    )
    .await;
    assert!(
        adoption.is_err(),
        "the adoption should still have been waiting for the kernel, got {:?}",
        adoption
    );

    wait_until_exited(&client, &session_id, Duration::from_secs(5)).await;
    check_session_can_be_deleted(&client, &session_id).await;

    heartbeats.abort();
    drop(server);
}
