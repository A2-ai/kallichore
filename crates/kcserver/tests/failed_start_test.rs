//
// failed_start_test.rs
//
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//

//! Tests for what a start that fails leaves behind: no kernel process, and a
//! session that has exited and can be deleted.

#![allow(unused_imports)]

#[path = "common/mod.rs"]
mod common;

use common::test_utils::{create_session_with_client, create_test_session};
use common::TestServer;
use std::time::Duration;
use uuid::Uuid;

/// Whether a process is still running.
#[cfg(unix)]
fn process_is_running(pid: i32) -> bool {
    // Signal 0 performs kill()'s existence check without sending anything.
    #[allow(unsafe_code)]
    let alive = unsafe { libc::kill(pid, 0) } == 0;
    alive
}

/// Starts a session whose "kernel" never binds a port and never performs a
/// handshake, with a short connection timeout. The start must fail, leave no
/// process running, and leave a session that has exited and can be deleted.
#[cfg(unix)]
async fn check_failed_start_leaves_nothing_running(protocol_version: &str) {
    let server = TestServer::start().await;
    let client = server.create_client().await;

    let session_id = format!("failed-start-test-{}", Uuid::new_v4());

    // The process writes its own ID to a file before it becomes `sleep`: the
    // session no longer reports a process ID once it has exited.
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

    let kallichore_api::GetSessionResponse::SessionDetails(details) = client
        .get_session(session_id.clone())
        .await
        .expect("failed to get session")
    else {
        panic!("expected the session's details");
    };
    assert_eq!(
        details.status,
        kallichore_api::models::Status::Exited,
        "a session whose start failed should have exited"
    );
    let pid: i32 = std::fs::read_to_string(&pid_file)
        .expect("the process should have written its ID")
        .trim()
        .parse()
        .expect("the process ID should be a number");
    let _ = std::fs::remove_file(&pid_file);
    assert!(
        !process_is_running(pid),
        "process {} should not outlive its failed start",
        pid
    );

    let response = client
        .delete_session(session_id.clone())
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
