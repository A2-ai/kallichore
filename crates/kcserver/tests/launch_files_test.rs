//
// launch_files_test.rs
//
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//

//! Tests for the files a launch writes for its kernel to read as it starts
//! (the connection file): there while the kernel runs, removed once its
//! process has exited, and a restart's own file is not removed by the exit of
//! the kernel before it.

#![allow(unused_imports)]

#[path = "common/mod.rs"]
mod common;

use common::test_utils::{
    create_session_with_client, create_test_session, get_python_executable, is_ipykernel_available,
};
use common::TestServer;
use kallichore_api::models::{self, Status};
use kallichore_api::{ApiNoContext, RestartSessionResponse, StartSessionResponse};
use std::path::PathBuf;
use std::time::Duration;
use uuid::Uuid;

type Client = Box<dyn ApiNoContext<common::test_utils::ClientContext> + Send + Sync>;

async fn status_of(client: &Client, session_id: &str) -> Status {
    match client
        .get_session(session_id.to_string())
        .await
        .expect("failed to get session")
    {
        kallichore_api::GetSessionResponse::SessionDetails(details) => details.status,
        other => panic!("expected the session's details, got {:?}", other),
    }
}

/// Waits until a session has the given status.
async fn wait_for_status(client: &Client, session_id: &str, status: Status, within: Duration) {
    let deadline = std::time::Instant::now() + within;
    loop {
        let current = status_of(client, session_id).await;
        if current == status {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the session should have been {} within {:?}; it is {}",
            status,
            within,
            current
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The connection file kcserver writes for a session's kernel.
fn connection_file(session_id: &str) -> PathBuf {
    std::env::temp_dir().join(format!("connection_{}.json", session_id))
}

async fn start(client: &Client, session_id: &str) {
    let response = client
        .start_session(session_id.to_string())
        .await
        .expect("start request failed");
    assert!(
        matches!(response, StartSessionResponse::Started(_)),
        "expected the kernel to start, got {:?}",
        response
    );
}

async fn kill(client: &Client, session_id: &str) {
    let response = client
        .kill_session(session_id.to_string())
        .await
        .expect("kill request failed");
    assert!(
        matches!(response, kallichore_api::KillSessionResponse::Killed(_)),
        "expected the session to be killed, got {:?}",
        response
    );
    wait_for_status(client, session_id, Status::Exited, Duration::from_secs(10)).await;
}

/// A kernel's connection file is there while it runs and gone once it has
/// exited; a start after that writes it again; a restart keeps the file its
/// new kernel was started with.
///
/// Before, every connection file stayed in the temporary directory for good,
/// one for each session ever started, each holding that session's key.
#[tokio::test]
async fn test_a_kernels_connection_file_is_removed_once_it_has_exited() {
    let result = tokio::time::timeout(Duration::from_secs(120), async {
        let Some(python_cmd) = get_python_executable().await else {
            println!("Skipping test: no Python executable found");
            return;
        };
        if !is_ipykernel_available().await {
            println!("Skipping test: ipykernel not available");
            return;
        }
        let server = TestServer::start().await;
        let client = server.create_client().await;

        let session_id = format!("launch-files-test-{}", Uuid::new_v4());
        let mut session = create_test_session(session_id.clone(), &python_cmd);
        session.protocol_version = Some("5.3".to_string());
        create_session_with_client(&client, session).await;
        let file = connection_file(&session_id);

        start(&client, &session_id).await;
        assert!(file.exists(), "{} should be there while the kernel runs", file.display());
        kill(&client, &session_id).await;
        assert!(!file.exists(), "{} should be gone once the kernel has exited", file.display());

        // Started again: written again; restarted: the new kernel's file stays
        start(&client, &session_id).await;
        assert!(file.exists(), "{} should be written for the next start", file.display());
        let response = client
            .restart_session(session_id.clone(), None)
            .await
            .expect("restart request failed");
        assert!(
            matches!(response, RestartSessionResponse::Restarted(_)),
            "expected the restart to succeed, got {:?}",
            response
        );
        wait_for_status(&client, &session_id, Status::Idle, Duration::from_secs(20)).await;
        assert!(
            file.exists(),
            "{} should be there for the restarted kernel; the exit of the one before must not remove it",
            file.display()
        );

        kill(&client, &session_id).await;
        assert!(!file.exists(), "{} should be gone once the kernel has exited", file.display());
    })
    .await;
    assert!(result.is_ok(), "the test did not finish within 120 s");
}

/// A start that fails (its kernel never connects) leaves no connection file.
#[cfg(unix)]
#[tokio::test]
async fn test_a_failed_start_leaves_no_connection_file() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let server = TestServer::start().await;
        let client = server.create_client().await;

        let session_id = format!("launch-files-test-{}", Uuid::new_v4());
        let mut session = create_test_session(session_id.clone(), "/bin/sh");
        session.argv = vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "exec sleep 60".to_string(),
            "{connection_file}".to_string(),
        ];
        session.connection_timeout = Some(2);
        session.protocol_version = Some("5.3".to_string());
        create_session_with_client(&client, session).await;
        let file = connection_file(&session_id);

        let response = client
            .start_session(session_id.clone())
            .await
            .expect("start request failed");
        assert!(
            matches!(response, StartSessionResponse::StartFailed(_)),
            "the start should fail (its kernel never connects), got {:?}",
            response
        );
        wait_for_status(
            &client,
            &session_id,
            Status::Exited,
            Duration::from_secs(10),
        )
        .await;
        assert!(
            !file.exists(),
            "{} should be gone after the failed start",
            file.display()
        );
    })
    .await;
    assert!(result.is_ok(), "the test did not finish within 60 s");
}
