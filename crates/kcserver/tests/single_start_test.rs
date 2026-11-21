//
// single_start_test.rs
//
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//

//! Tests for a session's kernel being started once: a session that was
//! registered and never started says so, can be deleted and is not restarted;
//! a second start, while the kernel runs or while the first start is still on
//! its way, is refused and launches nothing.

#![allow(unused_imports)]

#[path = "common/mod.rs"]
mod common;

use common::test_utils::{
    create_session_with_client, create_test_session, get_python_executable, is_ipykernel_available,
};
use common::TestServer;
use kallichore_api::models::{self, Status};
use kallichore_api::{
    ApiNoContext, DeleteSessionResponse, RestartSessionResponse, StartSessionResponse,
};
use std::time::Duration;
use uuid::Uuid;

type Client = Box<dyn ApiNoContext<common::test_utils::ClientContext> + Send + Sync>;

/// Whether a process is still running.
#[cfg(unix)]
fn process_is_running(pid: i32) -> bool {
    // Signal 0 performs kill()'s existence check without sending anything.
    #[allow(unsafe_code)]
    let alive = unsafe { libc::kill(pid, 0) } == 0;
    alive
}

async fn session_details(client: &Client, session_id: &str) -> models::ActiveSession {
    match client
        .get_session(session_id.to_string())
        .await
        .expect("failed to get session")
    {
        kallichore_api::GetSessionResponse::SessionDetails(details) => details,
        other => panic!("expected the session's details, got {:?}", other),
    }
}

/// Waits until a session has the given status.
async fn wait_for_status(client: &Client, session_id: &str, status: Status, within: Duration) {
    let deadline = std::time::Instant::now() + within;
    loop {
        let current = session_details(client, session_id).await.status;
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

/// A session registered and not started has no kernel. Its status says so
/// (`uninitialized`, not `idle` like a kernel that is ready), it can be
/// deleted, and a restart of it is answered at once with an error.
///
/// Before, it was `idle`, a delete was refused as "running", and a restart
/// waited for a kernel that was not there to exit, and was never answered.
#[tokio::test]
async fn test_a_session_never_started_is_uninitialized_and_can_be_deleted() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let server = TestServer::start().await;
        let client = server.create_client().await;

        let session_id = format!("single-start-test-{}", Uuid::new_v4());
        create_session_with_client(&client, create_test_session(session_id.clone(), "/bin/sh"))
            .await;

        let details = session_details(&client, &session_id).await;
        assert_eq!(
            details.status,
            Status::Uninitialized,
            "a session that was never started should say so"
        );
        assert_eq!(details.process_id, None);

        let response = tokio::time::timeout(
            Duration::from_secs(5),
            client.restart_session(session_id.clone(), None),
        )
        .await
        .expect("a restart of a session never started should be answered")
        .expect("restart request failed");
        match response {
            RestartSessionResponse::RestartFailed(failure) => assert!(
                failure.error.message.contains("not been started"),
                "the error should say that the session has not been started: {:?}",
                failure.error
            ),
            other => panic!("expected the restart to be refused, got {:?}", other),
        }
        assert_eq!(
            session_details(&client, &session_id).await.status,
            Status::Uninitialized,
            "the refused restart should leave the session as it was"
        );

        let response = client
            .delete_session(session_id.clone())
            .await
            .expect("delete request failed");
        assert!(
            matches!(response, DeleteSessionResponse::SessionDeleted(_)),
            "a session never started should be deleted, got {:?}",
            response
        );
        assert!(
            matches!(
                client.get_session(session_id.clone()).await,
                Ok(kallichore_api::GetSessionResponse::SessionNotFound)
            ),
            "the deleted session should be gone"
        );
    })
    .await;
    assert!(result.is_ok(), "the test did not finish within 60 s");
}

/// A second start of a session whose kernel is running is refused, and the
/// kernel the session has is the one that goes on: the same process, which a
/// kill of the session ends.
///
/// Before, the second start launched the kernel command again; the session
/// took the second process for its own, and the first was left running, out
/// of reach of a kill.
#[cfg(unix)]
#[tokio::test]
async fn test_a_second_start_of_a_running_session_is_refused() {
    let result = tokio::time::timeout(Duration::from_secs(90), async {
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

        let session_id = format!("single-start-test-{}", Uuid::new_v4());
        create_session_with_client(
            &client,
            create_test_session(session_id.clone(), &python_cmd),
        )
        .await;
        let response = client
            .start_session(session_id.clone())
            .await
            .expect("start request failed");
        assert!(
            matches!(response, StartSessionResponse::Started(_)),
            "expected the kernel to start, got {:?}",
            response
        );
        let pid = session_details(&client, &session_id)
            .await
            .process_id
            .expect("session should report a process ID");

        let response = client
            .start_session(session_id.clone())
            .await
            .expect("second start request failed");
        match response {
            StartSessionResponse::StartFailed(failure) => assert!(
                failure.error.message.contains("second start"),
                "the error should say why the start was refused: {:?}",
                failure.error
            ),
            other => panic!("expected the second start to be refused, got {:?}", other),
        }

        let details = session_details(&client, &session_id).await;
        assert_eq!(
            details.process_id,
            Some(pid),
            "the session should still have its first kernel"
        );
        assert_ne!(details.status, Status::Exited);
        assert!(
            process_is_running(pid),
            "the first kernel should be running"
        );

        let response = client
            .kill_session(session_id.clone())
            .await
            .expect("kill request failed");
        assert!(
            matches!(response, kallichore_api::KillSessionResponse::Killed(_)),
            "expected the session to be killed, got {:?}",
            response
        );
        wait_for_status(
            &client,
            &session_id,
            Status::Exited,
            Duration::from_secs(10),
        )
        .await;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while process_is_running(pid) && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            !process_is_running(pid),
            "the kill should have ended the session's only kernel"
        );
    })
    .await;
    assert!(result.is_ok(), "the test did not finish within 90 s");
}

/// A start that comes while the first start is still waiting for its kernel
/// is refused at once, without launching a process. The first start goes on
/// to its own end (here: its kernel never connects, and the start fails), and
/// the session can then be deleted.
#[cfg(unix)]
#[tokio::test]
async fn test_a_start_while_one_is_on_its_way_is_refused() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let server = TestServer::start().await;
        let client = server.create_client().await;

        // A "kernel" that never connects; every process it is launched as
        // appends its own ID to a file
        let session_id = format!("single-start-test-{}", Uuid::new_v4());
        let pid_file = std::env::temp_dir().join(format!("{}.pids", session_id));
        let mut session = create_test_session(session_id.clone(), "/bin/sh");
        session.argv = vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "echo $$ >> \"$0\"; exec sleep 60".to_string(),
            pid_file.to_string_lossy().to_string(),
            "{connection_file}".to_string(),
        ];
        session.connection_timeout = Some(3);
        session.protocol_version = Some("5.3".to_string());
        create_session_with_client(&client, session).await;

        let first = {
            let session_id = session_id.clone();
            let client = server.create_client().await;
            tokio::spawn(async move { client.start_session(session_id).await })
        };
        wait_for_status(
            &client,
            &session_id,
            Status::Starting,
            Duration::from_secs(5),
        )
        .await;

        let asked = std::time::Instant::now();
        let response = client
            .start_session(session_id.clone())
            .await
            .expect("second start request failed");
        assert!(
            asked.elapsed() < Duration::from_secs(1),
            "the second start should be refused at once, not after {:?}",
            asked.elapsed()
        );
        assert!(
            matches!(response, StartSessionResponse::StartFailed(_)),
            "expected the second start to be refused, got {:?}",
            response
        );

        let response = first
            .await
            .expect("the first start's task failed")
            .expect("first start request failed");
        assert!(
            matches!(response, StartSessionResponse::StartFailed(_)),
            "the first start should fail (its kernel never connects), got {:?}",
            response
        );
        wait_for_status(
            &client,
            &session_id,
            Status::Exited,
            Duration::from_secs(10),
        )
        .await;

        let launched = std::fs::read_to_string(&pid_file).unwrap_or_default();
        let _ = std::fs::remove_file(&pid_file);
        let pids: Vec<i32> = launched
            .lines()
            .filter_map(|l| l.trim().parse().ok())
            .collect();
        assert_eq!(
            pids.len(),
            1,
            "one process should have been launched: {:?}",
            pids
        );
        assert!(
            !process_is_running(pids[0]),
            "the failed start should have stopped its process"
        );

        let response = client
            .delete_session(session_id.clone())
            .await
            .expect("delete request failed");
        assert!(
            matches!(response, DeleteSessionResponse::SessionDeleted(_)),
            "the session should be deleted after its failed start, got {:?}",
            response
        );
    })
    .await;
    assert!(result.is_ok(), "the test did not finish within 60 s");
}
