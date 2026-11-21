//
// heartbeat_test.rs
//
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//

//! Tests for the heartbeat monitor: a kernel that stops answering heartbeats
//! for a while and then answers again.

#![allow(unused_imports)]

#[path = "common/mod.rs"]
mod common;

use common::test_utils::{
    create_session_with_client, create_test_session, get_python_executable, is_ipykernel_available,
};
use common::TestServer;
use kallichore_api::models::{ExecuteRequest, Status};
use kallichore_api::ApiNoContext;
use std::time::Duration;
use uuid::Uuid;

type Client = Box<dyn ApiNoContext<common::test_utils::ClientContext> + Send + Sync>;

/// A kernel process that the test stops for a while. Whatever way the test
/// ends, the process is resumed and killed: a stopped process would otherwise
/// be left behind, and the server could not end it either.
#[cfg(unix)]
struct StoppedKernel {
    pid: i32,
}

#[cfg(unix)]
impl StoppedKernel {
    fn stop(pid: i32) -> Self {
        #[allow(unsafe_code)]
        let stopped = unsafe { libc::kill(pid, libc::SIGSTOP) } == 0;
        assert!(stopped, "failed to stop kernel {}", pid);
        Self { pid }
    }

    fn resume(&self) {
        #[allow(unsafe_code)]
        let resumed = unsafe { libc::kill(self.pid, libc::SIGCONT) } == 0;
        assert!(resumed, "failed to resume kernel {}", self.pid);
    }
}

#[cfg(unix)]
impl Drop for StoppedKernel {
    fn drop(&mut self) {
        #[allow(unsafe_code)]
        unsafe {
            libc::kill(self.pid, libc::SIGCONT);
            libc::kill(self.pid, libc::SIGKILL);
        }
    }
}

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

/// Starts an ipykernel session and returns its ID and the kernel's process
/// ID, or `None` if there is no ipykernel to start.
#[cfg(unix)]
async fn start_python_session(client: &Client) -> Option<(String, i32)> {
    let Some(python_cmd) = get_python_executable().await else {
        println!("Skipping test: no Python executable found");
        return None;
    };
    if !is_ipykernel_available().await {
        println!("Skipping test: ipykernel not available");
        return None;
    }

    let session_id = format!("heartbeat-test-{}", Uuid::new_v4());
    create_session_with_client(client, create_test_session(session_id.clone(), &python_cmd)).await;

    let response = client
        .start_session(session_id.clone())
        .await
        .expect("start request failed");
    assert!(
        matches!(response, kallichore_api::StartSessionResponse::Started(_)),
        "expected the kernel to start, got {:?}",
        response
    );

    let pid = match client
        .get_session(session_id.clone())
        .await
        .expect("failed to get session")
    {
        kallichore_api::GetSessionResponse::SessionDetails(details) => details
            .process_id
            .expect("session should report a process ID"),
        other => panic!("expected the session's details, got {:?}", other),
    };
    Some((session_id, pid))
}

/// Stops the kernel's process until the supervisor has marked it offline
/// (after 5 s without an answer), lets the time for the next heartbeat pass
/// too, and resumes it.
#[cfg(unix)]
async fn stop_until_offline(client: &Client, session_id: &str, pid: i32) -> StoppedKernel {
    let kernel = StoppedKernel::stop(pid);
    wait_for_status(client, session_id, Status::Offline, Duration::from_secs(20)).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    kernel.resume();
    kernel
}

/// A kernel that stops answering heartbeats is marked offline, and must be
/// marked idle again once it answers: the heartbeat monitor has to keep
/// running across a heartbeat that timed out.
#[cfg(unix)]
#[tokio::test]
async fn test_session_returns_to_idle_after_missed_heartbeat() {
    let result = tokio::time::timeout(Duration::from_secs(90), async {
        let server = TestServer::start().await;
        let client = server.create_client().await;
        let Some((session_id, pid)) = start_python_session(&client).await else {
            return;
        };
        wait_for_status(&client, &session_id, Status::Idle, Duration::from_secs(10)).await;

        let _kernel = stop_until_offline(&client, &session_id, pid).await;

        wait_for_status(&client, &session_id, Status::Idle, Duration::from_secs(15)).await;

        let _ = client.kill_session(session_id.clone()).await;
        drop(server);
    })
    .await;

    result.expect("test timed out");
}

/// A kernel that was busy when it stopped answering heartbeats is busy when
/// it answers again, not idle: it is still running what it was running.
#[cfg(unix)]
#[tokio::test]
async fn test_session_returns_to_busy_after_missed_heartbeat() {
    let result = tokio::time::timeout(Duration::from_secs(90), async {
        let server = TestServer::start().await;
        let client = server.create_client().await;
        let Some((session_id, pid)) = start_python_session(&client).await else {
            return;
        };

        // Something that keeps the kernel busy for longer than the test takes
        let executing = {
            let client = server.create_client().await;
            let session_id = session_id.clone();
            tokio::spawn(async move {
                let request = ExecuteRequest::new("import time; time.sleep(60)".to_string());
                let _ = client.execute_code(session_id, request).await;
            })
        };
        wait_for_status(&client, &session_id, Status::Busy, Duration::from_secs(10)).await;

        let _kernel = stop_until_offline(&client, &session_id, pid).await;

        // The first status the session has after being offline is busy
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        let status = loop {
            let status = status_of(&client, &session_id).await;
            if status != Status::Offline {
                break status;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "a kernel that answers heartbeats again should not stay offline"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!(
            status,
            Status::Busy,
            "a kernel that was busy before it went offline is busy when it is back"
        );

        executing.abort();
        let _ = client.kill_session(session_id.clone()).await;
        drop(server);
    })
    .await;

    result.expect("test timed out");
}
