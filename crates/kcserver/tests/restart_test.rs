//
// restart_test.rs
//
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//

//! Tests for restarting a session: one whose kernel has already exited, and
//! a restart whose client stops waiting for the kernel to exit.

#![allow(unused_imports)]

#[path = "common/mod.rs"]
mod common;

use common::test_utils::{
    create_session_with_client, create_test_session, get_python_executable, is_ipykernel_available,
};
use common::TestServer;
use kallichore_api::models::{self, Status};
use kallichore_api::{ApiNoContext, RestartSessionResponse, StartSessionResponse};
use std::time::Duration;
use uuid::Uuid;

type Client = Box<dyn ApiNoContext<common::test_utils::ClientContext> + Send + Sync>;

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

/// Starts an ipykernel session and returns its ID and the kernel's process
/// ID, or `None` if there is no ipykernel to start.
async fn start_python_session(client: &Client) -> Option<(String, i32)> {
    let Some(python_cmd) = get_python_executable().await else {
        println!("Skipping test: no Python executable found");
        return None;
    };
    if !is_ipykernel_available().await {
        println!("Skipping test: ipykernel not available");
        return None;
    }

    let session_id = format!("restart-test-{}", Uuid::new_v4());
    create_session_with_client(client, create_test_session(session_id.clone(), &python_cmd)).await;

    let response = client
        .start_session(session_id.clone())
        .await
        .expect("start request failed");
    assert!(
        matches!(response, StartSessionResponse::Started(_)),
        "expected the kernel to start, got {:?}",
        response
    );
    let pid = session_details(client, &session_id)
        .await
        .process_id
        .expect("session should report a process ID");
    Some((session_id, pid))
}

/// A restart of a session whose kernel has already exited is answered, with
/// an error that says so, and leaves the session as it was: it can be started
/// again, and restarted then.
///
/// There is no kernel to ask to shut down and no exit to wait for. A restart
/// that waited for one was never answered.
#[tokio::test]
async fn test_restart_of_an_exited_session_is_refused() {
    let result = tokio::time::timeout(Duration::from_secs(90), async {
        let server = TestServer::start().await;
        let client = server.create_client().await;
        let Some((session_id, _pid)) = start_python_session(&client).await else {
            return;
        };

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

        let response = tokio::time::timeout(
            Duration::from_secs(10),
            client.restart_session(session_id.clone(), None),
        )
        .await
        .expect("a restart of an exited session should be answered")
        .expect("restart request failed");
        match response {
            RestartSessionResponse::RestartFailed(failure) => {
                assert!(
                    failure.error.message.contains("exited"),
                    "the error should say that the kernel has exited: {:?}",
                    failure.error
                );
            }
            other => panic!("expected the restart to be refused, got {:?}", other),
        }
        assert_eq!(
            session_details(&client, &session_id).await.status,
            Status::Exited,
            "a refused restart should leave the session exited"
        );

        // The session is not left restarting
        let response = client
            .start_session(session_id.clone())
            .await
            .expect("start request failed");
        assert!(
            matches!(response, StartSessionResponse::Started(_)),
            "expected the kernel to start again, got {:?}",
            response
        );
        let response = client
            .restart_session(session_id.clone(), None)
            .await
            .expect("restart request failed");
        assert!(
            matches!(response, RestartSessionResponse::Restarted(_)),
            "expected the running kernel to be restarted, got {:?}",
            response
        );

        let _ = client.kill_session(session_id.clone()).await;
        drop(server);
    })
    .await;

    result.expect("test timed out");
}

/// A kernel process that the test stops for a while. If the test ends before
/// it has resumed the process, the process is resumed and killed: a stopped
/// process would otherwise be left behind.
#[cfg(unix)]
struct StoppedKernel {
    pid: i32,
    resumed: bool,
}

#[cfg(unix)]
impl StoppedKernel {
    fn stop(pid: i32) -> Self {
        #[allow(unsafe_code)]
        let stopped = unsafe { libc::kill(pid, libc::SIGSTOP) } == 0;
        assert!(stopped, "failed to stop kernel {}", pid);
        Self {
            pid,
            resumed: false,
        }
    }

    fn resume(&mut self) {
        #[allow(unsafe_code)]
        let resumed = unsafe { libc::kill(self.pid, libc::SIGCONT) } == 0;
        assert!(resumed, "failed to resume kernel {}", self.pid);
        self.resumed = true;
    }
}

#[cfg(unix)]
impl Drop for StoppedKernel {
    fn drop(&mut self) {
        if !self.resumed {
            #[allow(unsafe_code)]
            unsafe {
                libc::kill(self.pid, libc::SIGCONT);
                libc::kill(self.pid, libc::SIGKILL);
            }
        }
    }
}

/// A restart whose client stops waiting before the kernel has exited is over
/// with that: the kernel, which was asked to shut down, exits and is not
/// started again, and the session is not left restarting. It can be started
/// again, and restarted then.
///
/// The kernel here is stopped when the restart is asked for, so it does not
/// exit, and the client gives up after 1.5 s. Then the kernel is resumed.
#[cfg(unix)]
#[tokio::test]
async fn test_restart_given_up_by_its_client_leaves_a_session_that_can_be_restarted() {
    let result = tokio::time::timeout(Duration::from_secs(120), async {
        let server = TestServer::start().await;
        let client = server.create_client().await;
        let Some((session_id, pid)) = start_python_session(&client).await else {
            return;
        };
        wait_for_status(&client, &session_id, Status::Idle, Duration::from_secs(10)).await;

        let mut kernel = StoppedKernel::stop(pid);
        let restart = tokio::time::timeout(
            Duration::from_millis(1500),
            client.restart_session(session_id.clone(), None),
        )
        .await;
        assert!(
            restart.is_err(),
            "the restart should still have been waiting for the kernel to exit, got {:?}",
            restart
        );
        kernel.resume();

        // The kernel takes the request to shut down, and exits
        wait_for_status(
            &client,
            &session_id,
            Status::Exited,
            Duration::from_secs(20),
        )
        .await;

        // The session can be started again, and restarted then
        let response = client
            .start_session(session_id.clone())
            .await
            .expect("start request failed");
        assert!(
            matches!(response, StartSessionResponse::Started(_)),
            "expected the kernel to start again, got {:?}",
            response
        );
        let response = tokio::time::timeout(
            Duration::from_secs(30),
            client.restart_session(session_id.clone(), None),
        )
        .await
        .expect("the second restart should be answered")
        .expect("restart request failed");
        assert!(
            matches!(response, RestartSessionResponse::Restarted(_)),
            "expected the kernel to be restarted, got {:?}",
            response
        );
        wait_for_status(&client, &session_id, Status::Idle, Duration::from_secs(10)).await;

        let _ = client.kill_session(session_id.clone()).await;
        drop(server);
    })
    .await;

    result.expect("test timed out");
}
