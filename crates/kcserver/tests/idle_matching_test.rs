//
// idle_matching_test.rs
//
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//

//! Tests for the session's status and execution queue while a request is
//! handled beside a running execution: a control-channel request, which a
//! kernel answers while it executes code, reports a busy and an idle of its
//! own, and that idle is not the execution's.

#[path = "common/mod.rs"]
mod common;

use common::test_utils::{
    create_session_with_client, create_test_session, get_python_executable, is_ipykernel_available,
};
use common::transport::CommunicationChannel;
use common::TestServer;
use kallichore_api::models::{self, Status};
use kallichore_api::ApiNoContext;
use kcshared::jupyter_message::{JupyterChannel, JupyterMessage, JupyterMessageHeader};
use kcshared::websocket_message::WebsocketMessage;
use serde_json::json;
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A request on the given channel, and its message ID.
fn request(
    channel: JupyterChannel,
    msg_type: &str,
    content: serde_json::Value,
) -> (String, WebsocketMessage) {
    let msg_id = Uuid::new_v4().to_string();
    let message = WebsocketMessage::Jupyter(JupyterMessage {
        header: JupyterMessageHeader {
            msg_id: msg_id.clone(),
            msg_type: msg_type.to_string(),
        },
        parent_header: None,
        channel,
        content,
        metadata: json!({}),
        buffers: vec![],
    });
    (msg_id, message)
}

fn execute(code: &str) -> (String, WebsocketMessage) {
    request(
        JupyterChannel::Shell,
        "execute_request",
        json!({
            "code": code,
            "silent": false,
            "store_history": false,
            "user_expressions": {},
            "allow_stdin": false,
            "stop_on_error": true
        }),
    )
}

/// Reads the client's messages until one of the given type answers the given
/// request, and returns it.
async fn wait_for_answer(
    comm: &mut CommunicationChannel,
    msg_type: &str,
    parent: &str,
    within: Duration,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let text = tokio::time::timeout_at(deadline, comm.receive_message())
            .await
            .unwrap_or_else(|_| panic!("no {} to {} within {:?}", msg_type, parent, within))
            .expect("the connection failed")
            .expect("the connection closed");
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
            continue;
        };
        if value["header"]["msg_type"] == msg_type && value["parent_header"]["msg_id"] == parent {
            return value;
        }
    }
}

/// A control-channel request answered while an execution runs, with a second
/// execution waiting in the queue, leaves the session busy with the first
/// execution active and the second still waiting; the second is sent once the
/// first is over, and the session is idle when both are.
///
/// Before, the idle of the control request was taken for the end of the
/// execution: the session said idle while the kernel was still running the
/// first execution, and the second was taken out of the queue.
#[tokio::test]
async fn test_an_idle_for_another_request_does_not_end_the_running_execution() {
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

        let session_id = format!("idle-matching-test-{}", Uuid::new_v4());
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
            matches!(response, kallichore_api::StartSessionResponse::Started(_)),
            "expected the kernel to start, got {:?}",
            response
        );
        wait_for_status(&client, &session_id, Status::Idle, Duration::from_secs(10)).await;

        let ws_url = format!(
            "ws://localhost:{}/sessions/{}/channels",
            server.port(),
            session_id
        );
        let mut comm = CommunicationChannel::create_websocket(&ws_url)
            .await
            .expect("failed to connect to the session");

        // The first execution runs for three seconds; the second waits
        let (first, message) = execute("import time; time.sleep(3)");
        comm.send_message(&message).await.expect("send failed");
        wait_for_status(&client, &session_id, Status::Busy, Duration::from_secs(5)).await;
        let (second, message) = execute("print('second')");
        comm.send_message(&message).await.expect("send failed");

        // A request the kernel answers on its control channel while it runs
        // the first execution
        let (control, message) = request(JupyterChannel::Control, "kernel_info_request", json!({}));
        comm.send_message(&message).await.expect("send failed");
        // Its reply comes on the control channel and its statuses on iopub,
        // in either order
        let (mut replied, mut idle) = (false, false);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !(replied && idle) {
            let text = tokio::time::timeout_at(deadline, comm.receive_message())
                .await
                .expect("the control request was not answered, with its idle, within 2 s")
                .expect("the connection failed")
                .expect("the connection closed");
            let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
                continue;
            };
            if value["parent_header"]["msg_id"] != control.as_str() {
                continue;
            }
            replied |= value["header"]["msg_type"] == "kernel_info_reply";
            idle |= value["header"]["msg_type"] == "status"
                && value["content"]["execution_state"] == "idle";
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        let details = session_details(&client, &session_id).await;
        assert_eq!(
            details.status,
            Status::Busy,
            "the session should be busy while the first execution runs"
        );
        let active = details.execution_queue.active.as_ref().map(|a| {
            a["header"]["msg_id"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        });
        assert_eq!(
            active.as_deref(),
            Some(first.as_str()),
            "the first execution should still be the active one"
        );
        assert_eq!(
            details.execution_queue.length, 1,
            "the second execution should still be waiting"
        );

        wait_for_answer(&mut comm, "execute_reply", &first, Duration::from_secs(10)).await;
        wait_for_answer(&mut comm, "execute_reply", &second, Duration::from_secs(10)).await;
        wait_for_status(&client, &session_id, Status::Idle, Duration::from_secs(5)).await;
        let details = session_details(&client, &session_id).await;
        assert!(details.execution_queue.active.is_none());
        assert_eq!(details.execution_queue.length, 0);

        comm.close().await.ok();
    })
    .await;
    assert!(result.is_ok(), "the test did not finish within 90 s");
}
