//
// client_handoff_test.rs
//
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//

//! Tests for a session's client being replaced by another: the client that
//! was there is disconnected, and everything from then on goes to the new one.

#![allow(unused_imports)]

#[path = "common/mod.rs"]
mod common;

use common::test_utils::{
    create_session_with_client, create_test_session, get_python_executable, is_ipykernel_available,
};
use common::TestServer;
use futures::{SinkExt, StreamExt};
use kallichore_api::models;
use kcshared::jupyter_message::{JupyterChannel, JupyterMessage, JupyterMessageHeader};
use kcshared::kernel_message::KernelMessage;
use kcshared::websocket_message::WebsocketMessage;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

type Client =
    Box<dyn kallichore_api::ApiNoContext<common::test_utils::ClientContext> + Send + Sync>;
type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_session_ws(server: &TestServer, session_id: &str) -> Ws {
    let url = format!(
        "{}/sessions/{}/channels",
        server.base_url().replace("http://", "ws://"),
        session_id
    );
    let (stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("failed to open the session's WebSocket");
    stream
}

/// Sends a request to run `code` and returns the request's message ID.
async fn execute(ws: &mut Ws, code: &str) -> String {
    let msg_id = Uuid::new_v4().to_string();
    let request = WebsocketMessage::Jupyter(JupyterMessage {
        header: JupyterMessageHeader {
            msg_id: msg_id.clone(),
            msg_type: "execute_request".to_string(),
        },
        parent_header: None,
        channel: JupyterChannel::Shell,
        content: serde_json::json!({
            "code": code,
            "silent": false,
            "store_history": false,
            "user_expressions": {},
            "allow_stdin": false,
            "stop_on_error": true
        }),
        metadata: serde_json::json!({}),
        buffers: vec![],
    });
    ws.send(Message::text(serde_json::to_string(&request).unwrap()))
        .await
        .expect("failed to send an execute request");
    msg_id
}

/// What a client read from its connection.
#[derive(Default, Debug)]
struct Read {
    /// The numbers of the markers found in the output it was sent
    markers: Vec<usize>,
    /// It was told that another client took its place
    told_it_was_replaced: bool,
    /// The connection came to an end
    ended: bool,
}

const MARKER: &str = "handoff-marker-";

impl Read {
    fn take(&mut self, text: &str) {
        match serde_json::from_str::<WebsocketMessage>(text) {
            Ok(WebsocketMessage::Jupyter(message)) => {
                if message.header.msg_type != "stream" {
                    return;
                }
                let Some(output) = message.content["text"].as_str() else {
                    return;
                };
                for part in output.split(MARKER).skip(1) {
                    let digits: String = part.chars().take_while(|c| c.is_ascii_digit()).collect();
                    if let Ok(number) = digits.parse() {
                        self.markers.push(number);
                    }
                }
            }
            Ok(WebsocketMessage::Kernel(KernelMessage::ClientDisconnected(_))) => {
                self.told_it_was_replaced = true;
            }
            _ => {}
        }
    }
}

/// Reads a connection until it ends, for `at_most`.
async fn read_to_the_end(ws: &mut Ws, at_most: Duration, read: &mut Read) {
    let deadline = tokio::time::Instant::now() + at_most;
    loop {
        match tokio::time::timeout_at(deadline, ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => read.take(&text),
            Ok(Some(Ok(Message::Close(_)))) => {
                // Reading once more sends this side's close in answer
                read.ended = true;
                let _ = tokio::time::timeout(Duration::from_secs(1), ws.next()).await;
                return;
            }
            Ok(Some(Err(_))) | Ok(None) => {
                read.ended = true;
                return;
            }
            Ok(Some(Ok(_))) => {}
            Err(_) => return,
        }
    }
}

/// Starts a server and an ipykernel session on it, or returns `None` if
/// there is no ipykernel to start.
async fn start_python_session() -> Option<(TestServer, Client, String)> {
    let Some(python_cmd) = get_python_executable().await else {
        println!("Skipping test: no Python executable found");
        return None;
    };
    if !is_ipykernel_available().await {
        println!("Skipping test: ipykernel not available");
        return None;
    }

    let server = TestServer::start().await;
    let client = server.create_client().await;
    let session_id = format!("handoff-test-{}", Uuid::new_v4());
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
    Some((server, client, session_id))
}

/// Sends `count` requests that each print a numbered marker.
async fn print_markers(ws: &mut Ws, count: usize) {
    for number in 0..count {
        execute(ws, &format!("print('{}{}')", MARKER, number)).await;
    }
}

/// Reads a connection until the markers of `count` requests have come, for
/// `at_most`.
async fn read_markers(ws: &mut Ws, count: usize, at_most: Duration, read: &mut Read) {
    let deadline = tokio::time::Instant::now() + at_most;
    while read.markers.len() < count && !read.ended {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return;
        }
        let step = (deadline - now).min(Duration::from_millis(200));
        read_to_the_end(ws, step, read).await;
    }
}

/// A client whose place another takes is told so, and its connection is
/// closed; the session's messages go to the new client, and the session
/// stays connected.
#[tokio::test]
async fn test_client_is_told_when_another_takes_its_place() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
        let Some((server, client, session_id)) = start_python_session().await else {
            return;
        };

        let mut first = connect_session_ws(&server, &session_id).await;
        let mut second = connect_session_ws(&server, &session_id).await;

        let mut first_read = Read::default();
        read_to_the_end(&mut first, Duration::from_secs(10), &mut first_read).await;
        assert!(
            first_read.told_it_was_replaced,
            "the first client should have been told that another took its place"
        );
        assert!(
            first_read.ended,
            "the first client's connection should have been closed"
        );

        print_markers(&mut second, 5).await;
        let mut second_read = Read::default();
        read_markers(&mut second, 5, Duration::from_secs(20), &mut second_read).await;
        second_read.markers.sort();
        assert_eq!(
            second_read.markers,
            (0..5).collect::<Vec<usize>>(),
            "the new client should be sent the output of each of its requests"
        );
        assert!(
            session_details(&client, &session_id).await.connected,
            "the session has a client, and should say so"
        );

        drop(second);
        let _ = client.kill_session(session_id.clone()).await;
        drop(server);
    })
    .await;

    result.expect("test timed out");
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
async fn wait_for_status(
    client: &Client,
    session_id: &str,
    status: models::Status,
    within: Duration,
) {
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
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A client that had stopped reading its connection, while the kernel had a
/// lot to say, is still replaced when another client connects: it is told
/// and its connection is closed, the session's messages from then on all go
/// to the new client, and the session stays connected.
///
/// The first client here asks for 24 MB of output and reads none of it, so
/// the server is in the middle of a write to it when the second connects.
/// From then on the first client reads again.
#[tokio::test]
async fn test_client_that_is_not_reading_is_replaced() {
    let result = tokio::time::timeout(Duration::from_secs(180), async {
        let Some((server, client, session_id)) = start_python_session().await else {
            return;
        };

        // The first client asks for more output than its connection can hold
        // and reads none of it
        let mut first = connect_session_ws(&server, &session_id).await;
        execute(
            &mut first,
            "import sys, time\ntime.sleep(0.5)\nfor i in range(48):\n    sys.stdout.write('x' * 524288 + '\\n')\n    sys.stdout.flush()\n",
        )
        .await;
        wait_for_status(&client, &session_id, models::Status::Busy, Duration::from_secs(10)).await;
        wait_for_status(&client, &session_id, models::Status::Idle, Duration::from_secs(60)).await;

        // The second client connects. The first reads again from here on:
        // what was already on its way to it, and then, if it has been
        // replaced, the message that says so and the end of its connection.
        let mut second = connect_session_ws(&server, &session_id).await;
        let first_reading = tokio::spawn(async move {
            let mut read = Read::default();
            read_to_the_end(&mut first, Duration::from_secs(15), &mut read).await;
            (first, read)
        });

        // The second client runs 40 small pieces of code, and is sent their
        // output after the rest of the first client's
        print_markers(&mut second, 40).await;
        let mut second_read = Read::default();
        read_markers(&mut second, 40, Duration::from_secs(30), &mut second_read).await;
        let (first, first_read) = first_reading
            .await
            .expect("reading the first client failed");

        assert!(
            first_read.markers.is_empty(),
            "the client that was replaced was sent the output of {} of the new client's 40 requests: {:?}",
            first_read.markers.len(),
            first_read.markers
        );
        second_read.markers.sort();
        assert_eq!(
            second_read.markers,
            (0..40).collect::<Vec<usize>>(),
            "the new client should be sent the output of every one of its 40 requests"
        );
        assert!(
            first_read.told_it_was_replaced,
            "the client that was replaced should have been told so"
        );
        assert!(
            first_read.ended,
            "the connection of the client that was replaced should have been ended"
        );

        // The old connection is gone for good; the session still has a client
        drop(first);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            assert!(
                session_details(&client, &session_id).await.connected,
                "the session has a client, and should say so after the replaced client's connection ended"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        drop(second);
        let _ = client.kill_session(session_id.clone()).await;
        drop(server);
    })
    .await;

    result.expect("test timed out");
}
