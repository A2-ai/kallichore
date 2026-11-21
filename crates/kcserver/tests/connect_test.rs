//
// connect_test.rs
//
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//

//! Tests for connecting to a kernel's sockets: a kernel that binds its ports
//! late, and a kernel whose connections take a while to be set up.

#![allow(unused_imports)]

#[path = "common/mod.rs"]
mod common;

use common::test_utils::{
    create_session_with_client, create_test_session, get_python_executable, is_ipykernel_available,
};
use common::TestServer;
use kallichore_api::models;
use std::time::Duration;
use uuid::Uuid;

/// A kernel that binds its ports several seconds after it was launched is
/// connected to soon after it has, and so within the session's connection
/// timeout. The kernel here is held back 3.6 s and given 6 s. ZeroMQ's own
/// pause between attempts doubles each time; its attempt after the one at
/// about 3.2 s comes at about 6.4 s.
#[cfg(unix)]
#[tokio::test]
async fn test_kernel_that_binds_late_is_reached_within_the_connection_timeout() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
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

        let session_id = format!("connect-test-{}", Uuid::new_v4());
        let mut session = create_test_session(session_id.clone(), &python_cmd);
        let mut argv = vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            "sleep 3.6; exec \"$@\"".to_string(),
            "sh".to_string(),
        ];
        argv.append(&mut session.argv);
        session.argv = argv;
        session.connection_timeout = Some(6);
        create_session_with_client(&client, session).await;

        let response = client
            .start_session(session_id.clone())
            .await
            .expect("start request failed");
        assert!(
            matches!(response, kallichore_api::StartSessionResponse::Started(_)),
            "expected the kernel to start, got {:?}",
            response
        );

        let _ = client.kill_session(session_id.clone()).await;
        drop(server);
    })
    .await;

    result.expect("test timed out");
}

/// Accepts TCP connections on a port of the system's choosing and passes
/// each on to `target_port`, after holding it back for `delay`: nothing the
/// client sends reaches the target, and nothing comes back, until then.
async fn slow_forwarder(target_port: i32, delay: Duration) -> i32 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind a forwarder");
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut inbound, _)) = listener.accept().await {
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let Ok(mut outbound) =
                    tokio::net::TcpStream::connect(("127.0.0.1", target_port as u16)).await
                else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
            });
        }
    });
    port as i32
}

/// A connection to a kernel can take a while to be set up after it has been
/// accepted: the kernel's process is short of time, or the machine is. The
/// connection must be waited for, within the session's connection timeout,
/// not given up after a fraction of a second and made again.
///
/// A real kernel is reached here through forwarders that hold every
/// connection back for 600 ms, and adopted that way.
#[tokio::test]
async fn test_connection_that_is_slow_to_be_set_up_is_waited_for() {
    let result = tokio::time::timeout(Duration::from_secs(60), async {
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

        // A running kernel, and how to reach it
        let kernel_session_id = format!("connect-test-{}", Uuid::new_v4());
        create_session_with_client(
            &client,
            create_test_session(kernel_session_id.clone(), &python_cmd),
        )
        .await;
        let response = client
            .start_session(kernel_session_id.clone())
            .await
            .expect("start request failed");
        assert!(
            matches!(response, kallichore_api::StartSessionResponse::Started(_)),
            "expected the kernel to start, got {:?}",
            response
        );
        let kallichore_api::ConnectionInfoResponse::ConnectionInfo(direct) = client
            .connection_info(kernel_session_id.clone())
            .await
            .expect("connection info request failed")
        else {
            panic!("expected the kernel's connection information");
        };

        // The same kernel through the forwarders
        let delay = Duration::from_millis(600);
        let through_forwarders = models::ConnectionInfo {
            shell_port: slow_forwarder(direct.shell_port, delay).await,
            iopub_port: slow_forwarder(direct.iopub_port, delay).await,
            stdin_port: slow_forwarder(direct.stdin_port, delay).await,
            control_port: slow_forwarder(direct.control_port, delay).await,
            hb_port: slow_forwarder(direct.hb_port, delay).await,
            ..direct
        };

        let session_id = format!("connect-test-{}", Uuid::new_v4());
        let mut session = create_test_session(session_id.clone(), &python_cmd);
        session.connection_timeout = Some(8);
        create_session_with_client(&client, session).await;

        let response = client
            .adopt_session(session_id.clone(), through_forwarders)
            .await
            .expect("adopt request failed");
        assert!(
            matches!(response, kallichore_api::AdoptSessionResponse::Adopted(_)),
            "expected the kernel to be adopted, got {:?}",
            response
        );

        let _ = client.kill_session(kernel_session_id.clone()).await;
        drop(server);
    })
    .await;

    result.expect("test timed out");
}
