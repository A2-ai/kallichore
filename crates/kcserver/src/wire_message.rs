//
// wire_message.rs
//
// Copyright (C) 2024-2025 Posit Software, PBC. All rights reserved.
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//
//

/// Separates ZeroMQ socket identities from the message body payload.
pub const MSG_DELIM: &[u8] = b"<IDS|MSG>";

use bytes::Bytes;
use kcshared::jupyter_message::{JupyterChannel, JupyterMessage, JupyterMessageHeader};
use zeromq::ZmqMessage;

use crate::{kernel_connection::KernelConnection, wire_message_header::WireMessageHeader};

use base64::engine::Engine;
use hmac::Mac;
use sha2::Sha256;

pub struct WireMessage {
    /// The session ID the message is destined for
    pub session_id: String,

    /// The channel the message is destined for
    pub channel: JupyterChannel,

    /// The parts of the message, as an array of byte arrays
    pub parts: Vec<Vec<u8>>,
}

impl WireMessage {
    /// Create a new wire message from a Jupyter message.
    pub fn from_jupyter(
        msg: JupyterMessage,
        connection: KernelConnection,
    ) -> Result<Self, anyhow::Error> {
        let mut parts: Vec<Vec<u8>> = Vec::new();
        let username = connection.username.clone();
        let session = connection.session_id.clone();
        let hmac_key = connection.hmac_key.clone();

        // Derive a wire message header from the Jupyter message header
        let header = WireMessageHeader::new(
            msg.header,
            session.clone(),
            username.clone(),
            connection.protocol_version.clone(),
        );
        parts.push(serde_json::to_vec(&header)?);

        // Add the parent header, if any
        if msg.parent_header.is_some() {
            let parent_header = WireMessageHeader::new(
                msg.parent_header.unwrap(),
                session.clone(),
                username.clone(),
                connection.protocol_version.clone(),
            );
            parts.push(serde_json::to_vec(&parent_header)?);
        } else {
            parts.push(serde_json::to_vec(&serde_json::Map::new())?);
        }

        // Add the metadata
        parts.push(serde_json::to_vec(&msg.metadata)?);

        // Add the content
        parts.push(serde_json::to_vec(&msg.content)?);

        if let Some(hmac_key) = hmac_key {
            // If we have a key, compute the HMAC signature from all of the existing parts
            // and prepend it
            let mut signature = hmac_key.clone();
            for part in &parts {
                signature.update(part);
            }
            let signature = hex::encode(signature.finalize().into_bytes());
            parts.insert(0, signature.as_bytes().to_vec());
        } else {
            // No key, insert an empty signature
            parts.insert(0, Vec::new());
        }

        // Add the buffers
        // We do this here because we need to compute the HMAC signature first
        // https://jupyter-client.readthedocs.io/en/latest/messaging.html#the-wire-protocol
        for buf_str in msg.buffers.iter() {
            let decoded_buf = base64::engine::general_purpose::STANDARD.decode(buf_str)?;
            parts.push(decoded_buf);
        }

        Ok(WireMessage {
            session_id: connection.session_id.clone(),
            channel: msg.channel,
            parts,
        })
    }

    /// Convert the wire message to a Jupyter message with optional HMAC validation.
    pub fn to_jupyter(
        &self,
        channel: JupyterChannel,
        hmac_key: Option<hmac::Hmac<Sha256>>,
    ) -> Result<JupyterMessage, anyhow::Error> {
        let mut parts = self.parts.clone();
        let mut iter = self.parts.iter();
        let pos = match iter.position(|buf| &buf[..] == MSG_DELIM) {
            Some(pos) => pos,
            None => return Err(anyhow::anyhow!("No message delimiter found")),
        };
        let parts = parts.drain(pos + 1..).collect::<Vec<_>>();

        // After the delimiter come the signature, the header, the parent
        // header, the metadata and the content; buffers may follow
        if parts.len() < 5 {
            return Err(anyhow::anyhow!(
                "Message has {} parts after the delimiter; a message has at least 5",
                parts.len()
            ));
        }

        // Validate HMAC signature if a key is provided. A connection with a
        // key takes no unsigned message: an empty signature is what a sender
        // without the key would send.
        if let Some(key) = hmac_key {
            let signature_bytes = &parts[0];
            if signature_bytes.is_empty() {
                return Err(anyhow::anyhow!("Message is not signed"));
            }
            let signature = hex::decode(signature_bytes)
                .map_err(|_| anyhow::anyhow!("Invalid signature encoding"))?;

            // The signature is over parts 1-4 (header, parent_header, metadata, content)
            let mut expected_mac = key.clone();
            for part in parts.iter().skip(1).take(4) {
                expected_mac.update(part);
            }
            expected_mac
                .verify_slice(&signature)
                .map_err(|_| anyhow::anyhow!("HMAC signature validation failed"))?;
        }
        let header: WireMessageHeader = serde_json::from_value(Self::parse_buffer(&parts[1])?)?;
        let jupyter_header: JupyterMessageHeader = header.into();
        let parent_header: Option<JupyterMessageHeader> = if parts[2].len() < 5 {
            None
        } else {
            let header: WireMessageHeader = serde_json::from_value(Self::parse_buffer(&parts[2])?)?;
            Some(header.into())
        };

        // The remaining parts of the message are buffers; base64 encode their
        // contents for the Jupyter message
        let mut buffers = Vec::<String>::new();
        for (_i, buf) in parts.iter().enumerate().skip(5) {
            buffers.push(base64::engine::general_purpose::STANDARD.encode(buf));
        }

        Ok(JupyterMessage {
            header: jupyter_header,
            parent_header,
            metadata: serde_json::from_slice(&parts[3])?,
            content: serde_json::from_slice(&parts[4])?,
            channel,
            buffers,
        })
    }

    fn parse_buffer(buf: &[u8]) -> Result<serde_json::Value, anyhow::Error> {
        let contents = std::str::from_utf8(buf)?;
        let val = serde_json::from_str(contents)?;
        Ok(val)
    }

    pub fn from_zmq(session_id: String, channel: JupyterChannel, msg: ZmqMessage) -> Self {
        let parts: Vec<Vec<u8>> = msg.iter().map(|frame| frame.to_vec()).collect();
        Self {
            session_id,
            channel,
            parts,
        }
    }
}

impl Into<ZmqMessage> for WireMessage {
    fn into(self) -> ZmqMessage {
        let mut zmq_message = match self.channel {
            JupyterChannel::Shell | JupyterChannel::Stdin => {
                // The Shell and Stdin channels share a socket identity, which we derive from the session ID
                let mut message = ZmqMessage::from(Bytes::from(self.session_id));
                message.push_back(Bytes::from(MSG_DELIM));
                message
            }
            _ => ZmqMessage::from(MSG_DELIM.to_vec()),
        };
        for part in self.parts {
            zmq_message.push_back(Bytes::from(part));
        }
        zmq_message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hmac::Hmac;

    fn connection(key: &str) -> KernelConnection {
        let (key, hmac_key) = if key.is_empty() {
            (None, None)
        } else {
            (
                Some(key.to_string()),
                Some(Hmac::<Sha256>::new_from_slice(key.as_bytes()).unwrap()),
            )
        };
        KernelConnection {
            session_id: "test-session".to_string(),
            username: "test-user".to_string(),
            key,
            protocol_version: "5.3".to_string(),
            hmac_key,
        }
    }

    /// A message as it arrives from a socket: the delimiter, then the parts
    /// `from_jupyter` produces for a message sent on `sender`'s connection.
    fn received(sender: &KernelConnection) -> WireMessage {
        let message = JupyterMessage {
            header: JupyterMessageHeader {
                msg_id: "an-id".to_string(),
                msg_type: "status".to_string(),
            },
            parent_header: None,
            channel: JupyterChannel::IOPub,
            content: serde_json::json!({"execution_state": "idle"}),
            metadata: serde_json::json!({}),
            buffers: vec![],
        };
        let mut wire = WireMessage::from_jupyter(message, sender.clone()).unwrap();
        wire.parts.insert(0, MSG_DELIM.to_vec());
        wire
    }

    fn read(
        message: &WireMessage,
        receiver: &KernelConnection,
    ) -> Result<JupyterMessage, anyhow::Error> {
        message.to_jupyter(JupyterChannel::IOPub, receiver.hmac_key.clone())
    }

    #[test]
    fn a_message_signed_with_the_key_is_read() {
        let message = received(&connection("the-key"));
        let read = read(&message, &connection("the-key")).unwrap();
        assert_eq!(read.header.msg_type, "status");
    }

    #[test]
    fn a_message_signed_with_another_key_is_refused() {
        let message = received(&connection("another-key"));
        let error = read(&message, &connection("the-key")).unwrap_err();
        assert_eq!(error.to_string(), "HMAC signature validation failed");
    }

    #[test]
    fn an_unsigned_message_is_refused_on_a_connection_with_a_key() {
        let message = received(&connection(""));
        assert!(
            message.parts[1].is_empty(),
            "the message has an empty signature"
        );
        let error = read(&message, &connection("the-key")).unwrap_err();
        assert_eq!(error.to_string(), "Message is not signed");
    }

    #[test]
    fn an_unsigned_message_is_read_on_a_connection_without_a_key() {
        let message = received(&connection(""));
        assert!(read(&message, &connection("")).is_ok());
    }

    #[test]
    fn a_signature_that_is_not_hexadecimal_is_refused() {
        let mut message = received(&connection("the-key"));
        message.parts[1] = b"not hexadecimal".to_vec();
        let error = read(&message, &connection("the-key")).unwrap_err();
        assert_eq!(error.to_string(), "Invalid signature encoding");
    }

    #[test]
    fn a_message_with_too_few_parts_is_refused() {
        for receiver in [connection("the-key"), connection("")] {
            let mut message = received(&receiver);
            message.parts.truncate(4);
            let error = read(&message, &receiver).unwrap_err();
            assert!(
                error.to_string().starts_with("Message has 3 parts"),
                "{}",
                error
            );
        }
    }
}
