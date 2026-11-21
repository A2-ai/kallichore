//
// connection_file.rs
//
// Copyright (C) 2024-2025 Posit Software, PBC. All rights reserved.
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//
//

use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;

use kallichore_api::models::ConnectionInfo;
use kcshared::handshake_protocol::HandshakeVersion;
use serde::{Deserialize, Serialize};

use kcshared::port_picker::{pick_unused_tcp_port, pick_unused_tcp_port_on};

/// The ports reserved for one kernel that is about to be started.
///
/// The server picks a kernel's ports before the kernel binds them, and keeps
/// a list of the ports it has picked, so that two kernels that are starting
/// are not given the same port. A reservation holds the ports picked for one
/// kernel, and takes exactly those off the list when it is dropped. Whoever
/// holds it decides how long that is; however a start, or the proxy that
/// follows it, comes to its end, the ports come off the list.
#[derive(Debug)]
pub struct PortReservation {
    /// The ports this reservation added to the list
    ports: Vec<i32>,

    /// The list of reserved ports, shared by all kernel sessions
    reserved_ports: Arc<RwLock<Vec<i32>>>,
}

impl PortReservation {
    fn new(reserved_ports: Arc<RwLock<Vec<i32>>>) -> Self {
        Self {
            ports: Vec::new(),
            reserved_ports,
        }
    }

    /// Reserve a port, unless it is reserved already.
    ///
    /// The list is checked and added to under one lock: between a check and
    /// a later addition, another start could reserve the same port.
    fn reserve(&mut self, port: i32) -> bool {
        let mut reserved_ports = self.reserved_ports.write().unwrap();
        if reserved_ports.contains(&port) {
            return false;
        }
        reserved_ports.push(port);
        self.ports.push(port);
        true
    }
}

impl Drop for PortReservation {
    fn drop(&mut self) {
        if self.ports.is_empty() {
            return;
        }
        // A lock poisoned by a panic elsewhere still guards the list
        let mut reserved_ports = match self.reserved_ports.write() {
            Ok(reserved_ports) => reserved_ports,
            Err(poisoned) => poisoned.into_inner(),
        };
        reserved_ports.retain(|port| !self.ports.contains(port));
        log::trace!(
            "Released {} reserved ports; there are now {} reserved ports",
            self.ports.len(),
            reserved_ports.len()
        );
    }
}

/// The contents of the Connection File as listed in the Jupyter specfication;
/// directly parsed from JSON.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectionFile {
    pub info: ConnectionInfo,
}

impl ConnectionFile {
    /// Create a ConnectionFile from a ConnectionInfo struct.
    pub fn from_info(info: ConnectionInfo) -> Self {
        Self { info }
    }

    /// Create a ConnectionFile by parsing the contents of a connection file.
    #[allow(dead_code)]
    pub fn from_file<P: AsRef<Path>>(connection_file: P) -> Result<Self, Box<dyn Error>> {
        let file = File::open(connection_file)?;
        let reader = BufReader::new(file);
        let control = serde_json::from_reader(reader)?;

        Ok(control)
    }

    /// Write the connection file to disk, readable only by the user the
    /// server runs as: it holds the kernel's signing key.
    pub fn to_file<P: AsRef<Path>>(&self, connection_file: P) -> Result<(), Box<dyn Error>> {
        let file = crate::private_file::create(connection_file)?;
        serde_json::to_writer_pretty(file, &self.info)?;
        Ok(())
    }

    /// Find a free port that is not in the reserved list, and reserve it.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the port to find. This is used for logging.
    /// * `ip` - The address the kernel will bind the port on.
    /// * `reservation` - The reservation the port is added to.
    fn find_port(
        name: String,
        ip: &str,
        reservation: &mut PortReservation,
    ) -> Result<u16, anyhow::Error> {
        // Ask for the port on the address the kernel will bind, so that a
        // port another process is listening on at that address is not picked.
        // An `ip` that is not an IP address is tested on the unspecified one.
        let ip: Option<std::net::IpAddr> = ip.parse().ok();

        // The number of times we've tried to find an unused, unreserved port
        let mut tries = 0;

        loop {
            // Find a free port
            let picked = match ip {
                Some(ip) => pick_unused_tcp_port_on(ip),
                None => pick_unused_tcp_port(),
            };
            let candidate = match picked {
                Some(port) => port,
                None => {
                    return Err(anyhow::anyhow!(
                        "Failed to pick {} port; no free ports available or port range exhausted",
                        name
                    ));
                }
            };

            // Reserve the port, unless it is reserved already
            if reservation.reserve(candidate.into()) {
                log::trace!("Picked {} port: {}", name, candidate);
                return Ok(candidate);
            }

            // Try up to 10 times to find an unreserved port. Since we're
            // picking from a large range of ports, hitting a previously
            // reserved port is unlikely, but possible. If it happens 10 times
            // in a row, something is probably wrong.
            tries += 1;
            if tries > 10 {
                return Err(anyhow::anyhow!(
                    "Failed to pick unreserved {} port after 10 tries",
                    name
                ));
            }
            log::trace!(
                "Port {} is reserved; trying again (attempt {})",
                candidate,
                tries
            );
        }
    }

    /// Generate a new ConnectionFile by picking free ports.
    ///
    /// Returns the connection file and the reservation of its ports. The
    /// ports stay reserved for as long as the reservation is kept. If a port
    /// cannot be found, the ports found before it are released.
    ///
    /// # Arguments
    ///
    /// * `ip` - The IP address to bind to
    /// * `reserved_ports` - A list of ports that should not be used. These are
    /// generally ports that are already in use by other running kernels, or
    /// have been reserved for use by another kernel that's also starting up.
    /// * `key` - The HMAC-256 signing key to use for messages
    pub fn generate(
        ip: String,
        reserved_ports: Arc<RwLock<Vec<i32>>>,
        key: Option<String>,
    ) -> Result<(Self, PortReservation), anyhow::Error> {
        let mut reservation = PortReservation::new(reserved_ports);
        let control_port =
            ConnectionFile::find_port(String::from("control"), &ip, &mut reservation)?;
        let shell_port = ConnectionFile::find_port(String::from("shell"), &ip, &mut reservation)?;
        let iopub_port = ConnectionFile::find_port(String::from("iopub"), &ip, &mut reservation)?;
        let hb_port = ConnectionFile::find_port(String::from("heartbeat"), &ip, &mut reservation)?;
        let stdin_port = ConnectionFile::find_port(String::from("stdin"), &ip, &mut reservation)?;
        let info = ConnectionInfo {
            control_port: control_port.into(),
            shell_port: shell_port.into(),
            stdin_port: stdin_port.into(),
            iopub_port: iopub_port.into(),
            hb_port: hb_port.into(),
            transport: "tcp".to_string(),
            signature_scheme: "hmac-sha256".to_string(),
            key: match key {
                Some(key) => key,
                None => String::new(),
            },
            ip,
        };
        log::trace!(
            "Reserved {} ports for a kernel; there are now {} reserved ports",
            reservation.ports.len(),
            reservation.reserved_ports.read().unwrap().len()
        );
        Ok((Self { info }, reservation))
    }

    /// Given a port, return a URI-like string that can be used to connect to
    /// the port, given the other parameters in the connection file.
    ///
    /// Example: `32` => `"tcp://127.0.0.1:32"`
    #[allow(dead_code)]
    pub fn endpoint(&self, port: u16) -> String {
        format!("{}://{}:{}", self.info.transport, self.info.ip, port)
    }

    /// Checks whether a protocol version requires JEP 66 handshaking
    /// i.e., if it's version 5.5 or higher
    pub fn requires_handshaking(protocol_version: &str) -> bool {
        HandshakeVersion::supports_handshaking(protocol_version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ports_of(file: &ConnectionFile) -> Vec<i32> {
        let mut ports = vec![
            file.info.control_port,
            file.info.shell_port,
            file.info.iopub_port,
            file.info.hb_port,
            file.info.stdin_port,
        ];
        ports.sort();
        ports
    }

    fn sorted(reserved_ports: &Arc<RwLock<Vec<i32>>>) -> Vec<i32> {
        let mut ports = reserved_ports.read().unwrap().clone();
        ports.sort();
        ports
    }

    #[test]
    fn a_reservation_releases_its_own_ports_and_no_others() {
        let reserved_ports = Arc::new(RwLock::new(Vec::new()));
        let (first, first_reservation) =
            ConnectionFile::generate("127.0.0.1".to_string(), reserved_ports.clone(), None)
                .unwrap();
        let (second, second_reservation) =
            ConnectionFile::generate("127.0.0.1".to_string(), reserved_ports.clone(), None)
                .unwrap();
        assert_eq!(reserved_ports.read().unwrap().len(), 10);

        drop(first_reservation);
        assert_eq!(sorted(&reserved_ports), ports_of(&second));
        assert!(ports_of(&first)
            .iter()
            .all(|port| !ports_of(&second).contains(port)));

        drop(second_reservation);
        assert!(reserved_ports.read().unwrap().is_empty());
    }

    #[test]
    fn a_port_is_reserved_by_one_reservation_at_a_time() {
        let reserved_ports = Arc::new(RwLock::new(Vec::new()));
        let mut first = PortReservation::new(reserved_ports.clone());
        let mut second = PortReservation::new(reserved_ports.clone());

        assert!(first.reserve(40001));
        assert!(!second.reserve(40001));
        assert!(second.reserve(40002));

        // The second reservation did not get port 40001, so it does not
        // release it either
        drop(second);
        assert_eq!(sorted(&reserved_ports), vec![40001]);

        drop(first);
        let mut third = PortReservation::new(reserved_ports.clone());
        assert!(third.reserve(40001));
    }

    #[test]
    fn a_search_that_finds_no_port_reserves_nothing() {
        // Every port the system can offer on this address is "reserved" by
        // another kernel, so no port can be found for the first of the five
        let reserved_ports = Arc::new(RwLock::new((1..=65535).collect::<Vec<i32>>()));
        let result =
            ConnectionFile::generate("127.0.0.1".to_string(), reserved_ports.clone(), None);
        assert!(result.is_err());
        assert_eq!(reserved_ports.read().unwrap().len(), 65535);
    }
}
