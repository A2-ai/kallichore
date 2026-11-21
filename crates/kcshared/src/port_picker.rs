//
// port_picker.rs
//
// Copyright (C) 2024-2025 Posit Software, PBC. All rights reserved.
// Licensed under the Elastic License 2.0. See LICENSE.txt for license information.
//
//

use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, TcpListener, ToSocketAddrs,
};

// Try to bind to a socket using TCP
fn test_bind_tcp<A: ToSocketAddrs>(addr: A) -> Option<u16> {
    Some(TcpListener::bind(addr).ok()?.local_addr().ok()?.port())
}

/// Asks the OS for a free port.
///
/// Tries to bind to both IPv4 and IPv6 addresses with port 0, which lets the OS
/// pick an available port.  Returns the port number if successful, or None if
/// no port is available.
fn ask_free_tcp_port() -> Option<u16> {
    let ipv4 = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0);
    let ipv6 = SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0);

    test_bind_tcp(ipv6).or_else(|| test_bind_tcp(ipv4))
}

/// Picks an available TCP port
pub fn pick_unused_tcp_port() -> Option<u16> {
    // Try up to 5 times to find a free port
    for _ in 0..5 {
        if let Some(port) = ask_free_tcp_port() {
            return Some(port);
        }
    }
    None
}

/// Picks a TCP port that is available on the given address.
///
/// A port is only free on the address it was tested on. A port the OS gives
/// out for the unspecified address can be one that another process is
/// listening on at a specific address such as 127.0.0.1 (macOS does this), so
/// when the address the port will be bound on is known, ask for a port there.
pub fn pick_unused_tcp_port_on(ip: IpAddr) -> Option<u16> {
    // Try up to 5 times to find a free port
    for _ in 0..5 {
        if let Some(port) = test_bind_tcp(SocketAddr::new(ip, 0)) {
            return Some(port);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A port picked on an address is not one that already has a listener on
    /// that address.
    ///
    /// Picks more ports than the ephemeral range holds, so that an allocator
    /// that hands ports out in sequence comes round to the listeners' ports.
    #[test]
    fn picks_a_port_with_no_listener_on_the_address() {
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let listeners: Vec<TcpListener> = (0..32)
            .map(|_| TcpListener::bind(SocketAddr::new(ip, 0)).unwrap())
            .collect();
        let held: Vec<u16> = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().port())
            .collect();

        for _ in 0..70_000 {
            let port = pick_unused_tcp_port_on(ip).unwrap();
            assert!(
                !held.contains(&port),
                "picked port {}, which has a listener on {}",
                port,
                ip
            );
        }
    }
}
