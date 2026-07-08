//! Exercises the `tokio` backend's raw-fd `DtactIoFuture`/`OpCode`, awaited
//! via `.compat()` (`DtactCompatExt`) — both are `tokio`-backend-specific
//! (`.compat()` doesn't exist on the `native` backend's `DtactIoFuture`
//! at all), and Unix-only (wraps `AsyncFd<RawFd>`). Windows support for the
//! `tokio` backend is provided at the higher `DtactTcpStream`/
//! `DtactTcpListener` level instead.
#![cfg(all(unix, not(feature = "native")))]

use dtact_util::{DtactCompatExt, DtactIoFuture, OpCode, init_runtime, shutdown_runtime};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

#[dtact::dtact_init(workers = 4, capacity = 2048, safety = "Safety1")]
#[test]
fn test_io_future_complex() {
    // Initialize dtact_io runtime
    init_runtime(2, 1024, 4096, &[], 128);

    // Helper inside the test to convert SocketAddr to libc sockaddr
    fn to_libc_addr(addr: std::net::SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let len = match addr {
            std::net::SocketAddr::V4(a) => {
                let sin = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: a.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(a.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &sin as *const libc::sockaddr_in as *const u8,
                        &mut storage as *mut libc::sockaddr_storage as *mut u8,
                        std::mem::size_of::<libc::sockaddr_in>(),
                    );
                }
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
            }
            std::net::SocketAddr::V6(a) => {
                let sin6 = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: a.port().to_be(),
                    sin6_flowinfo: a.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: a.ip().octets(),
                    },
                    sin6_scope_id: a.scope_id(),
                };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &sin6 as *const libc::sockaddr_in6 as *const u8,
                        &mut storage as *mut libc::sockaddr_storage as *mut u8,
                        std::mem::size_of::<libc::sockaddr_in6>(),
                    );
                }
                std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
            }
        };
        (storage, len)
    }

    // Create a non-blocking TCP listener
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener_fd = listener.as_raw_fd();
    let local_addr = listener.local_addr().unwrap();

    let server_finished = Arc::new(AtomicU32::new(0));
    let server_finished_clone = server_finished.clone();

    // Spawn server fiber using DtactIoFuture and DtactCompatExt (.compat())
    dtact::spawn(async move {
        println!("Server Fiber: waiting for connection via DtactIoFuture...");
        // Construct the Accept DtactIoFuture
        let accept_fut = DtactIoFuture::new(
            0,
            listener_fd as u32,
            u32::MAX,
            OpCode::Accept,
            std::ptr::null_mut(),
            0,
            0,
            None,
            0,
            None,
        );

        // Wrap the future in DtactCompat and await it
        let client_fd = accept_fut.compat().await.expect("Accept failed") as i32;
        println!("Server Fiber: accepted connection with fd {}", client_fd);

        // Read message from client using DtactIoFuture
        let mut read_buf = [0u8; 64];
        let read_fut = DtactIoFuture::new(
            0,
            client_fd as u32,
            u32::MAX,
            OpCode::Read,
            read_buf.as_mut_ptr(),
            read_buf.len(),
            0,
            None,
            0,
            None,
        );
        let n = read_fut.compat().await.expect("Read failed");
        println!("Server Fiber: read {} bytes: {:?}", n, &read_buf[..n]);
        assert_eq!(&read_buf[..n], b"hello from client future");

        // Write response back to client using DtactIoFuture
        let resp = b"hello from server future";
        let write_fut = DtactIoFuture::new(
            0,
            client_fd as u32,
            u32::MAX,
            OpCode::Write,
            resp.as_ptr() as *mut u8,
            resp.len(),
            0,
            None,
            0,
            None,
        );
        let w = write_fut.compat().await.expect("Write failed");
        println!("Server Fiber: wrote {} bytes", w);
        assert_eq!(w, resp.len());

        // Close client fd
        unsafe {
            libc::close(client_fd);
        }

        server_finished_clone.store(1, Ordering::SeqCst);
    });

    let client_finished = Arc::new(AtomicU32::new(0));
    let client_finished_clone = client_finished.clone();

    // Spawn client fiber
    dtact::spawn(async move {
        println!("Client Fiber: connecting to {}...", local_addr);
        // Create client raw socket
        let client_fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
        assert!(client_fd >= 0);
        // Set non-blocking
        let flags = unsafe { libc::fcntl(client_fd, libc::F_GETFL, 0) };
        unsafe {
            libc::fcntl(client_fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        let (libc_addr, addr_len) = to_libc_addr(local_addr);

        // Connect future
        let connect_fut = DtactIoFuture::new(
            0,
            client_fd as u32,
            u32::MAX,
            OpCode::Connect,
            std::ptr::null_mut(),
            0,
            0,
            Some(libc_addr),
            addr_len,
            None,
        );
        connect_fut.compat().await.expect("Connect failed");
        println!("Client Fiber: connected successfully with fd {}", client_fd);

        // Write message
        let msg = b"hello from client future";
        let write_fut = DtactIoFuture::new(
            0,
            client_fd as u32,
            u32::MAX,
            OpCode::Write,
            msg.as_ptr() as *mut u8,
            msg.len(),
            0,
            None,
            0,
            None,
        );
        let w = write_fut.compat().await.expect("Write failed");
        assert_eq!(w, msg.len());

        // Read response
        let mut read_buf = [0u8; 64];
        let read_fut = DtactIoFuture::new(
            0,
            client_fd as u32,
            u32::MAX,
            OpCode::Read,
            read_buf.as_mut_ptr(),
            read_buf.len(),
            0,
            None,
            0,
            None,
        );
        let n = read_fut.compat().await.expect("Read failed");
        println!(
            "Client Fiber: read response {} bytes: {:?}",
            n,
            &read_buf[..n]
        );
        assert_eq!(&read_buf[..n], b"hello from server future");

        // Close client fd
        unsafe {
            libc::close(client_fd);
        }

        client_finished_clone.store(1, Ordering::SeqCst);
    });

    // Wait for completion
    for i in 0..100 {
        if server_finished.load(Ordering::SeqCst) == 1
            && client_finished.load(Ordering::SeqCst) == 1
        {
            println!("Both futures finished on iteration {}", i);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    assert_eq!(server_finished.load(Ordering::SeqCst), 1);
    assert_eq!(client_finished.load(Ordering::SeqCst), 1);

    // Clean up
    shutdown_runtime();
}
