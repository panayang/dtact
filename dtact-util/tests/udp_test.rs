//! Exercises `DtactUdpSocket` on both backends: bind two sockets, send a
//! datagram between them, and verify the payload and peer address round-trip
//! correctly. The native path uses the platform IOCP/io_uring/kqueue driver,
//! the tokio path wraps `tokio::net::UdpSocket`.

#[cfg(feature = "native")]
mod native_tests {
    use dtact_util::io::{DtactUdpSocket, init_runtime};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    #[dtact::dtact_init(workers = 4, capacity = 2048, safety = "Safety1")]
    #[test]
    fn udp_send_recv_roundtrip() {
        init_runtime(2, 128, 1024, 4096, &[]);

        let done = Arc::new(AtomicU32::new(0));
        let received: Arc<Mutex<Option<(Vec<u8>, std::net::SocketAddr)>>> =
            Arc::new(Mutex::new(None));

        let done_srv = done.clone();
        let received_srv = received.clone();

        // Bind the receiver up front so we know its address before spawning
        // the sender.
        let server_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();

        // Server fiber: bind, publish its address, then recv one datagram.
        let bound_addr: Arc<Mutex<Option<std::net::SocketAddr>>> = Arc::new(Mutex::new(None));
        let bound_addr_srv = bound_addr.clone();

        dtact::spawn(async move {
            let sock = DtactUdpSocket::bind(server_addr).await.unwrap();
            *bound_addr_srv.lock().unwrap() = Some(sock.local_addr().unwrap());

            let mut buf = [0u8; 64];
            let (n, from) = sock.recv_from(&mut buf).await.unwrap();
            *received_srv.lock().unwrap() = Some((buf[..n].to_vec(), from));
            done_srv.store(1, Ordering::SeqCst);
        });

        // Wait for the server to publish its bound address.
        let target = loop {
            if let Some(a) = *bound_addr.lock().unwrap() {
                break a;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };

        let done_cli = done.clone();
        let sender_addr: Arc<Mutex<Option<std::net::SocketAddr>>> = Arc::new(Mutex::new(None));
        let sender_addr_cli = sender_addr.clone();
        dtact::spawn(async move {
            let sock = DtactUdpSocket::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            *sender_addr_cli.lock().unwrap() = Some(sock.local_addr().unwrap());
            // Retry a few times: UDP is lossy and the receiver may not have
            // its recv posted yet on the very first send.
            for _ in 0..20 {
                let _ = sock.send_to(b"hello dtact udp", target).await;
                if done_cli.load(Ordering::SeqCst) == 1 {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        });

        for _ in 0..200 {
            if done.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(25));
        }

        assert_eq!(done.load(Ordering::SeqCst), 1, "server never received");
        let guard = received.lock().unwrap();
        let (payload, from) = guard.as_ref().expect("no datagram recorded");
        assert_eq!(payload.as_slice(), b"hello dtact udp");
        let expected_sender = sender_addr.lock().unwrap().unwrap();
        assert_eq!(*from, expected_sender, "peer address mismatch");

        dtact_util::io::shutdown_runtime();
    }
}

#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_tests {
    use dtact_util::io::{DtactUdpSocket, get_runtime_handle, init_runtime};

    #[test]
    fn udp_send_recv_roundtrip() {
        init_runtime(2, 0, 0, &[], 0);
        get_runtime_handle().block_on(async {
            let a = DtactUdpSocket::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let b = DtactUdpSocket::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let a_addr = a.local_addr().unwrap();
            let b_addr = b.local_addr().unwrap();

            a.send_to(b"hello dtact udp", b_addr).await.unwrap();
            let mut buf = [0u8; 64];
            let (n, from) = b.recv_from(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"hello dtact udp");
            assert_eq!(from, a_addr);
        });
    }

    #[test]
    fn udp_connected_send_recv() {
        init_runtime(2, 0, 0, &[], 0);
        get_runtime_handle().block_on(async {
            let a = DtactUdpSocket::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let b = DtactUdpSocket::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let a_addr = a.local_addr().unwrap();
            let b_addr = b.local_addr().unwrap();

            a.connect(b_addr).await.unwrap();
            b.connect(a_addr).await.unwrap();

            a.send(b"ping").await.unwrap();
            let mut buf = [0u8; 16];
            let n = b.recv(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"ping");
        });
    }
}
