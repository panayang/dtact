use dtact_util::io::{DtactTcpListener, DtactTcpStream, init_runtime};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

#[dtact::dtact_init(workers = 4, capacity = 2048, safety = "Safety1")]
#[test]
fn test_io_driver_tcp() {
    // Initialize dtact_io runtime with 2 workers, 1024 buffers, chunk size 4096, and ring depth 128
    init_runtime(2, 1024, 4096, &[], 128);

    // Bind a listener
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let local_addr = listener.local_addr().unwrap();
    println!("Bound std TcpListener to {}", local_addr);
    let dtact_listener = DtactTcpListener::from_std(listener).unwrap();
    println!("Converted to DtactTcpListener successfully");

    let server_finished = Arc::new(AtomicU32::new(0));
    let server_finished_clone = server_finished.clone();

    // Spawn server accept loop fiber
    dtact::spawn(async move {
        println!("Server: starting accept");
        let (stream, client_addr) = dtact_listener.accept().await.unwrap();
        println!("Server: accepted connection from {}", client_addr);

        // Write message
        let msg = b"hello from dtact-io server";
        let mut written = 0;
        while written < msg.len() {
            println!("Server: writing bytes...");
            let n = stream.write(&msg[written..]).await.unwrap();
            println!("Server: wrote {} bytes", n);
            written += n;
        }

        println!("Server: finished successfully");
        server_finished_clone.store(1, Ordering::SeqCst);
    });

    // Spawn client fiber
    let client_finished = Arc::new(AtomicU32::new(0));
    let client_finished_clone = client_finished.clone();

    dtact::spawn(async move {
        println!("Client: connecting to {}", local_addr);
        // Connect stream
        let stream = DtactTcpStream::connect(local_addr).await.unwrap();
        println!("Client: connected successfully");

        // Read message
        let mut buf = [0u8; 100];
        let mut total_read = 0;
        while total_read < 26 {
            println!("Client: reading bytes...");
            let n = stream.read(&mut buf[total_read..]).await.unwrap();
            println!("Client: read {} bytes", n);
            if n == 0 {
                break;
            }
            total_read += n;
        }

        println!(
            "Client: read total {} bytes: {:?}",
            total_read,
            String::from_utf8_lossy(&buf[0..total_read])
        );
        assert_eq!(&buf[0..26], b"hello from dtact-io server");
        client_finished_clone.store(1, Ordering::SeqCst);
    });

    // Wait for completion
    for i in 0..100 {
        if server_finished.load(Ordering::SeqCst) == 1
            && client_finished.load(Ordering::SeqCst) == 1
        {
            println!("Both finished on iteration {}", i);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    assert_eq!(server_finished.load(Ordering::SeqCst), 1);
    assert_eq!(client_finished.load(Ordering::SeqCst), 1);

    // Clean up
    dtact_util::io::shutdown_runtime();
}
