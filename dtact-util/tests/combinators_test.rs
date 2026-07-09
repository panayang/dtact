//! Exercises `io::{AsyncRead, AsyncWrite, BufReader, BufWriter, copy}`
//! against real TCP streams on both backends.

#[cfg(feature = "native")]
mod native_tests {
    use dtact_util::io::{
        BufReader, BufWriter, DtactTcpListener, DtactTcpStream, copy, init_runtime,
    };
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    static INIT: std::sync::Once = std::sync::Once::new();
    fn ensure_runtime() {
        INIT.call_once(|| {
            init_runtime(2, 128, 1024, 4096, &[]);
        });
    }

    #[dtact::dtact_init(workers = 4, capacity = 2048, safety = "Safety1")]
    #[test]
    fn buf_reader_and_buf_writer_roundtrip() {
        ensure_runtime();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let local_addr = listener.local_addr().unwrap();
        let dtact_listener = DtactTcpListener::from_std(listener).unwrap();

        let server_done = Arc::new(AtomicU32::new(0));
        let server_done2 = server_done.clone();
        dtact::spawn(async move {
            let (stream, _addr) = dtact_listener.accept().await.unwrap();
            // Small writes on purpose — BufWriter should coalesce them.
            let mut w = BufWriter::with_capacity(64, stream);
            for chunk in [b"hello " as &[u8], b"buffered ", b"world"] {
                w.write(chunk).await.unwrap();
            }
            w.flush().await.unwrap();
            server_done2.store(1, Ordering::SeqCst);
        });

        let client_done = Arc::new(AtomicU32::new(0));
        let client_done2 = client_done.clone();
        dtact::spawn(async move {
            let stream = DtactTcpStream::connect(local_addr).await.unwrap();
            let mut r = BufReader::with_capacity(4, stream); // tiny, forces refills
            let mut buf = [0u8; 4];
            let mut collected = Vec::new();
            let expected = b"hello buffered world";
            while collected.len() < expected.len() {
                let n = r.read(&mut buf).await.unwrap();
                assert_ne!(n, 0, "peer closed before sending everything");
                collected.extend_from_slice(&buf[..n]);
            }
            assert_eq!(collected, expected);
            client_done2.store(1, Ordering::SeqCst);
        });

        for _ in 0..100 {
            if server_done.load(Ordering::SeqCst) == 1 && client_done.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert_eq!(server_done.load(Ordering::SeqCst), 1);
        assert_eq!(client_done.load(Ordering::SeqCst), 1);
    }

    /// `copy()` end to end: a source connection that sends a payload then
    /// closes (EOF), copied live into a sink connection whose peer reads
    /// until its own EOF and checks the bytes match.
    #[test]
    fn copy_moves_all_bytes_and_reports_eof() {
        dtact_autostart();
        ensure_runtime();

        let payload = vec![0xABu8; 100_000];

        let src_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let src_addr = src_listener.local_addr().unwrap();
        let dtact_src_listener = DtactTcpListener::from_std(src_listener).unwrap();
        let payload_for_source = payload.clone();
        dtact::spawn(async move {
            let (stream, _addr) = dtact_src_listener.accept().await.unwrap();
            let mut sent = 0;
            while sent < payload_for_source.len() {
                sent += stream.write(&payload_for_source[sent..]).await.unwrap();
            }
            // `stream` drops here, closing the socket — the reading side
            // of the copy below observes this as EOF (a 0-byte read).
        });

        let sink_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let sink_addr = sink_listener.local_addr().unwrap();
        let dtact_sink_listener = DtactTcpListener::from_std(sink_listener).unwrap();
        let done = Arc::new(AtomicU32::new(0));
        let done2 = done.clone();
        let payload_for_sink = payload.clone();
        dtact::spawn(async move {
            let (stream, _addr) = dtact_sink_listener.accept().await.unwrap();
            let mut collected = Vec::with_capacity(payload_for_sink.len());
            let mut buf = [0u8; 4096];
            loop {
                let n = stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                collected.extend_from_slice(&buf[..n]);
            }
            assert_eq!(collected, payload_for_sink);
            done2.store(1, Ordering::SeqCst);
        });

        dtact::spawn(async move {
            let source = DtactTcpStream::connect(src_addr).await.unwrap();
            let sink = DtactTcpStream::connect(sink_addr).await.unwrap();
            let n = copy(&source, &sink).await.unwrap();
            assert_eq!(n, 100_000);
            // `sink` drops here, closing the socket — the sink-listener's
            // accepted stream observes this as its own EOF.
        });

        for _ in 0..100 {
            if done.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert_eq!(done.load(Ordering::SeqCst), 1);
    }
}

#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_tests {
    use dtact_util::io::{
        BufReader, BufWriter, DtactTcpListener, DtactTcpStream, copy, get_runtime_handle,
        init_runtime,
    };
    use std::net::TcpListener;

    #[test]
    fn buf_reader_and_buf_writer_roundtrip() {
        init_runtime(2, 0, 0, 0, &[]);
        get_runtime_handle().block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let local_addr = listener.local_addr().unwrap();
            let dtact_listener = DtactTcpListener::from_std(listener).unwrap();

            let server = tokio::spawn(async move {
                let (stream, _addr) = dtact_listener.accept().await.unwrap();
                let mut w = BufWriter::with_capacity(64, stream);
                for chunk in [b"hello " as &[u8], b"buffered ", b"world"] {
                    w.write(chunk).await.unwrap();
                }
                w.flush().await.unwrap();
            });

            let stream = DtactTcpStream::connect(local_addr).await.unwrap();
            let mut r = BufReader::with_capacity(4, stream);
            let mut buf = [0u8; 4];
            let mut collected = Vec::new();
            let expected = b"hello buffered world";
            while collected.len() < expected.len() {
                let n = r.read(&mut buf).await.unwrap();
                assert_ne!(n, 0, "peer closed before sending everything");
                collected.extend_from_slice(&buf[..n]);
            }
            assert_eq!(collected, expected);
            server.await.unwrap();
        });
    }

    #[test]
    fn copy_moves_all_bytes_and_reports_eof() {
        init_runtime(2, 0, 0, 0, &[]);
        get_runtime_handle().block_on(async {
            let payload = vec![0xABu8; 100_000];

            let src_listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let src_addr = src_listener.local_addr().unwrap();
            let dtact_src_listener = DtactTcpListener::from_std(src_listener).unwrap();
            let payload_for_source = payload.clone();
            let source_task = tokio::spawn(async move {
                let (stream, _addr) = dtact_src_listener.accept().await.unwrap();
                let mut sent = 0;
                while sent < payload_for_source.len() {
                    sent += stream.write(&payload_for_source[sent..]).await.unwrap();
                }
            });

            let sink_listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let sink_addr = sink_listener.local_addr().unwrap();
            let dtact_sink_listener = DtactTcpListener::from_std(sink_listener).unwrap();
            let payload_for_sink = payload.clone();
            let sink_task = tokio::spawn(async move {
                let (stream, _addr) = dtact_sink_listener.accept().await.unwrap();
                let mut collected = Vec::with_capacity(payload_for_sink.len());
                let mut buf = [0u8; 4096];
                loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    collected.extend_from_slice(&buf[..n]);
                }
                assert_eq!(collected, payload_for_sink);
            });

            let source = DtactTcpStream::connect(src_addr).await.unwrap();
            let sink = DtactTcpStream::connect(sink_addr).await.unwrap();
            let n = copy(&source, &sink).await.unwrap();
            assert_eq!(n, 100_000);
            drop(sink);

            source_task.await.unwrap();
            sink_task.await.unwrap();
        });
    }
}
