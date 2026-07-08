//! Integration tests driving the `ffi` C surface directly via `unsafe`
//! `extern "C"` calls: a happy path for each of the six primitives, a
//! null-pointer misuse guard, and the thread-local last-error path
//! surfacing a real message.

#![cfg(feature = "ffi")]

use dtact_util::ffi::dtact_util_last_error_message;
use dtact_util::ffi::fs::*;
use dtact_util::ffi::io::*;
use dtact_util::ffi::process::*;
use dtact_util::ffi::signal::*;
use dtact_util::ffi::stream::*;
use dtact_util::ffi::timer::*;
use std::ffi::{CStr, CString};

fn last_error() -> Option<String> {
    let p = unsafe { dtact_util_last_error_message() };
    if p.is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
    }
}

#[test]
fn timer_sleep_and_interval() {
    unsafe {
        dtact_util_timer_sleep_ms(2);

        let iv = dtact_util_timer_interval_create(2);
        assert!(!iv.is_null());
        dtact_util_timer_interval_tick(iv);
        dtact_util_timer_interval_tick(iv);
        dtact_util_timer_interval_free(iv);

        // Zero-period is an error, not a crash.
        let bad = dtact_util_timer_interval_create(0);
        assert!(bad.is_null());
        assert!(last_error().is_some());
    }
}

#[test]
fn stream_roundtrip() {
    unsafe {
        let mut a = std::ptr::null_mut();
        let mut b = std::ptr::null_mut();
        assert_eq!(dtact_util_stream_pair_create(64, &mut a, &mut b), 0);
        assert!(!a.is_null() && !b.is_null());

        let msg = b"hello ffi";
        let n = dtact_util_stream_write(a, msg.as_ptr(), msg.len());
        assert_eq!(n, msg.len() as isize);

        let mut buf = [0u8; 16];
        let got = dtact_util_stream_read(b, buf.as_mut_ptr(), buf.len());
        assert_eq!(got, msg.len() as isize);
        assert_eq!(&buf[..got as usize], msg);

        dtact_util_stream_free(a);
        dtact_util_stream_free(b);
    }
}

#[test]
fn fs_create_write_read() {
    unsafe {
        dtact_util_fs_init(2);
        let dir = std::env::temp_dir().join(format!("dtact-ffi-fs-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("ffi.txt");
        let cpath = CString::new(path.to_str().unwrap()).unwrap();

        let f = dtact_util_fs_file_create(cpath.as_ptr());
        assert!(!f.is_null(), "{:?}", last_error());
        let data = b"ffi-fs-payload";
        let w = dtact_util_fs_file_write(f, data.as_ptr(), data.len());
        assert_eq!(w, data.len() as isize);
        assert_eq!(dtact_util_fs_file_sync(f), 0);
        dtact_util_fs_file_close(f);

        let f2 = dtact_util_fs_file_open(cpath.as_ptr());
        assert!(!f2.is_null(), "{:?}", last_error());
        let mut buf = [0u8; 32];
        let r = dtact_util_fs_file_read(f2, buf.as_mut_ptr(), buf.len());
        assert_eq!(r, data.len() as isize);
        assert_eq!(&buf[..r as usize], data);
        dtact_util_fs_file_close(f2);

        let _ = std::fs::remove_file(&path);
    }
}

#[test]
fn io_tcp_echo() {
    unsafe {
        dtact_util_io_init(1);

        let addr = CString::new("127.0.0.1:0").unwrap();
        let listener = dtact_util_io_listener_bind(addr.as_ptr());
        assert!(!listener.is_null(), "{:?}", last_error());

        // Recover the actual bound port from the std listener isn't exposed
        // via FFI, so bind our own std listener to grab a free port, then
        // hand it to a second dtact listener. Simpler: bind to an explicit
        // ephemeral port via std to learn the address.
        // Instead: close the FFI listener and bind std to discover a port.
        dtact_util_io_listener_close(listener);

        let std_l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = std_l.local_addr().unwrap().port();
        drop(std_l);
        let bind_addr = CString::new(format!("127.0.0.1:{port}")).unwrap();
        let listener = dtact_util_io_listener_bind(bind_addr.as_ptr());
        assert!(!listener.is_null(), "{:?}", last_error());

        // Server thread: accept one connection and echo 5 bytes back.
        let listener_addr = listener as usize;
        let server = std::thread::spawn(move || {
            let listener = listener_addr as *mut _;
            let stream = dtact_util_io_listener_accept(listener);
            assert!(!stream.is_null());
            let mut buf = [0u8; 5];
            let n = dtact_util_io_stream_read(stream, buf.as_mut_ptr(), buf.len());
            assert_eq!(n, 5);
            let w = dtact_util_io_stream_write(stream, buf.as_ptr(), n as usize);
            assert_eq!(w, 5);
            dtact_util_io_stream_close(stream);
            dtact_util_io_listener_close(listener);
        });

        // Give the acceptor a moment to be ready.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let client = dtact_util_io_stream_connect(bind_addr.as_ptr());
        assert!(!client.is_null(), "{:?}", last_error());
        let msg = b"pingx";
        assert_eq!(dtact_util_io_stream_write(client, msg.as_ptr(), 5), 5);
        let mut buf = [0u8; 5];
        assert_eq!(dtact_util_io_stream_read(client, buf.as_mut_ptr(), 5), 5);
        assert_eq!(&buf, msg);
        dtact_util_io_stream_close(client);

        server.join().unwrap();
    }
}

#[test]
fn io_udp_send_recv_roundtrip() {
    unsafe {
        dtact_util_io_init(1);

        // Bound port isn't exposed over FFI, so discover two free loopback
        // ports via std first, then bind the FFI sockets to those fixed
        // addresses (same trick `io_tcp_echo` uses for the listener).
        let msg = b"pingx";
        let std_a = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let a_port = std_a.local_addr().unwrap().port();
        drop(std_a);
        let std_b = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let b_port = std_b.local_addr().unwrap().port();
        drop(std_b);

        let a_bind = CString::new(format!("127.0.0.1:{a_port}")).unwrap();
        let a = dtact_util_io_udp_bind(a_bind.as_ptr());
        assert!(!a.is_null(), "{:?}", last_error());
        let b_bind = CString::new(format!("127.0.0.1:{b_port}")).unwrap();
        let b = dtact_util_io_udp_bind(b_bind.as_ptr());
        assert!(!b.is_null(), "{:?}", last_error());

        let sent = dtact_util_io_udp_send_to(b, msg.as_ptr(), msg.len(), a_bind.as_ptr());
        assert_eq!(sent, msg.len() as isize, "{:?}", last_error());

        let mut buf = [0u8; 16];
        let mut out_addr = [0i8; 64];
        let n = dtact_util_io_udp_recv_from(
            a,
            buf.as_mut_ptr(),
            buf.len(),
            out_addr.as_mut_ptr(),
            out_addr.len(),
        );
        assert_eq!(n, msg.len() as isize, "{:?}", last_error());
        assert_eq!(&buf[..n as usize], msg);
        let peer = CStr::from_ptr(out_addr.as_ptr()).to_str().unwrap();
        assert!(peer.ends_with(&format!(":{b_port}")), "peer was {peer}");

        dtact_util_io_udp_close(a);
        dtact_util_io_udp_close(b);
    }
}

#[test]
fn process_spawn_wait() {
    unsafe {
        dtact_util_process_init(2);

        // A cross-platform command: `cmd /C exit 0` on Windows, `true` on Unix.
        #[cfg(windows)]
        let (prog, args): (&str, Vec<&str>) = ("cmd", vec!["/C", "exit", "7"]);
        #[cfg(unix)]
        let (prog, args): (&str, Vec<&str>) = ("sh", vec!["-c", "exit 7"]);

        let cprog = CString::new(prog).unwrap();
        let cargs: Vec<CString> = args.iter().map(|a| CString::new(*a).unwrap()).collect();
        let argv: Vec<*const std::ffi::c_char> = cargs.iter().map(|c| c.as_ptr()).collect();

        let child = dtact_util_process_spawn(cprog.as_ptr(), argv.as_ptr(), argv.len(), 0);
        assert!(!child.is_null(), "{:?}", last_error());
        assert!(dtact_util_process_child_id(child) > 0);

        let mut code = 0i32;
        let rc = dtact_util_process_child_wait(child, &mut code);
        assert_eq!(rc, 0, "{:?}", last_error());
        assert_eq!(code, 7);
    }
}

#[test]
fn process_stdout_pipe() {
    unsafe {
        dtact_util_process_init(2);

        #[cfg(windows)]
        let (prog, args): (&str, Vec<&str>) = ("cmd", vec!["/C", "echo", "hello"]);
        #[cfg(unix)]
        let (prog, args): (&str, Vec<&str>) = ("sh", vec!["-c", "printf hello"]);

        let cprog = CString::new(prog).unwrap();
        let cargs: Vec<CString> = args.iter().map(|a| CString::new(*a).unwrap()).collect();
        let argv: Vec<*const std::ffi::c_char> = cargs.iter().map(|c| c.as_ptr()).collect();

        let child =
            dtact_util_process_spawn(cprog.as_ptr(), argv.as_ptr(), argv.len(), DTACT_STDOUT_PIPE);
        assert!(!child.is_null(), "{:?}", last_error());

        let stdout = dtact_util_process_child_take_stdout(child);
        assert!(!stdout.is_null(), "{:?}", last_error());

        let mut collected = Vec::new();
        let mut buf = [0u8; 64];
        loop {
            let n = dtact_util_process_stdout_read(stdout, buf.as_mut_ptr(), buf.len());
            assert!(n >= 0, "{:?}", last_error());
            if n == 0 {
                break;
            }
            collected.extend_from_slice(&buf[..n as usize]);
        }
        dtact_util_process_stdout_free(stdout);

        let mut code = 0i32;
        assert_eq!(dtact_util_process_child_wait(child, &mut code), 0);
        assert!(
            String::from_utf8_lossy(&collected).contains("hello"),
            "stdout was {:?}",
            String::from_utf8_lossy(&collected)
        );
    }
}

#[test]
fn signal_register_and_free() {
    // Only exercises registration + free (delivering a real signal in a
    // test is flaky); recv() would block on an actual signal.
    unsafe {
        #[cfg(windows)]
        let sig = dtact_util_signal_ctrl_c();
        #[cfg(unix)]
        let sig = dtact_util_signal_register(libc::SIGUSR1);
        assert!(!sig.is_null());
        dtact_util_signal_free(sig);
    }
}

#[test]
fn null_pointer_guard_and_error_message() {
    unsafe {
        // Null handle must be reported, not crash.
        let r = dtact_util_stream_read(std::ptr::null_mut(), std::ptr::null_mut(), 0);
        assert_eq!(r, -1);
        let msg = last_error().expect("null misuse must record an error");
        assert!(msg.to_lowercase().contains("null"), "message was {msg:?}");

        // Freeing null is a no-op.
        dtact_util_stream_free(std::ptr::null_mut());

        // A real backend error (opening a nonexistent file) surfaces a
        // real message.
        dtact_util_fs_init(1);
        let missing = CString::new("Z:/definitely/not/here/nope.bin").unwrap();
        let f = dtact_util_fs_file_open(missing.as_ptr());
        assert!(f.is_null());
        assert!(last_error().is_some());
    }
}
