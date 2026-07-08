//! Exercises the dtact-process native and tokio backends: spawn + wait,
//! wait_with_output, stdin/stdout pipe roundtrip, and kill. Uses `cmd /C`
//! on Windows and `sh -c` on Unix so this runs on whatever platform hosts
//! CI without depending on a specific external binary being installed.

#[cfg(windows)]
fn shell_cmd(cmd: &str) -> (&'static str, Vec<String>) {
    ("cmd", vec!["/C".to_string(), cmd.to_string()])
}

#[cfg(unix)]
fn shell_cmd(cmd: &str) -> (&'static str, Vec<String>) {
    ("sh", vec!["-c".to_string(), cmd.to_string()])
}

#[cfg(feature = "native")]
mod native_tests {
    use super::shell_cmd;
    use dtact_util::process::{DtactChild, DtactCommand};
    use std::future::Future;
    use std::process::Stdio;

    fn block_on<F: Future>(fut: F) -> F::Output {
        use std::pin::pin;
        use std::sync::Arc;
        use std::task::{Context, Poll, Wake};

        struct NoopWaker;
        impl Wake for NoopWaker {
            fn wake(self: Arc<Self>) {}
        }
        let waker = Arc::new(NoopWaker).into();
        let mut cx = Context::from_waker(&waker);
        let mut fut = pin!(fut);
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(v) => return v,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn spawn_and_wait_success() {
        dtact_util::process::init(2);
        let (prog, args) = shell_cmd("exit 0");
        let mut cmd = DtactCommand::new(prog);
        cmd.args(args);
        let child = cmd.spawn().unwrap();
        let status = block_on(child.wait()).unwrap();
        assert!(status.success());
    }

    #[test]
    fn wait_with_output_captures_stdout() {
        dtact_util::process::init(2);
        let (prog, args) = shell_cmd("echo hello-dtact-process");
        let mut cmd = DtactCommand::new(prog);
        cmd.args(args).stdout(Stdio::piped());
        let child = cmd.spawn().unwrap();
        let output = block_on(child.wait_with_output()).unwrap();
        assert!(output.status.success());
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(text.contains("hello-dtact-process"), "got: {text:?}");
    }

    #[test]
    fn stdin_stdout_pipe_roundtrip() {
        dtact_util::process::init(2);
        // `sh -c cat` / `cmd /C more` both echo stdin back — use a small
        // portable roundtrip via a shell command that just reads a line
        // and prints it, on both platforms.
        #[cfg(unix)]
        let (prog, args): (&str, Vec<String>) = ("cat", vec![]);
        #[cfg(windows)]
        let (prog, args): (&str, Vec<String>) =
            ("findstr", vec!["/R".to_string(), ".".to_string()]);

        let mut cmd = DtactCommand::new(prog);
        cmd.args(args).stdin(Stdio::piped()).stdout(Stdio::piped());
        let mut child = cmd.spawn().unwrap();
        let mut stdin = child.take_stdin().unwrap();
        let mut stdout = child.take_stdout().unwrap();

        block_on(async {
            stdin.write(b"roundtrip-payload\n".to_vec()).await.unwrap();
            stdin.close();

            let mut collected = Vec::new();
            loop {
                let (n, buf) = stdout.read(vec![0u8; 64]).await.unwrap();
                if n == 0 {
                    break;
                }
                collected.extend_from_slice(&buf[..n]);
                if collected.len() >= "roundtrip-payload".len() {
                    break;
                }
            }
            let text = String::from_utf8_lossy(&collected);
            assert!(text.contains("roundtrip-payload"), "got: {text:?}");
        });

        let _ = block_on(child.wait());
    }

    #[test]
    fn kill_terminates_a_long_running_child() {
        dtact_util::process::init(2);
        #[cfg(unix)]
        let (prog, args) = ("sleep", vec!["30".to_string()]);
        #[cfg(windows)]
        let (prog, args) = shell_cmd("timeout /T 30");

        let mut cmd = DtactCommand::new(prog);
        cmd.args(args);
        let mut child: DtactChild = cmd.spawn().unwrap();
        child.kill().unwrap();
        let status = block_on(child.wait()).unwrap();
        assert!(!status.success(), "killed child should not report success");
    }
}

#[cfg(all(feature = "tokio", not(feature = "native")))]
mod tokio_tests {
    use super::shell_cmd;
    use dtact_util::process::DtactCommand;

    #[tokio::test]
    async fn spawn_and_wait_success() {
        let (prog, args) = shell_cmd("exit 0");
        let mut cmd = DtactCommand::new(prog);
        cmd.args(args);
        let mut child = cmd.spawn().unwrap();
        let status = child.wait().await.unwrap();
        assert!(status.success());
    }
}
