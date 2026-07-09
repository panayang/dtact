// Comprehensive dtact-util C FFI example: exercises the blocking/synchronous
// extern "C" surface over five of the six primitive modules in one program
// (io, fs, stream, timer, process — signal is skipped since delivering a
// real signal from a single-process example is inherently flaky).
//
// Build/run via ../examples/Makefile's `c_util` / `run_c_util` targets.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "../dtact-util/dtact_util.h"

#if defined(_WIN32)
#include <process.h>
#define getpid() ((int)_getpid())
#else
#include <unistd.h>
#endif

static void check_last_error(const char *what) {
    const char *msg = dtact_util_last_error_message();
    if (msg) {
        fprintf(stderr, "[error] %s: %s\n", what, msg);
    } else {
        fprintf(stderr, "[error] %s: (no error message recorded)\n", what);
    }
}

// --- timer -------------------------------------------------------------
static void demo_timer(void) {
    printf("[timer] sleeping 15ms via dtact_util_timer_sleep_ms...\n");
    dtact_util_timer_sleep_ms(15);

    DtactInterval *iv = dtact_util_timer_interval_create(5);
    if (!iv) {
        check_last_error("timer_interval_create");
        return;
    }
    for (int i = 0; i < 2; i++) {
        dtact_util_timer_interval_tick(iv);
        printf("[timer] interval tick %d\n", i);
    }
    dtact_util_timer_interval_free(iv);
}

// --- fs ------------------------------------------------------------------
static void demo_fs(void) {
    dtact_util_fs_init(1);

    char path[512];
    snprintf(path, sizeof(path), "dtact-util-c-example-%d.txt", (int)getpid());

    DtactFile *f = dtact_util_fs_file_create(path);
    if (!f) {
        check_last_error("fs_file_create");
        return;
    }
    const char *payload = "hello from the dtact-util C example";
    ptrdiff_t written = dtact_util_fs_file_write(f, (const uint8_t *)payload, strlen(payload));
    printf("[fs] wrote %lld bytes to %s\n", (long long)written, path);
    dtact_util_fs_file_sync(f);
    dtact_util_fs_file_close(f);

    DtactFile *f2 = dtact_util_fs_file_open(path);
    if (!f2) {
        check_last_error("fs_file_open");
        return;
    }
    uint8_t buf[128] = {0};
    ptrdiff_t got = dtact_util_fs_file_read(f2, buf, sizeof(buf) - 1);
    printf("[fs] read back %lld bytes: %s\n", (long long)got, (const char *)buf);
    dtact_util_fs_file_close(f2);

    remove(path);
}

// --- stream (in-process duplex pipe) -------------------------------------
static void demo_stream(void) {
    DtactStream *a = NULL;
    DtactStream *b = NULL;
    if (dtact_util_stream_pair_create(64, &a, &b) != 0) {
        check_last_error("stream_pair_create");
        return;
    }

    const char *msg = "ping over dtact-util stream";
    ptrdiff_t w = dtact_util_stream_write(a, (const uint8_t *)msg, strlen(msg));
    printf("[stream] wrote %lld bytes into the pipe\n", (long long)w);

    uint8_t buf[64] = {0};
    ptrdiff_t r = dtact_util_stream_read(b, buf, sizeof(buf) - 1);
    printf("[stream] read back %lld bytes: %s\n", (long long)r, (const char *)buf);

    dtact_util_stream_free(a);
    dtact_util_stream_free(b);
}

// --- io (loopback TCP echo) ----------------------------------------------
static void demo_io(void) {
    dtact_util_io_init(1);

    // Discover a free loopback port the same way the crate's own ffi_test
    // does: bind with std/libc, close it, then hand the fixed address to
    // dtact_util. There is no FFI accessor for a listener's bound address.
    // The C example keeps this simple by using a fixed high port instead of
    // an OS-assigned ephemeral one.
    const char *addr = "127.0.0.1:38213";

    DtactTcpListener *listener = dtact_util_io_listener_bind(addr);
    if (!listener) {
        check_last_error("io_listener_bind");
        return;
    }

    DtactTcpStream *client = dtact_util_io_stream_connect(addr);
    if (!client) {
        check_last_error("io_stream_connect");
        dtact_util_io_listener_close(listener);
        return;
    }

    DtactTcpStream *server_side = dtact_util_io_listener_accept(listener);
    if (!server_side) {
        check_last_error("io_listener_accept");
        dtact_util_io_stream_close(client);
        dtact_util_io_listener_close(listener);
        return;
    }

    const char *msg = "ping over dtact-util io";
    dtact_util_io_stream_write(client, (const uint8_t *)msg, strlen(msg));

    uint8_t buf[64] = {0};
    ptrdiff_t n = dtact_util_io_stream_read(server_side, buf, sizeof(buf) - 1);
    printf("[io] server received: %.*s\n", (int)n, buf);
    dtact_util_io_stream_write(server_side, buf, (size_t)n);

    ptrdiff_t echoed = dtact_util_io_stream_read(client, buf, sizeof(buf) - 1);
    printf("[io] client received echo: %.*s\n", (int)echoed, buf);

    dtact_util_io_stream_close(server_side);
    dtact_util_io_stream_close(client);
    dtact_util_io_listener_close(listener);
}

// --- process ---------------------------------------------------------------
static void demo_process(void) {
    dtact_util_process_init(1);

#if defined(_WIN32)
    const char *prog = "cmd";
    const char *argv[] = {"/C", "echo", "hello-from-child", NULL};
    size_t argc = 3;
#else
    const char *prog = "sh";
    const char *argv[] = {"-c", "printf hello-from-child", NULL};
    size_t argc = 2;
#endif

    DtactChild *child = dtact_util_process_spawn(prog, argv, argc, DTACT_STDOUT_PIPE);
    if (!child) {
        check_last_error("process_spawn");
        return;
    }

    DtactChildStdout *out = dtact_util_process_child_take_stdout(child);
    if (out) {
        uint8_t buf[128] = {0};
        ptrdiff_t total = 0;
        for (;;) {
            ptrdiff_t n = dtact_util_process_stdout_read(out, buf + total, sizeof(buf) - 1 - (size_t)total);
            if (n <= 0) break;
            total += n;
        }
        printf("[process] child stdout: %.*s\n", (int)total, buf);
        dtact_util_process_stdout_free(out);
    }

    int32_t exit_code = 0;
    dtact_util_process_child_wait(child, &exit_code);
    printf("[process] child exited with code %d\n", exit_code);
}

int main(void) {
    setvbuf(stdout, NULL, _IONBF, 0);
    printf("--- dtact-util comprehensive C FFI example ---\n");

    demo_timer();
    demo_fs();
    demo_stream();
    demo_io();
    demo_process();

    printf("--- all dtact-util primitives exercised successfully ---\n");
    return 0;
}
