CC = gcc
CFLAGS = -Wall -Wextra -O2
# CFLAGS = -Wall -Wextra -g -O0

# On Windows, `rustc`'s default host target (`x86_64-pc-windows-msvc`)
# produces MSVC-ABI import libs (`.lib`) that `gcc`/`ld` (MinGW) cannot
# link against — the two use incompatible name-mangling and runtime
# support symbols (`__chkstk`, C++ `type_info`, etc.), so linking a plain
# `-L../target/release -ldtact` against an MSVC-target build fails with a
# wall of "undefined reference" errors no matter how many extra `-l` flags
# are added. The fix is not more libraries; it's building the Rust side
# for the GNU-ABI target instead (`rustup target add x86_64-pc-windows-gnu`
# once), which emits a MinGW-compatible import lib (`libdtact.dll.a`) that
# `gcc` links against directly. Everywhere else (Linux/macOS), the host
# target's own `.so`/`.dylib` + `-Wl,-rpath` already works, so this only
# branches on Windows.
ifeq ($(OS),Windows_NT)
	RUST_TARGET := x86_64-pc-windows-gnu
	CARGO_TARGET_FLAG := --target $(RUST_TARGET)
	TARGET_DIR := ../target/$(RUST_TARGET)
	EXE_SUFFIX := .exe
	# MinGW's import libs are named `lib<crate>.dll.a`; `-l<crate>` finds
	# them the same way it'd find `lib<crate>.a`/`lib<crate>.so` elsewhere.
	DTACT_LDFLAGS = -L$(TARGET_DIR)/release -ldtact
	DTACT_UTIL_LDFLAGS = -I.. -L$(TARGET_DIR)/release -ldtact_util
	# Windows has no rpath equivalent for DLL search — the loader looks
	# next to the .exe (among other places), so copy the DLL there.
	POST_LINK = cp $(TARGET_DIR)/release/$(1).dll .
else
	CARGO_TARGET_FLAG :=
	TARGET_DIR := ../target
	EXE_SUFFIX :=
	DTACT_LDFLAGS = -L$(TARGET_DIR)/release -ldtact -Wl,-rpath,$(TARGET_DIR)/release
	DTACT_UTIL_LDFLAGS = -I.. -L$(TARGET_DIR)/release -ldtact_util -Wl,-rpath,$(TARGET_DIR)/release
	POST_LINK = true
endif

all: c_async c_dtact_util run_rust run_rust_util

build_dtact:
	cargo build --release -p dtact $(CARGO_TARGET_FLAG)

c_async: build_dtact c_async.c
	$(CC) $(CFLAGS) -o c_async$(EXE_SUFFIX) c_async.c $(DTACT_LDFLAGS)
	@$(call POST_LINK,dtact)

# `dtact-util`'s C FFI surface (io/fs/process/signal/stream/timer) is only
# built into the cdylib under its `ffi` feature — build that first.
build_dtact_util:
	cargo build --release -p dtact-util --features ffi $(CARGO_TARGET_FLAG)

c_dtact_util: build_dtact_util c_dtact_util.c
	$(CC) $(CFLAGS) -o c_dtact_util$(EXE_SUFFIX) c_dtact_util.c $(DTACT_UTIL_LDFLAGS)
	@$(call POST_LINK,dtact_util)

run_rust:
	cargo run --example rust_async

run_rust_util:
	cargo run --example rust_dtact_util

run: c_async
	./c_async$(EXE_SUFFIX)

run_util: c_dtact_util
	./c_dtact_util$(EXE_SUFFIX)

clean:
	rm -f c_async c_async.exe c_dtact_util c_dtact_util.exe dtact.dll dtact_util.dll
