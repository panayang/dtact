# Dtact


<img align="right" src="./logo.svg" height="200" />

[![Discord Server](https://img.shields.io/discord/1459399539403522074.svg?label=Discord&logo=discord&color=blue)](https://discord.gg/D5e2czMTT9)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Scc Count Badge Code](https://sloc.xyz/github/Apich-Organization/dtact/?category=code)](https://github.com/Apich-Organization/dtact/)

Dtact is a non-preemptive, stackful coroutine runtime designed for hardware-level control and heterogeneous orchestration. It provides a unique architecture based on a lock-free context arena, peer-to-peer (P2P) mesh scheduling, and architecture-specific assembly context switchers.

## Design Philosophy

Dtact explores an alternative approach to asynchronous execution by leveraging stackful coroutines (fibers) rather than stackless state machines. This design choice brings several interesting architectural patterns:

* **Lock-Free Context Arena**: Dtact manages fiber execution contexts using a pre-allocated, lock-free memory pool (`ContextPool`). This avoids heap allocation overhead during high-frequency task spawning.
* **P2P Mesh Scheduling**: Instead of a traditional global work-stealing queue, Dtact utilizes a decentralized P2P mesh. Workers communicate via bounded, lock-free mailboxes. This allows for localized work deflection and reduces cross-core synchronization contention under heavy load.
* **Zero-Copy Future Migration**: For Rust users, Dtact attempts to place `Future` payloads directly onto the pre-allocated stack of the fiber. This zero-copy approach helps minimize heap allocations for small-to-medium futures.
* **Cross-Language FFI**: Since fibers have their own stacks, they integrate naturally with foreign function interfaces. Dtact provides a C-FFI out of the box, allowing C and C++ code to seamlessly launch and await fibers.
* **Customizable Context Switchers**: Dtact provides different assembly-level context switchers (e.g., floating-point vs. no-floating-point preservation, cross-thread vs. same-thread) to allow developers to tailor the cost of a context switch to their specific workload.

## Performance Characteristics

Dtact's design makes trade-offs that influence its performance profile:

* **Task Spawning and Deflection**: The lock-free arena and mesh deflection allow Dtact to be highly efficient at spawning large numbers of tasks and handling localized contention (hot cores).
* **Yield Overhead**: Because Dtact uses stackful coroutines, yielding involves a full CPU register context switch. This means that raw yield efficiency is naturally heavier compared to stackless runtimes like Tokio. Dtact is best suited for workloads where the cost of yielding is amortized by the work being done, or where the C-FFI and stackful nature are primary requirements.

## Example Usage

### Rust

```rust
use dtact::{dtact_await, dtact_init, spawn, task, yield_now};

#[task(priority = "Normal", kind = "Compute", stack = "2M")]
async fn worker(id: u32) {
    println!("[Fiber {}] Starting async work...", id);

    for i in 0..3 {
        println!("[Fiber {}] Progress step {}", id, i);
        yield_now().await;
    }

    println!("[Fiber {}] Task Finished.", id);
}

#[dtact_init(workers = 4)]
fn main() {
    println!("--- Dtact Rust Macro Example ---");

    let mut handles = vec![];
    for i in 0..5 {
        println!("[Master] Launching Fiber {}", i);
        // spawn takes a future and returns a handle
        handles.push(spawn(worker(i)));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        println!("[Master] Waiting for Fiber {} to complete...", i);
        dtact_await(handle);
        println!("[Master] Fiber {} has been joined.", i);
    }

    println!("[Master] All sub-tasks completed. Exiting cleanly.");
}
```

### C FFI

```c
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include "../dtact.h"

// Worker fiber that simulates asynchronous work
void worker_fiber(void* arg) {
    int id = *(int*)arg;
    printf("[Fiber %d] Starting async work...\n", id);
    
    // Simulating work
    for(int i = 0; i < 3; i++) {
        printf("[Fiber %d] Progress step %d\n", id, i);
        // Normally we would yield here, but dtact handles cooperative switching
    }
    
    printf("[Fiber %d] Task Finished.\n", id);
    dtact_free_arg(arg);
}

// Master fiber that spawns and joins other fibers
void master_fiber(void* arg) {
    printf("[Master] Orchestrating sub-fibers...\n");
    
    dtact_handle_t handles[5];
    for(int i = 0; i < 5; i++) {
        int* val = malloc(sizeof(int));
        *val = i;
        printf("[Master] Launching Fiber %d\n", i);
        
        dtact_spawn_options_t opts = dtact_default_spawn_options();
        if (i % 2 == 0) {
            opts.mKind = 1; // IO
            opts.mSwitcher = 3; // SameThreadNoFloat
        } else {
            opts.mKind = 3; // System
            opts.mSwitcher = 0; // CrossThreadFloat
        }
        
        handles[i] = dtact_fiber_launch_ext(worker_fiber, val, &opts);
    }
    
    for(int i = 0; i < 5; i++) {
        printf("[Master] Waiting for Fiber %d to complete...\n", i);
        dtact_await(handles[i]);
        printf("[Master] Fiber %d has been joined.\n", i);
    }
    
    printf("[Master] All sub-tasks completed. Signaling shutdown.\n");
    dtact_shutdown();
}

int main() {
    setvbuf(stdout, NULL, _IONBF, 0);
    printf("--- Dtact C-FFI Example ---\n");
    
    // 1. Initialize Runtime
    dtact_config_t cfg = dtact_default_config();
    cfg.mWorkers = 4;
    void* rt = dtact_init(&cfg);
    
    // 2. Launch Initial Root Fiber
    dtact_fiber_launch(master_fiber, NULL);
    
    // 3. Start Execution
    // This call blocks the main thread and starts 4 worker threads.
    // It returns when dtact_shutdown() is called.
    printf("Entering Runtime execution loop...\n");
    dtact_run(rt);
    
    printf("Runtime exited cleanly.\n");
    return 0;
}
```

## License

This project is licensed under either of the MIT license or the Apache License (Version 2.0).
