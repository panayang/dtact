use crate::app::Page;
use leptos::prelude::*;

#[component]
pub fn HomePage() -> impl IntoView {
    let page = expect_context::<RwSignal<Page>>();

    view! {
        // ── Hero ────────────────────────────────────────────────────────────
        <section class="hero">
            <div class="hero-logo-wrap">
                <img src="logo.svg" alt="dtact logo" class="hero-logo" />
            </div>
            <p class="hero-eyebrow">"Non-Preemptive Stackful Coroutine Runtime"</p>
            <h1 class="hero-title">"dtact"</h1>
            <p class="hero-sub">
                "Distributed Task-Aware Coroutine Toolkit \u{2014} a hardware-precise,
                 lock-free coroutine runtime with P2P mesh scheduling, assembly-level
                 context switchers, and first-class C/C++ FFI. Built for systems where
                 every nanosecond and every cache line matter."
            </p>

            <div class="hero-badges">
                <a href="https://crates.io/crates/dtact" target="_blank" rel="noopener"
                   class="badge-link" title="crates.io">
                    <span class="badge badge-doi">"crates.io"</span>
                    <span class="badge badge-doi-val">"dtact"</span>
                </a>
                <a href="https://docs.rs/dtact" target="_blank" rel="noopener"
                   class="badge-link" title="API Documentation">
                    <span class="badge badge-success">"docs"</span>
                    <span class="badge badge-primary">"docs.rs/dtact"</span>
                </a>
                <a href="https://github.com/Apich-Organization/dtact" target="_blank" rel="noopener"
                   class="badge-link" title="GitHub Repository">
                    <span class="badge badge-primary">"v0.2.2"</span>
                    <span class="badge badge-neutral">"MIT / Apache-2.0"</span>
                </a>
                <a href="https://doi.org/10.5281/zenodo.20002105" target="_blank" rel="noopener"
                   class="badge-link" title="DOI — Zenodo">
                    <span class="badge badge-doi">"DOI"</span>
                    <span class="badge badge-doi-val">"10.5281/zenodo.20002105"</span>
                </a>
            </div>

            <div class="hero-actions">
                <a href="https://github.com/Apich-Organization/dtact"
                   target="_blank" rel="noopener" class="btn btn-primary">
                    <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
                        <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38
                                 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13
                                 -.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66
                                 .07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15
                                 -.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27
                                 .68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12
                                 .51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48
                                 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/>
                    </svg>
                    "View on GitHub"
                </a>
                <a href="https://docs.rs/dtact" target="_blank" rel="noopener" class="btn btn-ghost">
                    "API Docs"
                </a>
                <button class="btn btn-ghost" on:click=move |_| page.set(Page::Architecture)>
                    "Explore Architecture"
                </button>
                <button class="btn btn-ghost" on:click=move |_| page.set(Page::Bench)>
                    "Benchmarks"
                </button>
                <button class="btn btn-ghost" on:click=move |_| page.set(Page::Sponsor)>
                    "Sponsor"
                </button>
            </div>

            <div class="hero-scroll-hint">
                <svg class="scroll-arrow" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.8">
                    <path d="M10 4v12M4 11l6 6 6-6"/>
                </svg>
                "scroll to explore"
            </div>
        </section>

        // ── Feature Overview ─────────────────────────────────────────────────
        <section class="section page-wrap">
            <div class="mb-lg">
                <h2>"What dtact Provides"</h2>
                <p class="mt-sm">
                    "dtact is a non-preemptive, stackful coroutine runtime engineered for
                     hardware-level control. It replaces OS threads and async executors with
                     a decentralized P2P mesh scheduler, lock-free arena allocation, and
                     assembly-precision context switchers. Every component is designed to
                     operate predictably under real-time constraints."
                </p>
            </div>
            <div class="grid-2">
                <FeatureCard
                    icon="\u{1F5C4}"
                    title="Lock-Free Arena"
                    body="O(1) fiber context allocation and deallocation via an ABA-protected
                          free list. Supports huge page optimization (MAP_HUGETLB / MEM_LARGE_PAGES),
                          Transparent Huge Pages, and NUMA-node binding via mbind."
                />
                <FeatureCard
                    icon="\u{1F578}"
                    title="P2P Mesh Scheduler"
                    body="N\u{00D7}N SPSC mailbox matrix between workers. Hop-bounded deflection
                          with per-worker load levels and a 1M-task MPMC warehouse as emergency
                          backlog. No global queue, no single point of contention."
                />
                <FeatureCard
                    icon="\u{26A1}"
                    title="Assembly Context Switchers"
                    body="Native naked-function switchers for x86_64 (SysV + Windows ABI),
                          AArch64 (BTI/PAC), and RISC-V 64. FXSAVE/FXRSTOR for SSE/AVX state.
                          3-instruction L1/L2 prefetch before every swap."
                />
                <FeatureCard
                    icon="\u{1F4BE}"
                    title="Zero-Copy Futures"
                    body="Small futures (~8 KB) are placed directly on the fiber stack, bypassing
                          heap allocation entirely. Stackful coroutines carry full call-stack
                          depth without the pinning or polling overhead of stackless futures."
                />
                <FeatureCard
                    icon="\u{1F517}"
                    title="First-Class C/C++ FFI"
                    body="cbindgen-generated dtact.h and dtact.hpp with RAII C++ wrapper.
                          dtact_fiber_launch, dtact_await, dtact_shutdown and full
                          configuration structs are directly callable from any C or C++ codebase."
                />
                <FeatureCard
                    icon="\u{1F6E1}"
                    title="Tiered Hardware Safety"
                    body="Safety0 for maximum throughput; Safety1 adds guard pages every 32
                          contexts; Safety2 grants per-context hardware guard pages. Trade
                          protection for performance per workload without recompiling."
                />
            </div>
        </section>

        // ── Architecture Support ─────────────────────────────────────────────
        <section class="section page-wrap">
            <div class="mb-lg">
                <h2>"Multi-Architecture Support"</h2>
                <p class="mt-sm">
                    "dtact ships production-ready context switchers for all major ISAs,
                     each tuned to the ABI and hardware features of the target platform."
                </p>
            </div>
            <div class="arch-grid">
                <ArchCard
                    arch="x86_64"
                    subtitle="Linux / macOS (SysV ABI)"
                    detail="FXSAVE/FXRSTOR for full SSE/AVX register state. Hardware umwait/umonitor
                            idle on CPUs with WAITPKG. Branchless 4-way routing dispatch table."
                    badge="Tested"
                />
                <ArchCard
                    arch="x86_64"
                    subtitle="Windows (x64 ABI)"
                    detail="XMM6\u{2013}XMM15 callee-save. SEH ExceptionList pointer preservation.
                            TIB stack limit/base updates across every context switch (gs:[0x00]/[0x08])."
                    badge="Tested"
                />
                <ArchCard
                    arch="AArch64"
                    subtitle="Linux / macOS / Apple Silicon"
                    detail="BTI (Branch Target Identification) + PAC (Pointer Auth) for CFI hardening.
                            WFE hardware standby for low-power idle. x18 platform register reserved."
                    badge="Tested"
                />
                <ArchCard
                    arch="AArch64"
                    subtitle="Windows (ARM64 ABI)"
                    detail="Full callee-save register file (x19\u{2013}x28, d8\u{2013}d15). Frame chain
                            preservation for Windows stack unwinding. Tested on Windows on ARM."
                    badge="Tested"
                />
                <ArchCard
                    arch="RISC-V 64"
                    subtitle="Linux (LP64D ABI)"
                    detail="Covers the base integer and double-precision floating-point register file.
                            Platform-specific quirks may arise \u{2014} feedback and issues welcome."
                    badge="Community"
                    badge_class="badge-neutral"
                />
            </div>
        </section>

        // ── Task Lifecycle Pipeline ──────────────────────────────────────────
        <section class="section page-wrap">
            <div class="mb-lg">
                <h2>"Task Lifecycle"</h2>
                <p class="mt-sm">
                    "Every fiber follows a deterministic lifecycle from spawn to reclamation.
                     Each stage is designed to minimise latency, avoid allocator pressure,
                     and keep cache lines hot."
                </p>
            </div>
            <div class="pipeline-steps">
                <PipelineStep n="1" label="spawn() / SpawnBuilder"
                    detail="User calls spawn(future) or SpawnBuilder::new().priority(High).affinity(SameCCX).spawn(f).
                            Priority, affinity mode, WorkloadKind, and switcher variant are encoded into the task descriptor." />
                <PipelineStep n="2" label="Arena Allocation"
                    detail="alloc_context() performs a single CAS on the lock-free free list (O(1), ABA-protected).
                            Returns a 64-byte aligned FiberContext slot. Stack and 8 KB read buffer are pre-mapped;
                            on Linux huge pages are attempted first via MAP_HUGETLB." />
                <PipelineStep n="3" label="Affinity Routing"
                    detail="Scheduler inspects the affinity hint: Any (any worker mailbox), SameCore (exact core match),
                            SameCCX (same CCX cluster), SameNUMA (same NUMA node). Polling order is precomputed at
                            init time from CPU topology using CPUID / /sys/devices/system/cpu." />
                <PipelineStep n="4" label="Mailbox Delivery"
                    detail="Task is written into the SPSC mailbox from the spawning worker to the target worker
                            (capacity: 65,536 tasks). TaskChunk batching groups up to 32 tasks per mailbox write
                            to reduce cache-coherency traffic between cores." />
                <PipelineStep n="5" label="Deflection or Warehouse"
                    detail="If the target mailbox is full and hops < max_hops (= num_workers/2), the task is
                            deflected to an adjacent worker. If hops are exhausted, it enters the MPMC warehouse
                            (32,768 \u{00D7} 32-task chunks = 1,048,576 task emergency backlog). Staggered CAS
                            backoff with prime multiplier 7 prevents thundering herd." />
                <PipelineStep n="6" label="Worker Dequeue + 3-Tier Idle"
                    detail="Each worker drains its local queue (131,072-slot SPSC), then polls its incoming mailboxes,
                            then the warehouse. When empty: Tier 1 (256 spin_loop spins), Tier 2 (2048 pause/yield spins),
                            Tier 3 (WFE / umwait hardware standby). No OS syscalls in the idle path." />
                <PipelineStep n="7" label="Context Switch (Save Caller)"
                    detail="The assembly switcher saves caller-saved GPRs and XMM/SIMD state (FXSAVE on x86_64,
                            full Q-register file on AArch64). 3-instruction prefetch sequence warms the target
                            FiberContext into L1/L2 before completing the swap. TIB updated on Windows." />
                <PipelineStep n="8" label="Fiber Execution"
                    detail="The fiber runs on its dedicated stack with full call-stack depth.
                            yield_now() explicitly yields back to the scheduler. Futures are polled inline on the
                            fiber stack \u{2014} no heap allocation, no pinning, no Waker indirection." />
                <PipelineStep n="9" label="Yield / Suspend"
                    detail="yield_now() triggers a context switch back to the worker\u{2019}s event loop.
                            The FiberStatus transitions Yielded \u{2192} Notified when a waker fires.
                            Suspended fibers consume zero CPU; their context remains in the arena slot." />
                <PipelineStep n="10" label="Finish \u{2192} Pool Return"
                    detail="On return or panic, FiberStatus becomes Finished / Panicked. The context slot is
                            returned to the free list via free_context() (one CAS, no lock). The arena slot
                            is immediately reusable for the next spawn()." />
            </div>
        </section>

        // ── Quick Start ──────────────────────────────────────────────────────
        <section class="section page-wrap">
            <div class="mb-lg">
                <h2>"Quick Start"</h2>
                <p class="mt-sm">
                    "Add dtact to "
                    <span class="mono">"Cargo.toml"</span>
                    " and annotate your entry point:"
                </p>
            </div>
            <div class="grid-2">
                <div>
                    <p class="algo-section-title">"Rust — macro API"</p>
                    <pre class="code-block text-xs">
"# Cargo.toml
[dependencies]
dtact = \"0.2\"

# src/main.rs
use dtact::{dtact_await, dtact_init, spawn, task, yield_now};

#[task(
    priority = \"Normal\",
    kind = \"Compute\",
    stack = \"256K\",
    capacity = \"1024\"
)]
async fn worker(id: u32) {
    println!(\"[Fiber {}] Starting async work...\", id);

    for i in 0..3 {
        println!(\"[Fiber {}] Progress step {}\", id, i);
        yield_now().await;
    }

    println!(\"[Fiber {}] Task Finished.\", id);
}

#[dtact_init(workers = 4, stack = \"256K\", capacity = \"1024\")]
fn main() {
    println!(\"--- Dtact Rust Macro Example ---\");

    let mut handles = vec![];
    for i in 0..5 {
        println!(\"[Master] Launching Fiber {}\", i);
        // spawn takes a future and returns a handle
        handles.push(spawn(worker(i)));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        println!(\"[Master] Waiting for Fiber {} to complete...\", i);
        dtact_await(handle);
        println!(\"[Master] Fiber {} has been joined.\", i);
    }

    println!(\"[Master] All sub-tasks completed. Exiting cleanly.\");
}"
                    </pre>
                </div>
                <div>
                    <p class="algo-section-title">"C — raw FFI"</p>
                    <pre class="code-block text-xs">
"#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include \"../dtact.h\"

// Worker fiber that simulates asynchronous work
void worker_fiber(void* arg) {
    int id = *(int*)arg;
    printf(\"[Fiber %d] Starting async work...\n\", id);
    
    // Simulating work
    for(int i = 0; i < 3; i++) {
        printf(\"[Fiber %d] Progress step %d\n\", id, i);
        // Normally we would yield here, but dtact handles cooperative switching
    }
    
    printf(\"[Fiber %d] Task Finished.\n\", id);
    dtact_free_arg(arg);
}

// Master fiber that spawns and joins other fibers
void master_fiber(void* arg) {
    printf(\"[Master] Orchestrating sub-fibers...\n\");
    
    dtact_handle_t handles[5];
    for(int i = 0; i < 5; i++) {
        int* val = malloc(sizeof(int));
        *val = i;
        printf(\"[Master] Launching Fiber %d\n\", i);
        
        dtact_spawn_options_t opts = dtact_default_spawn_options();
        if (i % 2 == 0) {
            opts.mKind = 1; // IO
            opts.mSwitcher = 1; // CrossThreadNoFloat
        } else {
            opts.mKind = 3; // System
            opts.mSwitcher = 0; // CrossThreadFloat
        }
        
        handles[i] = dtact_fiber_launch_ext(worker_fiber, val, &opts);
    }
    
    for(int i = 0; i < 5; i++) {
        printf(\"[Master] Waiting for Fiber %d to complete...\n\", i);
        dtact_await(handles[i]);
        printf(\"[Master] Fiber %d has been joined.\n\", i);
    }
    
    printf(\"[Master] All sub-tasks completed. Signaling shutdown.\n\");
    dtact_shutdown();
}

int main() {
    setvbuf(stdout, NULL, _IONBF, 0);
    printf(\"--- Dtact C-FFI Example ---\n\");
    
    // 1. Initialize Runtime
    dtact_config_t cfg = dtact_default_config();
    cfg.mWorkers = 4;
    cfg.mFiberCapacity = 1024; // Limit to 1024 fibers for this example
    cfg.mStackSize = 256 * 1024; // 256KB stacks are sufficient
    void* rt = dtact_init(&cfg);
    
    // 2. Launch Initial Root Fiber
    dtact_fiber_launch(master_fiber, NULL);
    
    // 3. Start Execution
    // This call blocks the main thread and starts 4 worker threads.
    // It returns when dtact_shutdown() is called.
    printf(\"Entering Runtime execution loop...\n\");
    dtact_run(rt);
    
    printf(\"Runtime exited cleanly.\n\");
    return 0;
}"
                    </pre>
                </div>
            </div>
        </section>

        // ── Macro System ─────────────────────────────────────────────────────
        <section class="section page-wrap">
            <div class="glass card-pad-lg">
                <span class="section-chip">"Procedural Macro System"</span>
                <h2>"dtact-macros"</h2>
                <p class="mt-sm">
                    "The "
                    <span class="mono">"dtact-macros"</span>
                    " crate ships three proc-macros that encode task metadata at compile time
                     and generate C FFI wrappers automatically."
                </p>
                <div class="grid-3 mt-md">
                    <MacroCard
                        name="#[task(...)]"
                        desc="Attaches priority, kind, stack size, and switcher variant to an async fn.
                              Generates a dtact_metadata_<name> submodule with compile-time constants."
                        example="#[task(
    priority = \"High\",
    affinity = \"Any\",
    kind = \"Compute\",
    stack = \"256K\",
    switcher = \"CrossThreadNoFloat\",)]
async fn worker(id: u32) { ... }"
                    />
                    <MacroCard
                        name="#[export_async]"
                        desc="Wraps an async Rust fn in an extern \"C\" signature, producing
                              dtact_export_<name> callable from C or C++."
                        example="#[export_async]
async fn process(x: i32) -> i32 {
    // ... runs as a fiber
    x * 2
}"
                    />
                    <MacroCard
                        name="#[dtact_init(...)]"
                        desc="Entry-point macro on main. Initialises the global runtime singleton,
                              spawns worker threads, and calls user code."
                        example="#[dtact_init(
    topology = \"P2PMesh\",
    safety = \"Safety1\",
    workers = 4,
    stack   = \"256K\",
    capacity = \"1024\",
    numa = \"0\",)]
fn main() { /* user code */ }"
                    />
                </div>
            </div>
        </section>

        // ── Citation ─────────────────────────────────────────────────────────
        <section class="section page-wrap">
            <div class="grid-2">
                <div class="glass card-pad">
                    <span class="section-chip">"Citation"</span>
                    <h3>"Cite dtact"</h3>
                    <p class="mt-sm">
                        "If you use dtact in academic or industrial work, please cite. Archived on Zenodo:"
                    </p>
                    <div class="mt-sm">
                        <a href="https://doi.org/10.5281/zenodo.20002105"
                           target="_blank" rel="noopener" class="badge-link" title="Zenodo record">
                            <span class="badge badge-doi">"DOI"</span>
                            <span class="badge badge-doi-val">"10.5281/zenodo.20002105"</span>
                        </a>
                    </div>
                    <pre class="code-block text-xs mt-sm">
"@software{Yang_Dtact_An_Experimental_2026,
  author  = {Yang, Xinyu},
  doi     = {10.5281/zenodo.20002105},
  license = {Apache-2.0},
  month   = may,
  title   = {{Dtact: An Experimental Stackful
             Coroutine Runtime for Heterogeneous
             Orchestration and Hardware-Level
             Control}},
  url     = {https://github.com/
             Apich-Organization/dtact},
  year    = {2026}
}"
                    </pre>
                </div>

                <div class="glass card-pad">
                    <span class="section-chip">"Contact & Links"</span>
                    <h3>"Get in Touch"</h3>
                    <p class="mt-sm">
                        "General enquiries, collaboration proposals:"
                    </p>
                    <p class="mt-sm">
                        <a href="mailto:info@apich.org" class="mono">
                            "info@apich.org"
                        </a>
                    </p>
                    <p class="mt-md">
                        "Security vulnerabilities: "
                        <a href="mailto:security@apich.org" class="mono">
                            "security@apich.org"
                        </a>
                    </p>
                    <hr class="divider" />
                    <div class="flex gap-sm flex-wrap mt-sm">
                        <a href="https://crates.io/crates/dtact"
                           target="_blank" rel="noopener" class="btn btn-ghost btn-sm">
                            "crates.io \u{2197}"
                        </a>
                        <a href="https://docs.rs/dtact"
                           target="_blank" rel="noopener" class="btn btn-ghost btn-sm">
                            "docs.rs \u{2197}"
                        </a>
                        <a href="https://github.com/Apich-Organization/dtact"
                           target="_blank" rel="noopener" class="btn btn-ghost btn-sm">
                            "GitHub \u{2197}"
                        </a>
                    </div>
                </div>
            </div>
        </section>

        // ── License ──────────────────────────────────────────────────────────
        <section class="section page-wrap">
            <div class="glass-alt card-pad">
                <span class="section-chip">"License"</span>
                <h3>"MIT OR Apache-2.0"</h3>
                <p class="mt-sm">
                    "dtact is dual-licensed under MIT and Apache-2.0. You may choose either
                     license at your option. This means dtact is compatible with virtually
                     all open-source and commercial projects."
                </p>
                <p class="mt-sm text-sm text-muted">
                    "Copyright \u{00A9} 2026 Xinyu Yang (xinyu.yang@apich.org)."
                </p>
                <div class="flex gap-sm mt-md flex-wrap">
                    <span class="badge badge-success">"MIT"</span>
                    <span class="badge badge-success">"Apache-2.0"</span>
                    <span class="badge badge-primary">"Rust 1.90+"</span>
                    <span class="badge badge-primary">"x86_64"</span>
                    <span class="badge badge-primary">"AArch64"</span>
                    <span class="badge badge-primary">"RISC-V 64"</span>
                </div>
            </div>
        </section>

        // ── Community & Security ─────────────────────────────────────────────
        <section class="section page-wrap">
            <div class="grid-2">
                <div class="glass card-pad community-card">
                    <div class="community-icon">
                        <svg width="28" height="28" viewBox="0 0 24 24" fill="none"
                             stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
                            <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>
                            <circle cx="9" cy="7" r="4"/>
                            <path d="M23 21v-2a4 4 0 0 0-3-3.87"/>
                            <path d="M16 3.13a4 4 0 0 1 0 7.75"/>
                        </svg>
                    </div>
                    <span class="section-chip">"Community"</span>
                    <h3>"Code of Conduct"</h3>
                    <p class="mt-sm text-sm">
                        "The dtact project requires all participants to adhere to applicable law
                         and to maintain a safe, respectful, and legally compliant environment
                         across GitHub and all related channels."
                    </p>
                    <ul class="community-list mt-sm text-sm">
                        <li>"Doxxing and harassment in any form are strictly prohibited"</li>
                        <li>"All contributions must respect third-party intellectual property"</li>
                        <li>"Report violations to "
                            <a href="mailto:Xinyu.Yang@apich.org" class="mono">"Xinyu.Yang@apich.org"</a>
                        </li>
                    </ul>
                    <div class="mt-md">
                        <a href="https://github.com/Apich-Organization/dtact/blob/main/CODE_OF_CONDUCT.md"
                           target="_blank" rel="noopener" class="btn btn-ghost btn-sm">
                            "Read Full CoC \u{2197}"
                        </a>
                    </div>
                </div>

                <div class="glass card-pad community-card">
                    <div class="community-icon community-icon-sec">
                        <svg width="28" height="28" viewBox="0 0 24 24" fill="none"
                             stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
                            <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
                        </svg>
                    </div>
                    <span class="section-chip">"Security"</span>
                    <h3>"Security Policy"</h3>
                    <p class="mt-sm text-sm">
                        "Do not open GitHub issues for vulnerabilities. Report privately via email
                         or through the official security portal. PGP encryption is supported."
                    </p>
                    <ul class="community-list mt-sm text-sm">
                        <li>"Email "
                            <a href="mailto:security@apich.org" class="mono">"security@apich.org"</a>
                            " with full reproduction details"
                        </li>
                        <li>"Or visit "
                            <a href="https://security.apich.org" target="_blank" rel="noopener" class="mono">
                                "security.apich.org"
                            </a>
                            " for PGP key and portal"
                        </li>
                        <li>"Supported: branch 0.2.x (active) \u{2014} older versions are EOL"</li>
                    </ul>
                    <div class="mt-md">
                        <a href="https://github.com/Apich-Organization/dtact/blob/main/SECURITY.md"
                           target="_blank" rel="noopener" class="btn btn-ghost btn-sm">
                            "Read Security Policy \u{2197}"
                        </a>
                    </div>
                </div>
            </div>
        </section>
    }
}

#[component]
fn FeatureCard(icon: &'static str, title: &'static str, body: &'static str) -> impl IntoView {
    view! {
        <div class="glass card-pad">
            <div class="feature-icon">{icon}</div>
            <h3 class="mb-sm">{title}</h3>
            <p class="text-sm">{body}</p>
        </div>
    }
}

#[component]
fn ArchCard(
    arch: &'static str,
    subtitle: &'static str,
    detail: &'static str,
    badge: &'static str,
    #[prop(default = "badge-primary")] badge_class: &'static str,
) -> impl IntoView {
    view! {
        <div class="glass card-pad arch-card">
            <div class="arch-card-head">
                <span class="arch-name mono">{arch}</span>
                <span class=format!("badge {badge_class}")>{badge}</span>
            </div>
            <p class="arch-sub text-sm text-muted mt-sm">{subtitle}</p>
            <p class="text-sm mt-sm">{detail}</p>
        </div>
    }
}

#[component]
fn MacroCard(name: &'static str, desc: &'static str, example: &'static str) -> impl IntoView {
    view! {
        <div class="glass card-pad">
            <p class="mono font-semibold accent-text mb-sm">{name}</p>
            <p class="text-sm">{desc}</p>
            <pre class="code-block text-xs mt-sm">{example}</pre>
        </div>
    }
}

#[component]
fn PipelineStep(n: &'static str, label: &'static str, detail: &'static str) -> impl IntoView {
    view! {
        <div class="pipeline-step">
            <div class="pipeline-n">{n}</div>
            <div>
                <p class="pipeline-label">{label}</p>
                <p class="pipeline-detail">{detail}</p>
            </div>
        </div>
    }
}
