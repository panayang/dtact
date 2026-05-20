use leptos::prelude::*;

#[derive(Clone, Copy, PartialEq, Debug, Default)]
enum Section {
    #[default]
    Overview,
    MemoryArena,
    Scheduler,
    ContextSwitch,
    FiberLifecycle,
    PublicApi,
    MacroSystem,
    CFfi,
}

struct SectionMeta {
    key: Section,
    icon: &'static str,
    label: &'static str,
}

const SECTIONS: &[SectionMeta] = &[
    SectionMeta {
        key: Section::Overview,
        icon: "\u{1F4CB}",
        label: "Overview",
    },
    SectionMeta {
        key: Section::MemoryArena,
        icon: "\u{1F9E0}",
        label: "Memory Arena",
    },
    SectionMeta {
        key: Section::Scheduler,
        icon: "\u{1F578}",
        label: "P2P Mesh Scheduler",
    },
    SectionMeta {
        key: Section::ContextSwitch,
        icon: "\u{26A1}",
        label: "Context Switchers",
    },
    SectionMeta {
        key: Section::FiberLifecycle,
        icon: "\u{267B}",
        label: "Fiber Lifecycle",
    },
    SectionMeta {
        key: Section::PublicApi,
        icon: "\u{1F4D0}",
        label: "Public API",
    },
    SectionMeta {
        key: Section::MacroSystem,
        icon: "\u{2728}",
        label: "Macro System",
    },
    SectionMeta {
        key: Section::CFfi,
        icon: "\u{1F517}",
        label: "C/C++ FFI",
    },
];

#[component]
pub fn ArchitecturePage() -> impl IntoView {
    let current = RwSignal::new(Section::Overview);

    view! {
        <div class="page-wrap algo-layout">
            <aside class="algo-sidebar glass-alt card-pad">
                <p class="algo-section-title" style="margin-bottom:1rem">"Runtime Internals"</p>
                {SECTIONS.iter().map(|m| {
                    let key = m.key;
                    view! {
                        <button
                            class="algo-nav-btn"
                            class:active=move || current.get() == key
                            on:click=move |_| current.set(key)
                        >
                            <span class="algo-nav-icon">{m.icon}</span>
                            {m.label}
                        </button>
                    }
                }).collect_view()}
            </aside>

            <div class="algo-content">
                {move || match current.get() {
                    Section::Overview       => view! { <OverviewSection       /> }.into_any(),
                    Section::MemoryArena    => view! { <MemoryArenaSection    /> }.into_any(),
                    Section::Scheduler      => view! { <SchedulerSection      /> }.into_any(),
                    Section::ContextSwitch  => view! { <ContextSwitchSection  /> }.into_any(),
                    Section::FiberLifecycle => view! { <FiberLifecycleSection /> }.into_any(),
                    Section::PublicApi      => view! { <PublicApiSection      /> }.into_any(),
                    Section::MacroSystem    => view! { <MacroSystemSection    /> }.into_any(),
                    Section::CFfi           => view! { <CFfiSection           /> }.into_any(),
                }}
            </div>
        </div>
    }
}

// ── Overview ────────────────────────────────────────────────────────────────

#[component]
fn OverviewSection() -> impl IntoView {
    view! {
        <div class="algo-header">
            <h2>"\u{1F4CB} Runtime Overview"</h2>
            <p>"dtact is organised around four orthogonal subsystems that compose cleanly
                without shared mutable state between them."</p>
        </div>

        <div class="glass card-pad">
            <p class="algo-section-title">"Four-Pillar Architecture"</p>
            // Top-level architecture SVG diagram
            <div class="vis-frame">
                <svg viewBox="0 0 720 340" xmlns="http://www.w3.org/2000/svg" class="arch-overview-svg">
                    // Background lanes
                    <rect x="10"  y="10"  width="160" height="320" rx="12" class="arch-lane"/>
                    <rect x="190" y="10"  width="160" height="320" rx="12" class="arch-lane"/>
                    <rect x="370" y="10"  width="160" height="320" rx="12" class="arch-lane"/>
                    <rect x="550" y="10"  width="160" height="320" rx="12" class="arch-lane"/>
                    // Lane titles
                    <text x="90"  y="38" class="arch-lane-title" text-anchor="middle">"Memory"</text>
                    <text x="90"  y="52" class="arch-lane-title" text-anchor="middle">"Arena"</text>
                    <text x="270" y="38" class="arch-lane-title" text-anchor="middle">"P2P Mesh"</text>
                    <text x="270" y="52" class="arch-lane-title" text-anchor="middle">"Scheduler"</text>
                    <text x="450" y="38" class="arch-lane-title" text-anchor="middle">"Context"</text>
                    <text x="450" y="52" class="arch-lane-title" text-anchor="middle">"Switchers"</text>
                    <text x="630" y="38" class="arch-lane-title" text-anchor="middle">"Public"</text>
                    <text x="630" y="52" class="arch-lane-title" text-anchor="middle">"API / FFI"</text>
                    // Memory Arena nodes
                    <rect x="25"  y="68"  width="130" height="36" rx="6" class="arch-node arch-node-mem"/>
                    <text x="90"  y="90"  class="arch-node-text" text-anchor="middle">"FiberContext Pool"</text>
                    <rect x="25"  y="118" width="130" height="36" rx="6" class="arch-node arch-node-mem"/>
                    <text x="90"  y="140" class="arch-node-text" text-anchor="middle">"Lock-Free Free List"</text>
                    <rect x="25"  y="168" width="130" height="36" rx="6" class="arch-node arch-node-mem"/>
                    <text x="90"  y="190" class="arch-node-text" text-anchor="middle">"Huge Page Alloc"</text>
                    <rect x="25"  y="218" width="130" height="36" rx="6" class="arch-node arch-node-mem"/>
                    <text x="90"  y="240" class="arch-node-text" text-anchor="middle">"Safety0/1/2"</text>
                    <rect x="25"  y="268" width="130" height="36" rx="6" class="arch-node arch-node-mem"/>
                    <text x="90"  y="290" class="arch-node-text" text-anchor="middle">"NUMA Binding"</text>
                    // Scheduler nodes
                    <rect x="205" y="68"  width="130" height="36" rx="6" class="arch-node arch-node-sched"/>
                    <text x="270" y="90"  class="arch-node-text" text-anchor="middle">"N\u{00D7}N Mailbox Matrix"</text>
                    <rect x="205" y="118" width="130" height="36" rx="6" class="arch-node arch-node-sched"/>
                    <text x="270" y="140" class="arch-node-text" text-anchor="middle">"TaskChunk Batching"</text>
                    <rect x="205" y="168" width="130" height="36" rx="6" class="arch-node arch-node-sched"/>
                    <text x="270" y="190" class="arch-node-text" text-anchor="middle">"Hop Deflection"</text>
                    <rect x="205" y="218" width="130" height="36" rx="6" class="arch-node arch-node-sched"/>
                    <text x="270" y="240" class="arch-node-text" text-anchor="middle">"MPMC Warehouse"</text>
                    <rect x="205" y="268" width="130" height="36" rx="6" class="arch-node arch-node-sched"/>
                    <text x="270" y="290" class="arch-node-text" text-anchor="middle">"3-Tier Idle"</text>
                    // Context Switcher nodes
                    <rect x="385" y="68"  width="130" height="36" rx="6" class="arch-node arch-node-ctx"/>
                    <text x="450" y="90"  class="arch-node-text" text-anchor="middle">"x86_64 SysV"</text>
                    <rect x="385" y="118" width="130" height="36" rx="6" class="arch-node arch-node-ctx"/>
                    <text x="450" y="140" class="arch-node-text" text-anchor="middle">"x86_64 Windows"</text>
                    <rect x="385" y="168" width="130" height="36" rx="6" class="arch-node arch-node-ctx"/>
                    <text x="450" y="190" class="arch-node-text" text-anchor="middle">"AArch64 BTI/PAC"</text>
                    <rect x="385" y="218" width="130" height="36" rx="6" class="arch-node arch-node-ctx"/>
                    <text x="450" y="240" class="arch-node-text" text-anchor="middle">"RISC-V 64"</text>
                    <rect x="385" y="268" width="130" height="36" rx="6" class="arch-node arch-node-ctx"/>
                    <text x="450" y="290" class="arch-node-text" text-anchor="middle">"Float/NoFloat"</text>
                    // API/FFI nodes
                    <rect x="565" y="68"  width="130" height="36" rx="6" class="arch-node arch-node-api"/>
                    <text x="630" y="90"  class="arch-node-text" text-anchor="middle">"spawn() / yield_now()"</text>
                    <rect x="565" y="118" width="130" height="36" rx="6" class="arch-node arch-node-api"/>
                    <text x="630" y="140" class="arch-node-text" text-anchor="middle">"SpawnBuilder"</text>
                    <rect x="565" y="168" width="130" height="36" rx="6" class="arch-node arch-node-api"/>
                    <text x="630" y="190" class="arch-node-text" text-anchor="middle">"dtact-macros"</text>
                    <rect x="565" y="218" width="130" height="36" rx="6" class="arch-node arch-node-api"/>
                    <text x="630" y="240" class="arch-node-text" text-anchor="middle">"C/C++ FFI"</text>
                    <rect x="565" y="268" width="130" height="36" rx="6" class="arch-node arch-node-api"/>
                    <text x="630" y="290" class="arch-node-text" text-anchor="middle">"Handle Encoding"</text>
                </svg>
            </div>
            <p class="text-sm text-muted mt-sm">
                "Each pillar is independent. The arena does not know about the scheduler;
                 the switchers do not know about the arena. Integration happens only at
                 the task lifecycle boundary."
            </p>
        </div>

        <div class="glass-alt card-pad">
            <p class="algo-section-title">"Source Map"</p>
            <table class="cfg-table">
                <thead>
                    <tr>
                        <th>"File"</th>
                        <th>"Lines"</th>
                        <th>"Responsibility"</th>
                    </tr>
                </thead>
                <tbody>
                    <tr><td>"lib.rs"</td><td>"370"</td><td>"Global runtime singleton, backpressure, wake protocol"</td></tr>
                    <tr><td>"memory_management.rs"</td><td>"606"</td><td>"Lock-free arena, huge page alloc, fiber context pool"</td></tr>
                    <tr><td>"dta_scheduler.rs"</td><td>"1 582"</td><td>"P2P mesh, mailboxes, warehouse, worker heartbeat"</td></tr>
                    <tr><td>"context_switch.rs"</td><td>"1 819"</td><td>"Assembly context switchers (4 ISA × ABI variants)"</td></tr>
                    <tr><td>"api.rs"</td><td>"939"</td><td>"spawn/yield public API, SpawnBuilder, priorities"</td></tr>
                    <tr><td>"c_ffi.rs"</td><td>"889"</td><td>"C FFI boundary, handle encoding, config structs"</td></tr>
                    <tr><td>"future_bridge.rs"</td><td>"195"</td><td>"Future polling, Waker impl, TLS worker ID"</td></tr>
                    <tr><td>"dtact-macros/lib.rs"</td><td>"309"</td><td>"#[task], #[export_async], #[dtact_init] proc-macros"</td></tr>
                </tbody>
            </table>
        </div>
    }
}

// ── Memory Arena ─────────────────────────────────────────────────────────────

#[component]
fn MemoryArenaSection() -> impl IntoView {
    view! {
        <div class="algo-header">
            <h2>"\u{1F9E0} Memory Arena"</h2>
            <p>"Lock-free arena allocation for fiber contexts with O(1) alloc/dealloc,
                huge page optimisation, and tiered hardware safety."</p>
        </div>

        // Memory slot layout
        <div class="glass card-pad">
            <p class="algo-section-title">"Arena Slot Memory Layout"</p>
            <p class="text-sm mb-sm">
                "Each arena slot is a contiguous, page-aligned region. Within a slot the
                 three regions are laid out in ascending address order:"
            </p>
            <div class="vis-frame">
                <svg viewBox="0 0 680 180" xmlns="http://www.w3.org/2000/svg">
                    // Slot outline
                    <rect x="10" y="20" width="660" height="120" rx="10" class="mem-slot-outline"/>
                    // Stack region
                    <rect x="20" y="30" width="360" height="100" rx="7" class="mem-region-stack"/>
                    <text x="200" y="72" class="mem-region-label" text-anchor="middle">"Stack"</text>
                    <text x="200" y="90" class="mem-region-sub"  text-anchor="middle">"512 KB (configurable)"</text>
                    <text x="200" y="106" class="mem-region-sub" text-anchor="middle">"grows downward"</text>
                    // Guard page (Safety1+)
                    <rect x="388" y="30" width="28" height="100" rx="4" class="mem-region-guard"/>
                    <text x="402" y="80" class="mem-region-guard-label" text-anchor="middle"
                          transform="rotate(-90, 402, 80)">"GUARD"</text>
                    // Read buffer
                    <rect x="424" y="30" width="80" height="100" rx="7" class="mem-region-rbuf"/>
                    <text x="464" y="76" class="mem-region-label" text-anchor="middle">"Read"</text>
                    <text x="464" y="92" class="mem-region-sub"  text-anchor="middle">"Buffer"</text>
                    <text x="464" y="107" class="mem-region-sub" text-anchor="middle">"8 KB"</text>
                    // FiberContext struct
                    <rect x="512" y="30" width="158" height="100" rx="7" class="mem-region-ctx"/>
                    <text x="591" y="72" class="mem-region-label" text-anchor="middle">"FiberContext"</text>
                    <text x="591" y="90" class="mem-region-sub"  text-anchor="middle">"64-byte aligned"</text>
                    <text x="591" y="106" class="mem-region-sub" text-anchor="middle">"GPRs + SIMD + state"</text>
                    // Address arrows
                    <line x1="20" y1="155" x2="660" y2="155" class="mem-addr-line" marker-end="url(#arr)"/>
                    <text x="20"  y="172" class="mem-addr-text">"low addr"</text>
                    <text x="600" y="172" class="mem-addr-text">"high addr"</text>
                    <defs>
                        <marker id="arr" markerWidth="6" markerHeight="6" refX="3" refY="3" orient="auto">
                            <path d="M0,0 L0,6 L6,3 z" class="cfg-arrow"/>
                        </marker>
                    </defs>
                </svg>
            </div>
            <p class="text-sm text-muted mt-sm">
                "The guard page (present in Safety1 and Safety2) triggers a hardware page-fault
                 on stack overflow rather than silently corrupting the read buffer or FiberContext."
            </p>
        </div>

        // FiberContext struct layout
        <div class="glass card-pad">
            <p class="algo-section-title">"FiberContext Struct (64-byte aligned)"</p>
            <pre class="code-block text-xs">
"// memory_management.rs (simplified)
#[repr(C, align(64))]
pub struct FiberContext {
    pub regs:        Registers,      // 16×u64 GPRs + 512B SIMD state
    pub status:      FiberStatus,    // atomic: Initial|Running|Yielded|Finished|Panicked|…
    pub stack_ptr:   *mut u8,        // current rsp/sp
    pub stack_base:  *mut u8,        // bottom of allocated stack
    pub stack_limit: *mut u8,        // top (grows down → limit < base)
    pub closure_ptr: *mut (),        // boxed future / raw fn pointer
    pub result_ptr:  *mut (),        // where to write return value
    pub affinity:    AffinityHint,   // Any | SameCore | SameCCX | SameNUMA
    pub generation:  u32,            // ABA counter for handle safety
    pub worker_id:   u16,            // owning worker (for cross-thread returns)
    _pad:            [u8; …],        // pad to next 64-byte boundary
}"
            </pre>
        </div>

        // Lock-free free list
        <div class="glass card-pad">
            <p class="algo-section-title">"Lock-Free Free List (ABA-Protected)"</p>
            <p class="text-sm">
                "The free list is a Treiber stack with an ABA counter packed into the high
                 bits of the pointer (or a separate atomic on platforms without 128-bit CAS)."
            </p>
            <div class="vis-frame" style="min-height:160px">
                <svg viewBox="0 0 640 140" xmlns="http://www.w3.org/2000/svg">
                    // Head pointer
                    <rect x="10" y="50" width="90" height="36" rx="6" class="arch-node arch-node-api"/>
                    <text x="55" y="72" class="arch-node-text" text-anchor="middle">"head (atomic)"</text>
                    // Nodes
                    <rect x="140" y="50" width="90" height="36" rx="6" class="arch-node arch-node-mem"/>
                    <text x="185" y="68" class="arch-node-text" text-anchor="middle">"slot 3"</text>
                    <text x="185" y="82" class="arch-node-text text-xs" text-anchor="middle">"gen=5"</text>
                    <rect x="280" y="50" width="90" height="36" rx="6" class="arch-node arch-node-mem"/>
                    <text x="325" y="68" class="arch-node-text" text-anchor="middle">"slot 7"</text>
                    <text x="325" y="82" class="arch-node-text text-xs" text-anchor="middle">"gen=3"</text>
                    <rect x="420" y="50" width="90" height="36" rx="6" class="arch-node arch-node-mem"/>
                    <text x="465" y="68" class="arch-node-text" text-anchor="middle">"slot 1"</text>
                    <text x="465" y="82" class="arch-node-text text-xs" text-anchor="middle">"gen=9"</text>
                    <rect x="560" y="50" width="60" height="36" rx="6" class="arch-node"/>
                    <text x="590" y="72" class="arch-node-text" text-anchor="middle">"null"</text>
                    // Arrows
                    <line x1="100" y1="68" x2="138" y2="68" class="cfg-edge" marker-end="url(#arrf)"/>
                    <line x1="230" y1="68" x2="278" y2="68" class="cfg-edge" marker-end="url(#arrf)"/>
                    <line x1="370" y1="68" x2="418" y2="68" class="cfg-edge" marker-end="url(#arrf)"/>
                    <line x1="510" y1="68" x2="558" y2="68" class="cfg-edge" marker-end="url(#arrf)"/>
                    // CAS label
                    <text x="320" y="130" class="cfg-text" text-anchor="middle">
                        "alloc_context(): CAS(head, slot→next, gen+1)  |  free_context(): CAS(head, old_head, gen+1)"
                    </text>
                    <defs>
                        <marker id="arrf" markerWidth="6" markerHeight="6" refX="3" refY="3" orient="auto">
                            <path d="M0,0 L0,6 L6,3 z" class="cfg-arrow"/>
                        </marker>
                    </defs>
                </svg>
            </div>
        </div>

        // Huge page + safety levels
        <div class="grid-2">
            <div class="glass card-pad">
                <p class="algo-section-title">"Huge Page Allocation Path"</p>
                <div class="pipeline-steps" style="border-radius:var(--radius-md)">
                    <div class="pipeline-step">
                        <div class="pipeline-n">"1"</div>
                        <div>
                            <p class="pipeline-label">"Linux: MAP_HUGETLB"</p>
                            <p class="pipeline-detail">"mmap with MAP_HUGE_2MB flag. On success, 2 MB
                                huge pages are used \u{2014} reducing TLB pressure by 512\u{00D7}."</p>
                        </div>
                    </div>
                    <div class="pipeline-step">
                        <div class="pipeline-n">"2"</div>
                        <div>
                            <p class="pipeline-label">"Linux: THP hint"</p>
                            <p class="pipeline-detail">"madvise(MADV_HUGEPAGE) on the arena region to
                                request Transparent Huge Page promotion by the kernel."</p>
                        </div>
                    </div>
                    <div class="pipeline-step">
                        <div class="pipeline-n">"3"</div>
                        <div>
                            <p class="pipeline-label">"Windows: MEM_LARGE_PAGES"</p>
                            <p class="pipeline-detail">"VirtualAlloc with MEM_LARGE_PAGES when the
                                process holds SeLockMemoryPrivilege."</p>
                        </div>
                    </div>
                    <div class="pipeline-step">
                        <div class="pipeline-n">"4"</div>
                        <div>
                            <p class="pipeline-label">"Fallback: regular mmap/VirtualAlloc"</p>
                            <p class="pipeline-detail">"If huge pages are unavailable (permission or
                                system limit), falls back silently to standard 4 KB pages."</p>
                        </div>
                    </div>
                </div>
            </div>

            <div class="glass card-pad">
                <p class="algo-section-title">"Safety Levels"</p>
                <table class="cfg-table">
                    <thead>
                        <tr><th>"Level"</th><th>"Guard Pages"</th><th>"Use Case"</th></tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>"Safety0"</td>
                            <td>"None"</td>
                            <td>"Maximum throughput; trust stack sizing"</td>
                        </tr>
                        <tr>
                            <td>"Safety1"</td>
                            <td>"Every 32 contexts"</td>
                            <td>"Balanced: catches runaway fibers cheaply"</td>
                        </tr>
                        <tr>
                            <td>"Safety2"</td>
                            <td>"Per-context"</td>
                            <td>"Debug / hardened: guaranteed hw fault on overflow"</td>
                        </tr>
                    </tbody>
                </table>
                <p class="text-sm mt-md">
                    "NUMA binding is available on Linux via "
                    <span class="mono">"mbind()"</span>
                    " — call "
                    <span class="mono">"Arena::bind_numa(node)"</span>
                    " to pin arena pages to a specific NUMA node, keeping fiber stacks
                     local to the worker cores that use them."
                </p>
            </div>
        </div>
    }
}

// ── P2P Mesh Scheduler ───────────────────────────────────────────────────────

#[component]
fn SchedulerSection() -> impl IntoView {
    view! {
        <div class="algo-header">
            <h2>"\u{1F578} P2P Mesh Scheduler"</h2>
            <p>"Decentralised work-stealing scheduler with N\u{00D7}N SPSC mailboxes,
                hop-bounded deflection, and a 1M-task emergency warehouse."</p>
        </div>

        // Mailbox matrix diagram
        <div class="glass card-pad">
            <p class="algo-section-title">"N\u{00D7}N Mailbox Matrix (4 workers shown)"</p>
            <div class="vis-frame">
                <svg viewBox="0 0 620 360" xmlns="http://www.w3.org/2000/svg">
                    // Worker nodes
                    <circle cx="100" cy="80"  r="38" class="sched-worker"/>
                    <text x="100" y="76"  class="sched-worker-label" text-anchor="middle">"W0"</text>
                    <text x="100" y="92"  class="sched-worker-sub"   text-anchor="middle">"Core 0"</text>
                    <circle cx="320" cy="80"  r="38" class="sched-worker"/>
                    <text x="320" y="76"  class="sched-worker-label" text-anchor="middle">"W1"</text>
                    <text x="320" y="92"  class="sched-worker-sub"   text-anchor="middle">"Core 1"</text>
                    <circle cx="100" cy="280" r="38" class="sched-worker"/>
                    <text x="100" y="276" class="sched-worker-label" text-anchor="middle">"W2"</text>
                    <text x="100" y="292" class="sched-worker-sub"   text-anchor="middle">"Core 2"</text>
                    <circle cx="320" cy="280" r="38" class="sched-worker"/>
                    <text x="320" y="276" class="sched-worker-label" text-anchor="middle">"W3"</text>
                    <text x="320" y="292" class="sched-worker-sub"   text-anchor="middle">"Core 3"</text>
                    // Mailbox edges (one per ordered pair, bi-directional as two arrows)
                    // W0→W1
                    <path d="M138,72 Q210,40 282,72" class="sched-mailbox-edge" marker-end="url(#sarr)"/>
                    <text x="210" y="48" class="sched-edge-label" text-anchor="middle">"65 536 cap"</text>
                    // W1→W0
                    <path d="M282,88 Q210,120 138,88" class="sched-mailbox-edge-r" marker-end="url(#sarr2)"/>
                    // W0→W2
                    <path d="M72,118 Q40,180 72,242" class="sched-mailbox-edge" marker-end="url(#sarr)"/>
                    <text x="34" y="186" class="sched-edge-label" text-anchor="middle">"65 536"</text>
                    // W2→W0
                    <path d="M128,242 Q160,180 128,118" class="sched-mailbox-edge-r" marker-end="url(#sarr2)"/>
                    // W1→W3
                    <path d="M348,118 Q380,180 348,242" class="sched-mailbox-edge" marker-end="url(#sarr)"/>
                    // W3→W1
                    <path d="M292,242 Q260,180 292,118" class="sched-mailbox-edge-r" marker-end="url(#sarr2)"/>
                    // W2→W3
                    <path d="M138,288 Q210,320 282,288" class="sched-mailbox-edge" marker-end="url(#sarr)"/>
                    // W3→W2
                    <path d="M282,272 Q210,240 138,272" class="sched-mailbox-edge-r" marker-end="url(#sarr2)"/>
                    // Diagonals W0↔W3
                    <path d="M130,108 Q210,180 290,252" class="sched-mailbox-edge" marker-end="url(#sarr)" stroke-dasharray="5,3"/>
                    <path d="M290,252 Q210,180 130,108" class="sched-mailbox-edge-r" marker-end="url(#sarr2)" stroke-dasharray="5,3"/>
                    // Diagonals W1↔W2
                    <path d="M290,108 Q210,180 130,252" class="sched-mailbox-edge" marker-end="url(#sarr)" stroke-dasharray="5,3"/>
                    <path d="M130,252 Q210,180 290,108" class="sched-mailbox-edge-r" marker-end="url(#sarr2)" stroke-dasharray="5,3"/>
                    // Warehouse (central)
                    <rect x="420" y="140" width="180" height="80" rx="10" class="sched-warehouse"/>
                    <text x="510" y="172" class="sched-worker-label" text-anchor="middle">"Warehouse"</text>
                    <text x="510" y="190" class="sched-worker-sub"   text-anchor="middle">"1M tasks (MPMC)"</text>
                    // Arrows to warehouse
                    <line x1="360" y1="180" x2="418" y2="180" class="cfg-edge" marker-end="url(#arr2)"/>
                    <text x="390" y="172" class="sched-edge-label" text-anchor="middle">"overflow"</text>
                    <defs>
                        <marker id="sarr" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
                            <path d="M0,0 L0,6 L6,3 z" class="cfg-arrow"/>
                        </marker>
                        <marker id="sarr2" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
                            <path d="M0,0 L0,6 L6,3 z" fill="var(--c-accent)"/>
                        </marker>
                        <marker id="arr2" markerWidth="6" markerHeight="6" refX="3" refY="3" orient="auto">
                            <path d="M0,0 L0,6 L6,3 z" class="cfg-arrow"/>
                        </marker>
                    </defs>
                </svg>
            </div>
            <p class="text-sm text-muted mt-sm">
                "Solid arrows: SPSC mailbox (lock-free, power-of-two indexed, 65 536 task slots each).
                 Dashed diagonals: cross-CCX deflection paths. Workers only pull from mailboxes addressed
                 to them; they push into mailboxes addressed to other workers."
            </p>
        </div>

        // Deflection + warehouse
        <div class="glass card-pad">
            <p class="algo-section-title">"Hop-Bounded Deflection & Warehouse"</p>
            <pre class="code-block text-xs">
"// dta_scheduler.rs (conceptual)
fn route_task(task: Task, target: WorkerId, hops: u8) {
    let (space_ok, hops_ok) = (
        mailbox[current][target].has_space(),
        hops < max_hops,               // max_hops = num_workers / 2
    );
    // Branchless 4-way dispatch via function-pointer table:
    // (space_ok=T, hops_ok=T) → write to mailbox[current][target]
    // (space_ok=F, hops_ok=T) → deflect: pick adjacent worker, hops+1
    // (space_ok=T, hops_ok=F) → write local queue
    // (space_ok=F, hops_ok=F) → push to MPMC warehouse
    ROUTING_TABLE[space_ok as usize | (hops_ok as usize) << 1](task, …);
}

// Warehouse backpressure: when warehouse is >90% full,
// all new tasks short-circuit directly to warehouse
// to prevent mailbox saturation deadlock."
            </pre>
        </div>

        // TaskChunk + Warehouse specs
        <div class="grid-2">
            <div class="glass card-pad">
                <p class="algo-section-title">"TaskChunk Batching"</p>
                <p class="text-sm">
                    "Tasks are grouped into "
                    <span class="mono">"TaskChunk"</span>
                    " (32 tasks) before being written to a mailbox.
                     A single cache-line-sized write carries 32 task descriptors,
                     reducing the number of coherency transactions by 32\u{00D7}
                     compared to per-task delivery."
                </p>
                <pre class="code-block text-xs mt-sm">
"struct TaskChunk {
    tasks: [TaskDesc; 32],  // one cache-line write
    count: u8,
    _pad:  [u8; 7],
}"
                </pre>
            </div>
            <div class="glass card-pad">
                <p class="algo-section-title">"Warehouse Dimensions"</p>
                <table class="cfg-table">
                    <thead><tr><th>"Property"</th><th>"Value"</th></tr></thead>
                    <tbody>
                        <tr><td>"Structure"</td><td>"Bounded MPMC ring"</td></tr>
                        <tr><td>"Slots"</td><td>"32 768 TaskChunks"</td></tr>
                        <tr><td>"Tasks/slot"</td><td>"32"</td></tr>
                        <tr><td>"Total capacity"</td><td>"1 048 576 tasks"</td></tr>
                        <tr><td>"CAS backoff"</td><td>"Staggered, prime mult. 7"</td></tr>
                        <tr><td>"Backpressure"</td><td>">90% full \u{2192} hard-route"</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        // 3-Tier idle
        <div class="glass card-pad">
            <p class="algo-section-title">"3-Tier Worker Idle Strategy (Zero OS Syscalls)"</p>
            <div class="vis-frame" style="min-height:120px">
                <svg viewBox="0 0 680 100" xmlns="http://www.w3.org/2000/svg">
                    <rect x="10"  y="20" width="180" height="60" rx="8" class="arch-node arch-node-mem"/>
                    <text x="100" y="47" class="arch-node-text" text-anchor="middle">"Tier 1"</text>
                    <text x="100" y="63" class="arch-node-text" text-anchor="middle">"256 \u{00D7} spin_loop()"</text>
                    <text x="100" y="79" class="arch-node-text" text-anchor="middle">"~256 ns"</text>
                    <rect x="250" y="20" width="180" height="60" rx="8" class="arch-node arch-node-sched"/>
                    <text x="340" y="47" class="arch-node-text" text-anchor="middle">"Tier 2"</text>
                    <text x="340" y="63" class="arch-node-text" text-anchor="middle">"2048 \u{00D7} pause/yield"</text>
                    <text x="340" y="79" class="arch-node-text" text-anchor="middle">"x86 pause / ARM yield"</text>
                    <rect x="490" y="20" width="180" height="60" rx="8" class="arch-node arch-node-ctx"/>
                    <text x="580" y="47" class="arch-node-text" text-anchor="middle">"Tier 3"</text>
                    <text x="580" y="63" class="arch-node-text" text-anchor="middle">"WFE / umwait"</text>
                    <text x="580" y="79" class="arch-node-text" text-anchor="middle">"hardware standby"</text>
                    <line x1="190" y1="50" x2="248" y2="50" class="cfg-edge" marker-end="url(#iarr)"/>
                    <line x1="430" y1="50" x2="488" y2="50" class="cfg-edge" marker-end="url(#iarr)"/>
                    <defs>
                        <marker id="iarr" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
                            <path d="M0,0 L0,6 L6,3 z" class="cfg-arrow"/>
                        </marker>
                    </defs>
                </svg>
            </div>
            <p class="text-sm text-muted mt-sm">
                "The event_signal cache line is isolated to prevent false sharing.
                 On AArch64 a "
                <span class="mono">"SEV"</span>
                " instruction wakes WFE spinners. On x86 WAITPKG processors,
                 "
                <span class="mono">"umonitor + umwait"</span>
                " provides cache-line-level wake notification without an OS context switch."
            </p>
        </div>
    }
}

// ── Context Switchers ────────────────────────────────────────────────────────

#[component]
fn ContextSwitchSection() -> impl IntoView {
    view! {
        <div class="algo-header">
            <h2>"\u{26A1} Context Switchers"</h2>
            <p>"Naked-function assembly switchers, tuned per ISA and ABI, with SIMD state
                preservation and hardware CFI (BTI/PAC on AArch64)."</p>
        </div>

        // Platform matrix
        <div class="glass card-pad">
            <p class="algo-section-title">"ISA × ABI Matrix"</p>
            <table class="cfg-table">
                <thead>
                    <tr>
                        <th>"Switcher Variant"</th>
                        <th>"Platform"</th>
                        <th>"Saved State"</th>
                        <th>"Special"</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>"CrossThreadFloat"</td>
                        <td>"x86_64 SysV (Linux/macOS)"</td>
                        <td>"15 GPRs + FXSAVE (512B)"</td>
                        <td>"umwait/umonitor if WAITPKG"</td>
                    </tr>
                    <tr>
                        <td>"WindowsFloat"</td>
                        <td>"x86_64 Windows (x64 ABI)"</td>
                        <td>"RBX/RSI/RDI/RBP/R12-R15 + XMM6-15"</td>
                        <td>"TIB gs:[00/08/10/1478] + SEH list"</td>
                    </tr>
                    <tr>
                        <td>"AArch64Float"</td>
                        <td>"AArch64 (Linux / Apple Silicon)"</td>
                        <td>"x19-x28, x29, lr + Q8-Q23"</td>
                        <td>"BTI bti c; PAC pacibsp/autibsp; x18 reserved"</td>
                    </tr>
                    <tr>
                        <td>"RiscV64Float"</td>
                        <td>"RISC-V 64 (LP64D)"</td>
                        <td>"s0-s11, ra + f8-f9, f18-f27"</td>
                        <td>"Extensible for V extension"</td>
                    </tr>
                </tbody>
            </table>
        </div>

        // Context switch timing sequence diagram
        <div class="glass card-pad">
            <p class="algo-section-title">"Context Switch Sequence (x86_64 SysV)"</p>
            <div class="vis-frame">
                <svg viewBox="0 0 680 280" xmlns="http://www.w3.org/2000/svg">
                    // Timeline columns
                    <line x1="120" y1="10" x2="120" y2="260" class="seq-lifeline"/>
                    <line x1="340" y1="10" x2="340" y2="260" class="seq-lifeline"/>
                    <line x1="560" y1="10" x2="560" y2="260" class="seq-lifeline"/>
                    // Actor labels
                    <rect x="60"  y="0" width="120" height="30" rx="5" class="seq-actor"/>
                    <text x="120" y="19" class="seq-actor-text" text-anchor="middle">"Caller Fiber"</text>
                    <rect x="280" y="0" width="120" height="30" rx="5" class="seq-actor"/>
                    <text x="340" y="19" class="seq-actor-text" text-anchor="middle">"Switcher (naked)"</text>
                    <rect x="500" y="0" width="120" height="30" rx="5" class="seq-actor"/>
                    <text x="560" y="19" class="seq-actor-text" text-anchor="middle">"Target Fiber"</text>
                    // Step 1: call switcher
                    <line x1="120" y1="55" x2="330" y2="55" class="seq-msg" marker-end="url(#seq-arr)"/>
                    <text x="225" y="48" class="seq-msg-label" text-anchor="middle">"call switch_context()"</text>
                    // Step 2: FXSAVE + GPR push
                    <rect x="280" y="62" width="120" height="24" rx="3" class="seq-box"/>
                    <text x="340" y="78" class="seq-msg-label" text-anchor="middle">"FXSAVE + push r12-r15,rbp,rbx"</text>
                    // Step 3: prefetch target
                    <rect x="280" y="92" width="120" height="24" rx="3" class="seq-box"/>
                    <text x="340" y="108" class="seq-msg-label" text-anchor="middle">"prefetch ctx + stack top"</text>
                    // Step 4: save rsp
                    <rect x="280" y="122" width="120" height="24" rx="3" class="seq-box"/>
                    <text x="340" y="138" class="seq-msg-label" text-anchor="middle">"mov [caller.regs.rsp], rsp"</text>
                    // Step 5: swap rsp
                    <rect x="280" y="152" width="120" height="24" rx="3" class="seq-box"/>
                    <text x="340" y="168" class="seq-msg-label" text-anchor="middle">"mov rsp, [target.regs.rsp]"</text>
                    // Step 6: restore target
                    <line x1="340" y1="184" x2="550" y2="184" class="seq-msg" marker-end="url(#seq-arr)"/>
                    <text x="445" y="177" class="seq-msg-label" text-anchor="middle">"pop r12-r15,rbp,rbx + FXRSTOR"</text>
                    // Step 7: ret → target executes
                    <rect x="500" y="191" width="120" height="24" rx="3" class="seq-box"/>
                    <text x="560" y="207" class="seq-msg-label" text-anchor="middle">"ret → resume target"</text>
                    // Annotations
                    <text x="10" y="110" class="seq-note">"save"</text>
                    <text x="10" y="165" class="seq-note">"swap"</text>
                    <text x="600" y="200" class="seq-note">"restore"</text>
                    <defs>
                        <marker id="seq-arr" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
                            <path d="M0,0 L0,6 L6,3 z" class="cfg-arrow"/>
                        </marker>
                    </defs>
                </svg>
            </div>
            <p class="text-sm text-muted mt-sm">
                "The 3-instruction prefetch sequence runs "
                <em>"before"</em>
                " the stack pointer swap — this warms the target FiberContext and the top
                 of its stack into L1/L2 cache while the current fiber\u{2019}s state is
                 still being saved, hiding the memory latency in the common case."
            </p>
        </div>

        // AArch64 BTI/PAC detail
        <div class="glass card-pad">
            <p class="algo-section-title">"AArch64: BTI + PAC (Hardware CFI)"</p>
            <pre class="code-block text-xs">
"// context_switch.rs — AArch64 switcher prologue
naked_asm!(
    // BTI: marks this as a valid indirect-call target
    \"bti  c\",
    // PAC: sign the return address with the SP key
    \"pacibsp\",
    // save callee-saved GPRs (x19-x28, x29=fp, x30=lr)
    \"stp  x19, x20, [sp, #-16]!\",
    \"stp  x21, x22, [sp, #-16]!\",
    // ... (all pairs)
    // save SIMD registers Q8-Q23
    \"stp  q8,  q9,  [sp, #-32]!\",
    // ... (all pairs)
    // prefetch target context (3 instructions)
    \"prfm pldl1keep, [{target}]\",
    \"prfm pldl1keep, [{target}, #64]\",
    \"prfm pldl2keep, [{target}, #128]\",
    // swap stack pointers
    \"mov  {old_sp}, sp\",
    \"mov  sp, {new_sp}\",
    // restore target state + authenticate PAC
    \"ldp  q8, q9, [sp], #32\",
    // ... (restore all)
    \"autibsp\",
    \"ret\",
)"
            </pre>
        </div>
    }
}

// ── Fiber Lifecycle ──────────────────────────────────────────────────────────

#[component]
fn FiberLifecycleSection() -> impl IntoView {
    view! {
        <div class="algo-header">
            <h2>"\u{267B} Fiber Lifecycle"</h2>
            <p>"Seven atomic states govern a fiber from allocation to reclamation.
                All transitions are sequentially consistent to prevent torn reads
                across thread boundaries."</p>
        </div>

        // State machine diagram
        <div class="glass card-pad">
            <p class="algo-section-title">"FiberStatus State Machine"</p>
            <div class="vis-frame">
                <svg viewBox="0 0 700 320" xmlns="http://www.w3.org/2000/svg">
                    // States
                    <rect x="290" y="10"  width="120" height="44" rx="8" class="fs-state fs-initial"/>
                    <text x="350" y="37"  class="fs-label" text-anchor="middle">"Initial"</text>
                    <rect x="290" y="90"  width="120" height="44" rx="8" class="fs-state fs-running"/>
                    <text x="350" y="117" class="fs-label" text-anchor="middle">"Running"</text>
                    <rect x="100" y="180" width="120" height="44" rx="8" class="fs-state fs-yielded"/>
                    <text x="160" y="207" class="fs-label" text-anchor="middle">"Yielded"</text>
                    <rect x="290" y="180" width="120" height="44" rx="8" class="fs-state fs-suspending"/>
                    <text x="350" y="207" class="fs-label" text-anchor="middle">"Suspending"</text>
                    <rect x="100" y="260" width="120" height="44" rx="8" class="fs-state fs-notified"/>
                    <text x="160" y="287" class="fs-label" text-anchor="middle">"Notified"</text>
                    <rect x="480" y="180" width="120" height="44" rx="8" class="fs-state fs-finished"/>
                    <text x="540" y="207" class="fs-label" text-anchor="middle">"Finished"</text>
                    <rect x="480" y="260" width="120" height="44" rx="8" class="fs-state fs-panicked"/>
                    <text x="540" y="287" class="fs-label" text-anchor="middle">"Panicked"</text>
                    // Transitions
                    // Initial → Running
                    <line x1="350" y1="54"  x2="350" y2="88"  class="fs-edge" marker-end="url(#fsarr)"/>
                    <text x="370" y="74"    class="fs-edge-label">"first schedule"</text>
                    // Running → Yielded
                    <line x1="300" y1="112" x2="220" y2="180" class="fs-edge" marker-end="url(#fsarr)"/>
                    <text x="240" y="145"   class="fs-edge-label">"yield_now()"</text>
                    // Yielded → Notified
                    <line x1="160" y1="224" x2="160" y2="258" class="fs-edge" marker-end="url(#fsarr)"/>
                    <text x="175" y="244"   class="fs-edge-label">"waker.wake()"</text>
                    // Notified → Running
                    <path d="M220,275 Q350,300 350,136" class="fs-edge" fill="none" marker-end="url(#fsarr)"/>
                    <text x="340" y="310"   class="fs-edge-label">"re-schedule"</text>
                    // Running → Suspending (future pending)
                    <line x1="350" y1="134" x2="350" y2="178" class="fs-edge" marker-end="url(#fsarr)"/>
                    <text x="370" y="160"   class="fs-edge-label">"Poll::Pending"</text>
                    // Suspending → Running (woken before commit)
                    <path d="M290,190 Q260,160 290,112" class="fs-edge-r" fill="none" marker-end="url(#fsarr2)"/>
                    <text x="240" y="150"   class="fs-edge-label-r">"race: woken"</text>
                    // Running → Finished
                    <line x1="410" y1="112" x2="480" y2="180" class="fs-edge" marker-end="url(#fsarr)"/>
                    <text x="465" y="145"   class="fs-edge-label">"return"</text>
                    // Running → Panicked
                    <path d="M420,130 Q530,160 520,258" class="fs-edge" fill="none" marker-end="url(#fsarr)"/>
                    <text x="540" y="155"   class="fs-edge-label">"panic!"</text>
                    <defs>
                        <marker id="fsarr" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
                            <path d="M0,0 L0,6 L6,3 z" class="cfg-arrow"/>
                        </marker>
                        <marker id="fsarr2" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
                            <path d="M0,0 L0,6 L6,3 z" fill="var(--c-accent)"/>
                        </marker>
                    </defs>
                </svg>
            </div>
            <p class="text-sm text-muted mt-sm">
                "The Suspending state prevents a race between a future registering its waker
                 and the scheduler deciding to park the fiber. If a waker fires while the fiber
                 is in Suspending, the CAS from Suspending \u{2192} Running wins and the fiber
                 is immediately re-queued without ever being parked."
            </p>
        </div>

        // Wake protocol
        <div class="glass card-pad">
            <p class="algo-section-title">"Wake Protocol (lib.rs)"</p>
            <pre class="code-block text-xs">
"// lib.rs — fiber wake path
fn wake_fiber(id: FiberId) {
    let ctx = &ARENA.slot(id);
    // Attempt Yielded → Notified transition
    match ctx.status.compare_exchange(
        FiberStatus::Yielded,
        FiberStatus::Notified,
        Ordering::AcqRel, Ordering::Acquire,
    ) {
        Ok(_) => {
            // Successfully notified; re-queue to origin worker
            SCHEDULER.requeue(id, ctx.worker_id);
        }
        Err(FiberStatus::Suspending) => {
            // Future woke before fiber committed to park;
            // CAS Suspending → Running to abort the park
            ctx.status.store(FiberStatus::Running, Ordering::Release);
        }
        Err(_) => { /* already Running or Finished — no-op */ }
    }
}"
            </pre>
        </div>
    }
}

// ── Public API ───────────────────────────────────────────────────────────────

#[component]
fn PublicApiSection() -> impl IntoView {
    view! {
        <div class="algo-header">
            <h2>"\u{1F4D0} Public API"</h2>
            <p>"Ergonomic Rust API via "
               <span class="mono">"spawn()"</span>
               " and "
               <span class="mono">"yield_now()"</span>
               " free functions, plus a "
               <span class="mono">"SpawnBuilder"</span>
               " for fine-grained control."
            </p>
        </div>

        <div class="glass card-pad">
            <p class="algo-section-title">"Core Functions"</p>
            <pre class="code-block text-xs">
"// api.rs — entry points

// Simple spawn: any async block becomes a stackful fiber
pub fn spawn<F: Future + Send + 'static>(future: F) -> FiberHandle { … }

// Explicit cooperative yield
pub async fn yield_now() { … }

// Builder for fine-grained control:
SpawnBuilder::new()
    .priority(Priority::High)          // Low | Normal | High | Critical
    .affinity(Affinity::SameCCX)       // Any | SameCore | SameCCX | SameNUMA
    .kind(WorkloadKind::Compute)       // Compute | IO | Memory | System
    .stack_size(512 * 1024)            // override default stack
    .switcher(Switcher::CrossThreadFloat) // explicit switcher selection
    .spawn(async move { … });"
            </pre>
        </div>

        <div class="grid-2">
            <div class="glass card-pad">
                <p class="algo-section-title">"Priority Levels"</p>
                <table class="cfg-table">
                    <thead><tr><th>"Priority"</th><th>"Scheduler Hint"</th></tr></thead>
                    <tbody>
                        <tr><td>"Low"</td><td>"First to be deflected under load"</td></tr>
                        <tr><td>"Normal"</td><td>"Default; fair FIFO within local queue"</td></tr>
                        <tr><td>"High"</td><td>"Preferred placement; lower deflection rate"</td></tr>
                        <tr><td>"Critical"</td><td>"Never deflected; always local queue front"</td></tr>
                    </tbody>
                </table>
            </div>
            <div class="glass card-pad">
                <p class="algo-section-title">"Affinity Modes"</p>
                <table class="cfg-table">
                    <thead><tr><th>"Mode"</th><th>"Target Selection"</th></tr></thead>
                    <tbody>
                        <tr><td>"Any"</td><td>"Round-robin across all workers"</td></tr>
                        <tr><td>"SameCore"</td><td>"Pin to the spawning core"</td></tr>
                        <tr><td>"SameCCX"</td><td>"Prefer workers in the same CCX cluster"</td></tr>
                        <tr><td>"SameNUMA"</td><td>"Prefer workers on the same NUMA node"</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    }
}

// ── Macro System ─────────────────────────────────────────────────────────────

#[component]
fn MacroSystemSection() -> impl IntoView {
    view! {
        <div class="algo-header">
            <h2>"\u{2728} Macro System (dtact-macros)"</h2>
            <p>"Three proc-macros encode runtime metadata at compile time and auto-generate
                C FFI exports."</p>
        </div>

        <div class="glass card-pad">
            <p class="algo-section-title">"#[task(...)] — Compile-Time Metadata"</p>
            <p class="text-sm">
                "Attached to "
                <span class="mono">"async fn"</span>
                " bodies. Generates a hidden "
                <span class="mono">"dtact_metadata_<name>"</span>
                " module with "
                <span class="mono">"const"</span>
                " items that the scheduler reads at spawn time — no runtime overhead."
            </p>
            <pre class="code-block text-xs mt-sm">
"#[task(
    priority = \"High\",
    kind     = \"Compute\",
    stack    = \"256K\",
    switcher = \"CrossThreadFloat\",
)]
async fn image_encoder(frame: Vec<u8>) -> Vec<u8> {
    // … encode …
}

// Expands to:
mod dtact_metadata_image_encoder {
    pub const PRIORITY: u8  = 2;        // High
    pub const KIND:     u8  = 0;        // Compute
    pub const STACK_SZ: u32 = 262_144; // 256 KiB
    pub const SWITCHER: u8  = 1;        // CrossThreadFloat
}"
            </pre>
        </div>

        <div class="glass card-pad">
            <p class="algo-section-title">"#[export_async] — C FFI Export"</p>
            <pre class="code-block text-xs">
"// Rust side:
#[export_async]
async fn compress(data: *const u8, len: usize) -> usize {
    // runs as a dtact fiber; fully stackful
    lz4_compress(data, len).await
}

// Auto-generated extern \"C\" wrapper:
#[no_mangle]
pub extern \"C\" fn dtact_export_compress(
    data: *const u8,
    len: usize,
) -> dtact_handle_t {
    dtact_fiber_launch_raw(compress_fiber_trampoline, (data, len))
}

// C caller:
dtact_handle_t h = dtact_export_compress(buf, 4096);
size_t result = (size_t) dtact_await(rt, h);"
            </pre>
        </div>

        <div class="glass card-pad">
            <p class="algo-section-title">"#[dtact_init(...)] — Runtime Entry Point"</p>
            <pre class="code-block text-xs">
"#[dtact_init(
    workers  = 8,     // worker thread count
    stack    = \"512K\", // default fiber stack size
    capacity = \"4096\", // max concurrent fibers
    safety   = 1,     // Safety1: guard page every 32 contexts
)]
fn main() {
    // Runtime is initialised before this body executes.
    // Workers are already spinning.
    for i in 0..100 {
        spawn(async move { println!(\"fiber {i}\"); });
    }
    // dtact blocks here until all fibers finish or shutdown() is called.
}"
            </pre>
        </div>
    }
}

// ── C FFI ────────────────────────────────────────────────────────────────────

#[component]
fn CFfiSection() -> impl IntoView {
    view! {
        <div class="algo-header">
            <h2>"\u{1F517} C / C++ FFI"</h2>
            <p>"Generated by cbindgen. "
               <span class="mono">"dtact.h"</span>
               " and "
               <span class="mono">"dtact.hpp"</span>
               " are shipped in the repository root and updated on every release."
            </p>
        </div>

        // Handle encoding diagram
        <div class="glass card-pad">
            <p class="algo-section-title">"Fiber Handle Encoding (64-bit)"</p>
            <div class="vis-frame" style="min-height:120px">
                <svg viewBox="0 0 680 90" xmlns="http://www.w3.org/2000/svg">
                    <rect x="10"  y="20" width="200" height="50" rx="6" class="mem-region-stack"/>
                    <text x="110" y="42" class="mem-region-label" text-anchor="middle">"Fiber Index [63:32]"</text>
                    <text x="110" y="60" class="mem-region-sub"   text-anchor="middle">"u32 slot in arena"</text>
                    <rect x="218" y="20" width="160" height="50" rx="6" class="mem-region-rbuf"/>
                    <text x="298" y="42" class="mem-region-label" text-anchor="middle">"Worker Origin [31:16]"</text>
                    <text x="298" y="60" class="mem-region-sub"   text-anchor="middle">"spawning worker id"</text>
                    <rect x="386" y="20" width="160" height="50" rx="6" class="mem-region-ctx"/>
                    <text x="466" y="42" class="mem-region-label" text-anchor="middle">"Generation [15:0]"</text>
                    <text x="466" y="60" class="mem-region-sub"   text-anchor="middle">"ABA protection counter"</text>
                    <line x1="10" y1="82" x2="548" y2="82" class="mem-addr-line"/>
                    <text x="10"  y="90" class="mem-addr-text">"bit 63 (MSB)"</text>
                    <text x="500" y="90" class="mem-addr-text">"bit 0 (LSB)"</text>
                </svg>
            </div>
            <p class="text-sm text-muted mt-sm">
                "The generation counter prevents ABA: even if a slot is reallocated, the
                 old handle will fail validation because its generation no longer matches
                 the arena slot\u{2019}s current generation."
            </p>
        </div>

        // C API
        <div class="glass card-pad">
            <p class="algo-section-title">"Core C API (dtact.h)"</p>
            <pre class="code-block text-xs">
"// Initialise runtime; returns opaque handle
dtact_runtime_t* dtact_init(const dtact_config_t* cfg);

// Start worker threads (blocks until shutdown)
void dtact_run(dtact_runtime_t* rt);

// Spawn a fiber from C; returns a handle
dtact_handle_t dtact_fiber_launch(
    dtact_runtime_t* rt,
    void (*fn)(void*),
    void* arg);

// Spawn with custom options
dtact_handle_t dtact_fiber_launch_ext(
    dtact_runtime_t* rt,
    void (*fn)(void*),
    void* arg,
    const dtact_spawn_options_t* opts);

// Block calling thread until fiber completes; returns result
uintptr_t dtact_await(dtact_runtime_t* rt, dtact_handle_t handle);

// Signal cooperative shutdown to all workers
void dtact_shutdown(dtact_runtime_t* rt);"
            </pre>
        </div>

        // Configuration
        <div class="glass card-pad">
            <p class="algo-section-title">"Configuration Structs"</p>
            <pre class="code-block text-xs">
"typedef struct {
    uint32_t workers;    // number of worker threads
    uint32_t capacity;   // max concurrent fibers
    uint32_t stack_sz;   // fiber stack size in bytes
    uint8_t  safety;     // 0=Safety0, 1=Safety1, 2=Safety2
    uint8_t  numa_node;  // 0xFF = no binding; 0-N = bind to node
    uint8_t  _pad[2];
} dtact_config_t;

typedef struct {
    uint8_t  priority;   // 0=Low 1=Normal 2=High 3=Critical
    uint8_t  kind;       // 0=Compute 1=IO 2=Memory 3=System
    uint8_t  switcher;   // 0=SameThread 1=CrossThreadFloat 2=NoFloat
    uint8_t  affinity;   // 0=Any 1=SameCore 2=SameCCX 3=SameNUMA
    uint32_t stack_sz;   // 0 = use runtime default
} dtact_spawn_options_t;"
            </pre>
        </div>
    }
}
