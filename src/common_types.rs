/// Topology Strategy for the scheduler and fibers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyMode {
    /// Peer-to-Peer Mesh: Tasks are deflected to neighbors based on load.
    P2PMesh,
    /// Global: Tasks are shared across all cores via a common pool.
    Global,
    /// Pinned: Fiber must stay on its origin worker. No cross-core deflection.
    /// Set automatically when the fiber's switcher is a `SameThread` variant —
    /// `SameThread` context-switch assumes no thread migration, so deflection
    /// would violate the switcher's invariants.
    Pinned,
}

/// Metadata: Workload Hint for scheduling decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    /// Latency-sensitive compute tasks.
    Compute,
    /// Throughput-oriented I/O tasks.
    IO,
    /// Memory-intensive scanning or bulk transfers.
    Memory,
    /// Background maintenance or telemetry tasks.
    System,
}
