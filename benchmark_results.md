## Benchmark Environment

```text
Operating System: Fedora Linux 44
KDE Plasma Version: 6.6.4
KDE Frameworks Version: 6.25.0
Qt Version: 6.10.3
Kernel Version: 6.19.14-300.fc44.x86_64 (64-bit)
Graphics Platform: Wayland
Processors: 8 × Intel® Core™ i7-8665U CPU @ 1.90GHz
Memory: 32 GiB of RAM (31.0 GiB usable)
Graphics Processor: Intel® UHD Graphics 620
Manufacturer: Dell Inc.
Product Name: Latitude 5400
```

---

## Benchmark Comparison: Dtact vs. Tokio

| Benchmark Category | Implementation | Lower Bound | **Estimate (Mean)** | Upper Bound | Outliers (Total) | Notable Observations |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Spawn Efficiency** (1M tasks) | **Dtact** | 389.39 ms | **393.88 ms** | 399.13 ms | 5 (5.00%) | 3 mild, 2 severe |
| | **Tokio** | 837.70 ms | **956.79 ms** | 1.0870 s | 19 (19.00%) | Improved performance cited; 16 severe outliers |
| **Yield Efficiency** (10 tasks x 100 yields) | **Dtact** | 448.53 µs | **450.90 µs** | 453.47 µs | 2 (2.00%) | 2 mild |
| | **Tokio** | 174.25 µs | **175.28 µs** | 176.39 µs | 13 (13.00%) | 4 mild, 9 severe |
| **Work Deflection** (Hot Core) | **Dtact** | 2.5126 s | **3.1545 s** | 3.8315 s | 1 (1.00%) | 1 mild |
| | **Tokio** | 6.8170 s | **7.2690 s** | 7.8028 s | 13 (13.00%) | 5 mild, 8 severe |

---

### Key Data Insights

*   **Performance Leadership:** 
    *   **Dtact** significantly outperforms Tokio in **Spawn Efficiency** (approx. 2.4x faster) and **Work Deflection** (approx. 2.3x faster).
    *   **Tokio** maintains a clear lead in **Yield Efficiency**, performing roughly 2.5x faster than Dtact in task yielding.
*   **Statistical Stability:** 
    *   Across all tests, **Dtact** exhibited a much lower frequency of "severe" outliers, suggesting more predictable tail latency in these specific workloads.
    *   **Tokio** showed a high number of severe outliers (up to 19% in spawn tasks), which may indicate scheduling jitter or resource contention during the sample window.
