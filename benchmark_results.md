## Benchmark Environment

These benchmarks were conducted on a consumer-grade laptop using `dtact-v0.1.2`. Since dtact is architected for high-core-count server CPUs, this environment naturally limits its performance overhead advantages, shifting the comparison toward Tokio's optimal operating range.

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
| **Spawn Efficiency** (1M tasks) | **Dtact** | 339.81 ms | **348.73 ms** | 358.05 ms | 1 (1.00%) | 1 mild |
| | **Tokio** | 935.33 ms | **1.0346 s** | 1.1505 s | 10 (10.00%) | 2 mild, 8 severe |
| **Yield Efficiency** (10 tasks x 100 yields) | **Dtact** | 588.35 µs | **619.67 µs** | 655.95 µs | 5 (5.00%) | 3 mild, 2 severe |
| | **Tokio** | 243.77 µs | **248.46 µs** | 255.60 µs | 9 (9.00%) | 4 mild, 5 severe |
| **Work Deflection** (Hot Core) | **Dtact** | 3.4255 s | **3.6430 s** | 3.9242 s | 11 (11.00%) | 5 mild, 6 severe |
| | **Tokio** | 10.516 s | **11.107 s** | 11.793 s | 9 (9.00%) | 2 mild, 7 severe |

For a more detailed analysis and comprehensive metrics, please refer to the full report at [https://dtact.apich.org/report/index.html](https://dtact.apich.org/report/index.html).

---

### Key Data Insights

*   **Performance Leadership:**  
    *   **Dtact** significantly outperforms Tokio in **Spawn Efficiency** (approx. 2.9x faster) and **Work Deflection** (approx. 3.0x faster).
    *   **Tokio** maintains a clear lead in **Yield Efficiency**, performing roughly 2.5x faster than Dtact in task yielding.
*   **Statistical Stability:**  
    *   **Dtact** showed high stability in Spawn Efficiency with only 1% outliers, but encountered more variance in Work Deflection (11% outliers).
    *   **Tokio** continues to show significant "severe" outliers across all categories, particularly in Spawn Efficiency and Work Deflection, which may indicate scheduling jitter under high load.
