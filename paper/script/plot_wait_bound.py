"""
plot_wait_bound.py — Bounded Waiting Time: Parameter Sweep and Visualization
============================================================================
Numerically evaluates and plots the worst-case waiting time bound

    W(τ) / δ  ≤  2L  +  C_W·K / N

from paper/progress/bounded_wait.tex (Theorem 4.1) across a range of
worker counts N and warehouse-to-queue size ratios.

Outputs:
  1. Console table: W/δ and k-bound for each (N, scenario) pair.
  2. wait_bound_vs_N.png — W(τ)/δ vs N for fixed parameters,
       showing the 1/N decay of the warehouse component.
  3. k_bound_vs_N.png    — k-bounded fairness value vs N.
  4. component_breakdown.png — stacked bar: queue-depth vs warehouse
       component for each N at benchmark parameters.

All figures are saved to the same directory as this script.
"""

import math
import os
import sys
import numpy as np

# ── optional matplotlib ────────────────────────────────────────────────────
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    HAS_MPL = True
except ImportError:
    HAS_MPL = False
    print("matplotlib not found — skipping plot output, printing tables only.")

# ─────────────────────────────────────────────────────────────────────────────
# Parameter sets
# ─────────────────────────────────────────────────────────────────────────────

BENCHMARK = dict(
    name   = "Benchmark (N=4)",
    L      = 131_072,     # LOCAL_QUEUE_CAPACITY
    C_W    = 32_768,      # WAREHOUSE_CAPACITY (chunks)
    K      = 32,          # CHUNK_SIZE
    H      = 131_072 - 131_072 // 8,   # LOCAL_QUEUE_HIGH_WATERMARK
    D      = 64,          # drain cap (chunks per drain call)
)

# Additional scenarios to sweep
SCENARIOS = [
    dict(name="Small (L=4k, CW=512)",   L=4_096,    C_W=512,    K=32, D=64),
    dict(name="Medium (L=32k, CW=4k)",  L=32_768,   C_W=4_096,  K=32, D=64),
    dict(name="Benchmark",               **{k: BENCHMARK[k] for k in
                                             ("L","C_W","K","D")}),
    dict(name="Large (L=256k, CW=64k)", L=262_144,  C_W=65_536, K=32, D=64),
]

N_VALUES = [1, 2, 4, 8, 16, 32, 64]

# ─────────────────────────────────────────────────────────────────────────────
# Core formula
# ─────────────────────────────────────────────────────────────────────────────

def wait_bound_over_delta(N: int, L: int, C_W: int, K: int, **kw) -> float:
    """
    W(τ)/δ  ≤  2L  +  C_W·K / N
    (Theorem 4.1 of bounded_wait.tex)
    """
    queue_component     = 2 * L
    warehouse_component = (C_W * K) / N
    return queue_component + warehouse_component

def k_bound(N: int, L: int, C_W: int, K: int, **kw) -> int:
    """
    k  ≤  ceil(2  +  C_W·K / (N·L))
    (Theorem 5.2 of bounded_wait.tex)
    """
    return math.ceil(2 + (C_W * K) / (N * L))

def warehouse_component(N: int, C_W: int, K: int, **kw) -> float:
    return (C_W * K) / N

def queue_component(L: int, **kw) -> float:
    return 2 * L

# ─────────────────────────────────────────────────────────────────────────────
# Console output
# ─────────────────────────────────────────────────────────────────────────────

def print_table():
    print("\n" + "="*72)
    print("Bounded Waiting Time  W(τ)/δ  and  k-bound  vs  N")
    print("Formula: W/δ ≤ 2L + C_W·K/N  |  k ≤ ceil(2 + C_W·K/(N·L))")
    print("="*72)

    for sc in SCENARIOS:
        print(f"\n  Scenario: {sc['name']}")
        print(f"  L={sc['L']:>7,}   C_W={sc['C_W']:>6,}   K={sc['K']}")
        print(f"  {'N':>4}  {'W/δ (total)':>14}  {'Queue 2L':>10}  "
              f"{'Warehouse CK/N':>16}  {'k-bound':>8}")
        print("  " + "-"*58)
        for N in N_VALUES:
            wq  = queue_component(**sc)
            ww  = warehouse_component(N=N, **sc)
            w   = wq + ww
            k   = k_bound(N=N, **sc)
            dom = "←queue" if wq >= ww else "←wh"
            print(f"  {N:>4}  {w:>14,.0f}  {wq:>10,.0f}  {ww:>16,.0f}  "
                  f"{k:>8d}  {dom}")

    # Crossover point for benchmark scenario
    sc = BENCHMARK
    crossover_N = sc["C_W"] * sc["K"] / (2 * sc["L"])
    print(f"\n  Benchmark crossover (warehouse = queue component): N = {crossover_N:.1f}")
    print(f"  (For N < {crossover_N:.0f}: warehouse term dominates; "
          f"for N > {crossover_N:.0f}: queue term dominates)")

# ─────────────────────────────────────────────────────────────────────────────
# Plots
# ─────────────────────────────────────────────────────────────────────────────

def make_plots(outdir: str):
    if not HAS_MPL:
        return

    N_dense = np.logspace(0, np.log10(128), 300)
    sc = BENCHMARK

    # ── Figure 1: W/δ vs N (log-log) ────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(7, 4.5))
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]

    for idx, scenario in enumerate(SCENARIOS):
        W = [wait_bound_over_delta(N=n, **scenario) for n in N_dense]
        ax.plot(N_dense, W, color=colors[idx], linewidth=1.8,
                label=scenario["name"])

    # Benchmark markers at integer N values
    W_bench = [wait_bound_over_delta(N=n, **BENCHMARK) for n in N_VALUES]
    ax.scatter(N_VALUES, W_bench, color=colors[2], zorder=5, s=40)

    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel("Number of workers $N$", fontsize=11)
    ax.set_ylabel(r"$W(\tau)/\delta$  (dimensionless)", fontsize=11)
    ax.set_title(r"Worst-case waiting time bound $W(\tau)/\delta \leq 2L + C_W K/N$",
                 fontsize=11)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x)}"))
    ax.set_xticks([1, 2, 4, 8, 16, 32, 64, 128])
    ax.legend(fontsize=9)
    ax.grid(True, which="both", linestyle="--", alpha=0.4)
    ax.annotate(r"$\propto 1/N$ (warehouse)", xy=(16, warehouse_component(16, **BENCHMARK)),
                xytext=(32, warehouse_component(8, **BENCHMARK)*0.6),
                fontsize=8, color="gray",
                arrowprops=dict(arrowstyle="->", color="gray", lw=0.8))
    ax.axhline(queue_component(**BENCHMARK), linestyle=":", color=colors[2],
               alpha=0.5, linewidth=1.2)
    ax.text(1.2, queue_component(**BENCHMARK)*1.05, r"$2L$ (queue floor)",
            fontsize=8, color=colors[2], alpha=0.8)

    fig.tight_layout()
    path1 = os.path.join(outdir, "wait_bound_vs_N.png")
    fig.savefig(path1, dpi=150)
    plt.close(fig)
    print(f"\n  [saved] {path1}")

    # ── Figure 2: k-bound vs N ───────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(6, 4))
    for idx, scenario in enumerate(SCENARIOS):
        ks = [k_bound(N=n, **scenario) for n in N_VALUES]
        ax.plot(N_VALUES, ks, "o-", color=colors[idx], linewidth=1.6,
                markersize=5, label=scenario["name"])

    ax.set_xscale("log", base=2)
    ax.set_xlabel("Number of workers $N$", fontsize=11)
    ax.set_ylabel("$k$-bound (dispatch rounds)", fontsize=11)
    ax.set_title(r"$k$-Bounded Fairness: $k \leq \lceil 2 + C_W K/(NL) \rceil$",
                 fontsize=11)
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x)}"))
    ax.set_xticks(N_VALUES)
    ax.axhline(2, linestyle=":", color="gray", alpha=0.5)
    ax.text(1.2, 2.05, "k = 2 (no-warehouse floor)", fontsize=8, color="gray")
    ax.legend(fontsize=9)
    ax.grid(True, which="both", linestyle="--", alpha=0.4)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    fig.tight_layout()
    path2 = os.path.join(outdir, "k_bound_vs_N.png")
    fig.savefig(path2, dpi=150)
    plt.close(fig)
    print(f"  [saved] {path2}")

    # ── Figure 3: Component breakdown (stacked bar, benchmark) ───────────────
    fig, ax = plt.subplots(figsize=(7, 4))
    queue_vals = [queue_component(**BENCHMARK)] * len(N_VALUES)
    wh_vals    = [warehouse_component(N=n, **BENCHMARK) for n in N_VALUES]

    bars_q = ax.bar(range(len(N_VALUES)), queue_vals, label="Queue-depth: $2L$",
                    color="#2ca02c", alpha=0.85)
    bars_w = ax.bar(range(len(N_VALUES)), wh_vals, bottom=queue_vals,
                    label=r"Warehouse: $C_W K / N$", color="#ff7f0e", alpha=0.85)

    for i, (q, w) in enumerate(zip(queue_vals, wh_vals)):
        ax.text(i, q + w + 5000, f"{int(q+w):,}", ha="center", va="bottom",
                fontsize=7)

    ax.set_xticks(range(len(N_VALUES)))
    ax.set_xticklabels([f"N={n}" for n in N_VALUES])
    ax.set_ylabel(r"$W(\tau)/\delta$", fontsize=11)
    ax.set_title("Component breakdown: queue-depth vs warehouse\n"
                 "(Benchmark: $L=131072$, $C_W=32768$, $K=32$)", fontsize=10)
    ax.legend(fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda x, _: f"{int(x):,}"))
    fig.tight_layout()
    path3 = os.path.join(outdir, "component_breakdown.png")
    fig.savefig(path3, dpi=150)
    plt.close(fig)
    print(f"  [saved] {path3}")

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    outdir = os.path.dirname(os.path.abspath(__file__))

    print("DTA-V3 — Bounded Waiting Time Parameter Sweep")
    print("Theorem 4.1 (bounded_wait.tex):  W(τ)/δ ≤ 2L + C_W·K/N")
    print("Theorem 5.2 (bounded_wait.tex):  k ≤ ceil(2 + C_W·K/(N·L))")

    print_table()
    make_plots(outdir)

    # ── Verify monotonicity: W/δ is strictly decreasing in N ─────────────
    print("\n  Monotonicity check (W/δ strictly decreasing in N):")
    sc = BENCHMARK
    prev = None
    ok = True
    for N in N_VALUES:
        w = wait_bound_over_delta(N=N, **sc)
        if prev is not None and w >= prev:
            print(f"  ✗ FAIL: W/δ(N={N}) = {w:.0f} ≥ W/δ(N={N//2}) = {prev:.0f}")
            ok = False
        prev = w
    if ok:
        print("  ✓ W(τ)/δ is strictly decreasing in N for all tested values.")

    # ── Verify k-bound is non-increasing ──────────────────────────────────
    print("\n  k-bound non-increasing check:")
    prev_k = None
    ok2 = True
    for N in N_VALUES:
        k = k_bound(N=N, **sc)
        if prev_k is not None and k > prev_k:
            print(f"  ✗ FAIL: k(N={N}) = {k} > k(N={N//2}) = {prev_k}")
            ok2 = False
        prev_k = k
    if ok2:
        print("  ✓ k-bound is non-increasing in N for all tested values.")

    print("\n  Done.")

if __name__ == "__main__":
    main()
