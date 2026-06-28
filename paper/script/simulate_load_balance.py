"""
simulate_load_balance.py — Load Balance: Mean-Field vs. Simulation
==================================================================
Implements two things from load_balance.tex:

  1. ANALYTIC (mean-field):
     - Solves the self-consistency equation ρ* = F(ρ*) by iteration
     - Computes exact mean q̄ and variance σ²_q from the M/M/1/H
       truncated geometric distribution (Proposition 3.2)
     - Sweeps load ρ₀ = λ₀δ ∈ (0, 0.95) and worker count N ∈ {2,4,8,16}

  2. SIMULATION (discrete-event, finite N):
     - Runs a continuous-time Markov chain simulation of N workers
     - Measures empirical queue-length histogram
     - Compares to mean-field prediction, quantifying finite-N error ε_N

Outputs:
  - Console table of ρ*, q̄, σ_q, Δℓ for each (N, ρ₀)
  - load_balance_distribution.png — histogram vs mean-field PMF
  - load_imbalance_vs_load.png    — Δℓ vs ρ₀ for each N
  - finite_N_error.png            — ε_N vs N at fixed ρ₀
"""

import math
import os
import random
import numpy as np
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except ImportError:
    HAS_MPL = False
    print("matplotlib not available — skipping plots.")

OUTDIR = os.path.dirname(os.path.abspath(__file__))

# ─────────────────────────────────────────────────────────────
# Parameters (scaled down from production for tractable simulation)
# ─────────────────────────────────────────────────────────────

H_SCALE = 100      # scaled high-water mark (production: 114688)
L_SCALE = 115      # scaled local queue capacity (production: 131072)

# ─────────────────────────────────────────────────────────────
# Analytic: M/M/1/H truncated geometric
# ─────────────────────────────────────────────────────────────

def mmh_pmf(rho: float, H: int) -> np.ndarray:
    """
    Stationary PMF of the M/M/1/H birth-death chain.
    π(q; ρ) = (1-ρ)/(1-ρ^{H+1}) · ρ^q,  q = 0..H
    """
    if abs(rho - 1.0) < 1e-10:
        return np.ones(H + 1) / (H + 1)
    qs = np.arange(H + 1, dtype=float)
    unnorm = rho ** qs
    return unnorm / unnorm.sum()

def mmh_moments(rho: float, H: int):
    """Return (mean, variance) of the M/M/1/H distribution."""
    pi = mmh_pmf(rho, H)
    qs = np.arange(H + 1, dtype=float)
    mean = float(np.dot(qs, pi))
    var  = float(np.dot(qs**2, pi)) - mean**2
    return mean, var

def deflection_prob(rho: float, H: int) -> float:
    """p_d = π(H; ρ) — probability of being at the admission threshold."""
    pi = mmh_pmf(rho, H)
    return float(pi[H])

def solve_sc(rho0_base: float, N: int, H: int, max_iter: int = 500,
             tol: float = 1e-10) -> float:
    """
    Solve the self-consistency equation (SC) by fixed-point iteration:
        ρ* = ρ₀_base · (1 + p_d(ρ*))
    where ρ₀_base = λ₀δ is the base traffic intensity.
    Returns ρ* or nan if no convergence.
    """
    rho = rho0_base  # initial guess
    for _ in range(max_iter):
        pd   = deflection_prob(rho, H)
        rho_new = rho0_base * (1.0 + pd)
        if abs(rho_new - rho) < tol:
            return rho_new
        rho = rho_new
    return float("nan")  # no convergence

# ─────────────────────────────────────────────────────────────
# Simulation: continuous-time Markov chain (CTMC) for N workers
# ─────────────────────────────────────────────────────────────

def simulate_workers(N: int, H: int, L: int, rho0_base: float,
                     mu: float = 1.0, T_sim: float = 5e5,
                     seed: int = 42) -> np.ndarray:
    """
    Discrete-event CTMC simulation of N workers.
    Returns array of shape (N, H+1): empirical queue-length histogram
    (counts), one row per worker.

    Events:
      - Arrival to worker i (rate λ_eff when q_i < H, else deflect):
        effective rate λ₀ + deflections from overloaded peers
      - Departure from worker i (rate μ when q_i > 0)

    Deflection routing: when worker i is above H, new arrivals go to
    a uniformly random peer j ≠ i (mean-field routing).
    After h_max failed hops, task is lost (warehouse not modelled here).
    """
    rng    = random.Random(seed)
    lam0   = rho0_base * mu
    h_max  = max(1, N // 2)

    q   = [0] * N          # current queue lengths
    t   = 0.0
    hist = [defaultdict(int) for _ in range(N)]

    while t < T_sim:
        # Compute all event rates
        rates = []
        for i in range(N):
            # arrival to i (direct; accepted if q_i < H)
            arr_rate = lam0 if q[i] < H else 0.0
            # departure from i
            dep_rate = mu if q[i] > 0 else 0.0
            rates.append(arr_rate)
            rates.append(dep_rate)

        total_rate = sum(rates)
        if total_rate == 0:
            break

        # Sample next event time
        dt = rng.expovariate(total_rate)
        t += dt

        # Sample which event fires
        u   = rng.random() * total_rate
        cum = 0.0
        fired = -1
        for idx, r in enumerate(rates):
            cum += r
            if u < cum:
                fired = idx
                break

        worker_idx = fired // 2
        is_arrival = (fired % 2 == 0)

        if is_arrival:
            if q[worker_idx] < H:
                q[worker_idx] += 1
            else:
                # deflect: try random peer up to h_max times
                for _ in range(h_max):
                    target = rng.randrange(N)
                    while target == worker_idx:
                        target = rng.randrange(N)
                    if q[target] < H:
                        q[target] += 1
                        break
                # if all hops fail: task goes to warehouse (not counted)
        else:
            if q[worker_idx] > 0:
                q[worker_idx] -= 1

        # Record snapshot (sample every 100 events for efficiency)
        if rng.random() < 0.01:
            for i in range(N):
                hist[i][q[i]] += 1

    # Convert to array
    result = np.zeros((N, H + 1), dtype=float)
    for i in range(N):
        total = sum(hist[i].values())
        if total > 0:
            for qv, cnt in hist[i].items():
                if qv <= H:
                    result[i, qv] = cnt / total
    return result

# ─────────────────────────────────────────────────────────────
# Main analysis
# ─────────────────────────────────────────────────────────────

N_VALUES   = [2, 4, 8, 16]
RHO0_VALS  = np.linspace(0.05, 0.90, 18)
H          = H_SCALE
L          = L_SCALE

def run_analytic_sweep():
    """Return dict: (N, rho0) -> (rho_star, mean_q, sigma_q, delta_ell)"""
    results = {}
    for N in N_VALUES:
        for rho0 in RHO0_VALS:
            rho_star = solve_sc(rho0, N, H)
            if math.isnan(rho_star) or rho_star >= 1.0:
                continue
            mean_q, var_q = mmh_moments(rho_star, H)
            sigma_q = math.sqrt(max(var_q, 0.0))
            delta_ell = sigma_q / H
            results[(N, float(rho0))] = (rho_star, mean_q, sigma_q, delta_ell)
    return results

def print_table(results):
    print("\n" + "="*72)
    print("Mean-field load balance: ρ*, q̄, σ_q, Δℓ = σ_q/H")
    print(f"H={H}, L={L}")
    print("="*72)
    for N in N_VALUES:
        print(f"\n  N = {N}")
        print(f"  {'ρ₀':>6}  {'ρ*':>8}  {'q̄':>8}  {'σ_q':>8}  {'Δℓ':>8}  {'p_d':>8}")
        print("  " + "-"*54)
        for rho0 in RHO0_VALS[::3]:
            key = (N, float(rho0))
            if key not in results:
                continue
            rho_star, mean_q, sigma_q, delta_ell = results[key]
            pd = deflection_prob(rho_star, H)
            print(f"  {rho0:>6.2f}  {rho_star:>8.4f}  {mean_q:>8.2f}  "
                  f"{sigma_q:>8.2f}  {delta_ell:>8.4f}  {pd:>8.4f}")

def run_sim_comparison(rho0_fixed: float = 0.5):
    """Compare mean-field to simulation at fixed ρ₀."""
    print(f"\n  Simulation comparison at ρ₀ = {rho0_fixed}")
    print(f"  {'N':>4}  {'Δℓ_MF':>10}  {'Δℓ_sim':>10}  {'ε_N':>10}")
    print("  " + "-"*38)
    sim_results = {}
    for N in N_VALUES:
        rho_star = solve_sc(rho0_fixed, N, H)
        if math.isnan(rho_star):
            continue
        _, var_q   = mmh_moments(rho_star, H)
        delta_mf   = math.sqrt(max(var_q, 0.0)) / H

        # Run simulation
        hist = simulate_workers(N, H, L, rho0_fixed, T_sim=2e5, seed=7)
        # Average empirical std across workers
        qs   = np.arange(H + 1, dtype=float)
        sims_std = []
        for i in range(N):
            pi_sim   = hist[i]
            mean_sim = float(np.dot(qs, pi_sim))
            var_sim  = float(np.dot(qs**2, pi_sim)) - mean_sim**2
            sims_std.append(math.sqrt(max(var_sim, 0.0)))
        delta_sim = np.mean(sims_std) / H
        eps_N     = abs(delta_mf - delta_sim)

        sim_results[N] = (delta_mf, delta_sim, eps_N, hist, rho_star)
        print(f"  {N:>4}  {delta_mf:>10.4f}  {delta_sim:>10.4f}  {eps_N:>10.4f}")
    return sim_results

# ─────────────────────────────────────────────────────────────
# Plots
# ─────────────────────────────────────────────────────────────

def make_plots(analytic_results, sim_results, rho0_fixed):
    if not HAS_MPL:
        return

    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]

    # ── Fig 1: Δℓ vs ρ₀ for each N (analytic) ──────────────────────────
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for idx, N in enumerate(N_VALUES):
        rho0s   = sorted(r for (n, r) in analytic_results if n == N)
        deltas  = [analytic_results[(N, r)][3] for r in rho0s]
        ax.plot(rho0s, deltas, color=colors[idx], linewidth=1.8,
                label=f"N={N}")
    ax.set_xlabel(r"Base load $\rho_0 = \lambda_0 \delta$", fontsize=11)
    ax.set_ylabel(r"Load imbalance $\Delta\ell = \sigma_q / H$", fontsize=11)
    ax.set_title("Mean-field load imbalance vs. offered load\n"
                 r"(Theorem 4.1: $\Delta\ell \leq 1/2$)", fontsize=10)
    ax.axhline(0.5, color="gray", linestyle=":", linewidth=1, label="upper bound 1/2")
    ax.legend(fontsize=9)
    ax.grid(linestyle="--", alpha=0.4)
    ax.set_ylim(0, 0.55)
    fig.tight_layout()
    p = os.path.join(OUTDIR, "load_imbalance_vs_load.png")
    fig.savefig(p, dpi=150); plt.close(fig)
    print(f"\n  [saved] {p}")

    # ── Fig 2: histogram comparison for N=4 at rho0_fixed ───────────────
    N_plot = 4
    if N_plot in sim_results:
        delta_mf, delta_sim, eps_N, hist, rho_star = sim_results[N_plot]
        pi_mf = mmh_pmf(rho_star, H)
        qs    = np.arange(H + 1)

        fig, axes = plt.subplots(1, N_plot, figsize=(12, 3.5), sharey=True)
        for i, ax in enumerate(axes):
            pi_sim = hist[i]
            # show only non-negligible range
            cutoff = int(min(H, max(20, np.argmax(np.cumsum(pi_mf) > 0.999))))
            ax.bar(qs[:cutoff], pi_sim[:cutoff], width=1.0,
                   alpha=0.6, color=colors[i], label="simulation")
            ax.plot(qs[:cutoff], pi_mf[:cutoff], "k-", linewidth=1.4,
                    label="mean-field" if i == 0 else "")
            ax.set_title(f"Worker {i}", fontsize=9)
            ax.set_xlabel("queue length $q$", fontsize=8)
            if i == 0:
                ax.set_ylabel("probability", fontsize=8)
                ax.legend(fontsize=7)
            ax.tick_params(labelsize=7)
        fig.suptitle(
            rf"Queue-length distribution: $N={N_plot}$, $\rho_0={rho0_fixed}$, "
            rf"$\rho^*={rho_star:.3f}$  |  "
            rf"$\Delta\ell_\mathrm{{MF}}={delta_mf:.3f}$, "
            rf"$\Delta\ell_\mathrm{{sim}}={delta_sim:.3f}$, "
            rf"$\epsilon_N={eps_N:.3f}$",
            fontsize=9)
        fig.tight_layout()
        p = os.path.join(OUTDIR, "load_balance_distribution.png")
        fig.savefig(p, dpi=150); plt.close(fig)
        print(f"  [saved] {p}")

    # ── Fig 3: finite-N error ε_N vs N ───────────────────────────────────
    Ns   = sorted(sim_results.keys())
    errs = [sim_results[N][2] for N in Ns]
    fig, ax = plt.subplots(figsize=(5, 3.5))
    ax.plot(Ns, errs, "o-", color="#d62728", linewidth=1.8, markersize=6)
    ax.set_xscale("log", base=2)
    ax.set_xlabel("Number of workers $N$", fontsize=11)
    ax.set_ylabel(r"Finite-$N$ error $\epsilon_N = |\Delta\ell_\mathrm{MF} - \Delta\ell_\mathrm{sim}|$",
                  fontsize=9)
    ax.set_title(rf"Mean-field accuracy vs. $N$ at $\rho_0={rho0_fixed}$", fontsize=10)
    ax.set_xticks(Ns); ax.set_xticklabels([str(n) for n in Ns])
    ax.grid(linestyle="--", alpha=0.4)
    fig.tight_layout()
    p = os.path.join(OUTDIR, "finite_N_error.png")
    fig.savefig(p, dpi=150); plt.close(fig)
    print(f"  [saved] {p}")

# ─────────────────────────────────────────────────────────────
# Self-consistency visualisation (Figure 1 in the tex)
# ─────────────────────────────────────────────────────────────

def plot_sc_curve(rho0: float = 0.4):
    if not HAS_MPL:
        return
    rho_grid = np.linspace(0.01, 0.98, 300)
    F_vals   = rho0 * (1.0 + np.array([deflection_prob(r, H) for r in rho_grid]))
    rho_star = solve_sc(rho0, 4, H)

    fig, ax = plt.subplots(figsize=(5, 4))
    ax.plot(rho_grid, F_vals, color="#1f77b4", linewidth=2, label=r"$F(\rho)$")
    ax.plot([0, 1], [0, 1], "k--", linewidth=1, alpha=0.5, label=r"$F = \rho$")
    ax.axhline(rho0, color="gray", linestyle=":", linewidth=0.8)
    ax.axhline(2*rho0, color="gray", linestyle=":", linewidth=0.8)
    ax.text(0.02, rho0 + 0.01, rf"$\lambda_0\delta = {rho0}$", fontsize=8, color="gray")
    ax.text(0.02, 2*rho0 + 0.01, rf"$2\lambda_0\delta = {2*rho0}$", fontsize=8, color="gray")
    if not math.isnan(rho_star):
        ax.plot(rho_star, rho_star, "ro", markersize=8, zorder=5,
                label=rf"$\rho^* = {rho_star:.3f}$")
        ax.axvline(rho_star, color="red", linestyle=":", linewidth=0.8, alpha=0.5)
    ax.set_xlabel(r"$\rho$", fontsize=12)
    ax.set_ylabel(r"$F(\rho)$", fontsize=12)
    ax.set_title("Self-consistency equation $\\rho^* = F(\\rho^*)$\n"
                 r"(analogy: Weiss mean-field $m=\tanh(\beta J m)$)", fontsize=9)
    ax.legend(fontsize=9)
    ax.grid(linestyle="--", alpha=0.3)
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    fig.tight_layout()
    p = os.path.join(OUTDIR, "self_consistency.png")
    fig.savefig(p, dpi=150); plt.close(fig)
    print(f"  [saved] {p}")

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    RHO0_FIXED = 0.5  # representative load for simulation comparison

    print("DTA-V3 — Load Balance: Mean-Field Analysis")
    print(f"Model: M/M/1/H with H={H}, L={L}")
    print("Reference: load_balance.tex, Section 3--4")

    print("\n[1] Solving self-consistency equation (SC) and computing moments...")
    analytic_results = run_analytic_sweep()
    print_table(analytic_results)

    print(f"\n[2] Running discrete-event simulation (ρ₀={RHO0_FIXED})...")
    sim_results = run_sim_comparison(RHO0_FIXED)

    print("\n[3] Generating plots...")
    make_plots(analytic_results, sim_results, RHO0_FIXED)
    plot_sc_curve(rho0=RHO0_FIXED)

    # ── Verify Theorem 4.1: Δℓ ≤ 1/2 everywhere ──────────────────────
    print("\n  Verifying Theorem 4.1: Δℓ ≤ 1/2 for all (N, ρ₀)...")
    violations = [(N, r, d) for (N, r), (_, _, _, d) in analytic_results.items()
                  if d > 0.5 + 1e-9]
    if violations:
        for N, r, d in violations:
            print(f"  ✗ FAIL: Δℓ={d:.4f} > 0.5 at N={N}, ρ₀={r:.2f}")
    else:
        print("  ✓ Δℓ ≤ 1/2 holds for all computed (N, ρ₀) pairs.")

    # ── Verify monotonicity: Δℓ increases with ρ₀ ────────────────────
    print("\n  Verifying monotonicity: Δℓ non-decreasing in ρ₀ (per N)...")
    for N in N_VALUES:
        rho0s  = sorted(r for (n, r) in analytic_results if n == N)
        deltas = [analytic_results[(N, r)][3] for r in rho0s]
        ok = all(deltas[i] <= deltas[i+1] + 1e-8 for i in range(len(deltas)-1))
        status = "✓" if ok else "✗"
        print(f"  {status} N={N}: monotone={'yes' if ok else 'NO'}")

    print("\n  Done.")

if __name__ == "__main__":
    main()
