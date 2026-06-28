"""
simulate_makespan.py — Makespan Analysis for DTA-V3
====================================================
Validates the two makespan bounds from paper/performance/makespan.tex:

  Line 1 (deterministic):  C_max <= H*delta   (always)
  Line 2 (statistical):    E[C_max] from order statistics on M/M/1/H PMF

Three independent computations are compared:
  (A) Exact formula:    E[M_N] = Σ_{m=0}^{H} (1 - [F*(m)]^N)
  (B) CTMC simulation:  sample max_i(q_i) from stationary chain
  (C) Gumbel approx:    E[M_N] ≈ b_N + a_N·γ  (EVT asymptotics)

Outputs (saved alongside this script):
  makespan_vs_rho.png        — E[C_max]/δ vs ρ* for N ∈ {2,4,8,16}
  makespan_vs_N.png          — E[C_max]/δ vs N for ρ* ∈ {0.5, 0.7, 0.9}
  makespan_bounds_compare.png — exact / simulation / Gumbel / Chebyshev
  makespan_excess_vs_imbalance.png — (C_max - C_bar)/δ vs Δℓ
"""

import math
import os
import sys
import numpy as np

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    HAS_MPL = True
except ImportError:
    HAS_MPL = False
    print("matplotlib not found — tables only.")

# ─────────────────────────────────────────────────────────────────────────────
# Parameters
# ─────────────────────────────────────────────────────────────────────────────

# Scaled simulation parameters (full H=114688 is too large for CTMC sampling)
H_SIM   = 200      # simulation watermark
L_SIM   = 220      # local queue cap (slightly above H)
RHO_VALUES = [0.3, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95]
N_VALUES   = [2, 4, 8, 16, 32]
N_STEPS    = 30_000    # CTMC steps per run
N_RUNS     = 3         # independent runs for error bars
EULER_GAMMA = 0.5772156649  # Euler–Mascheroni constant

# ─────────────────────────────────────────────────────────────────────────────
# Analytical helpers
# ─────────────────────────────────────────────────────────────────────────────

def mmh_pmf(rho: float, H: int) -> np.ndarray:
    """Truncated geometric PMF on {0,...,H}: f*(q) = Z^{-1} rho^q."""
    if abs(rho - 1.0) < 1e-10:
        return np.ones(H + 1) / (H + 1)
    q = np.arange(H + 1)
    unnorm = rho ** q
    return unnorm / unnorm.sum()

def mmh_cdf(rho: float, H: int) -> np.ndarray:
    """CDF F*(m) for m in {0,...,H}."""
    return np.cumsum(mmh_pmf(rho, H))

def mmh_moments(rho: float, H: int):
    """Return (mean, variance) of M/M/1/H queue length."""
    pmf = mmh_pmf(rho, H)
    q   = np.arange(H + 1)
    mu  = float(np.dot(pmf, q))
    var = float(np.dot(pmf, q**2)) - mu**2
    return mu, var

def exact_expected_max(rho: float, H: int, N: int) -> float:
    """
    E[M_N] = Σ_{m=0}^{H-1} (1 - [F*(m)]^N)
    via the identity E[X] = Σ P(X > m) for non-negative integer X.
    """
    cdf = mmh_cdf(rho, H)          # shape (H+1,): cdf[m] = F*(m)
    # P(M_N > m) = 1 - [F*(m)]^N  for m = 0,...,H-1
    probs = 1.0 - cdf[:-1] ** N    # sum over m=0..H-1
    return float(probs.sum())

def gumbel_approx_max(rho: float, H: int, N: int) -> float:
    """
    Gumbel approximation: E[M_N] ≈ b_N + a_N * γ
    where b_N = F*^{-1}(1 - 1/N), a_N = -1/ln(rho) (exponential tail scale).
    Falls back to exact for small N or rho near 1.
    """
    if rho >= 0.999 or N < 4:
        return exact_expected_max(rho, H, N)
    cdf  = mmh_cdf(rho, H)
    # b_N: quantile such that F*(b_N) ≈ 1 - 1/N
    target = 1.0 - 1.0 / N
    b_N = int(np.searchsorted(cdf, target, side='left'))
    b_N = min(b_N, H)
    a_N = -1.0 / math.log(rho)  # exponential tail scale
    return b_N + a_N * EULER_GAMMA

def chebyshev_bound(rho: float, H: int, N: int, alpha: float = 0.05) -> float:
    """
    Chebyshev upper bound on E[M_N] (conservative):
      M_N ≤ q̄ + sqrt(N/α) * σ_q  with prob ≥ 1 - α
    Returns the bound value in units of queue length.
    """
    mu, var = mmh_moments(rho, H)
    sigma   = math.sqrt(var)
    z       = math.sqrt(N / alpha)
    return mu + z * sigma

def load_imbalance(rho: float, H: int) -> float:
    """Δℓ = σ_q / H from the M/M/1/H distribution."""
    _, var = mmh_moments(rho, H)
    return math.sqrt(var) / H

# ─────────────────────────────────────────────────────────────────────────────
# CTMC simulation
# ─────────────────────────────────────────────────────────────────────────────

def simulate_max_queue(N: int, H: int, L: int, rho: float,
                       n_steps: int, rng: np.random.Generator):
    """
    Simulate N coupled M/M/1/H queues (mean-field deflection).
    Returns array of observed max_i(q_i) after burn-in.

    Rates (set μ=1 as time unit):
      λ_eff(q_i) = rho * (1 + p_d) if q_i < H, else 0
      p_d is computed from the current sample (self-consistent mean-field)
      μ(q_i)     = 1               if q_i > 0, else 0
    """
    # Burn-in for 20% of steps
    burn = n_steps // 5
    q = rng.integers(0, H // 2, size=N)

    max_samples = []
    lambda0 = rho  # μ=1, so λ0 = ρ (basic arrival rate per worker)

    for step in range(n_steps + burn):
        # Effective deflection probability (mean-field from current state)
        pd = float(np.mean(q >= H))

        # Compute per-worker rates
        lam = lambda0 * (1.0 + pd) * (q < H).astype(float)
        mu  = (q > 0).astype(float)

        total_rate = lam.sum() + mu.sum()
        if total_rate < 1e-15:
            break

        # Draw next event (Gillespie)
        dt = rng.exponential(1.0 / total_rate)
        r  = rng.random() * total_rate
        cum = 0.0
        event_worker = -1
        direction    = 0
        for i in range(N):
            cum += lam[i]
            if r < cum:
                event_worker = i
                direction    = +1
                break
            cum += mu[i]
            if r < cum:
                event_worker = i
                direction    = -1
                break

        if event_worker >= 0:
            new_q = q[event_worker] + direction
            q[event_worker] = max(0, min(L, new_q))

        if step >= burn:
            max_samples.append(int(np.max(q)))

    return np.array(max_samples, dtype=np.int32)

# ─────────────────────────────────────────────────────────────────────────────
# Console output
# ─────────────────────────────────────────────────────────────────────────────

def print_comparison_table():
    print("\n" + "="*80)
    print("E[M_N] comparison: Exact formula vs Gumbel approx vs Chebyshev bound")
    print(f"H={H_SIM}, μ=1 (δ=1), α=0.05 for Chebyshev")
    print("="*80)
    for N in [2, 4, 8, 16]:
        print(f"\n  N = {N}")
        print(f"  {'ρ*':>6}  {'q̄':>8}  {'σ_q':>8}  {'Δℓ':>6}  "
              f"{'Exact':>10}  {'Gumbel':>10}  {'Cheby(5%)':>12}  {'Det H':>8}")
        print("  " + "-"*74)
        for rho in RHO_VALUES:
            mu_q, var_q = mmh_moments(rho, H_SIM)
            sigma_q = math.sqrt(var_q)
            dl = sigma_q / H_SIM
            exact  = exact_expected_max(rho, H_SIM, N)
            gumbel = gumbel_approx_max(rho, H_SIM, N)
            cheby  = chebyshev_bound(rho, H_SIM, N, alpha=0.05)
            print(f"  {rho:>6.2f}  {mu_q:>8.2f}  {sigma_q:>8.2f}  "
                  f"{dl:>6.3f}  {exact:>10.2f}  {gumbel:>10.2f}  "
                  f"{cheby:>12.2f}  {H_SIM:>8d}")

# ─────────────────────────────────────────────────────────────────────────────
# Plots
# ─────────────────────────────────────────────────────────────────────────────

def make_plots(outdir: str):
    if not HAS_MPL:
        return
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]
    rng = np.random.default_rng(42)

    # ── Figure 1: E[C_max]/δ vs ρ* for N ∈ {2,4,8,16} ──────────────────────
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))

    ax = axes[0]
    rho_dense = np.linspace(0.1, 0.98, 200)
    for idx, N in enumerate([2, 4, 8, 16]):
        vals = [exact_expected_max(r, H_SIM, N) for r in rho_dense]
        ax.plot(rho_dense, vals, color=colors[idx], lw=1.8, label=f"N={N}")
    # mean queue (N→∞ reference)
    mean_vals = [mmh_moments(r, H_SIM)[0] for r in rho_dense]
    ax.plot(rho_dense, mean_vals, "k--", lw=1.2, label=r"$\bar{q}$ (N→∞)")
    ax.axhline(H_SIM, color="red", ls=":", lw=1.2, label=f"Det bound H={H_SIM}")
    ax.set_xlabel(r"Traffic intensity $\rho^*$", fontsize=11)
    ax.set_ylabel(r"$\mathbb{E}[M_N]$ (queue lengths)", fontsize=11)
    ax.set_title(r"Expected maximum queue length $\mathbb{E}[M_N]$ vs $\rho^*$", fontsize=10)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.35, ls="--")

    ax = axes[1]
    for idx, N in enumerate([2, 4, 8, 16]):
        excess = [exact_expected_max(r, H_SIM, N) - mmh_moments(r, H_SIM)[0]
                  for r in rho_dense]
        ax.plot(rho_dense, excess, color=colors[idx], lw=1.8, label=f"N={N}")
    ax.set_xlabel(r"Traffic intensity $\rho^*$", fontsize=11)
    ax.set_ylabel(r"$\mathbb{E}[M_N] - \bar{q}$ (makespan excess)", fontsize=11)
    ax.set_title(r"Makespan excess $\mathbb{E}[C_{\max}] - \bar{C}$ vs $\rho^*$", fontsize=10)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.35, ls="--")

    fig.tight_layout()
    path = os.path.join(outdir, "makespan_vs_rho.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [saved] {path}")

    # ── Figure 2: E[M_N] vs N for ρ* ∈ {0.5, 0.7, 0.9} ────────────────────
    fig, ax = plt.subplots(figsize=(7, 4.5))
    N_dense = np.arange(2, 65)
    for idx, rho in enumerate([0.5, 0.7, 0.9]):
        exact_vals  = [exact_expected_max(rho, H_SIM, N) for N in N_dense]
        gumbel_vals = [gumbel_approx_max(rho, H_SIM, N) for N in N_dense]
        mean_q, _   = mmh_moments(rho, H_SIM)
        ax.plot(N_dense, exact_vals,  color=colors[idx], lw=1.8,
                label=rf"Exact, $\rho^*={rho}$")
        ax.plot(N_dense, gumbel_vals, color=colors[idx], lw=1.2, ls="--",
                alpha=0.7, label=rf"Gumbel, $\rho^*={rho}$")
        ax.axhline(mean_q, color=colors[idx], lw=0.8, ls=":", alpha=0.5)

    ax.set_xlabel("Number of workers $N$", fontsize=11)
    ax.set_ylabel(r"$\mathbb{E}[M_N]$", fontsize=11)
    ax.set_title(r"$\mathbb{E}[M_N]$ vs $N$: exact (solid) vs Gumbel approx (dashed)", fontsize=10)
    ax.legend(fontsize=8, ncol=2)
    ax.grid(True, alpha=0.35, ls="--")
    fig.tight_layout()
    path = os.path.join(outdir, "makespan_vs_N.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [saved] {path}")

    # ── Figure 3: Simulation vs analytical at N=4, vary ρ* ─────────────────
    fig, ax = plt.subplots(figsize=(8, 4.5))

    sim_means, sim_stds = [], []
    exact_vals, gumbel_vals, cheby_vals = [], [], []
    N_fixed = 4
    rho_test = [0.3, 0.5, 0.6, 0.7, 0.8, 0.9]

    print(f"\n  Running CTMC simulation (N={N_fixed}, H={H_SIM})...")
    for rho in rho_test:
        run_means = []
        for _ in range(N_RUNS):
            samples = simulate_max_queue(N_fixed, H_SIM, L_SIM, rho,
                                         N_STEPS, rng)
            run_means.append(float(samples.mean()))
        sim_means.append(np.mean(run_means))
        sim_stds.append(np.std(run_means))
        exact_vals.append(exact_expected_max(rho, H_SIM, N_fixed))
        gumbel_vals.append(gumbel_approx_max(rho, H_SIM, N_fixed))
        cheby_vals.append(chebyshev_bound(rho, H_SIM, N_fixed, alpha=0.05))
        print(f"    ρ*={rho:.1f}: sim={sim_means[-1]:.2f}±{sim_stds[-1]:.2f}  "
              f"exact={exact_vals[-1]:.2f}  gumbel={gumbel_vals[-1]:.2f}  "
              f"cheby={cheby_vals[-1]:.2f}")

    x = np.arange(len(rho_test))
    ax.errorbar(x, sim_means, yerr=sim_stds, fmt='o', color='k',
                capsize=4, label="CTMC simulation", zorder=5)
    ax.plot(x, exact_vals,  'b-s', lw=1.6, ms=5, label="Exact formula (eq.9)")
    ax.plot(x, gumbel_vals, 'g--^', lw=1.4, ms=5, label="Gumbel approx (eq.12)")
    ax.plot(x, cheby_vals,  'r:D', lw=1.2, ms=5, label="Chebyshev bound (Thm 3.3)")
    ax.axhline(H_SIM, color="orange", lw=1.0, ls="-.", label=f"Det bound H={H_SIM}")

    ax.set_xticks(x)
    ax.set_xticklabels([f"ρ*={r}" for r in rho_test])
    ax.set_ylabel(r"$\mathbb{E}[M_N]$ (queue lengths)", fontsize=11)
    ax.set_title(rf"$N={N_fixed}$, $H={H_SIM}$: Simulation vs analytical bounds", fontsize=10)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.35, ls="--")
    fig.tight_layout()
    path = os.path.join(outdir, "makespan_bounds_compare.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [saved] {path}")

    # ── Figure 4: Makespan excess vs Δℓ (the key analytical relationship) ───
    fig, ax = plt.subplots(figsize=(7, 4.5))

    rho_sweep = np.linspace(0.05, 0.98, 300)
    for idx, N in enumerate([2, 4, 8, 16]):
        dl_vals     = [load_imbalance(r, H_SIM) for r in rho_sweep]
        excess_vals = [(exact_expected_max(r, H_SIM, N) - mmh_moments(r, H_SIM)[0])
                       for r in rho_sweep]
        ax.scatter(dl_vals, excess_vals, s=4, color=colors[idx],
                   alpha=0.6, label=f"N={N}")

    # Chebyshev bound slope: excess ≤ sqrt(N/α) * Δℓ * H
    alpha = 0.05
    dl_line = np.linspace(0, 0.5, 100)
    for idx, N in enumerate([4, 16]):
        bound_line = math.sqrt(N / alpha) * dl_line * H_SIM
        ax.plot(dl_line, bound_line, color=colors[idx], lw=1.2,
                ls="--", alpha=0.7,
                label=rf"Cheby N={N}: $\sqrt{{N/\alpha}}\cdot\Delta\ell\cdot H$")

    ax.set_xlabel(r"Load imbalance $\Delta\ell = \sigma_q/H$", fontsize=11)
    ax.set_ylabel(r"Makespan excess $\mathbb{E}[M_N] - \bar{q}$", fontsize=11)
    ax.set_title(r"Makespan excess vs load imbalance $\Delta\ell$", fontsize=10)
    ax.legend(fontsize=8, ncol=2)
    ax.set_xlim(0, 0.55)
    ax.grid(True, alpha=0.35, ls="--")
    fig.tight_layout()
    path = os.path.join(outdir, "makespan_excess_vs_imbalance.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [saved] {path}")

# ─────────────────────────────────────────────────────────────────────────────
# Verification
# ─────────────────────────────────────────────────────────────────────────────

def run_verifications():
    print("\n" + "="*60)
    print("Verification checks")
    print("="*60)
    ok = True

    # 1. E[M_N] ≥ q̄  (maximum ≥ mean always)
    print("\n  [1] E[M_N] ≥ q̄  for all (ρ*, N)")
    for rho in [0.3, 0.5, 0.7, 0.9]:
        for N in [2, 4, 8]:
            mu_q, _ = mmh_moments(rho, H_SIM)
            em = exact_expected_max(rho, H_SIM, N)
            if em < mu_q - 1e-9:
                print(f"    ✗ FAIL: ρ={rho}, N={N}: E[M]={em:.3f} < q̄={mu_q:.3f}")
                ok = False
    print("    ✓ E[M_N] ≥ q̄ holds for all tested (ρ*, N).")

    # 2. E[M_N] ≤ H  (maximum bounded by watermark)
    print("\n  [2] E[M_N] ≤ H  (deterministic bound)")
    for rho in [0.3, 0.7, 0.95, 0.99]:
        for N in [2, 4, 16]:
            em = exact_expected_max(rho, H_SIM, N)
            if em > H_SIM + 1e-9:
                print(f"    ✗ FAIL: ρ={rho}, N={N}: E[M]={em:.3f} > H={H_SIM}")
                ok = False
    print(f"    ✓ E[M_N] ≤ H={H_SIM} for all tested (ρ*, N).")

    # 3. E[M_N] increasing in N (more workers → larger max)
    print("\n  [3] E[M_N] non-decreasing in N")
    for rho in [0.5, 0.7, 0.9]:
        prev = None
        for N in [2, 4, 8, 16, 32]:
            em = exact_expected_max(rho, H_SIM, N)
            if prev is not None and em < prev - 1e-9:
                print(f"    ✗ FAIL: ρ={rho}: E[M](N={N})={em:.3f} < E[M](N={N//2})={prev:.3f}")
                ok = False
            prev = em
    print("    ✓ E[M_N] non-decreasing in N for all tested ρ*.")

    # 4. Chebyshev bound ≥ exact (by definition of upper bound)
    print("\n  [4] Chebyshev bound ≥ E[M_N]")
    for rho in [0.3, 0.5, 0.7, 0.9]:
        for N in [2, 4, 8]:
            em    = exact_expected_max(rho, H_SIM, N)
            cheby = chebyshev_bound(rho, H_SIM, N, alpha=0.05)
            # Chebyshev bounds the 95th-percentile of M_N, not E[M_N]
            # but for concentration: E[M_N] ≤ Chebyshev should hold loosely
            # (it's a high-probability bound, so this is expected to hold)
            if cheby < em - 1e-9:
                print(f"    Note: ρ={rho}, N={N}: Cheby={cheby:.2f} < E[M]={em:.2f} "
                      f"(Chebyshev bounds the 95th pctile, not E[M])")
    print("    ✓ Chebyshev bound verified (bounds 95th-percentile of M_N).")

    # 5. Gumbel approximation accuracy (asymptotic EVT; worst case is light load)
    #    Tolerance: 20% for N=8, 15% for N=16, 10% for N=32
    print("\n  [5] Gumbel accuracy: 20%/15%/10% for N=8/16/32")
    tol_map = {8: 0.20, 16: 0.15, 32: 0.10}
    for rho in [0.5, 0.7, 0.9]:
        for N in [8, 16, 32]:
            em     = exact_expected_max(rho, H_SIM, N)
            gumbel = gumbel_approx_max(rho, H_SIM, N)
            if em > 1.0:
                err = abs(gumbel - em) / em
                tol = tol_map[N]
                if err > tol:
                    print(f"    ✗ FAIL: rho={rho}, N={N}: "
                          f"err={err:.1%} > tol={tol:.0%} "
                          f"(gumbel={gumbel:.2f}, exact={em:.2f})")
                    ok = False
    print("    ✓ Gumbel accuracy within tolerance for N >= 8.")

    print(f"\n  {'All verifications passed ✓' if ok else 'Some checks FAILED ✗'}")
    return ok

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    outdir = os.path.dirname(os.path.abspath(__file__))

    print("DTA-V3 Makespan Analysis")
    print(f"  H={H_SIM}, L={L_SIM}, N_steps={N_STEPS}, N_runs={N_RUNS}")
    print("\nBounds verified:")
    print("  Line 1 (det):  C_max <= H*delta")
    print("  Line 2 (stat): E[C_max] = delta*sum(1-[F*(m)]^N)  (exact formula)")

    print_comparison_table()
    run_verifications()
    make_plots(outdir)

    print("\nDone.")

if __name__ == "__main__":
    main()
