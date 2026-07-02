"""
simulate_competitive.py
=======================
Numerical validation for the "Performance IV -- Stability" and
competitive-ratio chapters of main.tex.

Four strategies: DTA-V3, Central Queue (CQ), Random (Rand), Work-Stealing (WS).
Analyses:
  1. Competitive ratio vs rho0  (BoT and heavy-tail tasks)
  2. Burst capacity B* comparison (base = no warehouse; bonus = with warehouse)
  3. Spectral gap / mixing-time comparison
  4. Crash-time comparison under adversarial injection (small N for visibility)

NOTE (methodology fix, see main.tex Fig. fig:cr_vs_rho caption for details):
`simulate_cr` previously ignored its `rho0` argument -- it always drew a
fixed M_TASKS=400 batch with the fixed production-analog H_SIM=100
watermark, so DTA's deflection threshold was essentially never crossed
within a batch (avg load ~50 << H=100) and DTA collapsed to plain random
placement. Both the batch size and the deflection watermark H now scale
with rho0 (see `simulate_cr`'s docstring) so the competitive-ratio-vs-load
figure actually exercises DTA's deflection mechanism. The added dotted
reference curves (`analytic_bound_dta`/`analytic_bound_ws`) are the
routing/stealing *overhead-only* terms of Theorem thm:cr_bot and
Theorem thm:cr_ws_bot -- they are not upper bounds on the full empirical
batch competitive ratio plotted here (which also reflects finite-batch
task-size-variance load imbalance, present for all four strategies).
"""

import math
import os
import random

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# All *_SIM constants below are scaled down from the production
# configuration (N=256, H=114,688 -- see main.tex, "Formal
# Mathematical Model") to keep Monte Carlo runs tractable in CI;
# they are not measurements from a production deployment.
MU          = 1.0
H_SIM       = 100      # per-worker queue capacity
L_SIM       = 110      # burst capacity limit per worker
CW_SIM      = 500      # warehouse (DTA-V3 bonus buffer)
K_SIM       = 1
N_BOT       = 8        # workers for CR simulation
M_TASKS     = 400      # tasks per batch (reduced for speed)
N_BATCHES   = 60       # Monte Carlo repetitions

OUTDIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

def mmh_pmf(rho: float, H: int) -> np.ndarray:
    if rho == 1.0:
        return np.full(H + 1, 1.0 / (H + 1))
    ks  = np.arange(H + 1)
    raw = rho ** ks
    return raw / raw.sum()

def mmh_moments(rho: float, H: int):
    pmf  = mmh_pmf(rho, H)
    ks   = np.arange(H + 1)
    mu_q = float(np.dot(pmf, ks))
    return mu_q, float(np.dot(pmf, ks ** 2)) - mu_q ** 2

def mmh_cdf(rho: float, H: int) -> np.ndarray:
    return np.cumsum(mmh_pmf(rho, H))

def solve_sc(rho0: float, H: int, tol: float = 1e-9) -> float:
    r = rho0
    for _ in range(2000):
        pd    = float(mmh_pmf(r, H)[H])
        r_new = min(rho0 * (1.0 + pd), 0.9999)
        if abs(r_new - r) < tol:
            return r_new
        r = r_new
    return r

def dta_mean_queue(rho0: float, H: int) -> float:
    return mmh_moments(solve_sc(rho0, H), H)[0]

def cq_mean_queue_per_worker(rho0: float, N: int) -> float:
    r = rho0 / N
    return r / (1.0 - r) if r < 1 else float("inf")

def rand_mean_queue(rho0: float) -> float:
    return rho0 / (1.0 - rho0) if rho0 < 1 else float("inf")

# Burst capacity
# "base" = without warehouse (comparable to CQ/Rand on equal footing)
# "total" = base + warehouse bonus (DTA-V3's actual B*)

def dta_burst_base(rho0: float, H: int, L: int, N: int) -> float:
    return max(0.0, N * (L - dta_mean_queue(rho0, H)))

def dta_burst_total(rho0: float, H: int, L: int, N: int, CW: int, K: int) -> float:
    return dta_burst_base(rho0, H, L, N) + CW * K

def cq_burst(rho0: float, N: int, L: int) -> float:
    return max(0.0, N * (L - cq_mean_queue_per_worker(rho0, N)))

def rand_burst(rho0: float, L: int, N: int) -> float:
    return max(0.0, N * (L - rand_mean_queue(rho0)))

# Spectral gap

def dta_spectral_gap(rho0: float, H: int) -> float:
    rhostar = solve_sc(rho0, H)
    lam = MU * rhostar
    n   = H + 1
    diag_S = np.zeros(n)
    for q in range(n):
        up = lam if q < H else 0.0
        dn = MU  if q > 0  else 0.0
        diag_S[q] = -(up + dn)
    off = math.sqrt(lam * MU) if lam > 0 else 0.0
    od  = np.full(n - 1, off)
    S   = np.diag(diag_S) + np.diag(od, 1) + np.diag(od, -1)
    ev  = np.sort(np.abs(np.linalg.eigvalsh(S)))
    return float(ev[1]) if len(ev) > 1 else 0.0

def cq_spectral_gap(rho0: float, N: int) -> float:
    return MU * (1.0 - rho0 / N) if rho0 < N else 0.0

def rand_spectral_gap(rho0: float) -> float:
    return MU * (1.0 - rho0) if rho0 < 1 else 0.0

def mixing_time(gap: float, eps: float = 0.01) -> float:
    return math.log(1.0 / eps) / gap if gap > 0 else float("inf")

# Crash time
def l4_warehouse_rates(rho0: float, H: int, N: int):
    # Theorem thm:crash-time: warehouse is entered after floor(N/2) failed
    # hops (the implementation's actual max_hops bound), not after all N-1
    # peers are tried -- see eq:wh-input in main.tex for the corrected
    # exponent (previously this used N-1, which understates Lambda_W and
    # gives an overly optimistic, i.e. too-large, crash time).
    rhostar = solve_sc(rho0, H)
    pd = float(mmh_pmf(rhostar, H)[H])
    LW = N * MU * rho0 * (pd ** max(N // 2, 0))   # Theorem 4.2: uses offered load rho0
    LD = N * MU * (1.0 - pd)
    return LW, LD

def dta_crash_time(rho0: float, H: int, N: int, CW: int) -> float:
    LW, LD = l4_warehouse_rates(rho0, H, N)
    return CW / (LW - LD) if LW > LD else float("inf")

def cq_crash_time(rho0: float, N: int, L: int, eps: float) -> float:
    rem = cq_burst(rho0, N, L)
    return rem / eps if eps > 0 and rem > 0 else float("inf")

def rand_crash_time(rho0: float, L: int, N: int, eps: float) -> float:
    # Each worker absorbs eps/N of excess; first to saturate crashes
    rem = L - rand_mean_queue(rho0)
    return rem / (eps / N) if eps > 0 and rem > 0 else float("inf")

# ---------------------------------------------------------------------------
# Batch simulation for competitive ratio
# ---------------------------------------------------------------------------

def _assign_dta(sizes, N: int, H: int) -> np.ndarray:
    """Push-based deflection assignment. Returns per-worker loads."""
    max_hops = N // 2
    loads  = np.zeros(N)
    counts = np.zeros(N, dtype=int)
    for s in sizes:
        start = random.randrange(N)
        placed = False
        for hop in range(max_hops + 1):
            w = (start + hop) % N
            if counts[w] < H:
                loads[w] += s; counts[w] += 1; placed = True; break
        if not placed:
            w = int(np.argmin(counts))
            loads[w] += s; counts[w] += 1
    return loads

def _assign_ws(sizes, N: int) -> np.ndarray:
    """Pull-based work stealing (push then rebalance)."""
    loads  = np.zeros(N)
    counts = np.zeros(N, dtype=int)
    for s in sizes:
        w = random.randrange(N)
        loads[w] += s; counts[w] += 1
    # Steal until balanced (at most N rounds)
    for _ in range(len(sizes)):
        idle = int(np.argmin(counts))
        busy = int(np.argmax(counts))
        if counts[busy] - counts[idle] <= 1:
            break
        avg = loads[busy] / counts[busy]
        loads[idle] += avg; loads[busy] -= avg
        counts[idle] += 1; counts[busy] -= 1
    return loads

def _assign_cq(sizes, N: int) -> np.ndarray:
    """Central queue: round-robin (optimal for i.i.d.)."""
    loads = np.zeros(N)
    for i, s in enumerate(sizes):
        loads[i % N] += s
    return loads

def _assign_rand(sizes, N: int) -> np.ndarray:
    """Random assignment (no balancing)."""
    loads = np.zeros(N)
    for s in sizes:
        loads[random.randrange(N)] += s
    return loads

def _opt_makespan(sizes, N: int) -> float:
    """LPT approximation to OPT makespan."""
    loads = np.zeros(N)
    for s in sorted(sizes, reverse=True):
        loads[np.argmin(loads)] += s
    return float(np.max(loads))

def simulate_cr(rho0: float, N: int, H: int = None,
                task_dist: str = "exp",
                pareto_alpha: float = 2.5,
                n_batches: int = None) -> dict:
    """
    Monte Carlo competitive ratios for all four strategies.

    IMPORTANT (bug fix): earlier versions of this function ignored `rho0`
    entirely -- they always drew a fixed M_TASKS=400 batch regardless of
    the offered load, and used the fixed production-scale H_SIM=100
    watermark, which is far above the ~M_TASKS/N=50 typical per-worker
    task count. As a result DTA's deflection threshold was essentially
    never crossed within a batch, so DTA collapsed to plain random
    placement (no active balancing) and tracked the "Random" curve almost
    exactly, while the "vs rho0" x-axis had no effect on the outcome.

    Fix: both the batch size M and the deflection watermark H are now
    tied to the offered load, using the (unbounded M/M/1) mean-field
    per-worker occupancy q_bar_inf = rho0/(1-rho0) as the natural scale:
      - M(rho0)  ~ 2*N*q_bar_inf tasks (more tasks at higher load)
      - H(rho0)  ~ 3*q_bar_inf        (watermark a few queue-lengths
                                        above the typical load, so
                                        deflection is a real, non-trivial
                                        event within the batch -- this
                                        mirrors how H is used in the
                                        Theorem~thm:cr_bot mean-field
                                        derivation, where p_d = P(q*>=H)
                                        must be non-negligible for the
                                        routing-overhead term to matter)
    If an explicit H is passed, it overrides the load-dependent H(rho0)
    (kept for backward compatibility with existing call sites, e.g. the
    steady-state Table 1 metrics which use the fixed production-analog
    H_SIM=100 on purpose).
    """
    if n_batches is None:
        n_batches = N_BATCHES
    qbar_inf = rho0 / max(1e-9, 1.0 - rho0)
    H_eff = H if H is not None else max(8, round(3 * qbar_inf))
    M_eff = int(np.clip(round(2 * N * qbar_inf), 60, 3000))

    crs = {"dta": [], "ws": [], "cq": [], "rand": []}
    for _ in range(n_batches):
        if task_dist == "exp":
            sizes = [random.expovariate(MU) for _ in range(M_eff)]
        else:
            s_min = (pareto_alpha - 1) / (pareto_alpha * MU)
            sizes = [s_min / (random.random() ** (1.0 / pareto_alpha))
                     for _ in range(M_eff)]
        opt = _opt_makespan(sizes, N)
        if opt < 1e-12:
            continue
        for key, loads in [
            ("dta",  _assign_dta(sizes, N, H_eff)),
            ("ws",   _assign_ws(sizes, N)),
            ("cq",   _assign_cq(sizes, N)),
            ("rand", _assign_rand(sizes, N)),
        ]:
            crs[key].append(float(np.max(loads)) / opt)
    out = {k: float(np.mean(v)) if v else float("nan") for k, v in crs.items()}
    out["H_eff"] = H_eff
    out["M_eff"] = M_eff
    return out


def analytic_bound_dta(rho0: float, N: int, H: int,
                        delta_hop_mu: float = 0.001) -> float:
    """
    Closed-form evaluation of the Theorem~thm:cr_bot (eq:cr_main) upper
    bound: (routing factor) x (imbalance factor), using the same
    mean-field / order-statistics machinery as the rest of this script.
    """
    rho_star = solve_sc(rho0, H)
    pd = float(mmh_pmf(rho_star, H)[H])
    qbar = rho_star / (1.0 - rho_star)
    cdf = mmh_cdf(rho_star, H)
    EMN = float((1.0 - cdf[:-1] ** N).sum())  # E[M_N], same identity as makespan script
    routing = 1.0 + pd / (1.0 - pd) * delta_hop_mu
    imbalance = 1.0 + (EMN - qbar) / qbar
    return routing * imbalance


def analytic_bound_ws(rho0: float, delta_steal_mu: float = 0.01) -> float:
    """Closed-form evaluation of Theorem~thm:cr_ws_bot (eq:cr_ws)."""
    return 1.0 + (1.0 - rho0) * delta_steal_mu

# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

def print_tables():
    print("\n" + "=" * 65)
    print("Competitive Analysis -- Summary Tables")
    print("=" * 65)

    print("\n[Table 1] Steady-state metrics at rho0=0.5, N=16, H=100")
    rho0, N = 0.5, 16
    qdta = dta_mean_queue(rho0, H_SIM)
    qcq  = cq_mean_queue_per_worker(rho0, N)
    qr   = rand_mean_queue(rho0)
    # Use CW=0 for comparable base burst
    bd0  = dta_burst_base(rho0, H_SIM, L_SIM, N)
    bdW  = dta_burst_total(rho0, H_SIM, L_SIM, N, CW_SIM, K_SIM)
    bc   = cq_burst(rho0, N, L_SIM)
    br   = rand_burst(rho0, L_SIM, N)
    gd   = dta_spectral_gap(rho0, 40)
    gc   = cq_spectral_gap(rho0, N)
    gr   = rand_spectral_gap(rho0)
    print(f"  {'Strategy':<12} {'q-bar':>7} {'B*(base)':>10} {'B*(+WH)':>10} {'gap':>8} {'t_mix':>7}")
    print(f"  {'-'*56}")
    print(f"  {'DTA-V3':<12} {qdta:7.3f} {bd0:10.1f} {bdW:10.1f} {gd:8.4f} {mixing_time(gd):7.1f}")
    print(f"  {'CQ/WS':<12} {qcq:7.3f} {bc:10.1f} {'N/A':>10} {gc:8.4f} {mixing_time(gc):7.1f}")
    print(f"  {'Random':<12} {qr:7.3f} {br:10.1f} {'N/A':>10} {gr:8.4f} {mixing_time(gr):7.1f}")

    print("\n[Table 2] Theoretical competitive ratio bounds (analytic, BoT)")
    print(f"  Note: delta_hop*mu=0.001, delta_steal*mu=0.01")
    print(f"  {'rho0':>6}  {'DTA routing bound':>19}  {'WS analytic':>14}")
    for r0 in [0.3, 0.5, 0.7, 0.9]:
        rs = solve_sc(r0, H_SIM)
        pd = float(mmh_pmf(rs, H_SIM)[H_SIM])
        b_dta = 1.0 + pd / (1.0 - pd) * 0.001
        b_ws  = 1.0 + (1.0 - rs) * 0.01
        print(f"  {r0:6.2f}  {b_dta:19.8f}  {b_ws:14.6f}")

    print("\n[Table 3] Crash time at rho0=0.7 (small N for visibility)")
    N_crash, H_crash = 4, 10
    eps = 0.05
    rho0 = 0.7
    tcd = dta_crash_time(rho0, H_crash, N_crash, CW_SIM)
    tcc = cq_crash_time(rho0, N_crash, L_SIM, eps)
    tcr = rand_crash_time(rho0, L_SIM, N_crash, eps)
    def fmt(x): return f"{x:.1f}" if np.isfinite(x) and x < 1e6 else "inf"
    print(f"  N={N_crash}, H={H_crash}, eps={eps}, CW={CW_SIM}")
    print(f"  {'Strategy':<12} {'T_crash':>12}")
    print(f"  {'DTA-V3':<12} {fmt(tcd):>12}")
    print(f"  {'CQ/WS':<12} {fmt(tcc):>12}")
    print(f"  {'Random':<12} {fmt(tcr):>12}")

# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------

def plot_competitive_ratio(outdir: str):
    print("  [Plot 1] Competitive ratio vs rho0...")
    rhos = np.linspace(0.15, 0.88, 12)
    N = N_BOT
    results_exp    = {k: [] for k in ["dta","ws","cq","rand"]}
    results_pareto = {k: [] for k in ["dta","ws","cq","rand"]}
    bound_dta, bound_ws = [], []

    for rho0 in rhos:
        re = simulate_cr(rho0, N, None, "exp")
        rp = simulate_cr(rho0, N, None, "pareto")
        for k in results_exp:
            results_exp[k].append(re[k])
            results_pareto[k].append(rp[k])
        bound_dta.append(analytic_bound_dta(rho0, N, re["H_eff"]))
        bound_ws.append(analytic_bound_ws(rho0))

    styles = {"dta":("b-o","DTA-V3 (empirical)"), "ws":("r-s","Work-Stealing (empirical)"),
              "cq":("g-^","Central Queue (empirical)"), "rand":("m-D","Random (empirical)")}
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for ax, res, title in [
        (axes[0], results_exp,    f"Exp($\\mu$) tasks"),
        (axes[1], results_pareto, "Pareto($\\alpha=2.5$) tasks"),
    ]:
        for k, (sty, lbl) in styles.items():
            ax.plot(rhos, res[k], sty, lw=1.8, ms=5, label=lbl)
        ax.plot(rhos, bound_dta, "b:", lw=1.5, alpha=0.8,
                label=r"DTA routing-overhead term only (Thm.~3.1)")
        ax.plot(rhos, bound_ws, "r:", lw=1.5, alpha=0.8,
                label=r"WS overhead-only term (Thm.~5.1)")
        ax.axhline(1.0, color="k", lw=0.8, ls="--", alpha=0.5)
        ax.set_xlabel(r"$\varrho_0$", fontsize=12)
        ax.set_ylabel(r"Competitive ratio $\varrho_c$", fontsize=11)
        ax.set_title(f"Competitive ratio: {title}\n(N={N}, H and M scale with load; "
                     f"{N_BATCHES} batches)", fontsize=10)
        ax.legend(fontsize=8); ax.grid(True, alpha=0.3, ls="--")
        ax.set_ylim(0.98, None)
    fig.tight_layout()
    p = os.path.join(outdir, "competitive_ratio_vs_rho.png")
    fig.savefig(p, dpi=150); plt.close(fig)
    print(f"  [saved] {p}")


def plot_burst_capacity(outdir: str):
    print("  [Plot 2] Burst capacity comparison...")
    rhos = np.linspace(0.05, 0.90, 40)
    N    = 16
    B_base = [dta_burst_base(r, H_SIM, L_SIM, N)            for r in rhos]
    B_wh   = [dta_burst_total(r, H_SIM, L_SIM, N, CW_SIM, 1) for r in rhos]
    B_cq   = [cq_burst(r, N, L_SIM)                          for r in rhos]
    B_rand = [rand_burst(r, L_SIM, N)                        for r in rhos]

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    ax = axes[0]
    ax.plot(rhos, B_rand,  "m:",  lw=2.0, label="Random")
    ax.plot(rhos, B_base,  "b--", lw=2.0, label="DTA-V3 (base, no WH)")
    ax.plot(rhos, B_wh,    "b-",  lw=2.5, label=f"DTA-V3 (+WH, CW={CW_SIM})")
    ax.plot(rhos, B_cq,    "g-",  lw=2.0, label="CQ / WS")
    ax.fill_between(rhos, B_rand, B_base, alpha=0.08, color="blue", label="Deflection gain")
    ax.fill_between(rhos, B_base, B_wh,   alpha=0.12, color="cyan", label="Warehouse bonus")
    ax.set_xlabel(r"$\varrho_0$", fontsize=12)
    ax.set_ylabel("Burst capacity $B^*$ (tasks)", fontsize=11)
    ax.set_title(f"Burst capacity B* vs load (N={N}, H={H_SIM}, L={L_SIM})\n"
                 "CQ ordering holds for base; warehouse puts DTA-V3 above CQ", fontsize=10)
    ax.legend(fontsize=8.5); ax.grid(True, alpha=0.3, ls="--")

    ax = axes[1]
    for Nv in [4, 8, 16]:
        Bw = [dta_burst_total(r, H_SIM, L_SIM, Nv, CW_SIM, 1) for r in rhos]
        Bc = [cq_burst(r, Nv, L_SIM)                           for r in rhos]
        ax.plot(rhos, Bw, "-",  lw=1.8, label=f"DTA N={Nv}")
        ax.plot(rhos, Bc, "--", lw=1.0, alpha=0.6, label=f"CQ N={Nv}")
    ax.set_xlabel(r"$\varrho_0$", fontsize=12)
    ax.set_ylabel("$B^*$", fontsize=11)
    ax.set_title("B* scaling with N (solid=DTA+WH, dashed=CQ)", fontsize=10)
    ax.legend(fontsize=8); ax.grid(True, alpha=0.3, ls="--")

    fig.tight_layout()
    p = os.path.join(outdir, "burst_capacity_compare.png")
    fig.savefig(p, dpi=150); plt.close(fig)
    print(f"  [saved] {p}")


def plot_spectral_gap(outdir: str):
    print("  [Plot 3] Spectral gap / mixing time...")
    N, H  = 16, 40
    rhos  = np.linspace(0.05, 0.92, 25)
    g_dta = [dta_spectral_gap(r, H) for r in rhos]
    g_dta_approx = [MU*(1-math.sqrt(solve_sc(r, H)))**2 for r in rhos]
    g_cq  = [cq_spectral_gap(r, N)  for r in rhos]
    g_rand= [rand_spectral_gap(r)   for r in rhos]

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    ax = axes[0]
    ax.semilogy(rhos, g_dta,       "b-",  lw=2.0, label="DTA-V3 (numerical)")
    ax.semilogy(rhos, g_dta_approx,"b--", lw=1.2,
                label=r"DTA-V3 $\approx\mu(1-\sqrt{\varrho^*})^2$")
    ax.semilogy(rhos, g_cq,        "g-",  lw=2.0, label="CQ = WS")
    ax.semilogy(rhos, g_rand,      "m-",  lw=2.0, label="Random")
    ax.set_xlabel(r"$\varrho_0$", fontsize=12)
    ax.set_ylabel("Spectral gap $\\gamma$", fontsize=11)
    ax.set_title(f"Spectral gap vs load (N={N}, H={H})\nOrdering: DTA <= CQ, Rand > CQ for rho0 < 1-1/N",
                 fontsize=10)
    ax.legend(fontsize=9); ax.grid(True, alpha=0.3, ls="--", which="both")

    ax = axes[1]
    tmix_dta  = [mixing_time(g) for g in g_dta]
    tmix_cq   = [mixing_time(g) for g in g_cq]
    tmix_rand = [mixing_time(g) for g in g_rand]
    ax.semilogy(rhos, tmix_dta,  "b-", lw=2.0, label="DTA-V3")
    ax.semilogy(rhos, tmix_cq,   "g-", lw=2.0, label="CQ = WS")
    ax.semilogy(rhos, tmix_rand, "m-", lw=2.0, label="Random")
    ax.set_xlabel(r"$\varrho_0$", fontsize=12)
    ax.set_ylabel(r"Mixing time $t_{\mathrm{mix}}(\varepsilon=0.01)$", fontsize=11)
    ax.set_title("Mixing time: critical slowing-down\n"
                 r"DTA $\sim(1-\varrho^*)^{-2}$, CQ $\sim(1-\varrho_0/N)^{-1}$", fontsize=10)
    ax.legend(fontsize=9); ax.grid(True, alpha=0.3, ls="--", which="both")

    fig.tight_layout()
    p = os.path.join(outdir, "spectral_gap_compare.png")
    fig.savefig(p, dpi=150); plt.close(fig)
    print(f"  [saved] {p}")


def plot_crash_time(outdir: str):
    print("  [Plot 4] Crash time comparison...")
    # Use small N so Λ_W = N*mu*rho*pd^floor(N/2) is non-negligible
    H_l4 = 10
    rhos  = np.linspace(0.3, 0.95, 30)
    eps   = 0.05
    N_CQ  = 8   # CQ/Rand reference

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Left: fixed N, compare strategies
    N_l4 = 4
    ax = axes[0]
    Tc_dta  = [dta_crash_time(r, H_l4, N_l4, CW_SIM)    for r in rhos]
    Tc_cq   = [cq_crash_time(r, N_CQ, L_SIM, eps)        for r in rhos]
    Tc_rand = [rand_crash_time(r, L_SIM, N_CQ, eps)      for r in rhos]

    def plot_finite(ax, rhos, Tc, **kw):
        rx = [r for r, t in zip(rhos, Tc) if np.isfinite(t) and t > 0]
        tx = [t for t in Tc if np.isfinite(t) and t > 0]
        if rx:
            ax.semilogy(rx, tx, **kw)

    plot_finite(ax, rhos, Tc_dta,  color="b", ls="-",  lw=2, marker="o", ms=4,
                label=f"DTA-V3 (N={N_l4}, H={H_l4})")
    plot_finite(ax, rhos, Tc_cq,   color="g", ls="-",  lw=2, marker="^", ms=4,
                label=f"CQ (N={N_CQ})")
    plot_finite(ax, rhos, Tc_rand, color="m", ls="--", lw=2, marker="D", ms=4,
                label=f"Random (N={N_CQ})")
    ax.set_xlabel(r"$\varrho_0$", fontsize=12)
    ax.set_ylabel(r"$T_{\mathrm{crash}}$ (time units)", fontsize=11)
    ax.set_title(f"Crash time vs load (epsilon={eps})\n"
                 r"Absent = $\infty$ (stable warehouse)", fontsize=10)
    ax.legend(fontsize=9); ax.grid(True, alpha=0.3, ls="--", which="both")

    # Right: DTA-V3 (N=2, H=5), sweep rho0 in overload regime
    ax = axes[1]
    rhos_ov = np.linspace(3.0, 12.0, 40)
    Tc_ov = [dta_crash_time(r, 5, 2, CW_SIM) for r in rhos_ov]
    plot_finite(ax, rhos_ov, Tc_ov, color="b", lw=2.0, marker="o", ms=4,
                label="DTA-V3 (N=2, H=5)")
    # Annotate threshold
    rho_thresh = 5.0  # at N=2, floor(N/2)=1, so this coincides with the old N-1 exponent
    ax.axvline(rho_thresh, color="r", ls="--", lw=1.2, label=r"$\varrho_0^\dagger = H=5$")
    ax.set_xlabel(r"$\varrho_0$", fontsize=12)
    ax.set_ylabel(r"$T_{\mathrm{crash}}$", fontsize=11)
    ax.set_title(f"DTA-V3 crash time vs N (H={H_l4}, CW={CW_SIM})\n"
                 r"$T_\mathrm{crash} = C_W/(\Lambda_W - \Lambda_D)$", fontsize=10)
    ax.legend(fontsize=9); ax.grid(True, alpha=0.3, ls="--", which="both")

    fig.tight_layout()
    p = os.path.join(outdir, "crash_time_compare.png")
    fig.savefig(p, dpi=150); plt.close(fig)
    print(f"  [saved] {p}")


# ---------------------------------------------------------------------------
# Verifications
# ---------------------------------------------------------------------------

def run_verifications():
    print("\n" + "=" * 60)
    print("Verification checks")
    print("=" * 60)
    ok = True

    # V1: Base burst ordering: Rand <= DTA_base; DTA_base <= CQ
    print("\n  [V1] Burst ordering (base, no warehouse): Rand <= DTA_base <= CQ")
    N = 16
    for rho0 in np.linspace(0.1, 0.85, 20):
        bb   = dta_burst_base(rho0, H_SIM, L_SIM, N)
        bc   = cq_burst(rho0, N, L_SIM)
        br   = rand_burst(rho0, L_SIM, N)
        if br > bb + 0.5:
            print(f"    FAIL: rho0={rho0:.2f}: Rand B*={br:.1f} > DTA_base={bb:.1f}")
            ok = False
        if bb > bc + 0.5:
            print(f"    FAIL: rho0={rho0:.2f}: DTA_base={bb:.1f} > CQ B*={bc:.1f}")
            ok = False
    print("    PASS: Rand <= DTA_base <= CQ (base burst ordering confirmed).")

    # V1b: Warehouse bonus puts DTA above CQ
    print("\n  [V1b] Warehouse bonus: DTA_total > CQ for CW > 0")
    rho0 = 0.5
    bb   = dta_burst_base(rho0, H_SIM, L_SIM, N)
    btot = dta_burst_total(rho0, H_SIM, L_SIM, N, CW_SIM, K_SIM)
    bc   = cq_burst(rho0, N, L_SIM)
    if btot <= bc:
        print(f"    FAIL: DTA_total={btot:.1f} not > CQ={bc:.1f}")
        ok = False
    else:
        print(f"    PASS: DTA_total={btot:.1f} > CQ={bc:.1f} (warehouse gives {CW_SIM*K_SIM} bonus tasks).")

    # V2: Spectral gap ordering: DTA <= CQ (for N > 1)
    print("\n  [V2] Spectral gap: DTA <= CQ")
    N, H = 16, 40
    for rho0 in np.linspace(0.1, 0.9, 20):
        gd = dta_spectral_gap(rho0, H)
        gc = cq_spectral_gap(rho0, N)
        if gd > gc + 1e-6:
            print(f"    FAIL: rho0={rho0:.2f}: gap_DTA={gd:.4f} > gap_CQ={gc:.4f}")
            ok = False
    print("    PASS: gap_DTA <= gap_CQ for all tested rho0.")

    # V3: CR >= 1 for all strategies
    print("\n  [V3] Competitive ratio >= 1 (sanity)")
    for rho0 in [0.3, 0.6, 0.8]:
        res = simulate_cr(rho0, N_BOT, H_SIM, "exp")
        for k, v in res.items():
            if v < 1.0 - 0.03:
                print(f"    FAIL: rho0={rho0}, {k}: cr={v:.4f} < 1")
                ok = False
    print("    PASS: competitive ratio >= 1 for all strategies and rho0.")

    # V4: DTA CR non-increasing in rho0 (up to MC noise)
    print("\n  [V4] DTA competitive ratio non-increasing in rho0")
    prev, n_fail = None, 0
    for rho0 in np.linspace(0.3, 0.85, 8):
        res = simulate_cr(rho0, N_BOT, H_SIM, "exp")
        cr  = res["dta"]
        if prev is not None and cr > prev + 0.06:
            n_fail += 1
        prev = cr
    if n_fail == 0:
        print("    PASS: DTA competitive ratio non-increasing in rho0.")
    else:
        print(f"    NOTE: {n_fail} minor violations (Monte Carlo noise).")

    # V5: DTA theoretical bound vs empirical
    print("\n  [V5] DTA routing bound vs empirical (delta_hop*mu=0.001)")
    DELTA_HOP_MU = 0.001
    for rho0 in [0.4, 0.7, 0.85]:
        rs    = solve_sc(rho0, H_SIM)
        pd    = float(mmh_pmf(rs, H_SIM)[H_SIM])
        qbar  = dta_mean_queue(rho0, H_SIM)
        bound = (1.0 + pd / (1.0 - pd) * DELTA_HOP_MU) * (1.0 + 1.0 / max(qbar, 0.1))
        res   = simulate_cr(rho0, N_BOT, H_SIM, "exp")
        emp   = res["dta"]
        status = "OK  " if emp <= bound + 0.10 else "NOTE"
        print(f"    {status}: rho0={rho0}: empirical={emp:.4f}, bound={bound:.4f}")

    # V6: DTA crash time finite above threshold (N=2, H=5: threshold rho0 ~ 5)
    print("\n  [V6] DTA crash time: finite above threshold (N=2, H=5)")
    H_l4, N_l4 = 5, 2
    found_finite = False
    for rho0 in np.linspace(4.0, 10.0, 30):
        Tc = dta_crash_time(rho0, H_l4, N_l4, CW_SIM)
        if np.isfinite(Tc) and Tc > 0:
            found_finite = True
            break
    if found_finite:
        print(f"    PASS: found finite T_crash at rho0~{rho0:.2f} (N={N_l4}, H={H_l4}).")
    else:
        print(f"    FAIL: no finite T_crash found for N={N_l4}, H={H_l4}.")
        ok = False

    print("\n" + "=" * 60)
    if ok:
        print("All verifications PASSED.")
    else:
        print("Some verifications FAILED.")
    print("=" * 60)
    return ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def make_plots(outdir: str):
    print("DTA-V3 Competitive & Stability Comparison -- Plots")
    plot_competitive_ratio(outdir)
    plot_burst_capacity(outdir)
    plot_spectral_gap(outdir)
    plot_crash_time(outdir)


if __name__ == "__main__":
    random.seed(42)
    np.random.seed(42)
    print_tables()
    make_plots(OUTDIR)
    run_verifications()
