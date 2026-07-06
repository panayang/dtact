"""
plot_pd_decomposition.py
=========================
Regenerates figure/component_breakdown.png -- Decomposition of the
deflection probability p_d into its Level-0 (mean-field) and Level-1
(pair-correlation) components across load levels, for the
"Level-1 corrected load imbalance" discussion in main_acmart.tex
(Remark "Explicit level-1 correction", eq:pd-level1, and
\\label{fig:component_breakdown}).

Formula used (paper eq:pd-level1):
    p_d^(2)  ~=  p_d * (1 + rho_0 * p_d / (N-1))
Level-0 component: p_d itself (mean-field, i.e. the M/M/1/H tail
probability pi(H; rho*)). Level-1 component: the additive correction
    Delta p_d^(1) = p_d * rho_0 * p_d / (N - 1).

IMPORTANT -- this replaces a MISMATCHED figure, not a missing one
--------------------------------------------------------------------
figure/component_breakdown.png previously held the output of an
unrelated plot: script/plot_wait_bound.py's Figure 3 (a "queue-depth
vs warehouse" stacked bar for the bounded-waiting-time bound
W(tau)/delta, Progress~I), which happens to save to a file of the same
name. This script generates what the caption at
\\label{fig:component_breakdown} actually describes, reusing the same
M/M/1/H and self-consistency machinery as simulate_load_balance.py, at
the same benchmark N=4 and H_SCALE=100 as its sibling figure
finite_N_error.png (so the two panels are numerically consistent: at
rho0=0.95 this script reproduces p_d~0.000305, matching the paper's
own quoted finite-N error epsilon_N ~ 0.0003-0.0004 at that point).

Plot choice: a *fractional* (100%-stacked) bar chart was tried first
and rejected -- at H=100 the Level-1 correction stays below 0.3% of
p_d across the entire rho0 in (0,1) range achievable in this model
(the paper's own asymptotic claim that the correction approaches
1/(N-1) is a rho0->1, H->infinity double limit not attained at finite,
fixed H), so a stacked/fractional rendering makes Level-1 invisible.
Instead we plot Level-0 and Level-1 as two separate curves on a log-y
axis vs rho0, which is the only rendering that keeps both components
visible and legible across the ~4 orders of magnitude they span --
and is an honest picture: it shows the correction really is small at
every load level in this range, which is exactly what the surrounding
text (Remark "Explicit level-1 correction") argues.

Run from repo root:
    python script/plot_pd_decomposition.py
Output: figure/component_breakdown.png
"""

from pathlib import Path

import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

H_SCALE = 100
N_BENCHMARK = 4


def mmh_pmf(rho: float, H: int) -> np.ndarray:
    if abs(rho - 1.0) < 1e-10:
        return np.ones(H + 1) / (H + 1)
    qs = np.arange(H + 1, dtype=float)
    unnorm = rho ** qs
    return unnorm / unnorm.sum()


def deflection_prob(rho: float, H: int) -> float:
    return float(mmh_pmf(rho, H)[H])


def solve_sc(rho0_base: float, N: int, H: int, max_iter: int = 500,
             tol: float = 1e-10) -> float:
    rho = rho0_base
    for _ in range(max_iter):
        pd = deflection_prob(rho, H)
        rho_new = rho0_base * (1.0 + pd)
        if abs(rho_new - rho) < tol:
            return rho_new
        rho = rho_new
    return float("nan")


rho0_sweep = np.linspace(0.5, 0.99, 60)
rho0_markers = [0.5, 0.7, 0.9, 0.95, 0.99]

level0_curve, level1_curve = [], []
for rho0 in rho0_sweep:
    rho_star = solve_sc(rho0, N_BENCHMARK, H_SCALE)
    pd0 = deflection_prob(rho_star, H_SCALE)
    dpd1 = pd0 * rho0 * pd0 / (N_BENCHMARK - 1)
    level0_curve.append(max(pd0, 1e-300))
    level1_curve.append(max(dpd1, 1e-300))

plt.rcParams.update({"font.family": "serif", "font.size": 10.5})
fig, ax = plt.subplots(figsize=(6.6, 4.4))

ax.plot(rho0_sweep, level0_curve, "-", color="#2E86C1", lw=2.2,
        label=r"Level-0 (mean field): $p_d$")
ax.plot(rho0_sweep, level1_curve, "--", color="#E67E22", lw=2.2,
        label=r"Level-1 (pair correlation): $\Delta p_d^{(1)}$")

for r in rho0_markers:
    rs = solve_sc(r, N_BENCHMARK, H_SCALE)
    pd0 = deflection_prob(rs, H_SCALE)
    dpd1 = pd0 * r * pd0 / (N_BENCHMARK - 1)
    ax.scatter([r], [max(pd0, 1e-300)], color="#2E86C1", zorder=5, s=28)
    ax.scatter([r], [max(dpd1, 1e-300)], color="#E67E22", zorder=5, s=28)

ax.set_yscale("log")
ax.set_xlabel(r"Offered load $\varrho_0$")
ax.set_ylabel(r"Deflection probability component")
ax.set_title(f"Level-0/Level-1 decomposition of $p_d$ across load levels\n"
             f"(Benchmark: $N={N_BENCHMARK}$, $H={H_SCALE}$)", fontsize=10.5)
ax.legend(fontsize=8.5, loc="lower right")
ax.grid(True, which="both", linestyle="--", alpha=0.3)
fig.tight_layout()

out = Path(__file__).parent.parent / "figure" / "component_breakdown.png"
out.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(out, dpi=150)
print(f"Saved: {out}")
for r in rho0_markers:
    rs = solve_sc(r, N_BENCHMARK, H_SCALE)
    pd0 = deflection_prob(rs, H_SCALE)
    dpd1 = pd0 * r * pd0 / (N_BENCHMARK - 1)
    print(f"  rho0={r}: p_d(L0)={pd0:.6g}  Delta_p_d(L1)={dpd1:.6g}  "
          f"L1/L0 fraction={dpd1/max(pd0,1e-300):.4%}")
