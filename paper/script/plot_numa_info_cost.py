"""
plot_numa_info_cost.py
======================
Generates figure/numa_info_cost.png: four-panel visualisation for Part VI
§5 "Concrete Analysis on a Dual-Socket NUMA Topology".

Panels
------
A  β_WS / β_DTA vs N  (shows Ω(log N) growth)
B  Absolute β_WS and β_DTA vs N  (ns per task)
C  Total cost J(π; γ) vs γ  for three workload sizes W (crossover at γ*)
D  Relative information gap (Δq)²/(4μN) / C*_WS vs workload W

Units note
----------
β  = information-acquisition cost per task (ns/task).
     β_DTA = c_SPSC; β_WS = δ̄ + c_CAS^(0)·log₂N.
J(π; γ) = E[C_max] + γ · β_π · W   (all in ns; γ dimensionless)
γ* = (C*_DTA − C*_WS) / [(β_WS − β_DTA) · W]

Run from repo root:
    python scripts/plot_numa_info_cost.py
Output: paper/figure/numa_info_cost.png
"""

import matplotlib
import numpy as np

matplotlib.use("Agg")
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

# ── hardware parameters ───────────────────────────────────────────────────────
D_INTRA = 80.0  # ns
D_INTER = 300.0  # ns
CAS0 = 100.0  # ns  (uncontended)
C_SPSC = D_INTRA  # ns


def delta_bar(N):
    """Mean NUMA latency, 2-socket system."""
    Nc = N / 2
    return D_INTRA * (Nc - 1) / (N - 1) + D_INTER * Nc / (N - 1)


def beta_dta(_N):
    """Information cost per task for DTA (ns/task)."""
    return C_SPSC


def beta_ws(N):
    """Information cost per task for WS (ns/task), Ω(log N) contention."""
    return delta_bar(N) + CAS0 * np.log2(N)


# ── queueing parameters ───────────────────────────────────────────────────────
MU = 1e-3  # service rate (tasks / ns)  → mean service = 1 µs
N0 = 64  # representative core count

# ── colours ───────────────────────────────────────────────────────────────────
BLUE = "#2E86C1"
ORANGE = "#E67E22"
GREEN = "#27AE60"
GREY = "#7F8C8D"
RED = "#C0392B"

plt.rcParams.update(
    {
        "font.family": "serif",
        "font.size": 10,
        "axes.titlesize": 10.5,
        "axes.labelsize": 9.5,
        "legend.fontsize": 8.5,
        "xtick.labelsize": 8.5,
        "ytick.labelsize": 8.5,
    }
)

fig = plt.figure(figsize=(11, 9))
fig.suptitle(
    r"Information-Acquisition Cost: DTA vs.\ WS on Dual-Socket NUMA"
    "\n"
    r"($\delta_\mathrm{intra}=80\,\mathrm{ns},\ "
    r"\delta_\mathrm{inter}=300\,\mathrm{ns},\ "
    r"c_\mathrm{CAS}^{(0)}=100\,\mathrm{ns},\ "
    r"c_\mathrm{SPSC}=80\,\mathrm{ns}$)",
    fontsize=10.5,
)
fig.subplots_adjust(top=0.88, hspace=0.45, wspace=0.38)
axes = fig.subplots(2, 2)

Ns = np.array([4, 8, 16, 32, 64, 128, 256])

# ─── Panel A: β_WS / β_DTA vs N ──────────────────────────────────────────────
ax = axes[0, 0]
ratio = np.array([beta_ws(n) / beta_dta(n) for n in Ns])
logN_ref = np.log2(Ns.astype(float))
scale = ratio[0] / logN_ref[0]

ax.plot(
    Ns,
    ratio,
    "o-",
    color=BLUE,
    lw=2,
    ms=6,
    label=r"$\beta_\mathrm{WS}/\beta_\mathrm{DTA}$",
)
ax.plot(
    Ns,
    logN_ref * scale,
    "--",
    color=GREY,
    lw=1.4,
    label=r"$k\!\cdot\!\log_2 N$ reference",
)

ax.set_xscale("log", base=2)
ax.set_xticks(Ns)
ax.set_xticklabels([str(n) for n in Ns])
ax.set_xlabel("Number of workers $N$")
ax.set_ylabel(r"$\beta_\mathrm{WS}\ /\ \beta_\mathrm{DTA}$")
ax.set_title(r"(A)  Cost ratio — $\Omega(\log N)$ growth")
ax.legend(loc="upper left")
ax.grid(True, alpha=0.3)

for n, r in zip([8, 64, 256], [beta_ws(n) / beta_dta(n) for n in [8, 64, 256]]):
    ax.annotate(
        f"{r:.1f}×",
        xy=(n, r),
        xytext=(n * 1.2, r - 0.7),
        fontsize=8,
        color=BLUE,
        arrowprops=dict(arrowstyle="-", color=BLUE, lw=0.7),
    )

# ─── Panel B: absolute β vs N ─────────────────────────────────────────────────
ax = axes[0, 1]
bw = np.array([beta_ws(n) for n in Ns])
bd = np.array([beta_dta(n) for n in Ns])

ax.plot(Ns, bw, "s-", color=ORANGE, lw=2, ms=6, label=r"$\beta_\mathrm{WS}$  (WS)")
ax.plot(Ns, bd, "o-", color=BLUE, lw=2, ms=6, label=r"$\beta_\mathrm{DTA}$ (DTA)")
ax.fill_between(Ns, bd, bw, alpha=0.13, color=GREEN, label="DTA cost advantage")

# decompose WS cost at N=64
n64 = 64
ybot = delta_bar(n64)
ytop = beta_ws(n64)
ax.annotate(
    f"$\\delta_{{\\mathrm{{avg}}}}={ybot:.0f}\\,\\mathrm{{ns}}$\n"
    f"CAS: $+{ytop - ybot:.0f}\\,\\mathrm{{ns}}$",
    xy=(n64, (ybot + ytop) / 2),
    xytext=(n64 * 1.7, (ybot + ytop) / 2 + 40),
    fontsize=7.5,
    color=ORANGE,
    arrowprops=dict(arrowstyle="->", color=ORANGE, lw=0.9),
)

ax.set_xscale("log", base=2)
ax.set_xticks(Ns)
ax.set_xticklabels([str(n) for n in Ns])
ax.set_xlabel("Number of workers $N$")
ax.set_ylabel("Info cost per task $\\beta$ (ns/task)")
ax.set_title("(B)  Absolute acquisition costs")
ax.legend(loc="upper left")
ax.grid(True, alpha=0.3)

# ─── Panel C: J(π; γ) vs γ for three workload sizes ──────────────────────────
ax = axes[1, 0]

DQ = 5.0  # inter-socket queue imbalance (tasks); representative mid-load value
bw0 = beta_ws(N0)
bd0 = beta_dta(N0)
denom = bw0 - bd0  # > 0 since WS is always more expensive

configs = [
    (1_000, "-", r"$W=10^3$"),
    (10_000, "--", r"$W=10^4$"),
    (100_000, ":", r"$W=10^5$"),
]

g_maxes = []
for W, ls, lbl in configs:
    Cws = W / (N0 * MU)  # C*_WS  (ns)
    Cdta = Cws + DQ**2 / (4 * MU * N0)  # C*_DTA (ns); info gap added
    gstar = (Cdta - Cws) / (denom * W)  # dimensionless

    g_max = max(gstar * 8, 5e-4)
    g_maxes.append(g_max)

g_plot_max = max(g_maxes)
gammas = np.linspace(0, g_plot_max, 600)

colors_W = [ORANGE, GREEN, RED]
for (W, ls, lbl), col in zip(configs, colors_W):
    Cws = W / (N0 * MU)
    Cdta = Cws + DQ**2 / (4 * MU * N0)
    gstar = (Cdta - Cws) / (denom * W)

    J_ws = (Cws + gammas * bw0 * W) / 1e6  # ms
    J_dta = (Cdta + gammas * bd0 * W) / 1e6  # ms

    ax.plot(gammas, J_ws, ls, color=col, lw=1.8, alpha=0.9, label=f"WS, {lbl}")
    ax.plot(gammas, J_dta, ls, color=BLUE, lw=1.8, alpha=0.9, label=f"DTA, {lbl}")
    ax.axvline(gstar, color=col, lw=1.0, ls=ls, alpha=0.5)
    ax.text(
        gstar * 1.04,
        ax.get_ylim()[1] if ax.get_ylim()[1] > 0 else 1,
        f"$\\gamma^*_{{W={int(np.log10(W))}}}$",
        fontsize=7.5,
        color=col,
        va="top",
        rotation=90,
    )

# build legend manually to keep it small
proxies = [
    Line2D([0], [0], color=BLUE, lw=2, ls="-", label="DTA"),
    Line2D([0], [0], color="0.5", lw=2, ls="-", label="WS"),
    Line2D([0], [0], color=ORANGE, lw=1, ls="-", label=r"$W=10^3$"),
    Line2D([0], [0], color=GREEN, lw=1, ls="--", label=r"$W=10^4$"),
    Line2D([0], [0], color=RED, lw=1, ls=":", label=r"$W=10^5$"),
]
ax.legend(handles=proxies, loc="upper left", ncol=2, fontsize=7.5)
ax.set_xlabel(r"Cost weight $\gamma$ (dimensionless)")
ax.set_ylabel(r"Total cost $\mathcal{J}$ (ms)")
ax.set_title(
    r"(C)  $\mathcal{J}(\pi;\gamma)$ crossover  "
    r"($N=64$, $\Delta q=5\,\text{tasks}$)"
)
ax.grid(True, alpha=0.3)

# ─── Panel D: relative information gap vs W ───────────────────────────────────
ax = axes[1, 1]
Ws = np.logspace(2, 7, 400)

# For each W, imbalance Δq from balanced-load CLT: Δq ~ sqrt(N·ρ/W)·H
# We show three imbalance scenarios
RHO = 0.8

for dq_label, dq_fn, col, ls in [
    (r"$\Delta q = 5$ (fixed)", lambda W: 5.0, BLUE, "-"),
    (r"$\Delta q = \sqrt{N/W}$", lambda W: np.sqrt(N0 / W), GREEN, "--"),
    (r"$\Delta q = \sqrt{N\rho/W}$", lambda W: np.sqrt(N0 * RHO / W), ORANGE, ":"),
]:
    gap_W = np.array([dq_fn(w) ** 2 / (4 * MU * N0) for w in Ws])
    Cmax_W = Ws / (N0 * MU)
    rel = gap_W / Cmax_W * 100  # %

    ax.loglog(Ws, rel, ls, color=col, lw=1.8, label=dq_label)

ax.axhline(1.0, color=GREY, ls=":", lw=1.2, alpha=0.8, label="1\\% threshold")
ax.axhline(0.1, color=GREY, ls="--", lw=1.0, alpha=0.6, label="0.1\\% threshold")

ax.set_xlabel("Total work $W$ (tasks)")
ax.set_ylabel(r"Relative info gap $\Delta C^*/C^*_\mathrm{WS}$ (\%)")
ax.set_title(r"(D)  Info gap $\to 0$ at high load  ($N=64$)")
ax.legend(loc="upper right", fontsize=8)
ax.grid(True, which="both", alpha=0.3)

# ── save ──────────────────────────────────────────────────────────────────────
out = Path(__file__).parent / "paper" / "figure" / "numa_info_cost.png"
out.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(out, dpi=150, bbox_inches="tight")
print(f"Saved: {out}")
