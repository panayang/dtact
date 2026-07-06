"""
plot_variance_analysis.py
==========================
Regenerates figure/variance_analysis.png: four-panel statistical-moment
analysis of the real Criterion benchmark data (Spawn / Deflect, dtact vs.
Tokio), for the "Statistical Moments and Variance Analysis" section
(\\label{sec:variance_analysis} in main_acmart.tex).

IMPORTANT -- data provenance
-----------------------------
This is NOT a simulation. Every number plotted here is the measured
Criterion output already reported in the paper's Table~\\ref{tab:moments}
("Statistical moments from Criterion benchmark outputs") and
Table~\\ref{tab:gumbel} ("Observed standard deviation vs Gumbel-predicted
algorithmic variance"). Those two tables are themselves extracted from the
Criterion `estimates.json` outputs of the real Rust benchmark run (see
Sources column omitted here since bench artifacts are not part of the
anonymized mirror). This script exists purely to re-render the figure
from those already-published numbers, in case the original plotting
script that produced variance_analysis.png was lost; it should not be
read as a new measurement or a new analysis.

No script previously in script/ produced this figure (verified by
grepping every *.py file in script/ for "variance_analysis" -- no match),
which is why this replacement was written from the paper's own tables
rather than recovered from an original source file.

Panels
------
A  Spawn: coefficient of variation (CV = Std/Mean, %) vs task count M,
   dtact vs Tokio, with the theoretical Gumbel-noise CV curve
   pi/(sqrt(6)*M) (as %) overlaid for scale reference.
B  Deflect: same CV-vs-M comparison (no theory line: the Deflect
   workload's algorithmic variance does not reduce to the single-stage
   Gumbel formula used for Spawn).
C  Spawn: observed standard deviation of C_max vs Gumbel-predicted
   algorithmic standard deviation (dtact only), showing the several-orders-
   of-magnitude gap that indicates CI runner noise dominates.
D  Tail-shape diagnostic: MAD/Std ratio for all nine Spawn/Deflect
   benchmark x implementation combinations, with the Gaussian reference
   value 0.6745 shown as a dashed line.

Run from repo root:
    python script/plot_variance_analysis.py
Output: figure/variance_analysis.png
"""

import numpy as np
import matplotlib

matplotlib.use("Agg")
from pathlib import Path

import matplotlib.pyplot as plt

# ── Table tab:moments -- Mean / Std / MAD / CV% (dtact vs Tokio) ──────────────
# All times in microseconds. Source: main_acmart.tex, Table~\ref{tab:moments}.
MOMENTS = {
    # key: (M, impl) -> (mean_us, std_us, mad_us, cv_pct)
    ("spawn", 1e3, "dtact"): (161.0, 43.7, 28.2, 27.1),
    ("spawn", 1e3, "tokio"): (699.3, 192.5, 150.5, 27.5),
    ("spawn", 1e4, "dtact"): (2082.6, 370.9, 472.4, 17.8),
    ("spawn", 1e4, "tokio"): (5542.6, 1071.6, 984.7, 19.3),
    ("spawn", 1e5, "dtact"): (12308, 1242, 946, 10.1),
    ("spawn", 1e5, "tokio"): (45333, 9128, 9232, 20.1),
    ("spawn", 1e6, "dtact"): (105636, 7962, 8361, 7.5),
    ("spawn", 1e6, "tokio"): (650558, 90343, 103643, 13.9),
    ("deflect", 1e3, "dtact"): (280.7, 104.1, 48.1, 37.1),
    ("deflect", 1e3, "tokio"): (812.5, 223.1, 164.3, 27.5),
    ("deflect", 1e4, "dtact"): (2582.7, 340.7, 319.7, 13.2),
    ("deflect", 1e4, "tokio"): (5962.1, 751.1, 676.8, 12.6),
    ("deflect", 1e5, "dtact"): (17801, 2050, 1740, 11.5),
    ("deflect", 1e5, "tokio"): (48092, 6209, 5264, 12.9),
    ("deflect", 1e6, "dtact"): (170016, 13767, 13808, 8.1),
    ("deflect", 1e6, "tokio"): (669332, 78735, 82028, 11.8),
    ("deflect", 1e7, "dtact"): (1685310, 129257, 134427, 7.7),
    ("deflect", 1e7, "tokio"): (7090859, 681413, 726628, 9.6),
}

# ── Table tab:gumbel -- observed std (us) vs Gumbel-predicted std (ns) ───────
# Source: main_acmart.tex, Table~\ref{tab:gumbel}, Spawn only.
GUMBEL_NS = {
    1e3: {"dtact": 51.6, "tokio": 224.2},
    1e4: {"dtact": 66.8, "tokio": 177.7},
    1e5: {"dtact": 39.5, "tokio": 145.4},
    1e6: {"dtact": 33.9, "tokio": 208.6},
}

BLUE = "#2E86C1"
ORANGE = "#E67E22"

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
    "Statistical moment analysis: Criterion data vs theoretical predictions",
    fontsize=12,
)
fig.subplots_adjust(top=0.92, hspace=0.4, wspace=0.3)
axes = fig.subplots(2, 2)

spawn_Ms = sorted({k[1] for k in MOMENTS if k[0] == "spawn"})
deflect_Ms = sorted({k[1] for k in MOMENTS if k[0] == "deflect"})

# ─── Panel A: Spawn CV vs M ───────────────────────────────────────────────────
ax = axes[0, 0]
for impl, color, marker in [("dtact", BLUE, "o"), ("tokio", ORANGE, "s")]:
    cv = [MOMENTS[("spawn", m, impl)][3] for m in spawn_Ms]
    label = "dtact" if impl == "dtact" else "Tokio"
    ax.plot(spawn_Ms, cv, marker=marker, color=color, lw=2, ms=6, label=label)

# Gumbel-noise reference CV curve pi/(sqrt(6)*M), expressed as a percentage,
# shown purely for scale (it is not fit to the data -- see main text: this
# line illustrates how many orders of magnitude below the observed CV the
# pure-algorithmic-noise prediction would sit, it is not a claim that any
# curve here is tightly fit).
m_ref = np.array(spawn_Ms, dtype=float)
gumbel_cv_ref = 100 * np.pi / (np.sqrt(6) * m_ref)
ax.plot(
    m_ref, gumbel_cv_ref, ":", color="black", lw=1.6,
    label=r"Gumbel theory: $\pi/(\sqrt{6}\,M)$",
)

ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("Task count $M$")
ax.set_ylabel("CV = Std/Mean (%)")
ax.set_title("(A) Spawn: coefficient of variation vs $M$")
ax.legend(loc="lower left", fontsize=8)
ax.grid(True, which="both", alpha=0.25)

# ─── Panel B: Deflect CV vs M ─────────────────────────────────────────────────
ax = axes[0, 1]
for impl, color, marker in [("dtact", BLUE, "o"), ("tokio", ORANGE, "s")]:
    cv = [MOMENTS[("deflect", m, impl)][3] for m in deflect_Ms]
    label = "dtact" if impl == "dtact" else "Tokio"
    ax.plot(deflect_Ms, cv, marker=marker, color=color, lw=2, ms=6, label=label)

ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("Task count $M$")
ax.set_ylabel("CV = Std/Mean (%)")
ax.set_title("(B) Deflect: coefficient of variation vs $M$")
ax.legend(loc="lower left", fontsize=8)
ax.grid(True, which="both", alpha=0.25)

# ─── Panel C: Spawn observed std vs Gumbel-predicted std (dtact only) ────────
ax = axes[1, 0]
obs_std_us = [MOMENTS[("spawn", m, "dtact")][1] for m in spawn_Ms]
gumbel_std_us = [GUMBEL_NS[m]["dtact"] / 1000.0 for m in spawn_Ms]

ax.plot(spawn_Ms, obs_std_us, "o-", color=BLUE, lw=2, ms=6,
        label="Empirical $\\sigma$ (dtact)")
ax.plot(spawn_Ms, gumbel_std_us, ":", color="black", lw=1.8,
        label=r"Gumbel theory $\pi T_1/(\sqrt{6}\,MN)$")

ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("Task count $M$")
ax.set_ylabel(r"$\sigma[C_{\max}]$ ($\mu$s)")
ax.set_title("(C) Spawn: observed vs Gumbel-predicted std")
ax.legend(loc="center right", fontsize=8)
ax.grid(True, which="both", alpha=0.25)
ax.text(
    0.06, 0.9, "Runner noise dominates\n(ratio $\\gg 1000\\times$)",
    transform=ax.transAxes, fontsize=8.5, va="top",
    bbox=dict(boxstyle="round", facecolor="#f5deb3", edgecolor="gray", alpha=0.9),
)

# ─── Panel D: MAD/Std tail-shape diagnostic ──────────────────────────────────
ax = axes[1, 1]
categories = [
    ("Spawn\n1k", "spawn", 1e3), ("Spawn\n10k", "spawn", 1e4),
    ("Spawn\n100k", "spawn", 1e5), ("Spawn\n1M", "spawn", 1e6),
    ("Defl\n1k", "deflect", 1e3), ("Defl\n10k", "deflect", 1e4),
    ("Defl\n100k", "deflect", 1e5), ("Defl\n1M", "deflect", 1e6),
    ("Defl\n10M", "deflect", 1e7),
]
labels = [c[0] for c in categories]
dtact_ratio = [MOMENTS[(c[1], c[2], "dtact")][2] / MOMENTS[(c[1], c[2], "dtact")][1] for c in categories]
tokio_ratio = [MOMENTS[(c[1], c[2], "tokio")][2] / MOMENTS[(c[1], c[2], "tokio")][1] for c in categories]

x = np.arange(len(categories))
w = 0.38
ax.bar(x - w / 2, dtact_ratio, width=w, color=BLUE, label="dtact")
ax.bar(x + w / 2, tokio_ratio, width=w, color=ORANGE, label="Tokio")
ax.axhline(0.6745, color="black", ls="--", lw=1.4, label="Gaussian: MAD/Std = 0.6745")

ax.set_xticks(x)
ax.set_xticklabels(labels, fontsize=8)
ax.set_ylabel("MAD / Std")
ax.set_title("(D) Tail-shape diagnostic (MAD/Std)")
ax.legend(loc="upper right", fontsize=8)
ax.grid(True, axis="y", alpha=0.25)

# ── save ──────────────────────────────────────────────────────────────────────
out = Path(__file__).parent.parent / "figure" / "variance_analysis.png"
out.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(out, dpi=150, bbox_inches="tight")
print(f"Saved: {out}")
