#!/usr/bin/env python3
"""
simulate_dag_scheduling.py
Numerical validation of the DAG-scheduling kinetic-theory results.

Figures produced (written to ./figure/):
  dag_sync_cost_vs_K.png    -- E[max_K Exp(mu)] vs K, theory H_K/mu
  dag_variance_vs_K.png     -- Std[sync cost] vs K, pi/sqrt(6) asymptote
  dag_ode_dynamics.png      -- ODE f_l(t) for K=1,5,50 (smooth->sharp)
  dag_makespan_vs_depth.png -- Makespan vs depth d, theory vs simulation
  dag_phase_transition.png  -- Activation function phi_K showing 1st/2nd order
"""

import numpy as np
from scipy.integrate import solve_ivp
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os, math

RNG  = np.random.default_rng(42)
MU   = 1.0       # service rate (normalised to 1)
N    = 4         # DTA workers -- scaled down from the N=256 target
                 # deployment scale (main.tex, "Formal Mathematical
                 # Model") to keep this simulation fast; not a
                 # production measurement.
NREP = 10_000    # Monte Carlo repetitions per K value

os.makedirs('figure', exist_ok=True)

# ------------------------------------------------------------------ helpers --
def H(k):
    """k-th harmonic number."""
    return sum(1.0 / j for j in range(1, k + 1))

def H2(k):
    """sum_{j=1}^k 1/j^2."""
    return sum(1.0 / j**2 for j in range(1, k + 1))

def simulate_level(tasks, n_workers):
    """
    Greedy list-scheduling of `tasks` on `n_workers`.
    Returns makespan (max worker load).
    """
    loads = np.zeros(n_workers)
    for t in np.sort(tasks)[::-1]:       # longest-processing-time heuristic
        loads[np.argmin(loads)] += t
    return loads.max()

# ================================================================
# Figure 1 -- sync cost E[max_K Exp(mu)] vs K
# ================================================================
K_vals     = np.arange(1, 51)
theory_mean = np.array([H(k) / MU for k in K_vals])

sim_mean = np.zeros(len(K_vals))
sim_std  = np.zeros(len(K_vals))
for idx, k in enumerate(K_vals):
    samp = RNG.exponential(1.0 / MU, size=(NREP, k)).max(axis=1)
    sim_mean[idx] = samp.mean()
    sim_std[idx]  = samp.std()

fig, ax = plt.subplots(figsize=(6, 4))
ax.plot(K_vals, theory_mean, 'k-', linewidth=2,
        label=r'Theory: $H_K/\mu$')
ax.errorbar(K_vals[::4], sim_mean[::4],
            yerr=2 * sim_std[::4] / math.sqrt(NREP),
            fmt='o', color='steelblue', capsize=3,
            label=r'Simulation $(\pm 2\sigma_{\bar x})$')
ax.set_xlabel(r'Fan-in $K$', fontsize=12)
ax.set_ylabel(r'$\mathbb{E}[\tau_{\mathrm{join}}^{(K)}]$ ($1/\mu$ units)', fontsize=11)
ax.set_title('Fork-join synchronisation cost vs fan-in $K$', fontsize=12)
ax.legend(fontsize=10)
plt.tight_layout()
plt.savefig('figure/dag_sync_cost_vs_K.png', dpi=150)
plt.close()
print('Figure 1: dag_sync_cost_vs_K.png')

# ================================================================
# Figure 2 -- second-order correction delta^(A): Gumbel variance vs K
# ================================================================
theory_std  = np.array([math.sqrt(H2(k)) / MU for k in K_vals])
asymptote   = math.pi / math.sqrt(6) / MU

fig, ax = plt.subplots(figsize=(6, 4))
ax.plot(K_vals, theory_std, 'k-', linewidth=2,
        label=r'Theory: $\sqrt{\sum_{j=1}^K j^{-2}}\,/\mu$')
ax.axhline(asymptote, color='crimson', linestyle='--', linewidth=1.5,
           label=r'Asymptote $\pi/(\sqrt{6}\,\mu)$')
ax.errorbar(K_vals[::4], sim_std[::4],
            yerr=sim_std[::4] / math.sqrt(2 * NREP),
            fmt='s', color='steelblue', capsize=3, label='Simulation')
ax.set_xlabel(r'Fan-in $K$', fontsize=12)
ax.set_ylabel(r'$\mathrm{Std}[\tau_{\mathrm{join}}^{(K)}]$ ($1/\mu$ units)', fontsize=11)
ax.set_title(r'Gumbel fluctuation $\delta^{(A)}$: convergence to $\pi/\sqrt{6}$',
             fontsize=12)
ax.legend(fontsize=10)
plt.tight_layout()
plt.savefig('figure/dag_variance_vs_K.png', dpi=150)
plt.close()
print('Figure 2: dag_variance_vs_K.png')

# ================================================================
# Figure 3 -- ODE dynamics: smooth (K finite) vs sharp (K large)
# ================================================================
def dag_ode(t, F, K_list):
    """
    Mean-field ODE system:
        df_ell/dt = mu*(1 - f_ell) * f_{ell-1}^{K_ell}
    with f_{-1} = 1.
    """
    d   = len(F)
    dF  = np.zeros(d)
    for ell in range(d):
        f_prev    = 1.0 if ell == 0 else max(F[ell - 1], 0.0)
        f_curr    = min(max(F[ell], 0.0), 1.0)
        dF[ell]   = MU * (1.0 - f_curr) * (f_prev ** K_list[ell])
    return dF

t_end  = 30.0
t_eval = np.linspace(0, t_end, 600)
d_ode  = 3

fig, axes = plt.subplots(1, 3, figsize=(13, 4), sharey=True)
cases  = [(1, r'$K=1$ (chain, 2nd-order crossover)'),
          (5, r'$K=5$ (moderate fan-in)'),
          (50, r'$K=50$ ($\approx\infty$, fork-join, 1st-order)')]
lsty   = ['-', '--', '-.']
colors = ['#1f77b4', '#2ca02c', '#d62728']

for ax, (K_ode, title) in zip(axes, cases):
    K_list = [K_ode] * d_ode
    sol    = solve_ivp(dag_ode, (0, t_end), np.zeros(d_ode),
                       args=(K_list,), t_eval=t_eval,
                       method='RK45', rtol=1e-9, atol=1e-12)
    for ell in range(d_ode):
        ax.plot(sol.t * MU, sol.y[ell],
                linestyle=lsty[ell], color=colors[ell],
                linewidth=1.8, label=fr'$\ell={ell}$')
    ax.set_title(title, fontsize=10)
    ax.set_xlabel(r'$\mu t$', fontsize=11)
    if ax is axes[0]:
        ax.set_ylabel(r'$f_\ell(t)$ (fraction complete)', fontsize=11)
    ax.legend(fontsize=9)
    ax.set_ylim(-0.03, 1.05)
    ax.set_xlim(0, t_end)
    # Annotate phase-transition character
    char = 'Continuous (2nd-order)' if K_ode <= 5 else 'Discontinuous (1st-order)'
    ax.text(0.97, 0.05, char, transform=ax.transAxes,
            ha='right', va='bottom', fontsize=8,
            bbox=dict(boxstyle='round,pad=0.2', fc='wheat', alpha=0.6))

plt.suptitle('Mean-field DAG kinetics: smooth vs sharp layer activation',
             fontsize=12, y=1.01)
plt.tight_layout()
plt.savefig('figure/dag_ode_dynamics.png', dpi=150, bbox_inches='tight')
plt.close()
print('Figure 3: dag_ode_dynamics.png')

# ================================================================
# Figure 4 -- makespan vs depth d: theory vs simulation
# ================================================================
K_fj      = 4
M_level   = 16      # tasks per level
n_rep_sim = 2000
d_range   = np.arange(1, 11)

# Theory curves
# T_1 = sum_ell M_ell/mu = d * M_level/mu (constant M_level per level,
# matching main.tex's definition T_1 = sum_ell M_ell/mu); the first-order
# bound is T_1/N + d*H_K/mu (main.tex eq. for E[C_max], "first order" term).
theory_1st = np.array([d * M_level / (N * MU) + d * H(K_fj) / MU
                        for d in d_range])
theory_2nd = theory_1st + math.pi * np.sqrt(d_range) / (math.sqrt(6) * MU)

# Simulation
sim_means = []
sim_errs  = []
for d in d_range:
    runs = []
    for _ in range(n_rep_sim):
        t = 0.0
        for ell in range(d):
            tasks      = RNG.exponential(1.0 / MU, size=M_level)
            t         += simulate_level(tasks, N)
            if ell < d - 1:
                t     += RNG.exponential(1.0 / MU, size=K_fj).max()
        runs.append(t)
    arr = np.array(runs)
    sim_means.append(arr.mean())
    sim_errs.append(arr.std() / math.sqrt(n_rep_sim))

fig, ax = plt.subplots(figsize=(6.5, 4))
ax.plot(d_range, theory_1st, 'k-', linewidth=2,
        label=r'1st order: $T_1/N + dH_K/\mu$')
ax.plot(d_range, theory_2nd, 'k--', linewidth=1.5,
        label=r'+ $\delta^{(A)}$: $+\pi\sqrt{d}/(\sqrt{6}\,\mu)$')
ax.errorbar(d_range, sim_means, yerr=2 * np.array(sim_errs),
            fmt='o', color='steelblue', capsize=4, linewidth=1.5,
            label=fr'Simulation ($K={K_fj}$, $N={N}$, $M_\ell={M_level}$)')
ax.set_xlabel(r'DAG depth $d$', fontsize=12)
ax.set_ylabel(r'$\mathbb{E}[C_{\max}]$ ($1/\mu$ units)', fontsize=11)
ax.set_title(f'Fork-join makespan vs depth (K={K_fj}, N={N})', fontsize=12)
ax.legend(fontsize=9)
plt.tight_layout()
plt.savefig('figure/dag_makespan_vs_depth.png', dpi=150)
plt.close()
print('Figure 4: dag_makespan_vs_depth.png')

# ================================================================
# Figure 5 -- activation function phi_K: 1st vs 2nd order crossover
# ================================================================
f_prev = np.linspace(0.0, 1.0, 300)
K_show = [1, 2, 5, 10, 50]
cmap   = plt.cm.plasma(np.linspace(0.1, 0.85, len(K_show)))

fig, ax = plt.subplots(figsize=(6, 4))
for k, col in zip(K_show, cmap):
    phi = f_prev ** k
    ax.plot(f_prev, phi, color=col, linewidth=2,
            label=f'$K={k}$')

# Shade regions
ax.axvspan(0, 1, alpha=0.04, color='blue', label='_nolegend_')
ax.axvline(1.0, color='gray', linestyle=':', alpha=0.6)

# Critical fan-in annotation
K_star = 5
f_star = 1 - 1.0 / K_star
ax.axvline(f_star, color='green', linestyle=':', alpha=0.7, linewidth=1.2)
ax.text(f_star + 0.01, 0.6, r'$f=1-1/K^*$', fontsize=8, color='green')

ax.set_xlabel(r'$f_{\ell-1}$ (predecessor completion fraction)', fontsize=11)
ax.set_ylabel(r'$\varphi_K(f_{\ell-1}) = f_{\ell-1}^K$', fontsize=11)
ax.set_title('Activation function: 2nd-order (finite $K$) vs 1st-order ($K\\to\\infty$)',
             fontsize=11)
ax.legend(fontsize=9, loc='upper left')
ax.text(0.02, 0.10, 'Smooth crossover\n(2nd-order, finite $K$)',
        transform=ax.transAxes, fontsize=8, color='steelblue')
ax.text(0.55, 0.80, 'Step function\n($K\\!\\to\\!\\infty$, 1st-order)',
        transform=ax.transAxes, fontsize=8, color='darkred')
plt.tight_layout()
plt.savefig('figure/dag_phase_transition.png', dpi=150)
plt.close()
print('Figure 5: dag_phase_transition.png')

# ================================================================
# Print key theoretical constants
# ================================================================
print('\n--- Theoretical constants ---')
print(f'pi^2/6          = {math.pi**2/6:.6f}')
print(f'pi/sqrt(6)      = {math.pi/math.sqrt(6):.6f}')
for k in [1, 2, 4, 8, 16, 32]:
    print(f'H_{k:2d} = {H(k):.5f},  sqrt(H2_{k:2d}) = {math.sqrt(H2(k)):.5f}')
print('\nAll figures written to ./figure/')
