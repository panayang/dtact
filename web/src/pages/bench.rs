use leptos::prelude::*;

const BENCH_URL: &str = "https://dtact.apich.org/bench/";
const BENCH_COMMIT: &str = "13db4ff";
const BENCH_COMMIT_FULL: &str = "13db4ff883b5675f063edeb3a5269c47f8b2c328";
const BENCH_RUN_URL: &str =
    "https://github.com/Apich-Organization/dtact/actions/runs/26011780070/job/76453764923";

#[component]
pub fn BenchPage() -> impl IntoView {
    view! {
        <div class="bench-page pt-nav page-wrap">

            // ── Header ───────────────────────────────────────────────────
            <section class="section">
                <span class="section-chip">"Performance Measurements"</span>
                <h1 class="bench-title">"Benchmarks"</h1>
                <p class="mt-sm" style="max-width:62ch">
                    "Criterion-based benchmarks comparing dtact against Tokio (4 workers each)
                     across spawn efficiency, cooperative yielding, and hot-core work deflection.
                     The live Criterion HTML report reflects the most recent commit."
                </p>
                <div class="flex gap-sm mt-md flex-wrap">
                    <a href=BENCH_URL target="_blank" rel="noopener" class="btn btn-primary">
                        "\u{1F4CA} Open Live Report \u{2197}"
                    </a>
                    <a href=BENCH_RUN_URL target="_blank" rel="noopener" class="btn btn-ghost">
                        "CI Run \u{2197}"
                    </a>
                    <a href="https://github.com/Apich-Organization/dtact/tree/main/benches"
                       target="_blank" rel="noopener" class="btn btn-ghost">
                        "Bench Source"
                    </a>
                </div>
            </section>

            // ── Snapshot banner ──────────────────────────────────────────
            <section class="section">
                <div class="bench-snapshot glass card-pad">
                    <div class="bench-snapshot-row">
                        <span class="section-chip">"Data Snapshot"</span>
                        <span class="bench-snapshot-meta">
                            "Commit\u{00A0}"
                            <a href=format!(
                                "https://github.com/Apich-Organization/dtact/commit/{}",
                                BENCH_COMMIT_FULL)
                               target="_blank" rel="noopener" class="mono">{BENCH_COMMIT}</a>
                            "\u{00A0}\u{00B7}\u{00A0}ubuntu-latest\u{00A0}
                             \u{00B7}\u{00A0}4-core x86_64\u{00A0}
                             \u{00B7}\u{00A0}4 workers each\u{00A0}
                             \u{00B7}\u{00A0}200 samples / 600 s measurement"
                        </span>
                    </div>
                    <p class="text-sm text-muted mt-sm">
                        "The live report at "
                        <a href=BENCH_URL target="_blank" rel="noopener"
                           class="mono">"dtact.apich.org/bench"</a>
                        " always reflects the latest commit and may differ from the figures below."
                    </p>
                </div>
            </section>

            // ── Spawn Efficiency ─────────────────────────────────────────
            <section class="section">
                <BenchChartCard
                    title="Spawn Efficiency"
                    chip="Dtact vs Tokio — spawn \u{2192} run \u{2192} join all tasks"
                    note="Lower is better. Tokio bar anchored at 100 %; dtact bar shows proportional time."
                >
                    <BarGroup
                        label="1 000 tasks"
                        dtact_pct=21 tokio_pct=100
                        dtact_val="158 \u{00B5}s" tokio_val="751 \u{00B5}s"
                        speedup="4.8\u{00D7}"
                    />
                    <BarGroup
                        label="10 000 tasks"
                        dtact_pct=38 tokio_pct=100
                        dtact_val="2.01 ms" tokio_val="5.30 ms"
                        speedup="2.6\u{00D7}"
                    />
                    <BarGroup
                        label="100 000 tasks"
                        dtact_pct=19 tokio_pct=100
                        dtact_val="11.8 ms" tokio_val="63.1 ms"
                        speedup="5.3\u{00D7}"
                    />
                    <BarGroup
                        label="1 000 000 tasks"
                        dtact_pct=16 tokio_pct=100
                        dtact_val="105 ms" tokio_val="662 ms"
                        speedup="6.3\u{00D7}"
                    />
                </BenchChartCard>
            </section>

            // ── Yield Efficiency + Summary ────────────────────────────────
            <section class="section">
                <div class="grid-2">
                    <BenchChartCard
                        title="Yield Efficiency"
                        chip="10 tasks \u{00D7} 100 yield_now() calls each"
                        note="dtact anchored at 100 %. Stackful fibers carry full call depth — cooperative yield is heavier by design."
                    >
                        <BarGroupInverse
                            label="10 tasks \u{00D7} 100 yields"
                            dtact_pct=100 tokio_pct=23
                            dtact_val="828 \u{00B5}s" tokio_val="188 \u{00B5}s"
                            note="Tokio 4.4\u{00D7} faster — zero-copy stackful fibers trade yield speed for no heap allocation and no pinning."
                        />
                    </BenchChartCard>

                    <div class="glass card-pad bench-summary-card">
                        <span class="section-chip">"Speedup Summary"</span>
                        <h3 class="mt-sm">"dtact vs. Tokio"</h3>
                        <p class="text-sm text-muted mt-sm">
                            "Median times, Dtact\u{00A0}/\u{00A0}Tokio (4-core CI runner)"
                        </p>
                        <div class="bench-summary-grid mt-md">
                            <SpeedupCard label="Spawn 1M" val="6.3\u{00D7}" />
                            <SpeedupCard label="Spawn 100k" val="5.3\u{00D7}" />
                            <SpeedupCard label="Spawn 1k" val="4.8\u{00D7}" />
                            <SpeedupCard label="Deflect 10M" val="4.1\u{00D7}" />
                            <SpeedupCard label="Deflect 1M" val="4.0\u{00D7}" />
                            <SpeedupCard label="Deflect 100k" val="2.8\u{00D7}" />
                        </div>
                    </div>
                </div>
            </section>

            // ── Work Deflection ──────────────────────────────────────────
            <section class="section">
                <BenchChartCard
                    title="Work Deflection (Hot Core)"
                    chip="All tasks spawned from one core — P2P mesh redistributes load"
                    note="Each task: sum(0..100) with black_box. Lower is better. Tokio anchored at 100 %."
                >
                    <BarGroup
                        label="1 000 tasks"
                        dtact_pct=37 tokio_pct=100
                        dtact_val="285 \u{00B5}s" tokio_val="770 \u{00B5}s"
                        speedup="2.7\u{00D7}"
                    />
                    <BarGroup
                        label="10 000 tasks"
                        dtact_pct=43 tokio_pct=100
                        dtact_val="2.53 ms" tokio_val="5.84 ms"
                        speedup="2.3\u{00D7}"
                    />
                    <BarGroup
                        label="100 000 tasks"
                        dtact_pct=35 tokio_pct=100
                        dtact_val="17.7 ms" tokio_val="50.1 ms"
                        speedup="2.8\u{00D7}"
                    />
                    <BarGroup
                        label="1 000 000 tasks"
                        dtact_pct=25 tokio_pct=100
                        dtact_val="172 ms" tokio_val="688 ms"
                        speedup="4.0\u{00D7}"
                    />
                    <BarGroup
                        label="10 000 000 tasks"
                        dtact_pct=24 tokio_pct=100
                        dtact_val="1.68 s" tokio_val="6.94 s"
                        speedup="4.1\u{00D7}"
                    />
                </BenchChartCard>
            </section>

            // ── Methodology ──────────────────────────────────────────────
            <section class="section">
                <div class="glass card-pad-lg">
                    <span class="section-chip">"Methodology"</span>
                    <h2>"Benchmark Configuration"</h2>
                    <div class="grid-2 mt-md">
                        <div>
                            <p class="algo-section-title">"Criterion Settings"</p>
                            <pre class="code-block text-xs">
"criterion_group!(
    name   = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(600))
        .sample_size(200)
        .noise_threshold(0.05);
    targets =
        bench_spawn_efficiency_1m,
        bench_spawn_efficiency_100k,
        bench_spawn_efficiency_10k,
        bench_spawn_efficiency_1k,
        bench_yield_efficiency,
        bench_deflection_efficiency_10m,
        bench_deflection_efficiency_1m,
        bench_deflection_efficiency_100k,
        bench_deflection_efficiency_10k,
        bench_deflection_efficiency_1k
);"
                            </pre>
                        </div>
                        <div>
                            <p class="algo-section-title">"CI Environment"</p>
                            <table class="cfg-table">
                                <thead><tr><th>"Property"</th><th>"Value"</th></tr></thead>
                                <tbody>
                                    <tr><td>"Runner"</td><td>"ubuntu-latest (GitHub hosted)"</td></tr>
                                    <tr><td>"CPU"</td><td>"4-core x86_64 (public runner)"</td></tr>
                                    <tr><td>"Workers"</td><td>"4 (dtact and Tokio)"</td></tr>
                                    <tr><td>"Sampling"</td><td>"200 samples, 600 s measurement, 10 s warm-up"</td></tr>
                                    <tr><td>"Noise threshold"</td><td>"5 % (results below filtered)"</td></tr>
                                    <tr><td>"Report"</td><td>"Criterion HTML + JSON"</td></tr>
                                    <tr><td>"Trigger"</td><td>"Every push to main"</td></tr>
                                </tbody>
                            </table>
                            <p class="text-sm text-muted mt-sm">
                                "Each benchmark group runs for up to 10 minutes. GitHub-hosted
                                 runners share physical hardware — trends and Dtact/Tokio ratios
                                 are the reliable signal; absolute numbers reflect CI conditions."
                            </p>
                        </div>
                    </div>
                </div>
            </section>

            // ── CTA ───────────────────────────────────────────────────────
            <section class="section">
                <div class="bench-cta glass card-pad">
                    <h3>"\u{1F4CA} Full Criterion Report"</h3>
                    <p class="mt-sm">
                        "The live report includes per-benchmark violin plots, regression
                         detection, and historical trend data. Always reflects the latest commit."
                    </p>
                    <a href=BENCH_URL target="_blank" rel="noopener"
                       class="btn btn-primary mt-md">
                        "Open Benchmark Report \u{2197}"
                    </a>
                </div>
            </section>
        </div>
    }
}

// ── Chart container ──────────────────────────────────────────────────────────

#[component]
fn BenchChartCard(
    title: &'static str,
    chip: &'static str,
    note: &'static str,
    children: Children,
) -> impl IntoView {
    view! {
        <div class="glass card-pad bench-chart-card">
            <div class="bench-chart-head">
                <div>
                    <span class="section-chip">{chip}</span>
                    <h3 class="mt-sm">{title}</h3>
                    <p class="text-xs text-muted mt-sm">{note}</p>
                </div>
                <div class="bench-legend">
                    <span class="bench-legend-item bench-legend-dtact">"dtact"</span>
                    <span class="bench-legend-item bench-legend-tokio">"Tokio"</span>
                </div>
            </div>
            <div class="bench-groups mt-lg">
                {children()}
            </div>
        </div>
    }
}

// ── Bar group (dtact faster) ─────────────────────────────────────────────────

#[component]
fn BarGroup(
    label: &'static str,
    dtact_pct: u32,
    tokio_pct: u32,
    dtact_val: &'static str,
    tokio_val: &'static str,
    speedup: &'static str,
) -> impl IntoView {
    view! {
        <div class="bench-bar-group">
            <div class="bench-bar-group-label">{label}</div>
            <div class="bench-bar-row">
                <span class="bench-bar-name">"dtact"</span>
                <div class="bench-bar-track">
                    <div class="bench-bar-fill bench-fill-dtact"
                         style=format!("width:{}%", dtact_pct) />
                </div>
                <span class="bench-bar-val">{dtact_val}</span>
                <span class="bench-speedup-pill">{speedup} " faster"</span>
            </div>
            <div class="bench-bar-row">
                <span class="bench-bar-name">"Tokio"</span>
                <div class="bench-bar-track">
                    <div class="bench-bar-fill bench-fill-tokio"
                         style=format!("width:{}%", tokio_pct) />
                </div>
                <span class="bench-bar-val">{tokio_val}</span>
            </div>
        </div>
    }
}

// ── Bar group (tokio faster — for yield) ────────────────────────────────────

#[component]
fn BarGroupInverse(
    label: &'static str,
    dtact_pct: u32,
    tokio_pct: u32,
    dtact_val: &'static str,
    tokio_val: &'static str,
    note: &'static str,
) -> impl IntoView {
    view! {
        <div class="bench-bar-group">
            <div class="bench-bar-group-label">{label}</div>
            <div class="bench-bar-row">
                <span class="bench-bar-name">"dtact"</span>
                <div class="bench-bar-track">
                    <div class="bench-bar-fill bench-fill-dtact"
                         style=format!("width:{}%", dtact_pct) />
                </div>
                <span class="bench-bar-val">{dtact_val}</span>
            </div>
            <div class="bench-bar-row">
                <span class="bench-bar-name">"Tokio"</span>
                <div class="bench-bar-track">
                    <div class="bench-bar-fill bench-fill-tokio"
                         style=format!("width:{}%", tokio_pct) />
                </div>
                <span class="bench-bar-val">{tokio_val}</span>
            </div>
            <p class="bench-bar-note mt-sm">{note}</p>
        </div>
    }
}

// ── Speedup summary card ─────────────────────────────────────────────────────

#[component]
fn SpeedupCard(label: &'static str, val: &'static str) -> impl IntoView {
    view! {
        <div class="bench-speedup-card">
            <div class="bench-speedup-val">{val}</div>
            <div class="bench-speedup-label">{label}</div>
        </div>
    }
}
