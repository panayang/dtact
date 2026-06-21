use gloo_timers::callback::Interval;
use leptos::prelude::*;
use wasm_bindgen::JsCast;

#[component]
pub fn SponsorPage() -> impl IntoView {
    // ── Real-time clock state (RwSignal avoids WriteSignal call-operator issues) ──
    let hours = RwSignal::new(0u32);
    let minutes = RwSignal::new(0u32);
    let seconds = RwSignal::new(0u32);

    let update = move || {
        if let Some(arr) = js_sys::eval("window.getLocalTime()")
            .ok()
            .and_then(|v| v.dyn_into::<js_sys::Array>().ok())
        {
            hours.set(arr.get(0).as_f64().unwrap_or(0.0) as u32);
            minutes.set(arr.get(1).as_f64().unwrap_or(0.0) as u32);
            seconds.set(arr.get(2).as_f64().unwrap_or(0.0) as u32);
        }
    };
    update();
    // forget() hands ownership to the JS runtime (clearInterval never called from Rust).
    // Acceptable: the setInterval is cancelled automatically when the browser tab closes.
    Interval::new(1_000, update).forget();

    // ── Derived hand angles ───────────────────────────────────────────────
    let hour_deg = move || {
        let h = (hours.get() % 12) as f64;
        let m = minutes.get() as f64;
        let s = seconds.get() as f64;
        h * 30.0 + m * 0.5 + s * (0.5 / 60.0)
    };
    let min_deg = move || {
        let m = minutes.get() as f64;
        let s = seconds.get() as f64;
        m * 6.0 + s * 0.1
    };
    let sec_deg = move || seconds.get() as f64 * 6.0;
    let time_str = move || {
        format!(
            "{:02}:{:02}:{:02}",
            hours.get(),
            minutes.get(),
            seconds.get()
        )
    };
    let ampm_str = move || if hours.get() < 12 { "AM" } else { "PM" };

    view! {
        <div class="sponsor-page pt-nav">

            // ── Hero: clock + tagline ─────────────────────────────────────
            <div class="sponsor-hero page-wrap">

                // Clock
                <div class="sponsor-clock-wrap">
                    <div class="clock-glow-outer"></div>
                    <div class="clock-glow-mid"></div>
                    <div class="clock-glow-inner"></div>

                    <svg class="clock-svg" viewBox="0 0 260 260"
                         xmlns="http://www.w3.org/2000/svg">
                        // Bezel
                        <circle cx="130" cy="130" r="128" class="clock-bezel"/>
                        // Outer track
                        <circle cx="130" cy="130" r="122" class="clock-outer-track"/>
                        // Face — plain fill; visual depth comes from CSS shadows + glow layers
                        <circle cx="130" cy="130" r="120" class="clock-face"/>
                        // Inner ring accent
                        <circle cx="130" cy="130" r="112" class="clock-inner-ring"/>
                        // Very subtle secondary inner ring
                        <circle cx="130" cy="130" r="96"  class="clock-inner-ring-2"/>

                        // Minute ticks (all 60)
                        {(0..60u32).map(|i| {
                            let is_major = i % 5 == 0;
                            let angle = i as f64 * 6.0 - 90.0;
                            let rad   = angle.to_radians();
                            let r_out = 118.0_f64;
                            let r_in  = if is_major { 106.0_f64 } else { 112.0_f64 };
                            let x1 = 130.0 + r_out * rad.cos();
                            let y1 = 130.0 + r_out * rad.sin();
                            let x2 = 130.0 + r_in  * rad.cos();
                            let y2 = 130.0 + r_in  * rad.sin();
                            view! {
                                <line
                                    x1=format!("{:.2}", x1) y1=format!("{:.2}", y1)
                                    x2=format!("{:.2}", x2) y2=format!("{:.2}", y2)
                                    class=if is_major { "clock-tick-major" } else { "clock-tick-minor" }
                                />
                            }
                        }).collect_view()}

                        // Hour numerals
                        {[12u32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
                            .iter().enumerate().map(|(i, &n)| {
                            let angle = i as f64 * 30.0 - 90.0;
                            let rad   = angle.to_radians();
                            let x = 130.0 + 86.0 * rad.cos();
                            let y = 130.0 + 86.0 * rad.sin() + 4.0;
                            view! {
                                <text
                                    x=format!("{:.2}", x) y=format!("{:.2}", y)
                                    class="clock-numeral" text-anchor="middle"
                                >{n.to_string()}</text>
                            }
                        }).collect_view()}

                        // Hour hand (short, thick)
                        <line
                            x1="130" y1="142"
                            x2="130" y2="76"
                            class="clock-hand-hour"
                            transform=move || format!("rotate({:.4} 130 130)", hour_deg())
                        />
                        // Minute hand (longer, medium)
                        <line
                            x1="130" y1="148"
                            x2="130" y2="58"
                            class="clock-hand-minute"
                            transform=move || format!("rotate({:.4} 130 130)", min_deg())
                        />
                        // Second hand (thin, with counterbalance tail)
                        <g transform=move || format!("rotate({:.4} 130 130)", sec_deg())>
                            <line x1="130" y1="154" x2="130" y2="48" class="clock-hand-second"/>
                            <circle cx="130" cy="148" r="3.5" class="clock-hand-tail"/>
                        </g>

                        // Center jewel layers
                        <circle cx="130" cy="130" r="8"   class="clock-jewel-halo"/>
                        <circle cx="130" cy="130" r="5.5" class="clock-jewel-outer"/>
                        <circle cx="130" cy="130" r="2.5" class="clock-jewel-inner"/>
                    </svg>

                    // Digital readout
                    <div class="clock-digital">
                        <span class="clock-time-str">{time_str}</span>
                        <span class="clock-ampm">{ampm_str}</span>
                    </div>
                </div>

                // Tagline
                <div class="sponsor-tagline">
                    <p class="hero-eyebrow">"Investment in the Foundation"</p>
                    <h1 class="sponsor-main-title">"Sponsor dtact"</h1>
                    <p class="sponsor-sub">
                        "Rigorous infrastructure is not sustained by intention alone.
                         The scheduler, arena, and context switchers that power real-time
                         systems demand continuous investment \u{2014} in hardware, in testing,
                         in the quiet hours no milestone ever credits."
                    </p>
                    <p class="sponsor-sub-2">
                        "Your support makes the work possible."
                    </p>
                </div>
            </div>

            // ── Section 1: Fund Allocation ────────────────────────────────
            <section class="section page-wrap">
                <div class="sponsor-section-card glass card-pad-lg">
                    <span class="section-chip">"How Funds Are Used"</span>
                    <h2 class="mt-sm">"Allocation of Sponsored Resources"</h2>
                    <p class="mt-sm">
                        "All sponsored funds are strictly allocated to the following
                         technical and operational requirements:"
                    </p>
                    <div class="grid-2 mt-md">
                        <AllocationCard
                            icon="\u{1F5A5}"
                            title="Multi-Topology CPU Testing"
                            body="Provisioning and licensing of diverse hardware architectures \u{2014}
                                  x86_64, AArch64, RISC-V, and specialised multi-socket NUMA systems \u{2014}
                                  to verify runtime stability, cache coherency, and thread scheduling
                                  across complex CPU topologies."
                        />
                        <AllocationCard
                            icon="\u{26A1}"
                            title="High-Performance Server Infrastructure"
                            body="Acquisition, hosting, and maintenance of bare-metal servers and
                                  high-throughput cluster nodes dedicated to heavy-load stress testing,
                                  latency profiling, and compiler-to-runtime optimisation."
                        />
                        <AllocationCard
                            icon="\u{2601}"
                            title="Virtualised & Cloud Testing"
                            body="Maintenance of distributed testing environments simulating edge,
                                  cloud, and hybrid deployment scenarios for cross-platform validation."
                        />
                        <AllocationCard
                            icon="\u{1F4DA}"
                            title="Operations & Documentation"
                            body="Hosting and operational maintenance of the official project website,
                                  technical documentation, public registries, and secure distribution
                                  channels."
                        />
                    </div>
                </div>
            </section>

            // ── Section 2: Individual Sponsorship ────────────────────────
            <section class="section page-wrap">
                <div class="glass card-pad-lg">
                    <span class="section-chip">"Individual Sponsorship"</span>
                    <h2 class="mt-sm">"Personal Contributions"</h2>
                    <p class="mt-sm">
                        "For individual supporters, we accept voluntary contributions
                         via "
                        <strong>"Open Collective only"</strong>
                        "."
                    </p>

                    <div class="paypal-card glass-hi mt-md">
                        <div class="paypal-row">
                            <span class="paypal-label">"Open Collective Link"</span>
                            <span class="paypal-id mono">"https://opencollective.com/dtact"</span>
                        </div>
                    </div>

                    <div class="sponsor-notice mt-md">
                        <p class="sponsor-notice-title">
                            "\u{26A0}\u{FE0F} Required Transaction Note"
                        </p>
                        <p class="mt-sm text-sm">
                            "You "
                            <strong>"MUST"</strong>
                            " include a note in the transaction containing all of the following:"
                        </p>
                        <ol class="sponsor-notice-list mt-sm">
                            <li>
                                <em>
                                    "\u{201C}This is a voluntary donation/gift with no expectation
                                     of commercial return.\u{201D}"
                                </em>
                            </li>
                            <li>
                                <em>
                                    "\u{201C}I certify that these funds are derived from legal
                                     sources.\u{201D}"
                                </em>
                            </li>
                            <li class="text-muted">
                                "(Optional) Any name, handle, or organisation you wish
                                 to be credited on the official Sponsors page."
                            </li>
                        </ol>
                    </div>
                </div>
            </section>

            // ── Section 3: Corporate Sponsorship ─────────────────────────
            <section class="section page-wrap">
                <div class="mb-lg">
                    <span class="section-chip">"Corporate Sponsorship"</span>
                    <h2 class="mt-sm">"Structured Organisational Tiers"</h2>
                    <p class="mt-sm">
                        "Corporate sponsors must provide "
                        <strong>"proof of legal entity or incorporation"</strong>
                        " before any funds are accepted."
                    </p>
                </div>

                <div class="corp-tier-grid">
                    <CorpTierCard
                        tier="Tier 1"
                        title="Strategic Support"
                        price="$1,000+ USD / month"
                        color="var(--c-primary)"
                        perks=vec![
                            "Officially recognised Lead Corporate Sponsor of dtact",
                            "Priority technical communication channels",
                            "Direct consulting for runtime integration and optimisation",
                            "Must sign a formal declaration certifying legal origin of funds and full compliance with dual-use technology regulations",
                        ]
                    />
                    <CorpTierCard
                        tier="Tier 2"
                        title="Core Engineering Support"
                        price="$10,000+ USD / month"
                        color="var(--c-accent)"
                        perks=vec![
                            "All Tier 1 benefits",
                            "Ability to request highly specialised runtime plugins or tailored scheduler modifications targeting custom hardware architectures or proprietary virtualisation environments",
                            "Up to 12 months of \u{201C}grey-box\u{201D} staging and testing for custom runtime modules before evaluation for potential inclusion in the open-source main branch",
                        ]
                    />
                </div>

                <div class="sponsor-contact-row mt-lg">
                    <a
                        href="mailto:xinyu.yang@apich.org?subject=dtact%20Corporate%20Sponsorship%20Enquiry"
                        class="btn btn-primary btn-sponsor"
                    >
                        "Corporate Sponsorship Enquiry"
                    </a>
                    <a href="https://opencollective.com/dtact" class="btn btn-ghost btn-sponsor">
                        "Individual: Open Collective https://opencollective.com/dtact"
                    </a>
                </div>
            </section>

            // ── Section 4: Ethics & AML ───────────────────────────────────
            <section class="section page-wrap">
                <div class="aml-card glass card-pad-lg">
                    <span class="section-chip section-chip-warn">
                        "\u{1F6AB} Ethical Statement & AML Policy"
                    </span>
                    <h2 class="mt-sm">"Anti-Money Laundering & Responsible Funding"</h2>
                    <p class="mt-sm">
                        "The author and core maintainers of dtact maintain a
                         zero-tolerance policy toward illicit financial and software activities."
                    </p>

                    <div class="aml-rules mt-md">
                        <AmlRule
                            icon="\u{20BF}"
                            title="Strict Prohibition of Cryptocurrency"
                            body="We REFUSE all forms of cryptocurrency payments \u{2014} including
                                  Bitcoin, Monero, and all other digital assets \u{2014} to ensure
                                  financial transparency, comply with local accounting frameworks,
                                  and prevent money laundering."
                            is_danger=true
                        />
                        <AmlRule
                            icon="\u{1F6E1}"
                            title="Anti-Malware and Safe Use Policy"
                            body="We resolutely oppose the use of the dtact framework for illegal
                                  operations, including the creation of malware, unauthorised system
                                  intrusion, or evasion of lawful monitoring systems."
                            is_danger=false
                        />
                        <AmlRule
                            icon="\u{2696}"
                            title="Law Enforcement Cooperation"
                            body="In the event that any law enforcement or regulatory agency identifies
                                  a contribution as originating from illegal proceeds, we reserve the
                                  right to immediately terminate the sponsor association and will fully
                                  cooperate by turning over all relevant transaction details and funds
                                  to the appropriate authorities."
                            is_danger=false
                        />
                    </div>

                    <blockquote class="mt-md">
                        "By sponsoring dtact, you acknowledge that this is a research-oriented
                         and utility-focused contribution, and that you adhere strictly to all
                         local and international laws regarding the funding of dual-use technologies
                         and open-source infrastructure."
                    </blockquote>
                </div>
            </section>

        </div>
    }
}

// ── Sub-components ───────────────────────────────────────────────────────────

#[component]
fn AllocationCard(icon: &'static str, title: &'static str, body: &'static str) -> impl IntoView {
    view! {
        <div class="glass card-pad allocation-card">
            <div class="allocation-icon">{icon}</div>
            <h4 class="mb-sm">{title}</h4>
            <p class="text-sm">{body}</p>
        </div>
    }
}

#[component]
fn CorpTierCard(
    tier: &'static str,
    title: &'static str,
    price: &'static str,
    color: &'static str,
    perks: Vec<&'static str>,
) -> impl IntoView {
    view! {
        <div class="corp-tier glass card-pad">
            <div class="corp-tier-badge" style=format!("background:{color}")>{tier}</div>
            <h3 class="corp-tier-title mt-sm">{title}</h3>
            <p class="corp-tier-price mono mt-xs">{price}</p>
            <ul class="sponsor-perks mt-md">
                {perks.into_iter().map(|p| view! {
                    <li class="sponsor-perk">
                        <span class="sponsor-perk-dot"
                            style=format!("background:{color}")></span>
                        <span class="text-sm">{p}</span>
                    </li>
                }).collect_view()}
            </ul>
        </div>
    }
}

#[component]
fn AmlRule(
    icon: &'static str,
    title: &'static str,
    body: &'static str,
    is_danger: bool,
) -> impl IntoView {
    let border_color = if is_danger {
        "var(--c-danger)"
    } else {
        "var(--c-border-hi)"
    };
    view! {
        <div class="aml-rule" style=format!("border-left-color:{border_color}")>
            <span class="aml-rule-icon">{icon}</span>
            <div>
                <p class="aml-rule-title">{title}</p>
                <p class="aml-rule-body text-sm mt-sm">{body}</p>
            </div>
        </div>
    }
}
