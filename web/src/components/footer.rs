use leptos::prelude::*;

#[component]
pub fn Footer() -> impl IntoView {
    view! {
        <footer class="footer page-wrap">
            <span>
                "\u{00A9} 2026 Xinyu Yang \u{00B7} "
                <a href="https://opensource.org/licenses/MIT" target="_blank" rel="noopener">"MIT"</a>
                " / "
                <a href="https://www.apache.org/licenses/LICENSE-2.0" target="_blank" rel="noopener">"Apache-2.0"</a>
            </span>
            <div class="footer-links">
                <a href="https://github.com/Apich-Organization/dtact" target="_blank" rel="noopener">
                    "Source"
                </a>
                <a href="https://docs.rs/dtact" target="_blank" rel="noopener">
                    "docs.rs"
                </a>
                <a href="https://crates.io/crates/dtact" target="_blank" rel="noopener">
                    "crates.io"
                </a>
                <a href="https://github.com/Apich-Organization/dtact/issues" target="_blank" rel="noopener">
                    "Issues"
                </a>
                <a href="https://github.com/Apich-Organization/dtact/blob/main/SECURITY.md"
                   target="_blank" rel="noopener">
                    "Security"
                </a>
            </div>
        </footer>
    }
}
