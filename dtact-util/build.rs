//! Build script for `dtact-util`.
//!
//! When the environment variable `DEV=1` is set, generates C and C++ header
//! files (`dtact_util.h` / `dtact_util.hpp`) for the `ffi` feature's
//! `extern "C"` surface via cbindgen. Ordinary builds skip this entirely so
//! the crate builds without needing cbindgen to run.

use std::env;

fn main() {
    println!("cargo:rustc-cfg=tokio_unstable");
    println!("cargo:rustc-check-cfg=cfg(tokio_unstable)");

    const ENV_VAR_NAME: &str = "DEV";
    match env::var(ENV_VAR_NAME) {
        Ok(value) if value == "1" => {
            println!("cargo:warning=DEV=1: generating dtact-util C/C++ headers.");
            if let Err(e) = generate_headers() {
                println!("cargo:warning=dtact-util header generation error: {e:?}");
            }
        }
        _ => {
            println!(
                "cargo:warning=Skipping dtact-util header generation. Set {ENV_VAR_NAME}='1' to enable."
            );
        }
    }
}

fn generate_headers() -> Result<(), Box<dyn std::error::Error>> {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR")?;

    // C header from cbindgen.toml.
    match cbindgen::generate(&crate_dir) {
        Ok(bindings) => {
            bindings.write_to_file("dtact_util.h");
            println!("cargo:warning=Generated dtact_util.h");
        }
        Err(e) => {
            println!("cargo:warning=Failed to generate C bindings: {e:?}");
        }
    }

    // C++ header: same config, C++ language.
    let cpp_config = cbindgen::Config {
        language: cbindgen::Language::Cxx,
        ..cbindgen::Config::from_file("cbindgen-cxx.toml").unwrap_or_default()
    };
    match cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(cpp_config)
        .generate()
    {
        Ok(bindings) => {
            bindings.write_to_file("dtact_util.hpp");
            println!("cargo:warning=Generated dtact_util.hpp");
        }
        Err(e) => {
            println!("cargo:warning=Failed to generate C++ bindings: {e:?}");
        }
    }

    Ok(())
}
