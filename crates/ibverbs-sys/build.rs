use std::env;
use std::path::PathBuf;

fn main() {
    let lib = pkg_config::Config::new()
        .atleast_version("1.0")
        .probe("libibverbs")
        .expect("libibverbs not found. Install rdma-core-devel.");

    let mut builder = bindgen::Builder::default()
        .header("wrapper.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Only generate explicitly allowed items
        .allowlist_function("ibv_.*")
        .allowlist_type("ibv_.*")
        .allowlist_var("IBV_.*")
        .allowlist_var("IB_.*")
        // Blocklist problematic types in newer rdma-core that bindgen can't handle
        .blocklist_type("ib_uverbs_.*")
        .blocklist_type("ibv_flow_action_esp_attr")
        .blocklist_type("ibv_flow_action_esp_mask")
        // Prevent recursive/duplicate types
        .opaque_type("ibv_context_ops")
        .opaque_type("ibv_device_ops")
        // Layout tests
        .layout_tests(false)
        // Generate enums as Rust enums
        .default_enum_style(bindgen::EnumVariation::Rust {
            non_exhaustive: false,
        });

    for path in &lib.include_paths {
        builder = builder.clang_arg(format!("-I{}", path.display()));
    }

    let bindings = builder.generate().expect("Failed to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Failed to write bindings");

    // Compile thin C wrappers for static inline functions
    cc::Build::new()
        .file("src/wrapper_fns.c")
        .includes(&lib.include_paths)
        .compile("ibverbs_wrappers");
}
