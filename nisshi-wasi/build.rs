fn main() {
    println!("cargo:rerun-if-changed=lz4_wasi.c");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("wasi") {
        // lz4-sys 1.11.1 uses shim headers on WASI but excludes the Rust shim definitions.
        cc::Build::new()
            .file("lz4_wasi.c")
            .compile("nisshi_lz4_wasi");
    }
}
