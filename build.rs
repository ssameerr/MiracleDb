fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/cluster.proto")?;

    // Wasmer 4.x references __rust_probestack which was removed from the Rust LLD
    // linker in toolchains >= 1.84. Provide a minimal stub so the test binary links.
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    provide_rust_probestack();

    Ok(())
}

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
fn provide_rust_probestack() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let asm_path = format!("{out_dir}/probestack.s");
    let obj_path = format!("{out_dir}/probestack.o");

    // Minimal x86-64 stub: satisfies the linker symbol requirement.
    // Wasmer's wasmer_vm_probestack calls this; returning immediately disables
    // runtime stack probing for WASM frames (safe in test/dev builds).
    let asm = "\
.text\n\
.globl __rust_probestack\n\
.hidden __rust_probestack\n\
.type __rust_probestack, @function\n\
__rust_probestack:\n\
    ret\n\
.size __rust_probestack, .-__rust_probestack\n";

    std::fs::write(&asm_path, asm).expect("write probestack.s");

    let ok = std::process::Command::new("as")
        .args(["-o", &obj_path, &asm_path])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    if ok {
        println!("cargo:rustc-link-arg={obj_path}");
    }
}
