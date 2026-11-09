use std::process::Command;

fn main() {
    let output = Command::new(std::env::var_os("RUSTC").expect("Cargo sets RUSTC"))
        .arg("--version")
        .output()
        .expect("read compiler version");
    assert!(output.status.success());
    let version = String::from_utf8(output.stdout).expect("UTF-8 compiler version");
    println!("cargo:rustc-env=OAA_BENCH_COMPILER={}", version.trim());
    println!("cargo:rerun-if-changed=build.rs");
}
