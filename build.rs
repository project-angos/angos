use std::{env, path::Path, process::Command};

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let ui_dir = Path::new(&manifest_dir).join("ui");

    if !ui_dir.exists() {
        return;
    }

    println!("cargo:rerun-if-changed=ui/src");
    println!("cargo:rerun-if-changed=ui/package.json");
    println!("cargo:rerun-if-changed=ui/svelte.config.js");
    println!("cargo:rerun-if-changed=ui/vite.config.js");

    let node_modules = ui_dir.join("node_modules");
    if !node_modules.exists() {
        let status = Command::new("npm")
            .arg("install")
            .current_dir(&ui_dir)
            .status()
            .expect("failed to run npm install");

        if !status.success() {
            panic!("npm install failed");
        }
    }

    let status = Command::new("npm")
        .args(["run", "build"])
        .current_dir(&ui_dir)
        .status()
        .expect("failed to run npm run build");

    if !status.success() {
        panic!("npm run build failed");
    }
}
