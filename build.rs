use std::env;
use std::error::Error;
use std::path::Path;
use std::process::Command;

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR")?;
    let ui_dir = Path::new(&manifest_dir).join("ui");

    if !ui_dir.exists() {
        return Ok(());
    }

    println!("cargo:rerun-if-changed=ui/src");
    println!("cargo:rerun-if-changed=ui/package.json");
    println!("cargo:rerun-if-changed=ui/package-lock.json");
    println!("cargo:rerun-if-changed=ui/vite.config.js");

    let node_modules = ui_dir.join("node_modules");
    if !node_modules.exists() {
        let status = Command::new("npm")
            .arg("install")
            .current_dir(&ui_dir)
            .status()?;

        if !status.success() {
            return Err("npm install failed".into());
        }
    }

    let status = Command::new("npm")
        .args(["run", "build"])
        .current_dir(&ui_dir)
        .status()?;

    if !status.success() {
        return Err("npm run build failed".into());
    }
    Ok(())
}
