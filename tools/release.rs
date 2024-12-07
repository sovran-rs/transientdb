use toml_edit::Item;
use std::process::Command;
use std::fs;
use toml_edit::DocumentMut;

fn get_commit_history(previous_tag: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("git")
        .args(&["log", "--pretty=format:- %s", &format!("{}..HEAD", previous_tag)])
        .output()?;

    Ok(String::from_utf8(output.stdout)?)
}

fn get_latest_tag() -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("git")
        .args(&["describe", "--tags", "--abbrev=0"])
        .output()?;

    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Read current Cargo.toml
    let cargo_content = fs::read_to_string("Cargo.toml")?;
    let mut doc = cargo_content.parse::<DocumentMut>()?;

    // Get current version
    let current_version = doc["package"]["version"]
        .as_str()
        .expect("Could not find version in Cargo.toml");

    // Ask for new version
    println!("Current version is: {}", current_version);
    println!("Enter new version:");
    let mut new_version = String::new();
    std::io::stdin().read_line(&mut new_version)?;
    let new_version = new_version.trim();

    // Update Cargo.toml
    doc["package"]["version"] = Item::from(new_version);
    fs::write("Cargo.toml", doc.to_string())?;

    // Get the latest tag for commit history
    let previous_tag = get_latest_tag()?;
    let commit_history = get_commit_history(&previous_tag)?;

    // Git commands for version bump
    let initial_commands = [
        ("git add Cargo.toml", "Failed to stage Cargo.toml"),
        (&format!("git commit -m \"Bump version to {}\"", new_version), "Failed to commit version bump"),
        (&format!("git tag -a v{} -m \"Version {}\"", new_version, new_version), "Failed to create tag"),
        ("git push", "Failed to push commits"),
        ("git push --tags", "Failed to push tags"),
    ];

    // Execute initial commands
    for (cmd, error_msg) in initial_commands.iter() {
        let status = Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .status()?;

        if !status.success() {
            return Err(error_msg.to_string().into());
        }
    }

    // Build in release mode to update Cargo.lock
    let build_status = Command::new("cargo")
        .args(&["build", "--release"])
        .status()?;

    if !build_status.success() {
        return Err("Failed to build release version".into());
    }

    // Commit Cargo.lock changes
    let lock_commands = [
        ("git add Cargo.lock", "Failed to stage Cargo.lock"),
        ("git commit -m \"Update Cargo.lock for release\"", "Failed to commit Cargo.lock"),
        ("git push", "Failed to push Cargo.lock changes"),
    ];

    // Execute lock file commands
    for (cmd, error_msg) in lock_commands.iter() {
        let status = Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .status()?;

        if !status.success() {
            return Err(error_msg.to_string().into());
        }
    }

    // Publish to crates.io
    let publish_status = Command::new("cargo")
        .arg("publish")
        .status()?;

    if !publish_status.success() {
        return Err("Failed to publish to crates.io".into());
    }

    // Create GitHub release
    let create_release = Command::new("gh")
        .args(&[
            "release",
            "create",
            &format!("v{}", new_version),
            "--title", &format!("v{}", new_version),
            "--notes", &commit_history,
        ])
        .status()?;

    if !create_release.success() {
        return Err("Failed to create GitHub release".into());
    }

    println!("Successfully released version {}", new_version);
    Ok(())
}