use std::process::Command;

#[test]
fn test_binary_starts_with_help() {
    let binary_path = env!("CARGO_BIN_EXE_janus-bin");
    let output = Command::new(binary_path)
        .arg("--help")
        .output()
        .expect("Failed to execute janus-bin executable");
    assert!(
        output.status.success(),
        "Binary exited with an error status: {:?}",
        output.status
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Usage:"),
        "Stdout did not contain expected text. Got: {}",
        stdout
    );
    assert!(
        stdout.contains("--config"),
        "Stdout did not contain expected text. Got: {}",
        stdout
    );
}
