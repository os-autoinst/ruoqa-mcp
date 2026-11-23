//! Startup behaviour of the real binary.

#![cfg(unix)]

use std::process::{Command, Stdio};

#[test]
fn a_malformed_home_env_line_does_not_stop_startup() {
    let home = tempfile::tempdir().expect("tempdir");
    std::fs::write(home.path().join(".env"), "=foo\nOPENQA_SERVER=x\n").expect("write .env");

    let output = Command::new(env!("CARGO_BIN_EXE_ruoqa-mcp"))
        .arg("--help")
        .env_clear()
        .env("HOME", home.path())
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .stdin(Stdio::null())
        .output()
        .expect("spawn ruoqa-mcp");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{:?}\n{stderr}", output.status);
    assert!(stdout.contains("Usage"), "{stdout}");
}
