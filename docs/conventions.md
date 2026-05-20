# Janus Development Guidelines

This document details the formatting and linting rules enforced across all crates in the Janus workspace.

## 🛠️ Formatting & Linting

### 1. Code Formatting
All files must match the style guidelines defined in `.rustfmt.toml`. 

* **To check formatting** (without modifying files):
  ```bash
  cargo fmt --check
  ```
* **To automatically apply formatting**:
  ```bash
  cargo fmt
  ```

### 2. Linting (Clippy)
We use `clippy` to verify code quality. Crate roots deny common warnings (`clippy::all`) and warn on pedantic recommendations (`clippy::pedantic`).

* **To run the linter** (analyzing all targets and features):
  ```bash
  cargo clippy --workspace --all-targets --all-features
  ```
