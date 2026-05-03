# Contributing to Dtact

First off, thank you for considering contributing to Dtact! We welcome contributions from everyone.

## Code of Conduct

By participating in this project, you agree to abide by the [Code of Conduct](./CODE_OF_CONDUCT.md). We expect all contributors to foster a respectful and collaborative environment.

## How Can I Contribute?

### Reporting Bugs

If you find a bug, please create an issue on our repository. Include as much detail as possible:
* A descriptive title.
* Steps to reproduce the issue.
* Expected behavior vs. actual behavior.
* Environment details (OS, architecture, Rust version).

### Suggesting Enhancements

We are always looking for ways to improve Dtact's design and features. If you have an idea, feel free to open a feature request issue. We are particularly interested in discussions around:
* Enhancing the P2P Mesh scheduling algorithms.
* Improving assembly-level context switchers for new architectures.
* Expanding the C FFI capabilities.

### Pull Requests

1. **Fork the repository** and create your branch from `main`.
2. **Ensure your code formats cleanly**: Run `cargo fmt`.
3. **Run the test suite**: Make sure all tests pass with `cargo test`.
4. **Run benchmarks (optional but recommended for core changes)**: Check for performance regressions using `cargo bench`. 
5. **Document your changes**: Update the `README.md` or code comments if you are changing user-facing APIs or core runtime design.
6. **Submit the PR**: Provide a clear description of the problem you are solving and how your changes address it.

## Development Setup

To get started with local development:

```bash
git clone https://github.com/Apich-Organization/dtact.git
cd dtact
cargo build
cargo test
```

### Building the C FFI Example

```bash
cd examples
make
./c_async
```

## Review Process

We aim to review pull requests promptly. We appreciate your patience. During the review, we might ask for structural changes or additional documentation. Our goal is to ensure that Dtact remains stable and maintains its unique architectural focus.

Thank you for your interest in improving Dtact!
