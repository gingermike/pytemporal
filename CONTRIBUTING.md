# Contributing to PyTemporal

Thank you for your interest in contributing to PyTemporal! This document provides guidelines and instructions for contributing to this high-performance bitemporal data processing library.

## 🚀 Quick Start

### Prerequisites

- **Rust** (latest stable) - [Install from rustup.rs](https://rustup.rs/)
- **Python 3.9+** - Required for Python bindings
- **uv** - Python package manager - [Install from astral.sh](https://astral.sh/uv/)

### Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/gingermike/pytemporal.git
   cd pytemporal
   ```

2. **Install development dependencies**
   ```bash
   # Install uv if you haven't already
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # Install Python dependencies
   uv sync --group dev
   ```

3. **Build the project**
   ```bash
   # Build Rust library
   cargo build
   
   # Build Python bindings (required before running Python tests)
   uv run maturin develop
   ```

4. **Run tests**
   ```bash
   # Run Rust tests
   cargo test
   
   # Run Python tests
   uv run python -m pytest tests/ -v
   
   # Run benchmarks (optional)
   cargo bench
   ```

## 🧪 Testing

### Test Requirements
- ✅ All existing tests must pass
- ✅ New features must include tests
- ✅ Test coverage should be maintained or improved

### Running Tests
```bash
# Quick test (Rust only)
cargo test

# Full test suite (Rust + Python)
cargo test && uv run python -m pytest tests/ -v

# Run specific test
cargo test test_name
uv run python -m pytest tests/test_specific.py -v
```

### Performance Testing
```bash
# Run benchmarks
cargo bench

# Generate flamegraphs (requires specific benchmarks)
cargo bench --bench bitemporal_benchmarks medium_dataset -- --profile-time 5
```

## 📝 Code Style

### Rust Code
- Follow standard Rust formatting: `cargo fmt`
- Pass clippy lints: `cargo clippy`
- Use meaningful variable names
- Add inline documentation for public APIs
- **Important**: Do not add code comments unless explicitly requested

### Python Code
- Use type hints where possible
- Follow existing test patterns
- Maintain compatibility with Python 3.9+

### Commit Messages
Use clear, descriptive commit messages:
```
Add support for custom hash algorithms

- Implement configurable hash algorithms (XxHash, SHA256)
- Add performance benchmarks for different hash types
- Update documentation with usage examples
```

## 🔄 Development Workflow

### Making Changes

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Edit code in `src/lib.rs` for core functionality
   - Add tests in `tests/` directory
   - Update documentation if needed

3. **Test your changes**
   ```bash
   # CRITICAL: Rebuild Python bindings after Rust changes
   uv run maturin develop
   
   # Run full test suite
   cargo test && uv run python -m pytest tests/ -v
   ```

4. **Format and lint**
   ```bash
   cargo fmt
   cargo clippy
   ```

### Pull Request Process

1. **Push your branch**
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Create a pull request**
   - Use a clear, descriptive title
   - Explain what your changes do and why
   - Reference any related issues
   - Include test results if relevant

3. **Address review feedback**
   - Respond to comments constructively
   - Make requested changes
   - Update tests if needed

## 🏗️ Architecture Overview

### Core Components
- **`src/lib.rs`** - Main bitemporal processing algorithm (870 lines)
- **`tests/`** - Comprehensive test suite (Rust + Python)
- **`benches/`** - Performance benchmarks with flamegraph support
- **`python/`** - Python wrapper and utilities

### Key Concepts
- **Bitemporal Data**: Records with both effective time and as-of time
- **Conflation**: Merging adjacent segments with identical values
- **Parallel Processing**: Adaptive parallelization based on dataset size
- **Zero-Copy Operations**: Apache Arrow columnar format for efficiency

### Performance Considerations
- Changes to `src/lib.rs` may impact performance benchmarks
- Always run `cargo bench` for performance-critical changes
- Memory usage should be monitored for large datasets
- Maintain world-class performance (157k+ rows/sec)

## 🐛 Reporting Issues

### Bug Reports
When reporting bugs, please include:
- PyTemporal version (`pip show pytemporal`)
- Python version (`python --version`)
- Operating system
- Minimal code example that reproduces the issue
- Expected vs actual behavior
- Full error traceback

### Feature Requests
- Explain the use case and why it's needed
- Consider if it fits PyTemporal's bitemporal focus
- Suggest API design if you have ideas
- Consider performance implications

## 📚 Documentation

### API Documentation
- Update `docs/API_REFERENCE.md` for new public APIs
- Add examples to `docs/EXAMPLES.md` for new features
- Update `docs/PERFORMANCE.md` for performance changes

### Code Documentation
- Use Rust doc comments (`///`) for public APIs
- Focus on explaining *why* rather than *what*
- Include usage examples for complex functions

## 🎯 Areas for Contribution

### High Priority
- **Performance optimizations** - Always welcome!
- **Bug fixes** - Especially edge cases in temporal logic
- **Documentation improvements** - Examples, guides, tutorials
- **Platform support** - Windows, macOS compatibility

### Medium Priority
- **Additional hash algorithms** - Beyond XxHash and SHA256
- **Memory optimizations** - Reduce memory footprint
- **Enhanced error handling** - Better error messages
- **Benchmark improvements** - More comprehensive testing

### Ideas Welcome
- **New bitemporal operations** - Advanced temporal queries
- **Integration examples** - Pandas, Polars, DuckDB
- **Performance analysis tools** - Memory profiling, bottleneck detection

## ⚡ Performance Guidelines

PyTemporal is a performance-critical library. When contributing:

1. **Benchmark your changes**
   ```bash
   # Before changes
   cargo bench > before.txt
   
   # After changes  
   cargo bench > after.txt
   
   # Compare results
   ```

2. **Maintain performance standards**
   - Target: 157k+ rows/sec processing speed
   - Memory usage should scale reasonably with dataset size
   - Avoid performance regressions

3. **Use profiling tools**
   ```bash
   # Generate flamegraphs for analysis
   cargo bench --bench bitemporal_benchmarks medium_dataset -- --profile-time 5
   ```

## 🤝 Community

- **Be respectful** - Follow our code of conduct
- **Ask questions** - Use GitHub Discussions for help
- **Share knowledge** - Help others learn bitemporal concepts
- **Give feedback** - Your experience helps improve PyTemporal

## 📋 Checklist Before Submitting

- [ ] Tests pass: `cargo test && uv run python -m pytest tests/ -v`
- [ ] Code is formatted: `cargo fmt`
- [ ] No clippy warnings: `cargo clippy`
- [ ] Python bindings rebuilt: `uv run maturin develop`
- [ ] Documentation updated (if applicable)
- [ ] Performance impact assessed (if applicable)
- [ ] Commit messages are clear and descriptive

## 🚨 Important Notes

1. **Always rebuild Python bindings** after Rust changes with `uv run maturin develop`
2. **Run full test suite** before submitting (both Rust and Python tests)
3. **Performance matters** - PyTemporal targets world-class speed
4. **Compatibility** - Maintain support for Python 3.9+

## 📄 License

By contributing to PyTemporal, you agree that your contributions will be licensed under the same dual MIT/Apache 2.0 license that covers the project. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE) for details.

---

**Thank you for contributing to PyTemporal! 🚀**

Every contribution, whether it's code, documentation, bug reports, or feedback, helps make PyTemporal better for the entire community.