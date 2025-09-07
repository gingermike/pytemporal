<img src="docs/logo.svg" alt="PyTemporal" height="200" style="vertical-align: middle;"> 

**High-performance bitemporal data processing for Python**

PyTemporal is a Rust-powered library for processing bitemporal timeseries data with world-class performance (157,000+ rows/second). Perfect for financial services, audit systems, and applications requiring immutable data trails with both business and system time dimensions.

## Quick Start

```bash
# Install from source
git clone <your-repo>
cd pytemporal
uv run maturin develop --release
```

```python
import pandas as pd
from pytemporal import BitemporalTimeseriesProcessor

# Initialize processor
processor = BitemporalTimeseriesProcessor(
    id_columns=['id'],
    value_columns=['price']
)

# Process temporal updates
result = processor.process_updates(
    current_state=current_df,
    updates=updates_df, 
    system_date='2025-01-27'
)

print(f"Updated {len(result.to_insert)} records")
```

## Key Features

- **🚀 World-Class Performance**: 157,000+ rows/second throughput
- **🔄 Bitemporal Processing**: Track both business time and system time
- **🐍 Python-First**: High-level DataFrame API with pandas integration
- **⚡ Zero-Copy**: Apache Arrow columnar format for memory efficiency
- **🔧 Flexible Schema**: Configure ID and value columns dynamically
- **🎯 Two Update Modes**: Delta updates or full state replacement
- **🏗️ Production Ready**: Comprehensive test coverage and clean architecture

## Documentation

- **[📚 API Reference](docs/API_REFERENCE.md)** - Complete API documentation
- **[🎯 Usage Examples](docs/EXAMPLES.md)** - Practical code examples  
- **[⚡ Performance Guide](docs/PERFORMANCE.md)** - Benchmarks and optimization tips

## What is Bitemporal Data?

Bitemporal data tracks two time dimensions:
- **Effective Time**: When events occurred in the real world
- **As-Of Time**: When information was recorded in the system

This enables powerful queries like "What did we think the price was on Jan 15th, as of Jan 20th?"

## Use Cases

- **Financial Services**: Price histories, portfolio valuations, risk calculations
- **Audit Systems**: Immutable change tracking with full reconstruction capability  
- **Regulatory Compliance**: Time-accurate reporting for compliance requirements
- **Data Warehousing**: Slowly changing dimensions with full history preservation

## Performance

| Dataset Size | Processing Time | Throughput | Memory |
|--------------|-----------------|------------|---------|
| 800k × 80 cols | 5.4 seconds | 157k rows/sec | ~14GB |
| 100k × 20 cols | 0.6 seconds | 167k rows/sec | ~2GB |

*Benchmarked on modern hardware with optimized settings*

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Python DataFrame│───▶│ PyTemporal (Rust)│───▶│ Processed Results│
│ (Pandas)        │    │ • Arrow Columnar │    │ (DataFrame)     │  
│                 │    │ • Parallel Proc  │    │                 │
└─────────────────┘    │ • Timeline Logic │    └─────────────────┘
                       └──────────────────┘
```

**Built With:**
- **Rust**: Core processing engine for maximum performance
- **Apache Arrow**: Columnar data format for zero-copy operations  
- **PyO3**: Seamless Rust-Python integration
- **Rayon**: Data parallelism for multi-core performance

## Development

```bash
# Run tests
cargo test                                    # Rust tests
uv run python -m pytest tests/ -v           # Python tests

# Performance benchmarks  
cargo bench                                  # Detailed benchmarks
uv run python validate_refactoring.py       # End-to-end validation

# Build release
uv run maturin develop --release
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and add tests
4. Run the test suite: `cargo test && uv run pytest`
5. Submit a pull request

## License

[Your License Here]

## Acknowledgments

Built with modern Rust performance engineering and extensive profiling to achieve world-class bitemporal processing speeds while maintaining clean, maintainable code.