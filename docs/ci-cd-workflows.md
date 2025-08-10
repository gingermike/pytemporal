# CI/CD Workflows Overview

This project uses two complementary workflows that both run on version tags to create complete releases:

## 🏗️ Production Releases: `build-wheels.yml` (Gitea Actions)

**Location**: `.gitea/workflows/build-wheels.yml`

**Purpose**: Build and publish production Python wheels to package registry

### Triggers
- ✅ **Version tags only** (`v1.0.0`, `v2.1.3`, etc.)
- ✅ **Manual workflow dispatch**
- ❌ **No automatic runs on code changes**

### Jobs
1. **Test Job**:
   - Run Rust tests (`cargo test`)
   - Run Python tests with pytest
   - Build release binary (smoke test)
   - **No benchmarks** (removed for faster CI)

2. **Build Linux Wheels**:
   - Build optimized wheels for x86_64 architecture
   - Upload as artifacts

3. **Publish to Gitea**:
   - Publish wheels to Gitea package registry
   - Create GitHub/Gitea release with release notes
   - **Only runs on version tags**

### Key Features
- **Fast execution**: Focused on testing and building only
- **Production-ready**: Release builds with optimizations
- **Artifact storage**: Wheels are preserved and published
- **Automatic versioning**: Extracts version from git tags

---

## 📊 Release Benchmarks: `benchmarks.yml` (GitHub Actions)

**Location**: `.github/workflows/benchmarks.yml`

**Purpose**: Generate performance reports with flamegraphs for each release and publish to GitHub Pages

### Triggers
- ✅ **Version tags only** (`v1.0.0`, `v2.1.3`, etc.) - Same as build-wheels.yml
- ✅ **Manual workflow dispatch** for testing

### Benefits of Release-Based Benchmarks
- **Historical Performance Data**: Track performance across versions
- **Release Documentation**: Each release has associated performance metrics
- **No CI Overhead**: Expensive benchmarking only runs on releases
- **GitHub Pages History**: Performance data tied to specific releases

### Jobs
1. **Benchmark Job**:
   - Run core benchmarks with flamegraph generation (3-second profiling)
   - Run additional benchmarks without flamegraphs (for speed)
   - Post-process HTML reports to add flamegraph links
   - Create GitHub Pages landing page

2. **Deploy Job**:
   - Upload artifacts to GitHub Pages
   - Deploy to public URL

### Generated Flamegraphs
- `medium_dataset` - 100 records with 20 updates
- `conflation_effectiveness` - Adjacent segment merging
- `scaling_by_dataset_size/records/500000` - Large dataset (with timeout protection)

### Key Features
- **Performance focus**: Detailed flamegraph analysis
- **Public visibility**: GitHub Pages deployment
- **Automatic updates**: Runs on relevant code changes
- **Timeout protection**: Large benchmarks won't hang CI

---

## 🎯 Dual-Workflow Release Strategy

| Aspect | Build Wheels (Gitea) | Release Benchmarks (GitHub) |
|--------|---------------------|------------------------------|
| **Purpose** | Build & publish wheels | Performance documentation |
| **Frequency** | On version tags only | On version tags only |
| **Duration** | ~5-10 minutes | ~15-25 minutes |
| **Output** | Python wheels + Gitea release | HTML reports + GitHub Pages |
| **Audience** | End users (pip install) | Developers & users (performance) |
| **Platform** | Gitea Actions | GitHub Actions |

### Why This Approach Works

1. **Complete Releases**: Every version tag produces both wheels AND performance data
2. **Historical Tracking**: Performance evolution across releases is preserved
3. **Cost Efficiency**: Benchmarking only on releases (not every commit)
4. **Platform Optimization**: 
   - Gitea for private wheel publishing
   - GitHub Pages for public performance documentation
5. **Release Documentation**: Each version has complete technical documentation

---

## 🚀 Usage Patterns

### For Daily Development
```bash
# Regular development - no automatic benchmarks
git push origin main  # → Only standard CI/tests run

# Performance testing during development (optional)
cargo bench --bench bitemporal_benchmarks medium_dataset -- --profile-time 5
python3 scripts/add_flamegraphs_to_html.py
```

### For Creating Releases
```bash
# Create and push version tag - triggers BOTH workflows simultaneously
git tag v1.2.3
git push origin v1.2.3

# Results:
# 1. Gitea Actions: Wheels built and published to registry
# 2. GitHub Actions: Benchmarks run and deployed to GitHub Pages

# Install published wheel
pip install --index-url https://gitea.example.com/api/packages/user/pypi/simple/ bitemporal-timeseries==1.2.3

# View performance data for this release
# Visit: https://your-username.github.io/bitemporal-timeseries/
```

### Manual Triggers
```bash
# Force benchmark update (via GitHub Actions UI)
# Go to Actions → Benchmark Performance → Run workflow

# Force wheel build (via Gitea Actions UI)  
# Go to Actions → Build and Publish → Run workflow
```

---

## 🔧 Maintenance

### Adding New Benchmarks
1. Add benchmark function to `benches/bitemporal_benchmarks.rs`
2. Decide if it needs flamegraph generation (add to benchmark workflow)
3. Update benchmark documentation

### Modifying Workflows
- **build-wheels.yml**: Focus on build speed and reliability
- **benchmarks.yml**: Focus on comprehensive performance analysis
- Test changes with workflow_dispatch before merging

### Performance Expectations
- **Build workflow**: < 10 minutes total
- **Benchmark workflow**: < 30 minutes total (with timeout protection)
- **Flamegraph generation**: ~3-5 seconds per benchmark

## 📈 Historical Performance Tracking

### Benefits of Release-Based Benchmarks

1. **Version-Linked Performance**: Each release has dedicated performance data
2. **Regression Detection**: Compare performance across versions easily
3. **Release Documentation**: Performance metrics become part of release notes
4. **Cost Efficiency**: No benchmark overhead during development

### GitHub Pages Structure (per release)

```
https://your-username.github.io/bitemporal-timeseries/
├── index.html (latest release performance)
├── medium_dataset/
│   ├── report/index.html (with 🔥 Flamegraph links)
│   └── profile/flamegraph.svg
├── conflation_effectiveness/
│   ├── report/index.html
│   └── profile/flamegraph.svg
└── scaling_by_dataset_size/
    └── records/500000/
        ├── report/index.html  
        └── profile/flamegraph.svg
```

### Performance Evolution Tracking

Each release deployment includes:
- **Version information** in page titles and headers
- **Generation timestamp** for tracking when benchmarks were run
- **Links to release history** for comparing across versions
- **Complete flamegraph analysis** for identifying optimization opportunities

This release-based approach ensures that performance data is preserved as documentation for each version, making it easy to track performance evolution and identify regressions across releases.