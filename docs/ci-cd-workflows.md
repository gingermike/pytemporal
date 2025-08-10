# CI/CD Workflows Overview

This project uses a consolidated release workflow approach with platform-specific optimizations:

## 🏗️ Gitea Actions: `build-wheels.yml` 

**Location**: `.gitea/workflows/build-wheels.yml`  
**Purpose**: Build and publish production Python wheels to private Gitea package registry

### Triggers
- ✅ **Version tags only** (`v1.0.0`, `v2.1.3`, etc.)
- ✅ **Manual workflow dispatch**

### Jobs
1. **Test Job**: Run Rust tests, Python tests, build release binary
2. **Build Linux Wheels**: Build optimized wheels for x86_64 architecture  
3. **Publish to Gitea**: Publish wheels to Gitea package registry with release notes

**Duration**: ~5-10 minutes (fast, focused on wheel building)

---

## 📊 GitHub Actions: `build-wheels.yml`

**Location**: `.github/workflows/build-wheels.yml`  
**Purpose**: Complete release workflow with wheels, benchmarks, and documentation

### Triggers
- ✅ **Version tags only** (`v1.0.0`, `v2.1.3`, etc.)
- ✅ **Manual workflow dispatch**

### Jobs
1. **Test Job**: 
   - Run Rust tests (`cargo test`)
   - Run Python tests with pytest
   - Build release binary (smoke test)

2. **Build Linux Wheels**:
   - Build optimized wheels for x86_64 architecture
   - Upload as artifacts for later use

3. **Benchmarks Job**:
   - Generate flamegraphs for key benchmarks (3-second profiling each)
     - `medium_dataset` - 100 records with 20 updates
     - `conflation_effectiveness` - Adjacent segment merging  
     - `scaling_by_dataset_size/records/500000` - Large dataset analysis
   - Run additional benchmarks without flamegraphs for completeness
   - Add flamegraph links to Criterion HTML reports
   - Create versioned GitHub Pages content with download links
   - Package all benchmark results as versioned ZIP artifact

4. **Deploy Benchmarks**:
   - Deploy benchmark reports to GitHub Pages
   - Public performance documentation at your-username.github.io/bitemporal-timeseries/

5. **Publish Release**:
   - Create GitHub release including:
     - Python wheels (.whl files)
     - Benchmark data ZIP (`benchmarks-v1.2.3.zip`)
     - Performance metrics in release notes
     - Links to interactive benchmark reports
   - Rich release documentation with performance data

6. **Publish PyPI** (optional):
   - Publish wheels to public PyPI for broader distribution

**Duration**: ~20-30 minutes (comprehensive release package)

---

## 🎯 Consolidated Workflow Benefits

| Aspect | Gitea Actions | GitHub Actions |
|--------|---------------|----------------|
| **Purpose** | Private wheel publishing | Public release + documentation |
| **Frequency** | Version tags only | Version tags only |
| **Duration** | ~5-10 minutes | ~20-30 minutes |
| **Output** | Gitea wheels + release | GitHub wheels + benchmarks + pages |
| **Audience** | Internal users | Public users + developers |

### Why Consolidation Works

1. **No Conflicts**: Single GitHub workflow controls Pages deployment (eliminates race conditions)
2. **Complete Packages**: Every GitHub release includes benchmark data as downloadable ZIP
3. **Simplified Maintenance**: One consolidated workflow file to manage for GitHub
4. **Cost Efficiency**: Benchmarking only on releases (not every development push)
5. **Rich Documentation**: Each release has comprehensive technical documentation
6. **Platform Optimization**: Best of both platforms (private Gitea registry + public GitHub visibility)

---

## 🚀 Release Process

### Creating a Release
```bash
# Create and push version tag - triggers BOTH platforms simultaneously
git tag v1.2.3
git push origin v1.2.3

# Results:
# 1. Gitea Actions: Fast wheel build and publishing to private registry
# 2. GitHub Actions: Complete release with wheels, benchmarks, and documentation
```

### What Gets Created

**Gitea Release:**
- Python wheels published to private package registry
- Basic release notes

**GitHub Release:**
- Python wheels (public download)
- `benchmarks-v1.2.3.zip` (complete benchmark data)
- Rich release notes with performance metrics
- Links to interactive GitHub Pages documentation

**GitHub Pages:**
- Interactive benchmark reports with flamegraphs
- Version-specific performance documentation
- Historical performance tracking across releases

---

## 📦 Release Artifacts

### Wheel Installation
```bash
# From GitHub releases (public)
pip install --no-index --find-links="https://github.com/user/repo/releases/download/v1.2.3" bitemporal-timeseries==1.2.3

# From Gitea registry (private)
pip install --index-url https://gitea.example.com/api/packages/user/pypi/simple/ bitemporal-timeseries==1.2.3
```

### Benchmark Data Access
```bash
# View interactive reports online
# Visit: https://your-username.github.io/bitemporal-timeseries/

# Download complete benchmark data
wget https://github.com/user/repo/releases/download/v1.2.3/benchmarks-v1.2.3.zip
unzip benchmarks-v1.2.3.zip
# Contains: HTML reports, flamegraphs, raw criterion data
```

---

## 📈 Performance Documentation

### GitHub Pages Structure (per release)
```
https://your-username.github.io/bitemporal-timeseries/
├── index.html (version-specific landing page)
├── medium_dataset/
│   ├── report/index.html (with 🔥 Flamegraph links)
│   └── profile/flamegraph.svg
├── conflation_effectiveness/
│   ├── report/index.html
│   └── profile/flamegraph.svg
└── scaling_by_dataset_size/records/500000/
    ├── report/index.html  
    └── profile/flamegraph.svg
```

### Release Documentation Features
- **Version information** in page titles and headers
- **Generation timestamps** for release tracking
- **Download links** to complete benchmark ZIP files
- **Performance metrics** embedded in release notes
- **Interactive flamegraphs** for detailed analysis
- **Historical context** linking to previous releases

---

## 🔧 Maintenance

### Adding New Benchmarks
1. Add benchmark function to `benches/bitemporal_benchmarks.rs`
2. Update GitHub workflow to include flamegraph generation if needed
3. Test locally: `cargo bench --bench bitemporal_benchmarks new_benchmark -- --profile-time 5`

### Workflow Modifications
- **Gitea workflow**: Focus on speed and reliability for wheel building
- **GitHub workflow**: Balance comprehensive analysis with reasonable execution time
- Always test workflow changes with `workflow_dispatch` before release

### Expected Performance
- **Gitea workflow**: < 10 minutes (build focus)
- **GitHub workflow**: < 35 minutes (includes comprehensive benchmarking)
- **Flamegraph generation**: ~3-5 seconds per benchmark

This consolidated approach provides complete release packages with historical performance documentation while avoiding workflow conflicts and maintaining clear platform responsibilities.