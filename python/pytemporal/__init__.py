# Import the Rust extension module (it's a submodule now)
from .pytemporal import (
    compute_changes,
    compute_changes_with_hash_algorithm,
    add_hash_key_with_algorithm
)

# Import Python wrapper classes from the local processor module
from .processor import BitemporalTimeseriesProcessor, INFINITY_TIMESTAMP, add_hash_key

__all__ = [
    'BitemporalTimeseriesProcessor',
    'INFINITY_TIMESTAMP',
    'compute_changes',
    'compute_changes_with_hash_algorithm',
    'add_hash_key',
    'add_hash_key_with_algorithm'
]

# Dynamically get version from installed package metadata
# This reads from the wheel metadata set during build
try:
    from importlib.metadata import version as _get_version
    __version__ = _get_version("pytemporal")
except Exception:
    # Fallback for development installs or edge cases
    __version__ = "0.0.0.dev"