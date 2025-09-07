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
__version__ = '0.1.0'