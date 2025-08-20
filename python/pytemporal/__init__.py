# Import the Rust extension module (it's a submodule now)
from .pytemporal import compute_changes

# Import Python wrapper classes from the local processor module
from .processor import BitemporalTimeseriesProcessor, INFINITY_TIMESTAMP, add_hash_key

__all__ = [
    'BitemporalTimeseriesProcessor', 
    'INFINITY_TIMESTAMP', 
    'compute_changes',
    'add_hash_key'
]
__version__ = '0.1.0'