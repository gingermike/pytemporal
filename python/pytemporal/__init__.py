# Import the Rust extension module (it's a submodule now)
from .pytemporal import compute_changes

# Import Python wrapper classes from the local processor module
from .processor import BitemporalTimeseriesProcessor, INFINITY_TIMESTAMP

__all__ = [
    'BitemporalTimeseriesProcessor', 
    'INFINITY_TIMESTAMP', 
    'compute_changes'
]
__version__ = '0.1.0'