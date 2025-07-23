# Import the Rust extension module (it's a submodule now)
from .bitemporal_timeseries import compute_changes

# Import Python wrapper classes from the local processor module
from .processor import BitemporalTimeseriesProcessor, POSTGRES_INFINITY, apply_changes_to_postgres

__all__ = [
    'BitemporalTimeseriesProcessor', 
    'POSTGRES_INFINITY', 
    'apply_changes_to_postgres',
    'compute_changes'
]
__version__ = '0.1.0'