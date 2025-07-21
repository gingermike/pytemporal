from setuptools import setup
from setuptools_rust import Binding, RustExtension

setup(
    name="bitemporal-timeseries",
    version="0.1.0",
    rust_extensions=[
        RustExtension(
            "bitemporal_timeseries",
            binding=Binding.PyO3,
            debug=False,
        )
    ],
    packages=["bitemporal_timeseries"],
    install_requires=[
        "pyarrow>=14.0.0",
        "pandas>=2.0.0",
        "numpy>=1.24.0",
    ],
    setup_requires=["setuptools-rust>=1.5.2"],
    zip_safe=False,
    python_requires=">=3.8",
)
