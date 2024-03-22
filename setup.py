from setuptools import setup, find_packages
setup(
    name="danelfin_demo",
    version="0.1.1",
    description="A short description of your project",  # Add a description
    packages=find_packages(),
    install_requires=[  # List of external dependencies
        "pytest==8.1.1",
        "pydantic==1.10.13",
        "pydantic[email]",
        "fire==0.5.0",
        "loguru==0.7.2",
        "TinyDB",
        "pyarrow",
        "pytest",
        "pandas"
    ]
)
