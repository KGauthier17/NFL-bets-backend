from setuptools import setup, find_packages

setup(
    name="nfl-bets-backend",
    version="0.1.0",
    description="Backend ML model for NFL bet predictions",
    author="Your Name",
    packages=find_packages(where="app"),
    package_dir={"": "app"},
    install_requires=[
        "numpy",
        "pandas",
        "scikit-learn",
        "fastapi"
    ],
    python_requires=">=3.7",
)
