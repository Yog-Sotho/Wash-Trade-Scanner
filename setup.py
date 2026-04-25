#!/usr/bin/env python3
"""
Setup script for wash-trade-detector.
"""

from setuptools import setup, find_packages

with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="wash-trade-detector",
    version="1.0.0",
    description="Multi‑chain wash trade and fake volume detection system",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/Yog-Sotho/Wash-Trade-Scanner",
    packages=find_packages(include=["config", "core", "models", "scripts", "tests"]),
    python_requires=">=3.10",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "wash-audit = scripts.run_audit:main",
            "wash-train = scripts.train_model:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: MIT",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
