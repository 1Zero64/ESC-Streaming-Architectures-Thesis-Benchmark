# python3
# -*- coding: utf-8 -*-
# @author: Niko Kauz
# Description: Build Python Egg, module management
# How to:
# - Go to terminal in project directory
# - type command "pip install -e ."

from setuptools import setup, find_packages

setup(
    name = "ESC-Streaming-Architectures-Thesis-Benchmark",
    version = "1.0",
    packages = find_packages(),
    license = "MIT"
    )