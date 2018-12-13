#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(
    name='PyBS',
    version='0.1',
    description='PyBS - the Python Batch System',
    author='Tim-Oliver Husser',
    author_email='thusser@uni-goettingen.de',
    packages=find_packages(include=['PyBS', 'PyBS.*']),
    scripts=['bin/pybs', 'bin/pybsd']
)
