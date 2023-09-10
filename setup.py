#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 11, 2023
"""
from setuptools import setup, find_namespace_packages

from dataci import __version__

# Read the contents of README.md
with open('README.md') as f:
    long_description = f.read()

# Read the requirements.txt
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="ml-data-ci",
    version=__version__,
    description="A platform for tracking data-centric AI pipelines in dynamic streaming data",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Yuanming Li",
    author_email="yuanmingleee@gmail.com",
    url='https://github.com/MLSysOps/DataCI',
    license='MIT',
    keywords=['Data-centric AI', 'Streaming Data', 'MLOps', 'Pipeline Tracking', 'Data Versioning'],
    py_modules=['dataci'],
    packages=find_namespace_packages(exclude=['tests', 'exp', 'example']),
    install_requires=requirements,
    entry_points="""
        [console_scripts]
        dataci=dataci.command:cli
    """,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Education',
        'Intended Audience :: Financial and Insurance Industry',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Operating System :: POSIX :: Linux',
    ],
)
