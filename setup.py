#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 11, 2023
"""
from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="dataci",
    version='0.0.1',
    author="Yuanming Li",
    author_email="yuanmingleee@gmail.com",
    py_modules=['dataci'],
    install_requires=requirements,
    entry_points="""
        [console_scripts]
        dataci=dataci.command:cli
    """,
)
