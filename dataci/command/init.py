#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 01, 2023
"""
import click

from dataci.config import init as init_config


@click.command()
def init():
    """DataCI initialization."""
    init_config()
