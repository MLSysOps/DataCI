#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 20, 2023
"""
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
CACHE_ROOT = Path.home() / '.dataci'
CACHE_ROOT.mkdir(exist_ok=True)
CONFIG_FILE = CACHE_ROOT / 'config.ini'
CONFIG_FILE.touch(exist_ok=True, mode=0o600)
