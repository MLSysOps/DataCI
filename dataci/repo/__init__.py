#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Feb 21, 2023
"""
import os
from pathlib import Path

DATACI_DIR = '.dataci'


class Repo(object):
    DATACI_DIR = '.dataci'

    def __init__(self):
        self.root_dir = self.find_root()
        self.dataci_dir = self.root_dir / self.DATACI_DIR

    @property
    def dataset_dir(self):
        return self.dataci_dir / 'dataset'

    @property
    def pipeline_dir(self):
        return self.dataci_dir / 'pipeline'

    @property
    def tmp_dir(self):
        return self.dataci_dir / 'tmp'

    @staticmethod
    def find_root(root=None):
        root = Path(root or os.curdir)
        root_dir = root.resolve()

        if not root_dir.is_dir():
            raise FileNotFoundError(f"directory '{root}' does not exist")

        while True:
            dataci_dir = root_dir / DATACI_DIR
            if dataci_dir.is_dir():
                return root_dir
            # Reach the FS root
            if root_dir.parent == root_dir:
                break
            root_dir = root_dir.parent

        raise FileNotFoundError("you are not inside of a DVC repository")
