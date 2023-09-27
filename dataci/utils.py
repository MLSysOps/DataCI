#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 15, 2023
"""
import hashlib
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Union, List


@contextmanager
def cwd(path):
    oldpwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(oldpwd)


def symlink_force(target, link_name, target_is_directory=False):
    """Force to create a symbolic link"""
    try:
        os.unlink(link_name)
    except FileNotFoundError:
        pass
    os.symlink(target, link_name, target_is_directory)


def scantree(path):
    """Recursively yield DirEntry objects for given directory."""
    for entry in os.scandir(path):
        yield entry.path
        if entry.is_dir(follow_symlinks=False):
            yield from scantree(entry.path)


def hash_file(filepaths: 'Union[str, os.PathLike, List[Union[os.PathLike, str]]]'):
    """
    Compute the hash of a single file or a directory tree, including all files and subdirectories.

    Args:
        filepaths: A list of file path or the directory to hash

    Returns:
        The hash of the directory tree.

    References:
        https://stackoverflow.com/a/24937710
    """
    sha_hash = hashlib.md5()
    if isinstance(filepaths, (str, os.PathLike)):
        filepaths = [filepaths]
    # Find common prefix
    root = os.path.commonpath(filepaths)
    # Tree scan of all file paths / directories
    paths = list()
    for path in filepaths:
        # If path is a directory, scan it
        if os.path.isdir(path):
            paths.extend(scantree(path))
        else:
            paths.append(path)

    # Sort the file paths for consistent hash
    for path in sorted(paths):
        # hash relative name
        sha_hash.update(os.path.relpath(path, root).encode())
        with open(path, 'rb') as f:
            while True:
                # Read file in as little chunks
                buf = f.read(4096)
                if not buf:
                    break
                sha_hash.update(buf)

    return sha_hash.hexdigest()


def hash_binary(b: bytes):
    """
    Compute the hash of a binary.

    Args:
        b: file binary

    Returns:
        The hash of the directory tree.

    References:
        https://stackoverflow.com/a/24937710
    """
    sha_hash = hashlib.md5()
    sha_hash.update(b)

    return sha_hash.hexdigest()
