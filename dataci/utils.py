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


def hash_file(filepath):
    """
    Compute the hash of a single file or a directory tree, including all files and subdirectories.

    Args:
        filepath: File path or the directory to hash

    Returns:
        The hash of the directory tree.

    References:
        https://stackoverflow.com/a/24937710
    """
    sha_hash = hashlib.md5()
    # Append all files in the directory recursively
    filepath_list = list()
    if os.path.isfile(filepath):
        filepath_list.append(filepath)
    else:
        for root, dirs, files in os.walk(filepath):
            for names in files:
                filepath_list.append(os.path.join(root, names))
    # Sort the file paths
    filepath_list.sort()

    for path in filepath_list:
        # hash relative name
        sha_hash.update(os.path.relpath(path, filepath).encode())
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
