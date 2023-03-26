#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 26, 2023
"""
from itertools import groupby
from operator import attrgetter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import List


def table_groupby(seq: 'List', key_names: 'List[str]'):
    """Group a sequence by key
    """
    key_getter = attrgetter(*key_names)
    seq.sort(key=key_getter)
    groups = groupby(seq, key_getter)
    yield from groups
