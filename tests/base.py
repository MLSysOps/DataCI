#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Oct 13, 2023
"""
import abc
import inspect
import unittest


class AbstractTestCase(unittest.TestCase, abc.ABC):

    def __new__(cls, *args, **kwargs):
        """Prevent the abstract class from running the test cases."""
        if inspect.isabstract(cls):
            self = unittest.TestCase()
            self.runTest = unittest.skip(f"Skip the abstract test case {cls.__name__}.")(lambda: None)
            return self
        return super().__new__(cls)
