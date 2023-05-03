#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 03, 2023
"""
from functools import wraps
from typing import TYPE_CHECKING

from dataci.triggers import Event

if TYPE_CHECKING:
    from typing import Type, Union
    from dataci.models.base import BaseModel


def event(name: str, producer: str = None):
    def wrapper(func):
        @wraps(func)
        def inner_wrapper(self: 'Union[BaseModel, Type[BaseModel]]', *args, **kwargs):
            producer_ = producer
            if producer_ is None:
                if isinstance(self, type):
                    raise ValueError('`producer` must be specified when decorating a class method')
                producer_ = self.full_name
            evt = Event(name, producer=producer_)
            evt.start()
            try:
                result = func(self, *args, **kwargs)
                evt.end()
                return result
            except Exception as e:
                evt.fail()
                raise e

        return inner_wrapper

    return wrapper
