#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 15, 2023
"""
from functools import wraps
from typing import TYPE_CHECKING

from dataci.server.trigger import Event

if TYPE_CHECKING:
    from typing import Type, Union, TypeVar, Callable
    from dataci.models.base import BaseModel

    T = TypeVar('T', bound=Callable)


def event(name: str, producer: str = None):
    def wrapper(func: 'T') -> 'T':
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
                evt.success()
                return result
            except Exception as e:
                evt.fail()
                raise e

        return inner_wrapper

    return wrapper
