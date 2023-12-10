#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 15, 2023
"""
from functools import wraps
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Type, Union, TypeVar, Callable
    from dataci.models.base import Job

    T = TypeVar('T', bound=Callable)


def event(name: str = None, producer: str = None):
    def wrapper(func: 'T') -> 'T':
        @wraps(func)
        def inner_wrapper(self: 'Union[Job, Type[Job]]', *args, **kwargs):
            # Prevent circular import
            from dataci.models import Event

            producer_ = producer
            if producer_ is None:
                if isinstance(self, type):
                    raise ValueError('`producer` must be specified when decorating a class method')
            name_ = name or func.__name__
            evt = Event(name_, producer=self.identifier, producer_type=self.type_name, producer_alias=self.version_tag)
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
