#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Jun 20, 2023
"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Optional
    from dataci.models import Stage


class DecoratedOperatorStageMixin:
    def __init__(self, *args, **kwargs):
        self._stage: 'Optional[Stage]' = None
        super().__init__(*args, **kwargs)

    @property
    def workspace(self):
        return self._stage.workspace

    @property
    def name(self):
        return self._stage.name

    @property
    def version(self):
        return self._stage.version

    @property
    def full_name(self):
        return self._stage.full_name

    @property
    def identifier(self):
        return self._stage.identifier

    @property
    def create_date(self):
        return self._stage.create_date

    @property
    def script(self):
        return self._stage.script

    def test(self, *args, **kwargs):
        return self._stage.test(*args, **kwargs)

    def dict(self):
        return self._stage.dict()

    def from_dict(self, config):
        self._stage.from_dict(config)
        return self

    def reload(self, config=None):
        self._stage.reload(config)
        return self

    def save(self):
        self._stage.save()
        return self

    def publish(self):
        self._stage.publish()
        return self
