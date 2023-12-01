#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 22, 2023
"""
import warnings
from typing import TYPE_CHECKING, TypeVar

from dataci.db.lineage import (
    exist_many_downstream_lineage,
    exist_many_upstream_lineage,
    create_many_lineage,
)
from dataci.models.dataset import Dataset
from dataci.models.run import Run

if TYPE_CHECKING:
    from typing import List, Union

LineageAllowedType = TypeVar('LineageAllowedType', Dataset, Run)


class Lineage(object):

    def __init__(
            self,
            upstream: 'Union[List[LineageAllowedType], LineageAllowedType, dict]',
            downstream: 'Union[List[LineageAllowedType], LineageAllowedType, dict]',
    ):
        # only one of upstream and downstream can be list
        if isinstance(upstream, list) and isinstance(downstream, list):
            raise ValueError('Only one of upstream and downstream can be list.')
        self._upstream = upstream if isinstance(upstream, list) else [upstream]
        self._downstream = downstream if isinstance(downstream, list) else [downstream]

    def dict(self):
        return {
            'upstream': [node.dict(id_only=True) for node in self.upstream],
            'downstream': [node.dict(id_only=True) for node in self.downstream],
        }

    @classmethod
    def from_dict(cls, config):
        pass

    @property
    def upstream(self) -> 'List[LineageAllowedType]':
        """Lazy load upstream from database."""
        nodes = list()
        for node in self._upstream:
            if isinstance(node, (Dataset, Run)):
                nodes.append(node)
            elif isinstance(node, dict):
                node_type = node.pop('type', None)
                if node_type == 'run':
                    node_cls = Run
                    nodes.append(node_cls.get(**node))
                elif node_type == 'dataset':
                    node_cls = Dataset
                    nodes.append(node_cls.get(**node))
                else:
                    warnings.warn(f'Unknown node type {node_type}')
            else:
                warnings.warn(f'Unable to parse upstream {node}')
        self._upstream = nodes
        return self._upstream

    @property
    def downstream(self) -> 'List[LineageAllowedType]':
        """Lazy load downstream from database."""
        downstream = list()
        for node in self._downstream:
            if isinstance(node, (Dataset, Run)):
                downstream.append(node)
            elif isinstance(node, dict):
                node_type = node.pop('type', None)
                if node_type == 'run':
                    node_cls = Run
                    downstream.append(node_cls.get(**node))
                elif node_type == 'dataset':
                    node_cls = Dataset
                    downstream.append(node_cls.get(**node))
                else:
                    warnings.warn(f'Unknown node type {node_type}')
            else:
                warnings.warn(f'Unable to parse downstream {node}')
        self._downstream = downstream
        return self._downstream

    def save(self, exist_ok=True):
        config = self.dict()

        if len(config['upstream']) == 1:
            # Check if downstream lineage exists
            upstream_config = config['upstream'][0]
            lineage_exist_status_list = exist_many_downstream_lineage(
                upstream_config, config['downstream'],
            )

            if any(lineage_exist_status_list):
                if not exist_ok:
                    exist_downstreams = [
                        downstream_config for downstream_config, exist in zip(
                            config['downstream'], lineage_exist_status_list
                        ) if exist
                    ]
                    raise ValueError(f"Lineage exists: {upstream_config} -> {exist_downstreams}")
                else:
                    # Remove the existed lineage
                    config['downstream'] = [
                        node for node, exist in zip(config['downstream'], lineage_exist_status_list) if not exist
                    ]
        else:
            # Check if upstream lineage exists
            downstream_config = config['downstream'][0]
            lineage_exist_status_list = exist_many_upstream_lineage(
                config['upstream'], downstream_config,
            )

            if any(lineage_exist_status_list):
                if not exist_ok:
                    exist_upstreams = [
                        upstream_config for upstream_config, exist in zip(
                            config['upstream'], lineage_exist_status_list
                        ) if exist
                    ]
                    raise ValueError(f"Lineage exists: {exist_upstreams} -> {downstream_config}")
                else:
                    # Remove the existed lineage
                    config['upstream'] = [
                        node for node, exist in zip(config['upstream'], lineage_exist_status_list) if not exist
                    ]

            # Create dataset lineage
            create_many_lineage(config)

        return self

    def get(self, run_name, run_version):
        pass
