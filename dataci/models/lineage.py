#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 22, 2023
"""
import warnings
from typing import TYPE_CHECKING, TypeVar

import networkx as nx

from dataci.db.lineage import (
    exist_many_downstream_lineage,
    exist_many_upstream_lineage,
    create_many_lineage,
    list_many_upstream_lineage,
    list_many_downstream_lineage,
)

if TYPE_CHECKING:
    from typing import List, Union
    from dataci.models.dataset import Dataset
    from dataci.models.run import Run

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

class LineageGraph:

    def get_vertices(self):
        # Retrieves all vertices from the vertices table V.
        pass

    def get_edges(self):
        # Retrieves all edges from the edge table E.
        pass

    def get_vertex(self, vertex_id):
        # Retrieves a vertex from the vertices table V by its ID.
        pass

    def get_edge(self, edge_id):
        # Retrieves an edge from the edge table E by its ID.
        pass

    @classmethod
    def upstream(cls, job: 'Union[LineageAllowedType, dict]', n: 'int' = 1, type: 'str' = None) -> 'nx.DiGraph':
        """Retrieves incoming edges that are connected to a vertex.
        """
        if isinstance(job, dict):
            job_config = job
        else:
            job_config = job.dict(id_only=True)

        g = nx.DiGraph()
        g.add_node(job_config)
        job_configs = [job_config]
        # Retrieve upstream lineage up to n levels
        for _ in range(n):
            lineage_configs = list_many_upstream_lineage(job_configs)
            g.add_edges_from(
                (lineage_config['upstream'], lineage_config['downstream']) for lineage_config in lineage_configs
            )
            # With type filter, we need to retrieve extra upstream jobs if the current job does not match the type
            job_configs = [
                lineage_config for lineage_config in lineage_configs
                if type is not None and lineage_config['upstream']['type'] != type
            ]

            add_lineage_configs = list_many_upstream_lineage(job_configs)
            g.add_edges_from(
                (lineage_config['upstream'], lineage_config['downstream']) for lineage_config in add_lineage_configs
            )
            # Add upstream jobs to job_configs for next iteration of lineage retrieval
            job_configs = [
                lineage_config['upstream'] for lineage_config in lineage_configs
                if type is None or lineage_config['upstream']['type'] == type
            ] + [
                lineage_config['upstream'] for lineage_config in add_lineage_configs
            ]

        return g

    @classmethod
    def downstream(cls, job: 'Union[LineageAllowedType, dict]', n: 'int' = 1, type: 'str' = None) -> 'nx.DiGraph':
        """Retrieves outgoing edges that are connected to a vertex.
        """
        if isinstance(job, dict):
            job_config = job
        else:
            job_config = job.dict(id_only=True)

        g = nx.DiGraph()
        g.add_node(job_config)
        job_configs = [job_config]
        # Retrieve downstream lineage up to n levels
        for _ in range(n):
            lineage_configs = list_many_downstream_lineage(job_configs)
            g.add_edges_from(
                (lineage_config['upstream'], lineage_config['downstream']) for lineage_config in lineage_configs
            )
            # With type filter, we need to retrieve extra downstream jobs if the current job does not match the type
            job_configs = [
                lineage_config for lineage_config in lineage_configs
                if type is not None and lineage_config['downstream']['type'] != type
            ]

            add_lineage_configs = list_many_downstream_lineage(job_configs)
            g.add_edges_from(
                (lineage_config['upstream'], lineage_config['downstream']) for lineage_config in add_lineage_configs
            )
            # Add downstream jobs to job_configs for next iteration of lineage retrieval
            job_configs = [
                lineage_config['downstream'] for lineage_config in lineage_configs
                if type is None or lineage_config['downstream']['type'] == type
            ] + [
                lineage_config['downstream'] for lineage_config in add_lineage_configs
            ]

        return g
