#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 22, 2023
"""
import warnings
from itertools import chain
from typing import TYPE_CHECKING

import networkx as nx

from dataci.db.lineage import (
    exist_many_downstream_lineage,
    exist_many_upstream_lineage,
    create_many_lineage,
    list_many_upstream_lineage,
    list_many_downstream_lineage,
)
from dataci.models.base import Job, JobView

if TYPE_CHECKING:
    from typing import List, Union


class Lineage(object):

    def __init__(
            self,
            upstream: 'Union[List[Job], Job, dict]',
            downstream: 'Union[List[Job], Job, dict]',
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
    def upstream(self) -> 'List[Job]':
        """Lazy load upstream from database."""
        nodes = list()
        for node in self._upstream:
            if isinstance(node, Job):
                nodes.append(node)
            elif isinstance(node, dict):
                nodes.append(Job.get(**node))
            else:
                warnings.warn(f'Unable to parse upstream {node}')
        self._upstream = nodes
        return self._upstream

    @property
    def downstream(self) -> 'List[Job]':
        """Lazy load downstream from database."""
        nodes = list()
        for node in self._downstream:
            if isinstance(node, Job):
                nodes.append(node)
            elif isinstance(node, dict):
                nodes.append(Job.get(**node))
            else:
                warnings.warn(f'Unable to parse downstream {node}')
        self._downstream = nodes
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
        from dataci.models import Job

        if isinstance(job, Job):
            job_config = job.dict(id_only=True)
        else:
            job_config = job

        g = nx.DiGraph()
        g.add_node(JobView(**job_config))
        job_configs = [job_config]
        # Retrieve upstream lineage up to n levels
        for _ in range(n):
            # (level n) ->      .    .
            # job configs to query for next iteration, job configs to query and add to graph
            job_configs, job_configs_add = list(), job_configs
            # Recursively query for upstream lineage until all lineage_configs are the same `type` as the argument
            while len(job_configs_add) > 0:
                lineage_configs = list_many_upstream_lineage(job_configs_add)
                for upstreams, downstream in zip(lineage_configs, job_configs_add):
                    downstream_job_view = JobView(**downstream)
                    g.add_edges_from((JobView(**upstream), downstream_job_view) for upstream in upstreams)
                job_configs_add.clear()
                #  (level n+1) ->  .  . .  x
                #                   \/   \/
                # (level n)         .    .
                # upstreams that are the same `type` as the argument (represented as dot ".")
                # will be queried for next level of lineage
                # the others (represented as cross "x") will query for next iteration in the loop, because they
                # are not considered as a valid node for the current level
                for upstreams in chain.from_iterable(lineage_configs):
                    if type is None or upstreams['type'] == type:
                        job_configs.append(upstreams)
                    else:
                        job_configs_add.append(upstreams)
        return g

    @classmethod
    def downstream(cls, job: 'Union[LineageAllowedType, dict]', n: 'int' = 1, type: 'str' = None) -> 'nx.DiGraph':
        """Retrieves outgoing edges that are connected to a vertex.
        """
        from dataci.models import Job

        if isinstance(job, Job):
            job_config = job.dict(id_only=True)
        else:
            job_config = job

        g = nx.DiGraph()
        g.add_node(JobView(**job_config))
        job_configs = [job_config]
        # Retrieve downstream lineage up to n levels
        for _ in range(n):
            # (level n) ->      .    .
            # job configs to query for next iteration, job configs to query and add to graph
            job_configs, job_configs_add = list(), job_configs
            # Recursively query for downstream lineage until all lineage_configs are the same `type` as the argument
            while len(job_configs_add) > 0:
                lineage_configs = list_many_downstream_lineage(job_configs_add)
                for upstream, downstreams in zip(job_configs_add, lineage_configs):
                    upstream_job_view = JobView(**upstream)
                    g.add_edges_from((upstream_job_view, JobView(**downstream)) for downstream in downstreams)
                job_configs_add.clear()
                # (level n)         .    .
                #                   /\   /\
                #  (level n+1) ->  .  . .  x
                # downstreams that are the same `type` as the argument (represented as dot ".")
                # will be queried for next level of lineage
                # the others (represented as cross "x") will query for next iteration in the loop, because they
                # are not considered as a valid node for the current level
                for downstreams in chain.from_iterable(lineage_configs):
                    if type is None or downstreams['type'] == type:
                        job_configs.append(downstreams)
                    else:
                        job_configs_add.append(downstreams)
        return g
