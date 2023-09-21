#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Sep 16, 2023

Analysis the workflow code dependency and generate the patch plan.

For each stage in the workflow, it can have the following concepts:
1. Entrypoint: the entry point of the stage, the func to be called first during the stage execution.
2. Inner function: functions in the same stage code package. In the case of multi-stage shared code,
    it excludes callee tree of the other stages and the dag.
3. outer function: the func that is out-of the code package of the stage.
    In the case of multiple stages sharing one code package, the outer functions are any functions in
    the function callee tree of any other stages and dag.
4. Outer caller: the outer functions that call the function
5. Inter caller: the inner functions that call the function

For different cases, the patch plan is different:
|================|================|================|====================================================|
| Entrypoint     | Inner function | Entry function | Plan                                               |
| outer caller   | outer caller   | inter caller   |                                                    |
| -------------- | -------------- | -------------- | -------------------------------------------------- |
| 0             | 0               | Any           | Replace pkg                                         |
| 0             | 1+              | 0             | Replace func only                                   |
| 0             | 1+              | 1+            | Replace func only, take care of func / import name  |
| 1+            | 0               | Any           | Replace pkg, take care of func / import name        |
| 1+            | 1+              | Any           | Replace func only, take care of func / import name  |
|===============|=================|===============|=====================================================|

We can simplify the plan by formulating it as a logic expression:
Input 0 (A): Entrypoint outer caller = 0 / 1+
Input 1 (B): Inner function outer caller = 0 / 1+
Input 2 (C): Entrypoint inter caller = 0 / 1+
Output 0: Replace = pkg / func only
Output 1: Take care of func, import name = no / yes

We draw the Karnaugh map for the output 0 and output 1:
**Output 0:**
|-------|----|----|----|----|
| C\ AB | 00 | 01 | 11 | 10 |
|-------|----|----|----|----|
| 0     | 0  | 1  | 1  | 0  |
| 1     | 0  | 1  | 1  | 0  |
|-------|----|----|----|----|
output 0 = input 1

**Output 1:**
|-------|----|----|----|----|
| C\ AB | 00 | 01 | 11 | 10 |
|-------|----|----|----|----|
| 0     | 0  | 0  | 1  | 1  |
| 1     | 0  | 1  | 1  | 1  |
|-------|----|----|----|----|
output 1 = input 0 + input 1 * input 2

Therefore, the patch plan wil be:
If inner function outer caller > 0:
    replace the replace func only
Else:
    replace the whole package

If entrypoint outer caller > 0 or (entrypoint inter caller > 0 and inner function outer caller > 0):
    take care of func / import name
Else:
    no need to take care of func / import name
"""
from pathlib import Path
from typing import TYPE_CHECKING

import networkx as nx
import pygraphviz

from dataci.utils import cwd

if TYPE_CHECKING:
    import os


def replace_package(source: 'os.PathLike', target: 'os.PathLike'):
    print('replace package')


def replace_entry_func():
    print('replace entry func')


def fixup_entry_func_import_name():
    print('fixup entry func import name')


def path_to_module_name(path: Path):
    if path.is_dir():
        return '.'.join(path.parts).strip('/') or '.'

    return '.'.join(path.with_suffix('').parts).strip('/')


if __name__ == '__main__':
    from pyan import create_callgraph
    import logging

    # Disable logger for pyan
    logging.getLogger('pyan').setLevel('CRITICAL')

    from exp.text_classification.text_classification_dag import text_classification_dag
    from exp.text_classification.step00_data_augmentation import text_augmentation

    replace_func_name = 'default.text_augmentation'
    new_func = text_augmentation

    # Get all package and entrypoint info for all stages
    stage_pkg_info = dict()
    for stage_name, stage in text_classification_dag.stages.items():
        # Get stage package path
        package_relpath = Path(text_classification_dag.stage_script_paths[stage.full_name])

        package = path_to_module_name(package_relpath)
        entrypoint = package + '.' + stage.script['entrypoint'] if package != '.' else stage.script['entrypoint']

        stage_pkg_info[stage.full_name] = {
            'package': package or '.',
            'entrypoint': entrypoint,
            'include': [path_to_module_name(package_relpath / file) for file in stage.script['filelist']]
        }
    # Get dag entrypoint info
    dag_entrypoint = text_classification_dag.script['entrypoint']

    # Function to be patched
    replace_func_info = stage_pkg_info.pop(replace_func_name)
    package = replace_func_info['package']
    entrypoint = replace_func_info['entrypoint']
    include = replace_func_info['include']

    with cwd(text_classification_dag.script['path']):
        call_graph_dot_str = create_callgraph(
            '**/*.py', format='dot', colored=False, draw_defines=True, draw_uses=True, grouped=False,
            annotated=True,
        )  # Use node annotation to differentiate function and package
        define_graph_dot_str = create_callgraph(
            '**/*.py', format='dot', colored=False, draw_defines=True, draw_uses=False, grouped=False,
        )

    call_graph: nx.MultiDiGraph = nx.nx_agraph.from_agraph(pygraphviz.AGraph(call_graph_dot_str))
    define_graph: nx.MultiDiGraph = nx.nx_agraph.from_agraph(pygraphviz.AGraph(define_graph_dot_str))
    # Add a virtual top-level node (--), to connect every other nodes with 0 in-degree
    top_node_id = '__'
    call_graph.add_node(top_node_id, label='')
    call_graph.add_edges_from([(top_node_id, n) for n, d in call_graph.in_degree])
    define_graph.add_edges_from([(top_node_id, n) for n, d in define_graph.in_degree])

    # Get package node
    package_nodes = {k for k, v in call_graph.nodes(data=True) if '.py' not in v['label']}

    entrypoint_id = entrypoint.replace('.', '__')
    package_id = package.replace('.', '__')
    include_ids = list(map(lambda x: x.replace('.', '__'), include))
    other_stage_entrypoint_ids = list(map(lambda x: x['entrypoint'].replace('.', '__'), stage_pkg_info.values()))
    dag_entrypoint_id = dag_entrypoint.replace('.', '__')

    # Build graph for outer function scanning
    # 1. We manually remove the edge dag_entry_func -> entry_func
    # 2. We manually add the edge dag_entry_func -> other_stage_entry_func
    # 3. We search every tree nodes starting from dag_entry_func
    search_graph = call_graph.copy()
    search_graph.remove_edge(dag_entrypoint_id, entrypoint_id)
    search_graph.add_edges_from(map(lambda x: (dag_entrypoint_id, x), other_stage_entrypoint_ids))
    outer_func_sharing_nodes = set(nx.dfs_preorder_nodes(search_graph, dag_entrypoint_id)) - package_nodes
    outer_func_nodes = outer_func_sharing_nodes

    # Get list of functions within the func package
    search_graph = define_graph.copy()
    any_node = include_ids[0]
    search_graph.add_edges_from(map(lambda x: (any_node, x), include_ids))
    inner_func_nodes = set(nx.dfs_preorder_nodes(search_graph, any_node))
    inner_func_nodes = set(nx.dfs_preorder_nodes(define_graph, package_id)) & inner_func_nodes - {entrypoint_id} - package_nodes
    # Remove inner functions that appear in the outer function nodes

    # Build a call graph to search the outer caller
    # 1. Reverse the graph (for easier DFS search)
    # 2. Random select a node, connect it to others in the inner function nodes
    search_graph = call_graph.reverse()
    if len(inner_func_nodes) > 0:
        # Add edges from any inner func node to any other inner func node
        any_inner_func_node = inner_func_nodes.pop()
        search_graph.add_edges_from(map(lambda x: (any_inner_func_node, x), inner_func_nodes))
        inner_func_nodes.add(any_inner_func_node)
        inner_func_callers = set(nx.dfs_preorder_nodes(search_graph, any_inner_func_node))
    else:
        inner_func_callers = set()
    inner_func_inner_callers = inner_func_callers & inner_func_nodes
    inner_func_outer_callers = inner_func_callers & outer_func_nodes

    print('inner function nodes:')
    print(inner_func_nodes)
    print('inner function inner callers:')
    print(inner_func_inner_callers)
    print('inner function outer callers:')
    print(inner_func_outer_callers)

    # Check entrypoint outer caller number
    entrypoint_callers = set(nx.dfs_preorder_nodes(call_graph.reverse(), entrypoint_id)) - {entrypoint_id}
    entrypoint_inner_caller = entrypoint_callers & inner_func_nodes
    entrypoint_outer_caller = entrypoint_callers & outer_func_nodes

    print('entrypoint inner caller nodes:')
    print(entrypoint_inner_caller)
    print('entrypoint outer caller nodes:')
    print(entrypoint_outer_caller)

    if len(inner_func_outer_callers):
        replace_entry_func()
    else:
        replace_package(package, new_func.script['path'])

    if len(entrypoint_outer_caller) or (len(entrypoint_inner_caller) and len(inner_func_outer_callers)):
        fixup_entry_func_import_name()
