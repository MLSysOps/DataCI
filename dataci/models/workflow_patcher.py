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
import ast
from pathlib import Path

import networkx as nx
import pygraphviz

from dataci.plugins.orchestrator.script import locate_stage_function, locate_dag_function
from dataci.utils import cwd


def replace_package():
    pass


def replace_entry_func():
    pass


def fixup_entry_func_import_name():
    pass


if __name__ == '__main__':
    from pyan import create_callgraph
    import logging

    # Disable logger for pyan
    logging.getLogger('pyan').setLevel('CRITICAL')

    from exp.text_classification.text_classification_dag import text_classification_dag

    # Get all package and entrypoint info for all stages
    stage_pkg_info = dict()
    for stage_name, stage in text_classification_dag.stages.items():
        # Get stage package path
        package_relpath = Path(text_classification_dag.stage_script_paths[stage.full_name])
        package_abspath = Path(text_classification_dag.script['path']) / package_relpath

        # Get stage entry function name
        entryfile_path = package_abspath / stage.script['entrypoint']
        tree = ast.parse(entryfile_path.read_text())
        func_node = locate_stage_function(tree, stage.name)[0][0]
        entrypoint_relpath = package_relpath / stage.script['entrypoint'].split('.')[0] / func_node.name

        package = '.'.join(package_relpath.parts).strip('/') if str(package_relpath) != '.' else '.'
        entrypoint = '.'.join(entrypoint_relpath.with_suffix('').parts).strip('/')

        stage_pkg_info[stage.full_name] = {
            'package': package,
            'entrypoint': entrypoint,
        }
    # Get dag entrypoint info
    dag_entryfile_base = Path(text_classification_dag.script['path'])
    dag_entrypoint_path = dag_entryfile_base / text_classification_dag.script['entrypoint']
    tree = ast.parse(dag_entrypoint_path.read_text())
    dag_nodes = locate_dag_function(tree, text_classification_dag.name)[0]
    assert len(dag_nodes) == 1, 'DAG should only have one entrypoint'
    dag_entrypoint_relpath = text_classification_dag.script['entrypoint'].split('.')[0] / dag_nodes[0].name
    dag_entrypoint = '.'.join(dag_entrypoint_relpath.with_suffix('').parts).strip('/')

    # Function to be patched
    replace_func_info = stage_pkg_info.pop('default.text_augmentation')
    package = replace_func_info['package']
    entrypoint = replace_func_info['entrypoint']

    with cwd(text_classification_dag.script['path']):
        call_graph_dot_str = create_callgraph(
            '**/*.py', format='dot', colored=False, draw_defines=False, draw_uses=True, grouped=False,
        )
        define_graph_dot_str = create_callgraph(
            '**/*.py', format='dot', colored=False, draw_defines=True, draw_uses=False, grouped=False,
        )

    call_graph: nx.MultiDiGraph = nx.nx_agraph.from_agraph(pygraphviz.AGraph(call_graph_dot_str))
    define_graph: nx.MultiDiGraph = nx.nx_agraph.from_agraph(pygraphviz.AGraph(define_graph_dot_str))
    # Add a top-level node, to connect every other nodes with 0 in-degree
    call_graph.add_edges_from([('__', n) for n, d in call_graph.in_degree])
    define_graph.add_edges_from([('__', n) for n, d in define_graph.in_degree])

    entrypoint_id = entrypoint.replace('.', '__')
    package_id = package.replace('.', '__')
    other_stage_entrypoint_ids = list(map(lambda x: x['entrypoint'].replace('.', '__'), stage_pkg_info.values()))
    dag_entrypoint_id = dag_entrypoint.replace('.', '__')

    # Build graph for outer function scanning
    # 1. We manually remove the edge dag_entry_func -> entry_func
    # 2. We manually add the edge dag_entry_func -> other_stage_entry_func
    # 3. We search every tree nodes starting from dag_entry_func
    search_graph = call_graph.copy()
    search_graph.remove_edge(dag_entrypoint_id, entrypoint_id)
    search_graph.add_edges_from(map(lambda x: (dag_entrypoint_id, x), other_stage_entrypoint_ids))
    outer_func_sharing_nodes = list(nx.dfs_preorder_nodes(search_graph, dag_entrypoint_id))

    # Get list of functions within the func package
    inner_func_nodes = set(nx.dfs_preorder_nodes(define_graph, package_id)) - {entrypoint_id}
    # Remove inner functions that appear in the outer function nodes
    inner_func_nodes = set(filter(lambda x: x not in outer_func_sharing_nodes, inner_func_nodes))
    # Build a call graph to search the outer caller
    # 1. Reverse the graph (for easier DFS search)
    # 2. Random select a node, connect it to others in the inner function nodes
    search_graph = call_graph.reverse()
    if len(inner_func_nodes) > 0:
        # Add edges from any inner func node to any other inner func node
        any_inner_func_node = inner_func_nodes.pop()
        search_graph.add_edges_from(map(lambda x: (any_inner_func_node, x), inner_func_nodes))
        inner_func_nodes.add(any_inner_func_node)
        inner_func_callers = set(nx.dfs_preorder_nodes(search_graph, any_inner_func_node)) - inner_func_nodes
    else:
        inner_func_callers = set()
    inner_func_inner_callers = inner_func_callers.intersection(inner_func_nodes)
    inner_func_outer_callers = inner_func_callers - inner_func_nodes

    print('inner function nodes:')
    print(inner_func_nodes)
    print('inner function inner callers:')
    print(inner_func_inner_callers)
    print('inner function outer callers:')
    print(inner_func_outer_callers)

    # Check entrypoint outer caller number
    entrypoint_callers = set(nx.dfs_preorder_nodes(call_graph.reverse(), entrypoint_id)) - {entrypoint_id}
    entrypoint_inner_caller = entrypoint_callers.intersection(inner_func_nodes)
    entrypoint_outer_caller = entrypoint_callers - entrypoint_inner_caller

    print('entrypoint inner caller nodes:')
    print(entrypoint_inner_caller)
    print('entrypoint outer caller nodes:')
    print(entrypoint_outer_caller)

    if len(inner_func_outer_callers):
        replace_entry_func()
    else:
        replace_package()

    if len(entrypoint_outer_caller) or (len(entrypoint_inner_caller) and len(inner_func_outer_callers)):
        fixup_entry_func_import_name()
