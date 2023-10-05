#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Sep 16, 2023

Analysis the workflow code dependency and generate the patch plan.

For each stage in the workflow, it can have the following concepts:
1. Entrypoint: the entry point of the stage, the func to be called first during the stage execution.
2. Inner function: functions in the same stage code package. In the case of multi-stage shared code.
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
import os
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, List, Set

import networkx as nx
import pygraphviz

from dataci.models import Stage
from dataci.models.script import get_source_segment, replace_source_segment, format_code_diff
from dataci.utils import cwd

if TYPE_CHECKING:
    from typing import Any, Tuple, Union


def replace_package(basedir: 'Path', source: 'Stage', target: 'Stage'):
    new_mountdir = basedir / target.name

    print(f"replace package {source} -> {target}")
    rm_files = list(map(lambda x: basedir / x, source.script.filelist))
    for rm_file in rm_files:
        print(f"Remove file '{rm_file}'")
        rm_file.unlink()

    for add_file in map(lambda x: new_mountdir / x, target.script.filelist):
        print(f"Add file '{add_file}'")
    target.script.copy(new_mountdir, dirs_exist_ok=True)

    return new_mountdir


def replace_entry_func(basedir: Path, source: 'Stage', target: 'Stage', fix_import_name: bool = False):
    new_mountdir = basedir / target.name
    target_stage_entrypoint = path_to_module_name(new_mountdir.relative_to(basedir)) + '.' + target.script.entrypoint
    source_func_name = source.script.entrypoint.split('.')[-1]

    print(f"replace entry func {source} -> {target}")
    modify_file = basedir / source.script.entry_path
    modify_file_script = modify_file.read_text()
    new_file_script = replace_source_segment(
        modify_file_script,
        source.script.entry_node,
        f'import {target_stage_entrypoint} as {source_func_name}' if fix_import_name else '',
    )
    print(
        f"Modify file '{modify_file}'\n"
        "```\n" +
        format_code_diff(modify_file_script, new_file_script) +
        "```\n"
    )
    modify_file.write_text(new_file_script)

    for add_file in map(lambda x: new_mountdir / x, target.script.filelist):
        print(f"Add file '{add_file}'")
    target.script.copy(new_mountdir, dirs_exist_ok=True)

    return new_mountdir


def fixup_entry_func_import_name(
        paths: List[Path],
        source_stage_entrypoint: str,
        target_stage_entrypoint: str,
        source_stage_package: str,
        replace_package: bool = False,
):
    print('fixup entry func import name')
    pat = re.compile(
        re.escape((source_stage_package + '.').lstrip('.')) +
        r'(.*)\.([^.]+)'
    )
    if match := pat.match(source_stage_entrypoint):
        stage_pkg_name, func_name = match.groups()
    else:
        raise ValueError('Cannot parse stage package name and function name from '
                         f'source_stage_entrypoint={source_stage_entrypoint}')

    for path in paths:
        script = path.read_text()
        # Parse the ast of the script, and find the import statement
        tree = ast.parse(script)
        import_points = list()

        # Locate all module level stage function definition
        for node in ast.iter_child_nodes(tree):
            # Get var name of the dataci.plugins.decorators.stage function
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                module_name = '.' * getattr(node, 'level', 0) + getattr(node, 'module', '')
                for alias in node.names:
                    global_name = module_name + '.' + alias.name if module_name else alias.name
                    alias_name = alias.asname or alias.name
                    # Found module name or parent module name
                    if source_stage_entrypoint.startswith(global_name):
                        var_name = alias_name + source_stage_entrypoint.split(global_name)[-1]
                        import_points.append((node, global_name, var_name))

        replace_nodes, replace_segs = list(), list()
        # Replace the var name in the import statement
        for node, global_import_name, var_name in import_points:
            if '.' not in var_name:
                # stage is imported as a module name:
                # -import stage_root.stg_pkg.func as var_name
                # +import new_stage_root.new_stage_pkg.new_func as var_name
                replace_nodes.append(node)
                replace_segs.append(f"import {target_stage_entrypoint} as {var_name}")
            else:
                # stage is imported / ref as a package name
                if not replace_package:
                    # Case 1: replace the old function module name with the new function module name
                    if global_import_name.endswith(func_name):
                        # i. import statement include the function name,
                        #   replace the import statement, since the function is not valid
                        # ```diff
                        # -import stage_root.stg_pkg.func
                        # +import stage_root.stg_pkg
                        # +import stage_root.new_stage_pkg.new_func
                        # +stage_root.stg_pkg.func = new_stage_root.new_stage_pkg.new_func
                        # ```
                        replace_nodes.append(node)
                        replace_segs.append(
                            f"import {global_import_name.rstrip(func_name)}\n"
                            f"import {target_stage_entrypoint}\n"
                            f"{var_name} = {target_stage_entrypoint}"
                        )
                    else:
                        # ii. Otherwise,
                        # ```diff
                        # import stage_root.stg_pkg as alias
                        # +import stage_root.new_stage_pkg.new_func
                        # +alias.func = dag_pkg.new_stage_pkg.func
                        # ```
                        replace_nodes.append(node)
                        replace_segs.append(
                            get_source_segment(script, node) + '\n' +
                            f"import {target_stage_entrypoint}\n"
                            f"{var_name} = {target_stage_entrypoint}"
                        )
                else:
                    # Case 2: if stage package is replaced, need to mock the stage package
                    stage_pkg_mods = global_import_name.lstrip(source_stage_package + '.')
                    add_lines = list()
                    if f'{stage_pkg_name}.{func_name}'.lstrip('.').startswith(stage_pkg_mods):
                        # i. import statement include the stage package name
                        # Alias import is used
                        # ```diff
                        # -import stage_root.stg_mod1 as alias
                        # +alias = types.ModuleType('stage_root.stg_mod1')
                        # +alias.stg_mod2 = types.ModuleType('stage_root.stg_mod1.stg_mod2')
                        # ```
                        # Or direct import is used
                        # ```diff
                        # -import stage_root.stg_mod1.stg_mod2
                        # +import stage_root
                        # +stage_root.stg_mod1 = types.ModuleType('stage_root.stg_mod1')
                        # +stage_root.stg_mod1.stg_mod2 = types.ModuleType('stage_root.stg_mod1.stg_mod2')
                        # ```
                        import_name = ''
                        add_stage_root_import, add_types_import = False, True
                        for mod in var_name.split('.')[:-1]:
                            import_name = import_name + '.' + mod if import_name else mod  # prevent leading '.'
                            if source_stage_package.startswith(import_name):
                                # found: import stage_root_mod1, import stage_root_mod1.stage_root_mod2, etc
                                add_stage_root_import = True
                                continue
                            elif add_stage_root_import:
                                # Not found stage_root import any further, add stage_root import:
                                # +import stage_root_mod1.stage_root_mod2
                                add_lines.append(f'import {import_name}')
                                add_stage_root_import = False
                            if add_types_import:
                                add_lines.append(f'import types')
                                add_types_import = False
                            add_lines.append(f'{import_name} = types.ModuleType({mod!r})')
                    else:
                        # ii. Otherwise, do nothing for import fixing
                        add_lines.append(get_source_segment(script, node))

                    # Add the new function import and fix import
                    # ```diff
                    # +import new_stage_root.new_stage_pkg.new_func
                    # +alias.func = new_stage_root.new_stage_pkg.new_func
                    # ```
                    add_lines.append(f'import {target_stage_entrypoint}')
                    add_lines.append(f'{var_name} = {target_stage_entrypoint}')
                    replace_nodes.append(node)
                    replace_segs.append(f'{os.linesep}'.join(add_lines))

        # Replace all import statements at one time
        if len(replace_nodes) > 0:
            new_script = replace_source_segment(script, replace_nodes, replace_segs)
            print(
                f"Modify file '{path}'\n"
                "```\n" +
                format_code_diff(script, new_script) +
                "```\n"
            )
            path.write_text(new_script)


def path_to_module_name(path: Path):
    if path.is_dir():
        return '.'.join(path.parts).strip('/') or '.'

    return '.'.join(path.with_suffix('').parts).strip('/')


def get_all_predecessors(
        g: 'Union[nx.MultiDiGraph, nx.DiGraph]',
        source: 'Union[Set, List]',
        entry: 'Any' = None
):
    return get_all_successors(g.reverse(), source, entry)


def get_all_successors(
        g: 'Union[nx.MultiDiGraph, nx.Graph]',
        source: 'Union[Set, List]',
        entry: 'Any' = None
):
    # If source is a single node, convert it to a list
    source = [source] if not isinstance(source, (List, Set)) else list(source)
    # source is empty, return empty
    if len(source) == 0:
        return set()
    # Get the entry node, if not specified, use the first node in source
    entry = entry or source[0]

    # Checking if entry is in source, and every source is in the graph
    violate_nodes = list(filter(lambda x: x not in g.nodes, source + [entry]))
    assert len(violate_nodes) == 0, f'source nodes {violate_nodes} is not found in the graph {g}'

    # 1. connect entry node to all entry in the inner function nodes
    # 2. search all tree nodes starting from entry node
    search_graph = g.copy()
    search_graph.add_edges_from(map(lambda x: (entry, x), source))
    return set(nx.dfs_preorder_nodes(search_graph, entry))


if __name__ == '__main__':
    from pyan import create_callgraph
    import logging

    # Disable logger for pyan
    logging.getLogger('pyan').setLevel('CRITICAL')

    from exp.text_classification.text_classification_dag import text_classification_dag
    from example.text_process.text_process_ci import text_process_ci_pipeline
    from exp.text_classification.step00_data_augmentation import text_augmentation

    replace_func_name = 'default.text_augmentation'
    new_func = text_augmentation
    dag = text_process_ci_pipeline

    basedir = Path(dag.script.dir)
    # Get all package and entrypoint info for all stages
    stage_pkg_info = dict()
    for stage in dag.stages.values():
        # Get stage package path
        package_relpath = Path(dag.stage_script_paths[stage.full_name])

        package = path_to_module_name(package_relpath)
        entrypoint = package + '.' + stage.script.entrypoint if package != '.' else stage.script.entrypoint

        stage_pkg_info[stage.full_name] = {
            'path': basedir / package_relpath,
            'package': package or '.',
            'entrypoint': entrypoint,
            'include': [path_to_module_name(package_relpath / file) for file in stage.script.filelist]
        }
    # Get dag entrypoint info
    dag_entrypoint = dag.script.entrypoint

    # Function to be patched
    replace_func_info = stage_pkg_info.pop(replace_func_name)
    package = replace_func_info['package']
    entrypoint = replace_func_info['entrypoint']
    include = replace_func_info['include']

    with cwd(basedir):
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
    # We manually remove the edge dag_entry_func -> entry_func
    search_graph = call_graph.copy()
    search_graph.remove_edge(dag_entrypoint_id, entrypoint_id)
    outer_func_sharing_nodes = get_all_successors(search_graph, other_stage_entrypoint_ids + [dag_entrypoint_id],
                                                  dag_entrypoint_id) - package_nodes
    outer_func_nodes = outer_func_sharing_nodes

    # Get list of functions within the func package
    inner_func_nodes = get_all_successors(define_graph, include_ids)
    inner_func_nodes = (
            get_all_successors(define_graph, package_id) & inner_func_nodes
            - {entrypoint_id} - package_nodes
    )

    inner_func_callers = get_all_predecessors(call_graph, inner_func_nodes)
    inner_func_inner_callers = inner_func_callers & inner_func_nodes
    inner_func_outer_callers = inner_func_callers & outer_func_nodes

    print('inner function nodes:')
    print(inner_func_nodes)
    print('inner function inner callers:')
    print(inner_func_inner_callers)
    print('inner function outer callers:')
    print(inner_func_outer_callers)

    # Check entrypoint outer caller number
    entrypoint_callers = get_all_predecessors(call_graph, entrypoint_id) - {entrypoint_id}
    entrypoint_inner_caller = entrypoint_callers & inner_func_nodes
    entrypoint_outer_caller = entrypoint_callers & outer_func_nodes

    print('entrypoint inner caller nodes:')
    print(entrypoint_inner_caller)
    print('entrypoint outer caller nodes:')
    print(entrypoint_outer_caller)

    with TemporaryDirectory(dir=dag.workspace.tmp_dir) as tmp_dir:
        tmp_dir = Path(tmp_dir)
        # Copy the dag package to a temp dir
        dag.script.copy(tmp_dir, dirs_exist_ok=True)
        stage_base_dir = tmp_dir / replace_func_info['path'].relative_to(basedir)
        flg_replace_pkg = len(inner_func_outer_callers) == 0
        flg_fix_import_name = len(entrypoint_outer_caller) \
                              or (len(entrypoint_inner_caller) and len(inner_func_outer_callers))

        if flg_replace_pkg:
            new_stage_dir = replace_package(stage_base_dir, dag.stages[replace_func_name], new_func)
        else:
            new_stage_dir = replace_entry_func(stage_base_dir, dag.stages[replace_func_name], new_func)
        new_entrypoint = path_to_module_name(new_stage_dir.relative_to(tmp_dir)) + '.' + new_func.script.entrypoint

        if len(entrypoint_outer_caller) or (len(entrypoint_inner_caller) and len(inner_func_outer_callers)):
            paths = list()
            for caller in entrypoint_callers & package_nodes - {top_node_id}:
                label = call_graph.nodes[caller]['label']
                paths.append((tmp_dir / label.replace('.', '/')).with_suffix('.py'))
            fixup_entry_func_import_name(
                paths=paths,
                source_stage_entrypoint=entrypoint,
                target_stage_entrypoint=new_entrypoint,
                source_stage_package=package,
                replace_package=bool(len(inner_func_outer_callers))
            )
