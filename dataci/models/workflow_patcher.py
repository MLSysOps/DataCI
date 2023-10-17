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
import atexit
import inspect
import logging
import os
import re
import tempfile
from io import StringIO
from pathlib import Path
from pydoc import getpager, pipepager
from shutil import rmtree
from typing import TYPE_CHECKING, List, Set

import git

import networkx as nx
import pygraphviz
from pyan import create_callgraph

from dataci.models import Stage
from dataci.models.script import (
    get_source_segment, replace_source_segment,
    pretty_print_diff,
    pretty_print_dircmp
)
from dataci.utils import cwd, removeprefix, removesuffix

if TYPE_CHECKING:
    from typing import Any, Tuple, Union
    from dataci.models import Workflow


def replace_package(basedir: 'Path', source: 'Stage', target: 'Stage', logger: 'logging.Logger'):
    new_mountdir = basedir / target.name

    logger.debug(f"replace package {source} -> {target}")
    rm_files = list(map(lambda x: basedir / x, source.script.filelist))
    for rm_file in rm_files:
        logger.debug(f"Remove file '{rm_file}'")
        rm_file.unlink()

    for add_file in map(lambda x: new_mountdir / x, target.script.filelist):
        logger.debug(f"Add file '{add_file}'")
    target.script.copy(new_mountdir, dirs_exist_ok=True)

    # Make the new stage package importable
    init_file = new_mountdir / '__init__.py'
    if not init_file.exists():
        logger.debug(f"Add file '{init_file}'")
        init_file.touch()

    return new_mountdir


def replace_entry_func(
        basedir: Path,
        source: 'Stage',
        target: 'Stage',
        fix_import_name: bool,
        logger: 'logging.Logger'
):
    new_mountdir = basedir / target.name
    target_stage_entrypoint = path_to_module_name(new_mountdir.relative_to(basedir)) + '.' + target.script.entrypoint
    source_func_name = source.script.entrypoint.split('.')[-1]

    logger.debug(f"replace entry func {source} -> {target}")
    modify_file = basedir / source.script.entry_path
    modify_file_script = modify_file.read_text()
    new_file_script = replace_source_segment(
        modify_file_script,
        source.script.entry_node,
        gen_import_script(target_stage_entrypoint, alias=source_func_name) if fix_import_name else '',
    )
    logger.debug(f"Modify file '{modify_file}'")
    modify_file.write_text(new_file_script)

    for add_file in map(lambda x: new_mountdir / x, target.script.filelist):
        logger.debug(f"Add file '{add_file}'")
    target.script.copy(new_mountdir, dirs_exist_ok=True)

    # Make the new stage package importable
    init_file = new_mountdir / '__init__.py'
    if not init_file.exists():
        logger.debug(f"Add file '{init_file}'")
        init_file.touch()

    return new_mountdir


def fixup_entry_func_import_name(
        paths: List[Path],
        source_stage_entrypoint: str,
        target_stage_entrypoint: str,
        source_stage_package: str,
        replace_package: bool,
        logger: 'logging.Logger',
):
    logger.debug('fixup entry func import name')
    source_stage_package = (source_stage_package + '.').lstrip('.')
    pat = re.compile(
        re.escape(source_stage_package) +
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
                replace_segs.append(gen_import_script(target_stage_entrypoint, alias=var_name))
            else:
                # stage is imported / ref as a package name
                # Case 1: replace the old function module name with the new function module name
                if global_import_name == source_stage_entrypoint:
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
                        f"import {removesuffix(global_import_name, '.' + func_name)}\n"
                        f"{gen_import_script(target_stage_entrypoint, absolute_import=True)}\n"
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
                        f"{gen_import_script(target_stage_entrypoint, absolute_import=True)}\n"
                        f"{var_name} = {target_stage_entrypoint}"
                    )
                if replace_package:
                    # if stage package is replaced, need to mock the stage package
                    stage_pkg_mods = removeprefix(global_import_name, source_stage_package)
                    if f'{stage_pkg_name}.{func_name}'.lstrip('.').startswith(stage_pkg_mods):
                        # Fix import path by create a mock package
                        stage_base_dir = Path(source_stage_package.replace('.', '/'))
                        stage_pkg_file = Path(stage_pkg_name.replace('.', '/')).with_suffix('.py')
                        for par in reversed(stage_pkg_file.parents):
                            # 1. Create empty python module
                            (stage_base_dir / par).mkdir(exist_ok=True)
                            if not (init_file := (stage_base_dir / par / '__init__.py')).exists():
                                init_file.touch()

                        # 2. Create empty python file
                        if not (stage_file := stage_base_dir / stage_pkg_file).exists():
                            stage_file.touch(exist_ok=True)

        # Replace all import statements at one time
        if len(replace_nodes) > 0:
            new_script = replace_source_segment(script, replace_nodes, replace_segs)
            logger.debug(f"Modify file '{path}'")
            path.write_text(new_script)


def path_to_module_name(path: Path):
    if path.is_dir():
        return '.'.join(path.parts).strip('/') or '.'

    return '.'.join(path.with_suffix('').parts).strip('/')


def gen_import_script(entrypoint: str, absolute_import: bool = False, alias: str = None):
    func_name = entrypoint.split('.')[-1]
    mod = '.'.join(entrypoint.split('.')[:-1]) or '.'
    if absolute_import:
        return f'import {mod}'
    return f'from {mod} import {func_name}' + (f' as {alias}' if (alias and alias != func_name) else '')


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


def patch(
        workflow: 'Workflow',
        source_name: str,
        target: 'Stage',
        verbose: bool = True,
        logger: 'logging.Logger' = None
):
    logger = logger or logging.getLogger(__name__)
    # Disable logger for pyan
    logging.getLogger('pyan').setLevel('CRITICAL')

    basedir = Path(workflow.script.dir)
    # Get all package and entrypoint info for all stages
    stage_pkg_info = dict()
    for stage in workflow.stages.values():
        # Get stage package path
        package_relpath = Path(workflow.stage_script_paths[stage.full_name])

        package = path_to_module_name(package_relpath)
        entrypoint = package + '.' + stage.script.entrypoint if package != '.' else stage.script.entrypoint

        stage_pkg_info[stage.full_name] = {
            'path': basedir / package_relpath,
            'package': package or '.',
            'entrypoint': entrypoint,
            'include': [path_to_module_name(package_relpath / file) for file in stage.script.filelist]
        }
    # Get dag entrypoint info
    dag_entrypoint = workflow.script.entrypoint

    # Function to be patched
    replace_func_info = stage_pkg_info.pop(source_name)
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

    logger.debug(
        'inner function nodes:\n'
        f'{inner_func_nodes}\n'
        'inner function inner callers:\n'
        f'{inner_func_inner_callers}\n'
        'inner function outer callers:\n'
        f'{inner_func_outer_callers}\n'
    )

    # Check entrypoint outer caller number
    entrypoint_callers = get_all_predecessors(call_graph, entrypoint_id) - {entrypoint_id}
    entrypoint_inner_caller = entrypoint_callers & inner_func_nodes
    entrypoint_outer_caller = entrypoint_callers & outer_func_nodes

    logger.debug(
        'entrypoint inner caller nodes:\n'
        f'{entrypoint_inner_caller}\n'
        'entrypoint outer caller nodes:\n'
        f'{entrypoint_outer_caller}\n'
    )

    tmp_dir = tempfile.mkdtemp(dir=workflow.workspace.tmp_dir)
    atexit.register(tempfile.TemporaryDirectory._rmtree, tmp_dir)
    tmp_dir = Path(tmp_dir)

    # Copy the dag package to a temp dir
    workflow.script.copy(tmp_dir, dirs_exist_ok=True)

    if verbose:
        try:
            repo = git.Repo(tmp_dir)
            git_exists = True
        except git.InvalidGitRepositoryError:
            repo = git.Repo.init(tmp_dir)
            git_exists = False
        repo.git.add(all=True)
        repo.index.commit('main')

    stage_base_dir = tmp_dir / replace_func_info['path'].relative_to(basedir)
    flg_replace_pkg = len(inner_func_outer_callers) == 0
    flg_fix_import_name = bool(
        len(entrypoint_outer_caller) \
        or (len(entrypoint_inner_caller) and len(inner_func_outer_callers))
    )

    if flg_replace_pkg:
        new_stage_dir = replace_package(
            basedir=stage_base_dir,
            source=workflow.stages[source_name],
            target=target,
            logger=logger
        )
    else:
        new_stage_dir = replace_entry_func(
            basedir=stage_base_dir,
            source=workflow.stages[source_name],
            target=target,
            fix_import_name=flg_fix_import_name,
            logger=logger
        )
    new_entrypoint = path_to_module_name(new_stage_dir.relative_to(tmp_dir)) + '.' + target.script.entrypoint

    if flg_fix_import_name:
        paths = list()
        for caller in entrypoint_callers & package_nodes - {top_node_id}:
            label = call_graph.nodes[caller]['label']
            path = (tmp_dir / label.replace('.', '/')).with_suffix('.py')
            if path.exists():
                paths.append(path)
        fixup_entry_func_import_name(
            paths=paths,
            source_stage_entrypoint=entrypoint,
            target_stage_entrypoint=new_entrypoint,
            source_stage_package=package,
            replace_package=flg_replace_pkg,
            logger=logger,
        )

    if verbose:
        repo.git.add(all=True)
        diffs = repo.index.diff(None, staged=True, create_patch=True)
        # File compare (like git status)
        logger.info('File changed:')
        pretty_print_dircmp(diffs)
        # Code diff (like git diff), print to pager
        logger.info('Code diff:')
        diff_log_str = StringIO()
        pretty_print_diff(diffs, file=diff_log_str)

        # Print to pager, overwirte the default pager to support color output
        pager = getpager()
        # pager is a `less` called function
        pager_code = inspect.getsource(pager).strip()
        if pager_code == 'return lambda text: pipepager(text, \'less\')':
            pager = lambda text: pipepager(text, 'less -R')
        pager(diff_log_str.getvalue())

        # Clean up git
        if not git_exists:
            rmtree(str(repo.git_dir))
        else:
            repo.head.reset('HEAD~1', index=False)
    return workflow.from_path(tmp_dir, entry_path=workflow.script.entry_path)
