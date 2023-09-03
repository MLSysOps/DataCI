#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 30, 2023

DataCI Workflow and Stage script extractor
"""
import ast
import inspect
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Union, List


def locate_main_block(tree):
    # Locate __main__ block
    nodes = list()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.If) and isinstance(node.test, ast.Compare)):
            continue
        left, ops, comparators = node.test.left, node.test.ops, node.test.comparators
        # Check left == __name__
        if not (isinstance(left, ast.Name) and left.id == '__name__'):
            continue
        # Check ops == Eq
        if not (len(ops) == 1 and isinstance(ops[0], ast.Eq)):
            continue
        # Check comparators == '__main__'
        if not (len(comparators) == 1 and isinstance(comparators[0], ast.Constant) and comparators[0].value == '__main__'):
            continue
        nodes.append(node)

    return nodes


def locate_stage_function(tree, stage_names: 'Union[str, List[str]]', stage_deco_cls='dataci.plugins.decorators.stage'):
    stage_nodes = list()
    stage_names = [stage_names] if isinstance(stage_names, str) else stage_names
    stage_deco_var_names = set()

    # Locate all module level stage function definition
    for node in ast.iter_child_nodes(tree):
        # Get var name of the dataci.plugins.decorators.stage function
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            module_name = '.' * getattr(node, 'level', 0) + getattr(node, 'module', '')
            for alias in node.names:
                global_name = module_name + '.' + alias.name if module_name else alias.name
                alias_name = alias.asname or alias.name
                # Found module name or parent module name
                if stage_deco_cls.startswith(global_name):
                    stage_deco_var_names.add(alias_name + stage_deco_cls.split(global_name)[-1])
            continue

        # Check is function definition
        if not isinstance(node, ast.FunctionDef):
            continue
        # Check decorator
        for decorator in node.decorator_list:
            # Get decorator name (module1.module2.deco_name)
            decorator_attr = decorator.func if isinstance(decorator, ast.Call) else decorator
            deco_modules = list()
            while isinstance(decorator_attr, ast.Attribute):
                deco_modules.insert(0, decorator_attr.attr)
                decorator_attr = decorator_attr.value
            deco_modules.insert(0, decorator_attr.id)

            # Check decorator is @<stage_deco_var_name>(...) / @<stage_deco_var_name>
            if '.'.join(deco_modules) in stage_deco_var_names:
                break
        else:
            continue
        # Check function name is stage_name
        # Check decorator has task_id=stage_name
        if isinstance(decorator, ast.Call):
            # Build kwargs dict from ast.Call(
            #   args=[], keywords=[ast.keyword(arg=..., value=ast.Constant(...)]
            # )
            kwargs = {kwarg.arg: kwarg.value for kwarg in decorator.keywords}
            if ast.literal_eval(kwargs.get('task_id', f'"{node.name}"')) in stage_names:
                stage_nodes.append(node)
        elif node.name in stage_names:
            stage_nodes.append(node)
        return stage_nodes


def locate_dag_function(tree, dag_name: 'str', dag_deco_cls='dataci.plugins.decorators.dag'):
    from dataci.plugins.decorators import dag

    dag_nodes = list()
    dag_deco_var_names = set()
    # Locate all module level dag function definition
    for node in ast.iter_child_nodes(tree):
        # Get var name of the dataci.plugins.decorators.dag function
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            module_name = '.' * getattr(node, 'level', 0) + getattr(node, 'module', '')
            for alias in node.names:
                global_name = module_name + '.' + alias.name if module_name else alias.name
                alias_name = alias.asname or alias.name
                # Found module name or parent module name
                if dag_deco_cls.startswith(global_name):
                    dag_deco_var_names.add(alias_name + dag_deco_cls.split(global_name)[-1])
            continue

        # Check is function definition
        if not isinstance(node, ast.FunctionDef):
            continue
        # Check decorator
        for decorator in node.decorator_list:
            # Get decorator name (module1.module2.deco_name)
            decorator_attr = decorator.func if isinstance(decorator, ast.Call) else decorator
            deco_modules = list()
            while isinstance(decorator_attr, ast.Attribute):
                deco_modules.insert(0, decorator_attr.attr)
                decorator_attr = decorator_attr.value
            deco_modules.insert(0, decorator_attr.id)

            # Check decorator is @<dag_deco_var_name>(...)
            if '.'.join(deco_modules) in dag_deco_var_names:
                break
        else:
            continue

        # Check function name is dag_name
        # Check decorator has dag_id=dag_name
        if isinstance(decorator, ast.Call):
            # Build args and kwargs from ast.Call(
            #   args=[], keywords=[ast.keyword(arg=..., value=ast.Constant(...)]
            # )
            args = decorator.args
            kwargs = {kwarg.arg: kwarg.value for kwarg in decorator.keywords}
            dag_id = inspect.signature(dag).bind(*args, **kwargs).arguments.get('dag_id', f'"{node.name}"')
            if ast.literal_eval(dag_id) == dag_name:
                dag_nodes.append(node)
        elif node.name == dag_name:
            dag_nodes.append(node)

    return dag_nodes
