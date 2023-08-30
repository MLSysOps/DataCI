#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 30, 2023

DataCI Workflow and Stage script extractor
"""
import ast

with open('../../../example/text_process_ci.py', 'r') as f:
    script = f.read()

tree = ast.parse(script)


def remove_main_block(tree):
    # Remove __main__ block
    removes = list()
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
        removes.append((f'{node.lineno}:{node.col_offset}', f'{node.end_lineno}:{node.end_col_offset}'))

    return removes


def locate_stage_function(tree):
    pass

if __name__ == '__main__':
    print(locate_stage_function(tree))
