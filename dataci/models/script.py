#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Sep 25, 2023
"""
import ast
import tokenize
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from os import PathLike


class Script(object):
    def __init__(
            self,
            dir: 'PathLike',
            local_dir: 'PathLike',
            entry_path: 'PathLike',
            entry_node: 'ast.FunctionDef',
            **kwargs,
    ) -> None:
        # Original local dir of the script, it will be None if script load from database
        self.dir = Path(dir)
        self.local_dir = Path(local_dir)
        self.entry_path = Path(entry_path)
        self._entry_node = entry_node

    @property
    def entry_node(self):
        """Lazy parse entry node from entry path and location."""
        if type(self._entry_node) == ast.stmt:
            lineno, col_offset = self._entry_node.lineno, self._entry_node.col_offset
            # Parse entry node from location
            for node in ast.walk(ast.parse((self.dir / self.entry_path).read_text())):
                if node.lineno == lineno and node.col_offset == col_offset:
                    self._entry_node = node
                    break
            else:
                raise ValueError(f'Cannot find entry node from {self.entry_path} at {lineno}:{col_offset}')
            if not isinstance(self._entry_node, ast.FunctionDef):
                raise ValueError(f'Entry node {self._entry_node} is not a function definition')
        return self._entry_node

    @property
    def entrypoint(self):
        return '.'.join((*self.entry_path.with_suffix('').parts, self.entry_node.name)).strip('/')

    @property
    def entry_func_location(self):
        return f'{self.entry_node.lineno}:{self.entry_node.col_offset}'

    @property
    def source_segment(self):
        return get_source_segment(
            (self.dir / self.entry_path).read_text(), self.entry_node, padded=False
        )

    def dict(self):
        return {
            'dir': self.dir.as_posix(),
            'local_dir': self.local_dir.as_posix(),
            'entry': f'{self.entry_path.as_posix()} {self.entry_func_location}',
            'filelist': [self.entry_path.as_posix()],
        }

    @classmethod
    def from_dict(cls, config: dict):
        entry_path, loc = config.pop('entry').split(' ')
        # Parse entry node from location
        lineno, col_offset = loc.split(':')
        for node in ast.walk(ast.parse(Path(config['dir']) / entry_path)):
            if node.lineno == lineno and node.col_offset == col_offset:
                entry_node = node
                break

        return cls(**config, entry_path=entry_path, entry_node=entry_node)



def get_source_segment(script: 'str', node: 'ast.AST', *, padded: 'bool' = False):
    """Get source code segment of the *source* that generated *node*.
    Fix for ast.get_source_segment() in case of missing function decorator.

    If some location information (`lineno`, `end_lineno`, `col_offset`,
    or `end_col_offset`) is missing, return None.

    If *padded* is `True`, the first line of a multi-line statement will
    be padded with spaces to match its original position.
    """
    lines = list()
    if isinstance(node, ast.FunctionDef):
        for deco_node in node.decorator_list:
            # To include '@' with decorator, shift the col_offset to the left by 1
            deco_node.col_offset -= 1
            lines.append(ast.get_source_segment(script, deco_node, padded=padded))
            deco_node.col_offset += 1

    node_end_lineno, node_end_col_offset = node.end_lineno, node.end_col_offset
    # Get the node block padding
    if hasattr(node, 'body'):
        node_block_padding = node.body[-1].col_offset

        # Patch line ending comment
        tokens = list(tokenize.generate_tokens(iter(script.splitlines(keepends=True)).__next__))
        for token in tokens:
            # Locate the end of node token
            if token.end[0] < node_end_lineno:
                continue
            if token.end[0] == node_end_lineno and token.end[1] <= node_end_col_offset:
                continue
            # At the node's end line, search for the first comment token: xxxx # comment
            if token.start[0] == node_end_lineno and token.type == tokenize.COMMENT:
                node.end_col_offset = token.end[1]
                continue
            # Only allow comment / NL  / Newline token after the node's end line
            if token.type not in (tokenize.COMMENT, tokenize.NL, tokenize.NEWLINE):
                break
            # Search for comment lines in the node block, otherwise stop
            elif token.type == tokenize.COMMENT:
                if token.start[1] != node_block_padding:
                    break
                node.end_lineno, node.end_col_offset = token.end

    lines.append(ast.get_source_segment(script, node, padded=padded))
    node.end_lineno, node.end_col_offset = node_end_lineno, node_end_col_offset

    return '\n'.join(lines)
