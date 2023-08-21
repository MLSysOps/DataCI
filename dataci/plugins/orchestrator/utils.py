#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Aug 20, 2023
"""
import re


def parse_func_params(s: str):
    # Parse nested parentheses, we only care about the top level
    words = list()
    word_temp = ''
    levels = list()
    s = list(s)
    while s:
        c = s.pop(0)
        if c == '(':
            levels.append('(')
        elif c == '[':
            levels.append('[')
        elif c == '{':
            levels.append('{')
        elif c == ')' and levels[-1] == '(':
            levels.pop()
        elif c == ']' and levels[-1] == '[':
            levels.pop()
        elif c == '}' and levels[-1] == '{':
            levels.pop()
        elif c == '#':  # comment line
            # pop until the end of line
            while s and s.pop(0) != '\n':
                pass
            continue
        elif c == ',' and len(levels) == 0:
            words.append(word_temp.strip())
            word_temp = ''
            continue

        word_temp += c
    if word_temp := word_temp.strip():
        words.append(word_temp)

    # Parse each word
    args, kwargs = list(), dict()
    for word in words:
        # check if word is kwargs
        matched = re.match(r'^(\w+)=', word)
        if matched:
            kwargs[matched.group(1)] = word[matched.end():]
        else:
            if kwargs:
                raise ValueError(
                    f'Invalid function call: {s}, positional arguments {word} must be before keyword arguments.'
                )
            args.append(word)
    return args, kwargs
