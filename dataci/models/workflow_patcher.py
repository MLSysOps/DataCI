#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Sep 16, 2023

Analysis the workflow code dependency and generate the patch plan.

For each stage in the workflow, it can have the following concepts:
1. Replace func: the entry point of the stage, the func to be called first during the stage execution.
2. In-package other funcs' outer caller: all other funcs in the same stage code package
3. Replace func outer caller: the out-of-package functions that call the replace func
4. Replace func inter caller: the in-package functions that call the replace func

For different cases, the patch plan is different:
|==============|=====================|==============|=====================================================|
| Replace func | In-package other    | Replace func | Plan                                                |
| outer caller | funcs' outer caller | inter caller |                                                     |
| ------------ | ------------------- | ------------ | --------------------------------------------------- |
| 0            | 0                   | Any          | Replace pkg                                         |
| 0            | 1+                  | 0            | Replace func only                                   |
| 0            | 1+                  | 1+           | Replace func only, take care of func / import name  |
| 1+           | 0                   | Any          | Replace pkg, take care of func / import name        |
| 1+           | 1+                  | Any          | Replace func only, take care of func / import name  |
|==============|=====================|==============|=====================================================|

We can simplify the plan by formulating it as a logic expression:
Input 0 (A): Replace func outer caller = 0 / 1+
Input 1 (B): In-package other funcs' outer caller = 0 / 1+
Input 2 (C): Replace func inter caller = 0 / 1+
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
If in-package other funcs' outer caller = 0:
    replace the whole package
Else:
    replace the replace func only

If replace func outer caller = 1 or (replace func inter caller = 1 and in-package other funcs' outer caller = 1):
    take care of func / import name
Else:
    no need to take care of func / import name
"""


def replace_package():
    pass


def replace_entry_func():
    pass


def fixup_entry_func_import_name():
    pass
