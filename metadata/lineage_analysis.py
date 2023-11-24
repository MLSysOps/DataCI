#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 21, 2023
"""
import json
from copy import deepcopy

from metadata.models import RunEvent

JSON_PATH = 'bash_example_metadata.json'
# JSON_PATH = 'lineagetest_postgres.json'

if __name__ == '__main__':
    import builtins
    import rich

    builtins.print = rich.print

with open(JSON_PATH) as f:
    lines = f.readlines()



def merge_dicts(a: dict, b: dict):
    """Merge dictionaries b into a."""
    result = deepcopy(a)
    for k, v in b.items():
        if isinstance(v, dict):
            result[k] = merge_dicts(result.get(k, dict()), v)
        else:
            result[k] = v
    return result


events = []
for line in lines:
    events.append(json.loads(line))

runs = dict()
for event in events:
    run_id = event['run']['runId']
    if run_id not in runs:
        runs[run_id] = event
    else:
        # Merge the new event into the existing one
        existing_event = runs[run_id]
        if event['eventTime'] >= existing_event['eventTime']:
            runs[run_id] = merge_dicts(existing_event, event)
        else:
            runs[run_id] = merge_dicts(event, existing_event)


# r = runs['db33cca4-8d48-3ade-9111-384c86657c25']
r = runs['1ff96850-b38d-3247-b2cc-7ebe66e7d0d7']
# r2 = runs['3b80ac13-0b1b-38b5-a70d-97e15e33189f']

print(json.dumps(r))
