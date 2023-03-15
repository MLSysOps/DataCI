#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 15, 2023
"""
import logging
import os
from typing import TYPE_CHECKING

import yaml

from dataci.db.run import create_one_run
from dataci.utils import cwd

if TYPE_CHECKING:
    from .run import Run

logger = logging.getLogger(__name__)


def save(run: 'Run'):
    with cwd(run.workdir):
        #####################################################################
        # Step 1: Recover pipeline feat cached file (.dvc) from .lock
        #####################################################################
        with open('dvc.lock', 'r') as f:
            run_cache_lock = yaml.safe_load(f)
        for k, v in run_cache_lock['stages'].items():
            for out in v['outs']:
                logger.info(f'Recover dvc file {out["path"]}.dvc')
                with open(out['path'] + '.dvc', 'w') as f:
                    yaml.safe_dump({
                        'outs': [
                            {
                                'md5': out['md5'],
                                'path': os.path.splitext(out['path'])[0],
                                'size': out['size']
                            }
                        ]
                    }, f)

        #####################################################################
        # Step 2: Publish pipeline output feat
        #####################################################################
        for output in run.pipeline.outputs:
            output.publish()

        #####################################################################
        # Step 3: Publish run object to DB
        #####################################################################
        create_one_run(run.dict())
