#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 08, 2023
"""
import re
from collections import defaultdict
from typing import TYPE_CHECKING

from dataci.pipeline import Pipeline
from dataci.repo import Repo

if TYPE_CHECKING:
    from dataci.repo import Repo

LIST_PIPELINE_IDENTIFIER_PATTERN = re.compile(
    r'^([\w.*[\]]+?)(?:@([\da-f]{1,40}))?$', re.IGNORECASE
)

LIST_FEAT_IDENTIFIER_PATTERN = re.compile(
    r'^([1-9.*[\]]\d*):([\w.]+?)$', re.IGNORECASE
)


def list_pipeline(repo: Repo, pipeline_identifier):
    """List pipeline with optional pipeline identifier to query.

    Args:
        pipeline_identifier (str): Pipeline identifier. Default to query all pipelines.
    """
    pipeline_identifier = pipeline_identifier or '*'
    matched = LIST_PIPELINE_IDENTIFIER_PATTERN.match(pipeline_identifier)
    if not matched:
        raise ValueError(f'Invalid pipeline identifier {pipeline_identifier}')
    pipeline_name, version = matched.groups()
    version = (version or '').lower() + '*'

    # Check matched pipeline
    pipeline_names = list()
    for folder in repo.pipeline_dir.glob(pipeline_name):
        if folder.is_dir() and not folder.is_symlink():
            pipeline_names.append(folder.name)

    pipelines = list()
    for pipeline_name in pipeline_names:
        for ver in (repo.pipeline_dir / pipeline_name).glob(version):
            if ver.is_dir() and not ver.is_symlink():
                pipeline = Pipeline(name=pipeline_name, version=ver, basedir=repo.pipeline_dir)
                pipeline.restore()
                pipelines.append(pipeline)
    return pipelines


def list_pipeline_feat(repo: 'Repo', pipeline_identifier, feat_identifier=None, tree_view=True):
    if feat_identifier is None:
        # feat identifier is provided with the pipeline identifier:
        #   <pipeline_identifier>/<feat_identifier>
        pipeline_identifier, feat_identifier = pipeline_identifier.split('/')

    # Get pipeline
    pipelines = list_pipeline(repo, pipeline_identifier)
    # Get feat info
    matched = LIST_FEAT_IDENTIFIER_PATTERN.match(feat_identifier)
    if not matched:
        raise ValueError(f'Invalid pipeline feat identifier {feat_identifier}')
    run_num, feat_name = matched.groups()

    pipeline_feats_dict = defaultdict(dict)
    pipeline_feats_list = list()
    for pipeline in pipelines:
        feat = pipeline.runs[run_num].feat[feat_name]
        pipeline_feats_dict[pipeline.name][feat_name] = feat
        pipeline_feats_list.append(feat)
    return pipeline_feats_dict if tree_view else pipeline_feats_list


if __name__ == '__main__':
    print(list_pipeline_feat(repo=Repo(), pipeline_identifier='train_text*/1:text_augmentation.csv', tree_view=False))
