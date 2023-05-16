#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanming.li@alibaba-inc.com
Date: May 11, 2023
"""
from dataci.decorators.stage import stage


@stage()
def text_aug(inputs):
    import augly.text as txtaugs

    df, text_col = inputs

    transform = txtaugs.Compose(
        [
            txtaugs.InsertWhitespaceChars(p=0.5),
            txtaugs.InsertPunctuationChars(
                granularity="all",
                cadence=5.0,
                vary_chars=True,
            )
        ]
    )
    outputs = transform(df[text_col].tolist())
    df[text_col] = outputs
    return df


text_aug.publish()
