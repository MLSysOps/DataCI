#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 23, 2023
"""
from pathlib import Path

import pandas as pd
import augly.text as textaugs

from dataci.decorators import stage


@stage
def text_augmentation(df):
    aug_function = textaugs.OneOf([
        textaugs.ReplaceSimilarUnicodeChars(),
        textaugs.SimulateTypos(),
    ])
    df['text'] = aug_function(df['text'].tolist())

    return df


if __name__ == '__main__':
    # Parameters
    exp_time = '2021Q4'
    prefix = ''
    data_select_strategy = 'RS'

    # Configurations
    SAVE_DATASET_BASE_PATH = prefix + 'processed/'
    input_path = SAVE_DATASET_BASE_PATH + f'data_select_{exp_time}_{data_select_strategy}.csv'
    save_path = SAVE_DATASET_BASE_PATH + f'data_aug_{exp_time}_{data_select_strategy}.csv'
    Path(save_path).parent.mkdir(parents=True, exist_ok=True)

    # Read input data
    df = pd.read_csv(input_path)

    # Run stage: text_augmentation
    data_aug_df = text_augmentation.test(df)

    # Save output data
    data_aug_df.to_csv(save_path, index=False)
