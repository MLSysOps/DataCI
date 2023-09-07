#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Apr 04, 2023
"""
import pandas as pd


def feature_distribution(feature):
    """feature distribution"""
    pass


def statistics(df: pd.DataFrame):
    """Get statistics of a dataset
    Min, max, mean, median, mode, standard deviation, variance, skewness, kurtosis
    """
    statistics_data = dict()
    for col_name in df.columns:
        feature = df[col_name]
        # Common statistics of a feature
        # 1. Data type
        data_type = type(feature.head(1).item())
        # 2. Total count
        total = feature.count()
        # 3. Unique count
        unique = feature.nunique()
        # 4. Missing value count
        missing = feature.isna().sum()
        statistics_data[col_name] = {
            'data_type': data_type.__name__,
            'total': total,
            'unique': unique,
            'missing': missing,
        }
        # 5. Feature distribution
        if data_type in (str,) and unique < 100:
            # Categorical feature
            statistics_data[col_name]['opt_type'] = ['categorical', 'plain text']
            statistics_data[col_name]['distribution'] = feature.value_counts().to_dict()
        elif data_type in (int, float):
            # Numerical feature
            statistics_data[col_name]['opt_type'] = ['numerical']
            statistics_data[col_name]['distribution'] = {
                'min': feature.min(),
                'max': feature.max(),
                'mean': feature.mean(),
                'median': feature.median(),
                'mode': feature.mode().item(),
                'std': feature.std(),
                'var': feature.var(),
                'skew': feature.skew(),
                'kurt': feature.kurt(),
            }
        else:
            # Unsupported feature
            statistics_data[col_name]['opt_type'] = ['plain text']
            statistics_data[col_name]['distribution'] = {}

    return statistics_data
