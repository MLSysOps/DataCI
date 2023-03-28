#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 27, 2023
"""
import pandas as pd
import streamlit as st
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode

from dataci.benchmark import list_benchmarks


@st.cache_data
def fetch_traced_data(dataset_name: str, dataset_version: str):
    dataset_identifier = f'{dataset_name}@{dataset_version}'

    # Get benchmark result
    benchmarks = list_benchmarks(dataset_identifier)
    traced_data_dict = dict()

    for benchmark in benchmarks:
        # Visualize data flow
        # Input dataset
        train_input_dataset = benchmark.train_dataset.parent_dataset
        train_input_df = pd.read_csv(train_input_dataset.dataset_files, dtype={train_input_dataset.id_column: str})

        # Output dataset
        train_output_dataset = benchmark.train_dataset
        train_output_df = pd.read_csv(train_output_dataset.dataset_files, dtype={train_output_dataset.id_column: str})
        # Benchmark result
        train_benchmark_pred = benchmark.get_prediction('train')
        val_benchmark_pred = benchmark.get_prediction('val')
        test_benchmark_pred = benchmark.get_prediction('test')

        traced_data_dict[(benchmark.type, benchmark.ml_task)] = {
            'train_input': train_input_df.head(10),
            'train_output': train_output_df.head(10),
            'train_benchmark_pred': train_benchmark_pred.head(10),
            'val_benchmark_pred': val_benchmark_pred.head(10),
            'test_benchmark_pred': test_benchmark_pred.head(10),
        }
    return traced_data_dict


# Init session state
if 'traced_ids' not in st.session_state:
    st.session_state.traced_ids = list()

traced_df = fetch_traced_data('train_data_pipeline:text_aug', 'de785bdae')[('data_augmentation', 'text_classification')]

input_df = traced_df['train_input']
gb = GridOptionsBuilder.from_dataframe(input_df)
gb.configure_default_column(groupable=True, value=True, enableRowGroup=True)
gb.configure_selection('multiple', use_checkbox=True, groupSelectsChildren=True)

grid_options = gb.build()

output_df = traced_df['train_output']
train_benchmark_pred_df = traced_df['train_benchmark_pred']
val_benchmark_pred_df = traced_df['val_benchmark_pred']

st.title('Data Flow Trace')
response = AgGrid(
    input_df,
    theme="streamlit",
    key='id',
    gridOptions=grid_options,
    update_mode=GridUpdateMode.GRID_CHANGED,
    allow_unsafe_jscode=True,
    fit_columns_on_grid_load=True,
    reload_data=False,
    try_to_convert_back_to_original_types=False
)

ids = [row['id'] for row in response['selected_rows']]

st.text('Input Data')
st.dataframe(input_df[input_df['id'].isin(ids)])
st.text('>>> Data Augmentation Pipeline >>>')
st.text('Output Data')
st.dataframe(output_df[output_df['id'].isin(ids)])
st.text('>>> Model Training >>>')
st.text('Train Prediction')
st.dataframe(train_benchmark_pred_df[train_benchmark_pred_df['id'].isin(ids)])
st.text('Val Prediction')
st.dataframe(val_benchmark_pred_df[val_benchmark_pred_df['id'].isin(ids)])
