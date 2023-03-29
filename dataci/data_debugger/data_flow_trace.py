#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 27, 2023
"""
import pandas as pd
import streamlit as st
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, ColumnsAutoSizeMode

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
        test_input_dataset = benchmark.test_dataset.parent_dataset
        if test_input_dataset is None:
            # does not have a parent dataset for test dataset
            test_input_df = None
        else:
            test_input_df = pd.read_csv(test_input_dataset.dataset_files, dtype={test_input_dataset.id_column: str})

        # Output dataset
        train_output_dataset = benchmark.train_dataset
        train_output_df = pd.read_csv(train_output_dataset.dataset_files, dtype={train_output_dataset.id_column: str})
        test_output_dataset = benchmark.test_dataset
        test_output_df = pd.read_csv(test_output_dataset.dataset_files, dtype={test_output_dataset.id_column: str})

        # Benchmark result
        train_benchmark_pred = benchmark.get_prediction('train')
        train_benchmark_pred['split'] = 'train'
        val_benchmark_pred = benchmark.get_prediction('val')
        val_benchmark_pred['split'] = 'val'
        train_benchmark_pred = pd.concat([train_benchmark_pred, val_benchmark_pred], axis=0)
        test_benchmark_pred = benchmark.get_prediction('test')

        traced_data_dict[(benchmark.type, benchmark.ml_task)] = {
            'train_input': train_input_df,
            'train_output': train_output_df,
            'train_benchmark_pred': train_benchmark_pred,
            'test_input': test_input_df,
            'test_output': test_output_df,
            'test_benchmark_pred': test_benchmark_pred,
        }
    return traced_data_dict


def random_select_rows(df: pd.DataFrame, n: int):
    selected_data = df.sample(n=n)
    st.session_state.selected_data = selected_data.index.tolist()


# Init state
if 'selected_data' not in st.session_state:
    st.session_state.selected_data = []

# Page config
st.set_page_config(layout='wide')

# Sidebar
st.sidebar.title('Data Flow Trace')
# Benchmark dataset options (dataset name, dataset version)
config_dataset_name = st.sidebar.selectbox('Dataset Name', ['train_data_pipeline:text_aug'])
config_dataset_version = st.sidebar.selectbox('Dataset Version', ['de785bdae', '473f3bc', '02e2df9', 'fb3d5e4'])

# Benchmark type and ML task options
config_benchmark_type = st.sidebar.selectbox('Benchmark Type', ['data_augmentation'])
config_ml_task = st.sidebar.selectbox('ML Task', ['text_classification'])
config_view_split = st.sidebar.selectbox('Split', ['train', 'test'])

# Fetch data
traced_df = fetch_traced_data(
    config_dataset_name, config_dataset_version
)[(config_benchmark_type, config_ml_task)]

input_df = traced_df['train_input']
output_df = traced_df['train_output']
train_benchmark_pred_df = traced_df['train_benchmark_pred']

gb = GridOptionsBuilder.from_dataframe(input_df)
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
gb.configure_default_column(
    groupable=True, value=True, enableRowGroup=True, autoHeight=True, wrapText=True,
)
gb.configure_selection(
    'multiple', use_checkbox=True, groupSelectsChildren=True, pre_selected_rows=st.session_state.selected_data,
)

st.subheader('Select Data to Trace')
with st.container():
    button_texts = [
        'Random Select 10 Data', 'Select 10 True Positive Data',
        'Select 10 False Positive Data', 'Select 10 True Negative Data', 'Select 10 False Negative Data'
    ]
    draw_data_button_groups = st.columns(len(button_texts))
    for button, button_text in zip(draw_data_button_groups, button_texts):
        with button:
            # random select 10 data in input df
            st.button(button_text, on_click=lambda: random_select_rows(input_df, 10))

response = AgGrid(
    input_df,
    theme="streamlit",
    gridOptions=gb.build(),
    update_mode=GridUpdateMode.NO_UPDATE,
    try_to_convert_back_to_original_types=False,
    columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
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
