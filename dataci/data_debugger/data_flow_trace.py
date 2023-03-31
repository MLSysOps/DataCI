#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Mar 27, 2023
"""
import pandas as pd
import streamlit as st
from st_aggrid import GridOptionsBuilder, AgGrid, ColumnsAutoSizeMode, JsCode

from dataci.benchmark import list_benchmarks
from dataci.dataset import list_dataset


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


@st.cache_data
def fetch_dataset_versions(dataset_name: str):
    datasets = list_dataset(dataset_name, tree_view=False)
    datasets.sort(key=lambda x: x.create_date, reverse=True)
    return [dataset.version[:7] for dataset in datasets]


def random_select_rows(df: pd.DataFrame, n: int):
    selected_data = df.sample(n=n)
    st.session_state.selected_data = selected_data.id.tolist()


def random_select_multiclass_tp_rows(df: pd.DataFrame, label: str, n: int):
    df = df[(df['label'] == label) & (df['prediction'] == label)]
    selected_data = df.sample(n=min(n, len(df)))
    st.session_state.selected_data = selected_data.id.tolist()


def random_select_multiclass_fp_rows(df: pd.DataFrame, label: str, n: int):
    df = df[(df['label'] != label) & (df['prediction'] == label)]
    selected_data = df.sample(n=min(n, len(df)))
    st.session_state.selected_data = selected_data.id.tolist()


def random_select_multiclass_fn_rows(df: pd.DataFrame, label: str, n: int):
    df = df[(df['label'] == label) & (df['prediction'] != label)]
    selected_data = df.sample(n=min(n, len(df)))
    st.session_state.selected_data = selected_data.id.tolist()


def random_select_multiclass_tn_rows(df: pd.DataFrame, label: str, n: int):
    df = df[(df['label'] != label) & (df['prediction'] != label)]
    selected_data = df.sample(n=min(n, len(df)))
    st.session_state.selected_data = selected_data.id.tolist()


def on_click_sample_data_btn():
    if selection_method == 'Random Select':
        random_select_rows(input_df, num_sampled_data)
    elif selection_method == 'Select By Ground Truth Label':
        handler_fn_mapper = {
            'True Positive': random_select_multiclass_tp_rows,
            'False Positive': random_select_multiclass_fp_rows,
            'False Negative': random_select_multiclass_fn_rows,
            'True Negative': random_select_multiclass_tn_rows,
        }
        handler_fn_mapper[confusion_matrix_type](benchmark_pred_df, data_sample_label, num_sampled_data)


# Init state
if 'selected_data' not in st.session_state:
    st.session_state.selected_data = []

# Page config
st.set_page_config(layout='wide')

# Sidebar
st.sidebar.title('Data Flow Trace')
# Benchmark dataset options (dataset name, dataset version)
config_dataset_name = st.sidebar.selectbox('Dataset Name', ['train_data_pipeline:text_aug'])
config_dataset_version = st.sidebar.selectbox('Dataset Version', fetch_dataset_versions(config_dataset_name))

# Benchmark type and ML task options
config_benchmark_type = st.sidebar.selectbox('Benchmark Type', ['data_augmentation'])
config_ml_task = st.sidebar.selectbox('ML Task', ['text_classification'])
config_view_split = st.sidebar.selectbox('Split', ['train', 'test'])

# Fetch data
traced_df = fetch_traced_data(
    config_dataset_name, config_dataset_version
)[(config_benchmark_type, config_ml_task)]

if config_view_split == 'train':
    input_df = traced_df['train_input']
    output_df = traced_df['train_output']
    benchmark_pred_df = traced_df['train_benchmark_pred']
else:
    input_df = traced_df['test_input']
    output_df = traced_df['test_output']
    benchmark_pred_df = traced_df['test_benchmark_pred']

# Set column __sel as flag for data selection
view_df = input_df.copy()
view_df['__sel'] = view_df['id'].isin(st.session_state.selected_data)
# Sort selected data to the top
view_df.sort_values(by=['__sel'], ascending=False, inplace=True)
gb = GridOptionsBuilder.from_dataframe(input_df)
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
gb.configure_default_column(
    groupable=True, value=True, enableRowGroup=True, autoHeight=True, wrapText=True,
)
cellstyle_jscode = JsCode("""
function(params) {
   if (params.data.__sel) {
       params.node.setSelected(true);
   }
};
""")
gb.configure_column('id', cellStyle=cellstyle_jscode)
gb.configure_selection(
    'multiple', use_checkbox=True, groupSelectsChildren=True, header_checkbox=True,
)

st.header('Select Data to Trace')
with st.container():
    col1, col2 = st.columns(2)
    with col1:
        selection_method = st.selectbox(
            'Selection Method',
            ['Random Select', 'Select By Ground Truth Label'],
        )
    with col2:
        num_sampled_data = st.number_input('Number of Data to sample', min_value=1, max_value=20, value=5, step=1)

    if selection_method == 'Select By Ground Truth Label':
        # Select data by ground truth label
        col1, col2 = st.columns(2)
        with col1:
            # - Input: label appeared in benchmark prediction ground truth
            data_sample_label = st.selectbox('Label', benchmark_pred_df['label'].unique().tolist())
        with col2:
            # - Input: confusion matrix type
            confusion_matrix_type = st.selectbox(
                'Confusion Matrix Type', ['True Positive', 'False Positive', 'False Negative', 'True Negative'],
            )
    submit_button = st.button(label='Sample Data', on_click=on_click_sample_data_btn)

response = AgGrid(
    view_df,
    theme="streamlit",
    gridOptions=gb.build(),
    try_to_convert_back_to_original_types=False,
    allow_unsafe_jscode=True,
    columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
)

ids = [row['id'] for row in response['selected_rows']]

st.header('View Data Flow')
# Traced result
# - Column 1: Traced result display of each stage
# - Column 2: DAG of the data flow
traced_result_col, dag_col = st.columns([16, 8])
with traced_result_col:
    with st.expander('🗂️ Dataset', expanded=True):
        st.dataframe(input_df[input_df['id'].isin(ids)].sort_values('id'))

    with st.expander('🗂️ Dataset', expanded=True):
        st.dataframe(output_df[output_df['id'].isin(ids)].sort_values('id'))

    with st.expander('⚛️ Prediction', expanded=True):
        st.dataframe(benchmark_pred_df[benchmark_pred_df['id'].isin(ids)].sort_values('id'))
with dag_col:
    st.graphviz_chart("""
digraph L {
  node [shape=record style="rounded" color=gray80 fontname="Helvetica,Arial,sans-serif" ixedsize=true width=2.5 height=0.7]
  edge [fontname="Helvetica,Arial,sans-serif" color=royalblue1 arrowsize=0.5 tailclip=true fontsize=8 labeldistance=2]

  n1  [label=<🗂️ | <FONT POINT-SIZE="10.0">DATASET</FONT><BR ALIGN="LEFT"/>text_raw_train@v1>]
  n2  [label=<🗂️ | <FONT POINT-SIZE="10.0">DATASET</FONT><BR ALIGN="LEFT"/>text_classification@v1>]
  n3  [label=<⚛️ | <FONT POINT-SIZE="10.0">PREDICTION</FONT><BR ALIGN="LEFT"/>benchmark@v1>]

  n1 -> n2  [headlabel="📏 Pipeline"]
  n2 -> n3 [headlabel="📊 Data-centric Benchmark"]
}
""",
                      use_container_width=True,
                      )
