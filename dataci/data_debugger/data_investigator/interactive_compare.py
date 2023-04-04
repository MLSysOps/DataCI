#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Apr 04, 2023
"""
import pandas as pd
import streamlit as st

from dataci.data_debugger.data_investigator.sample_visualize import visualize_label, visualize_text
from dataci.dataset import list_dataset, get_dataset


@st.cache_data
def fetch_dataset_versions(dataset_name: str):
    datasets = list_dataset(dataset_name, tree_view=False)
    datasets.sort(key=lambda x: x.create_date, reverse=True)
    return [dataset.version[:7] for dataset in datasets]


@st.cache_data
def fetch_dataset(dataset_name: str, dataset_version: str):
    dataset = get_dataset(dataset_name, version=dataset_version)
    return pd.read_csv(dataset.dataset_files, dtype={dataset.id_column: str})


# Page config
st.set_page_config(layout='wide')

# Sidebar
st.sidebar.title('Data Flow Trace')
# Benchmark dataset options (dataset name, dataset version)
config_dataset_name = st.sidebar.selectbox('Dataset Name', ['train_data_pipeline:text_aug'])
config_dataset_version = st.sidebar.selectbox('Dataset Version', fetch_dataset_versions(config_dataset_name))

# Main
dataset_df = fetch_dataset(config_dataset_name, config_dataset_version)
dataset_df_raw = dataset_df.head(10)
dataset_df = dataset_df_raw.copy()

st.title('Data Visualization')
with st.expander('Show raw data'):
    st.dataframe(dataset_df_raw, use_container_width=True)
dataset_df['product_name'] = visualize_text(dataset_df['product_name'])
dataset_df['category_lv0'] = visualize_label(dataset_df['category_lv0'])
st.markdown(dataset_df.to_html(escape=False), unsafe_allow_html=True)
