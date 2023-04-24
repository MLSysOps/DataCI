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
from dataci.data_debugger.data_investigator.statistical_analysis import statistics

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


def random_select_rows(df: pd.DataFrame, n: int):
    selected_data = df.sample(n=n)
    st.session_state.selected_data = selected_data.id.tolist()


def on_click_sample_data_btn():
    if selection_method == 'Random Select':
        random_select_rows(dataset_df_raw, num_sampled_data)


# Init session state
if 'selected_data' not in st.session_state:
    st.session_state.selected_data = []

# Page config
st.set_page_config(layout='wide')

# Sidebar
st.sidebar.title('Data Visualization')
# Benchmark dataset options (dataset name, dataset version)
config_dataset_name = st.sidebar.selectbox('Dataset Name', ['train_data_pipeline:text_aug'])
config_dataset_version = st.sidebar.selectbox('Dataset Version', fetch_dataset_versions(config_dataset_name))

# Main
dataset_df_raw = fetch_dataset(config_dataset_name, config_dataset_version)

# Get id column
dataset_df_selected = dataset_df_raw[dataset_df_raw['id'].isin(st.session_state.selected_data)]
dataset_df_viz = dataset_df_selected.copy()

st.title('Samples Analysis')
col1, col2 = st.columns(2)
with col1:
    selection_method = st.selectbox(
        'Selection Method',
        ['Random Select'],
    )
with col2:
    num_sampled_data = st.number_input('Number of Data to sample', min_value=1, max_value=10, value=5, step=1)
submit_button = st.button(label='Sample Data', on_click=on_click_sample_data_btn)

with st.expander('Show raw data'):
    dataset_df_selected_styler = dataset_df_selected.style.hide_index()
    st.markdown(dataset_df_selected_styler.to_html(), unsafe_allow_html=True)
dataset_df_viz['product_name'] = visualize_text(dataset_df_viz['product_name'])
dataset_df_viz['category_lv0'] = visualize_label(dataset_df_viz['category_lv0'])
dataset_df_viz_styler = dataset_df_viz.style.hide_index()
st.markdown(dataset_df_viz_styler.to_html(), unsafe_allow_html=True)
st.empty()

# Statistics Analysis
st.title('Statistics Analysis')
# Tabs
statistics_tab, distribution_tab = st.tabs(['Statistics', 'Feature Distribution'])
# Statistics tab
with statistics_tab:
    statistics_data = statistics(dataset_df_raw)
    for statis_col, (name, data) in zip(st.columns(len(statistics_data)), statistics_data.items()):
        with statis_col:
            st.markdown(f'**{name}**')
            # get distribution statistics
            opt_type = data.pop('opt_type', list())
            distribution_statistics = data.pop('distribution', None)
            # common statistics
            common_statistics_df = pd.DataFrame(
                [
                    ('Data type', data['data_type']),
                    ('Total', data['total']),
                    ('Unique', data['unique']),
                    ('Missing Value', data['missing']),
                ]
            )
            common_statistics_styler = common_statistics_df.style.hide_index().hide_columns() \
                .set_properties(subset=common_statistics_df.columns[-1:], **{'text-align': 'right'}) \
                .format('{:,d}', subset=(common_statistics_df.index[1:], common_statistics_df.columns[-1:])) \
                .set_table_styles([
                dict(selector='td', props=[('width', '100vw'), ('border-width', '0px')]),
            ])
            st.write(common_statistics_styler.to_html(), unsafe_allow_html=True)
            # distribution statistics
            st.selectbox(
                '', map(lambda x: x.title() + ' Column', opt_type),
                key=f'{name}_opt_type_selectbox',
                disabled=len(opt_type) == 1)
