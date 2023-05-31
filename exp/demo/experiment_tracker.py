#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 31, 2023
"""
import json
import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from st_aggrid import GridOptionsBuilder, AgGrid, ColumnsAutoSizeMode

from exp.demo.tag_visualizer import tag

st.set_page_config(
    page_title='DataCI Experiment Tracker',
    page_icon='🧊',
    layout='wide',
)

run = {
    'id': 'ea08dfab0b7b45d5b71e65a0c69e13c9',
    'time': '2023/05/30 16:32:14',
    'triggered_by': 'pipeline update',
    'owner': 'Data Scientist 1',
    'eval_dataset': 'yelp_reviews@v23.05',
    'data_input': 'yelp_review',
    'data_input_range': '2023/02/28 - 2023/05/30',
    'pipeline': 'sentiment_analysis@v1',
    'status': 'A/B Testing',
    'model': 'bert-base-uncased',
    'model_version': 'v6',
    'metrics': {
        'val_accuracy': 0.95,
        'val_auc': 0.98,
        'val_loss': 0.0032,
    }
}

@st.cache_data
def get_all_leaderboard_runs():
    exp_runs = pd.DataFrame([{
        'id': 'ea08dfab0b7b45d5b71e65a0c69e13c9'[:7],
        'time': '2023/05/30 16:32:14',
        'pipeline': 'sentiment_analysis@v1',
        'model': 'bert-base-uncased@v6',
        'val_accuracy': 0.95,
        'val_auc': 0.98,
        'val_loss': 0.0032,
    },
        {
            'id': uuid.uuid4().hex[:7],
            'time': '2023/05/30 16:32:14',
            'pipeline': 'sentiment_analysis@v2',
            'model': 'bert-base-uncased@test1',
            'val_accuracy': 0.95,
            'val_auc': 0.98,
            'val_loss': 0.0032,
        },
        {
            'id': uuid.uuid4().hex[:7],
            'time': '2023/05/30 16:32:14',
            'pipeline': 'sentiment_analysis@v3',
            'model': 'bert-base-uncased@test2',
            'val_accuracy': 0.95,
            'val_auc': 0.98,
            'val_loss': 0.0032,
        },
    ])
    return exp_runs


@st.cache_data
def read_metrics_data(metric_path):
    with open(metric_path, 'r') as f:
        return json.load(f)


test_metrics = read_metrics_data('test_metrics.json')
online_service_metrics = read_metrics_data('online_metrics.json')
exp_runs = get_all_leaderboard_runs()


@st.cache_data
def build_ab_test_metrics_data():
    test_metrics_df = [
        {
            'Accuracy': test_metrics['accs'][i],
            'AUC': test_metrics['auc'] + random.random() * 0.01 - 0.005,  # dummy data
            'Time': datetime(2023, 6, 1) + timedelta(hours=i * 2),
        }
        for i in range(len(test_metrics['accs']))
    ]
    online_metrics_df = [
        {
            'Accuracy': online_service_metrics['accs'][i],
            'AUC': online_service_metrics['auc'] + random.random() * 0.01 - 0.005,  # dummy data
            'Time': datetime(2023, 6, 1) + timedelta(hours=i * 2),
        }
        for i in range(len(online_service_metrics['accs']))
    ]
    return pd.DataFrame(test_metrics_df), pd.DataFrame(online_metrics_df)


test_metrics_df, online_metrics_df = build_ab_test_metrics_data()

# For evaluation leaderboard
gb = GridOptionsBuilder.from_dataframe(exp_runs)
gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)
gb.configure_default_column(
    groupable=True, value=True, autoHeight=True,
)
gb.configure_column('id', pinned='left')
gb.configure_selection(
    'single', use_checkbox=True, groupSelectsChildren=True,
    pre_selected_rows=[0],
)
with st.sidebar:
    st.title('Evaluation Leaderboard')
    col1, col2 = st.columns([3, 2])
    with col1:
        st.selectbox('Select Evaluation Dataset', ['yelp_reviews'])
    with col2:
        st.selectbox('Version', ['v23.05'])
    response = AgGrid(
        exp_runs,
        theme="streamlit",
        gridOptions=gb.build(),
        try_to_convert_back_to_original_types=False,
        allow_unsafe_jscode=True,
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
    )

st.title('Experiment Tracker')

col1, col2 = st.columns([1, 1])
with col1:
    st.write(
        '### Run ID' + ('&nbsp;' * 4) + tag(run["id"][:7], variation='blue') + \
        ('&nbsp;') * 4 + tag('success', variation='green'), unsafe_allow_html=True)

st.write('#### Basic Info')

col1, col2, col3 = st.columns(3)
with col1:
    st.write('**Submit Time**:' + ('&nbsp;' * 4) + run["time"])
with col2:
    st.write(f'**Triggered by**:' + ('&nbsp;' * 4) + tag(run['triggered_by'], variation='gold'), unsafe_allow_html=True)
with col3:
    st.write('**Owner**:' + ('&nbsp;' * 4) + run['owner'])

col1, col2 = st.columns([2, 1])
with col1:
    st.write(
        f'**Eval Dataset**:' + ('&nbsp;' * 4) + tag(run['eval_dataset'], variation='orange') + \
        ('&nbsp;' * 4) + '(range ' + tag('2023/04/30 - 2023/05/30', variation='geekblue') + ')', unsafe_allow_html=True)
with col2:
    st.button('Check In Streaming Data Manager')

with col1:
    st.write(f'**Pipeline**:' + ('&nbsp;' * 4) + tag(run['pipeline'], variation='cyan'), unsafe_allow_html=True)
with col2:
    st.button('Check In Pipeline Registry')

with st.container():
    st.write('#### Evaluation Performance')
    col1, col2, col3 = st.columns([3, 4, 3])
    with col1:
        st.write('**Eval Loss**:' + ('&nbsp;' * 4) + tag('0.0023', variation='green'), unsafe_allow_html=True)
    with col2:
        st.write('**Eval Accuracy**:' + ('&nbsp;' * 4) + tag('0.9842', variation='volcano'), unsafe_allow_html=True)
    with col3:
        st.write('**Eval AUC**:' + ('&nbsp;' * 4) + tag('0.9905', variation='red'), unsafe_allow_html=True)

with st.container():
    st.write('#### Service Quality')
    col1, col2, col3 = st.columns([3, 4, 3])
    with col1:
        st.write('**Model**:' + ('&nbsp;' * 4) + tag(run['model'], variation='lime'), unsafe_allow_html=True)
    with col2:
        st.write('**Model Version**:' + ('&nbsp;' * 4) + tag(run['model_version'], variation='blue'),
                 unsafe_allow_html=True)
    with col3:
        st.button('Test Deploy This Model', type='primary', disabled=True)

    col1, col2, col3 = st.columns([3, 4, 3])
    with col1:
        st.write('**Status**:' + ('&nbsp;' * 4) + tag('Testing', variation='blue'), unsafe_allow_html=True)
    with col2:
        st.write('**Test Start Date**:' + ('&nbsp;' * 4) + '2023/06/01 00:00')
    with col3:
        st.write('**Test End Date**:' + ('&nbsp;' * 4) + '2023/06/30 23:59')

    col1, col2 = st.columns(2)
    with col1:
        st.write(
            '**Test Accuracy**:' + ('&nbsp;' * 4) +
            tag(test_metrics['acc'], variation='red') + ('&nbsp;' * 4) +
            f'<span style="color:rgb(255, 43, 43);">↓ {test_metrics["acc"] - online_service_metrics["acc"]:.4}</span>',
            unsafe_allow_html=True
        )
        fig = go.Figure()
        fig.add_traces([
            go.Scatter(
                x=online_metrics_df['Time'], y=online_metrics_df['Accuracy'], name='Online Accuracy',
                fill='tozeroy',
            ),
            go.Scatter(
                x=test_metrics_df['Time'], y=test_metrics_df['Accuracy'], name='Test Accuracy',
                fill='tonexty',
                line=dict(color='purple'),
            ),
        ])
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=200,
            # xaxis={'visible': False, 'showticklabels': False},
            yaxis={'range': [0.80, 1]},
        )
        st.plotly_chart(fig, use_container_width=True, sharing="streamlit", theme="streamlit")
    with col2:
        st.write('**Test AUC**: ' + ('&nbsp;' * 4) + tag(test_metrics['auc'], variation='green') + ('&nbsp;' * 4) +
                 f'<span style="color:rgb(9, 171, 59);">↑ {test_metrics["auc"] - online_service_metrics["auc"]:.4}</span>',
                 unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_traces([
            go.Scatter(
                x=test_metrics_df['Time'], y=test_metrics_df['AUC'], name='Test Accuracy',
                fill='tozeroy',
            ),
            go.Scatter(
                x=online_metrics_df['Time'], y=online_metrics_df['AUC'], name='Online Accuracy',
                fill='tozeroy',
            )]
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=200, yaxis_range=[0.9, 1])
        st.plotly_chart(fig, use_container_width=True, sharing="streamlit", theme="streamlit")
