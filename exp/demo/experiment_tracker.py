#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 31, 2023
"""
import streamlit as st

from exp.demo.tag_visualizer import tag

st.set_page_config(
    page_title='DataCI Experiment Tracker',
    page_icon='🧊',
    layout='wide',
)

with st.sidebar:
    st.title('Evaluation Leaderboard')
    col1, col2 = st.columns([3, 2])
    with col1:
        st.selectbox('Select Evaluation Dataset', ['yelp_reviews'])
    with col2:
        st.selectbox('Version', ['v23.04'])
    st.markdown('---')

run = {
    'id': 'ea08dfab0b7b45d5b71e65a0c69e13c9',
    'time': '2023/05/30 16:32:14',
    'triggered_by': 'pipeline update',
    'data_input': 'yelp_review',
    'data_input_range': '2023/02/28 - 2023/05/30',
    'pipeline': 'sentiment_analysis@v1',
    'status': 'success',
}

st.title('DataCI Experiment Tracker')

st.write(f'### Run ID {run["id"][:7]} &nbsp;' + tag(run['status'], variation='green'), unsafe_allow_html=True)
overview_tab, pipeline_tab, eval_dataset_tab, output_tab, performance_metrics_tab = st.tabs([
    'Overview', 'Pipeline', 'Eval Dataset', 'Outputs', 'Performance Metrics'])
