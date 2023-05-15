#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 14, 2023
"""
from typing import TYPE_CHECKING

import networkx as nx
import pandas as pd
import streamlit as st

from dataci.models import Workflow, Stage, Dataset

if TYPE_CHECKING:
    from pygraphviz import AGraph

if 'workflow' not in st.session_state:
    st.session_state.workflow = None
if 'ci_workflow' not in st.session_state:
    st.session_state.ci_workflow = None
if 'ci_workflow_select' not in st.session_state:
    st.session_state.ci_workflow_select = None
if 'input_data' not in st.session_state:
    st.session_state.input_data = False
if 'edit_workflow' not in st.session_state:
    st.session_state.edit_workflow = False
if 'add_action' not in st.session_state:
    st.session_state.add_action = False
if 'add_job' not in st.session_state:
    st.session_state.add_job = False

st.set_page_config(
    page_title='DataCI',
    page_icon='🧊',
    layout='wide',
)


@st.cache_data
def generate_workflow_dag(workflow_dag: dict):
    dag_edgelist = workflow_dag['edge']
    dag_nodes = workflow_dag['node']
    dag_agraph: 'AGraph' = nx.nx_agraph.to_agraph(nx.DiGraph(dag_edgelist))
    dag_agraph.graph_attr['rankdir'] = 'LR'
    dag_agraph.node_attr.update({
        'shape': 'record',
        'style': 'rounded',
        'color': 'gray80',
        'fontname': 'Helvetica,Arial,sans-serif',
        'ixedsize': 'true',
        'height': '0.5'
    })
    dag_agraph.edge_attr.update({
        'fontname': 'Helvetica,Arial,sans-serif',
        'color': 'royalblue1',
        'arrowsize': '0.5',
        'penwidth': '1.0'
    })
    for agraph_node in dag_agraph.nodes_iter():
        node_id = int(agraph_node.name)
        agraph_node.attr['label'] = dag_nodes[node_id]['name'] + '@' + dag_nodes[node_id]['version']

    return dag_agraph.to_string()


config_col, detail_col = st.columns([1, 1])

with config_col:
    st.header('Data Processing Workflow')
    with st.container():
        col1, col2 = st.columns([18, 6])
        all_workflows = Workflow.find('*@latest')
        with col1:
            workflow_name = st.selectbox('Select a workflow', [w.name for w in all_workflows])

        # Get version
        workflows = Workflow.find(workflow_name)
        with col2:
            version = st.selectbox(
                'Version', [w for w in workflows],
                key='workflow', index=len(workflows) - 1, format_func=lambda w: w.version,
            )

    with st.container():
        st.subheader('Workflow DAG Graph')
        _, col1, col2, col3 = st.columns([10, 5, 4, 5])
        with col1:
            st.button('Input Data', use_container_width=True, key='input_data')
        with col2:
            st.button('Edit', use_container_width=True, key='edit_workflow')
        with col3:
            st.button('Add Action', type='primary', use_container_width=True, key='add_action')

        if st.session_state.workflow is not None:
            st.graphviz_chart(
                generate_workflow_dag(st.session_state.workflow.dict()['dag']),
                use_container_width=True,
            )


def on_change_ci_workflow_select():
    if st.session_state.ci_workflow_select == '':
        return

    if st.session_state.ci_workflow_select == '➕ Create New Workflow':
        ci_workflow = Workflow('default_ci')
    else:
        ci_workflow = st.session_state.ci_workflow_select
    st.session_state.ci_workflow = ci_workflow


with config_col:
    if st.session_state.add_action and st.session_state.workflow is not None:
        st.header('CI/CD Actions')
        # if add_action_btn:
        # Display CI Actions configuration
        col1, col2, col3 = st.columns([3, 15, 6])
        with col1:
            st.write('Actions apply to')
        with col2:
            st.text_input('Workflow', value=st.session_state.workflow.name, disabled=True)
        with col3:
            st.text_input('Version', value=st.session_state.workflow.version, disabled=True)

        ci_workflow_list = ['', '➕ Create New Action'] + Workflow.find('*ci@latest')
        st.selectbox(
            'Select a CI Workflow',
            ci_workflow_list,
            index=0,
            key='ci_workflow_select',
            format_func=lambda w: w.name if isinstance(w, Workflow) else w,
            on_change=on_change_ci_workflow_select,
        )
        if st.session_state.ci_workflow:
            with st.container():
                _, col2, col3 = st.columns([15, 4, 5])
                with col2:
                    st.button('Add Job', use_container_width=True, key='add_job')
                with col3:
                    add_action_btn = st.button('Manual Run', type='primary', use_container_width=True)
                st.graphviz_chart(
                    generate_workflow_dag(st.session_state.ci_workflow.dict()['dag']),
                    use_container_width=True,
                )
        # ci_run_result = pd.DataFrame([
        #     {'id': 'run1', 'Execute_Workflow': 'success', 'Benchmark_Dataset': 'success', 'Publish_Dataset': 'success',
        #      'time': '2021-05-14 10:00:00', 'Action': ''},
        # ])
        #
        # st.dataframe(ci_run_result)


# Configure Input Data
def on_click_close_input_btn():
    st.session_state.input_data = False


def on_click_set_input_data_btn():
    st.session_state.workflow.params['input_data'] = f'{input_data}@{version}'
    st.session_state.input_data = False


with detail_col:
    if st.session_state.input_data:
        with st.expander('', expanded=True):
            col1, col2 = st.columns([23, 1])
            with col1:
                st.subheader('Input Data')
            with col2:
                st.button('✖', use_container_width=True, key='close_input_btn', on_click=on_click_close_input_btn)

            col1, col2 = st.columns([18, 6])
            datasets = Dataset.find('*@latest')
            with col1:
                input_data = st.selectbox('Input Dataset', [d.name for d in datasets])
            versions = Dataset.find(f'{input_data}@*')
            with col2:
                version = st.selectbox('Version', [v.version for v in versions])
            dataset = next(filter(lambda d: d.version == version, versions))
            st.dataframe(pd.read_csv(dataset.dataset_files, nrows=10))
            st.write(f'Size: {dataset.size}')
            st.button('Use This Dataset', use_container_width=True, on_click=on_click_set_input_data_btn)


# Configure Edit Workflow Stage
def on_click_close_edit_btn():
    st.session_state.edit_workflow = False


def on_click_publish_stage_btn():
    st.session_state.workflow.patch(STAGE)
    st.session_state.edit_workflow = False


with detail_col:
    if st.session_state.edit_workflow and st.session_state.workflow is not None:
        with st.expander('', expanded=True):
            col1, col2 = st.columns([23, 1])
            with col1:
                st.subheader('Edit Workflow')
            with col2:
                st.button('✖', use_container_width=True, key='close_edit_btn', on_click=on_click_close_edit_btn)
            # Record current stage version
            stage_dict = dict()
            for stage in st.session_state.workflow.stages:
                stage_dict[stage.name] = stage.version

            # Stage selection
            col1, col2 = st.columns([18, 6])
            with col1:
                stage_name = st.selectbox('Stage', [s for s in stage_dict.keys()])
            with col2:
                versions = [s.version for s in Stage.find(f'{stage_name}@*')]
                preselect_idx = versions.index(stage_dict[stage_name])
                stage_version = st.selectbox(
                    'Version', versions, index=preselect_idx,
                    format_func=lambda v: v if v != stage_dict[stage_name] else f'{v} (current)',
                )
            STAGE = Stage.get(f'{stage_name}@{stage_version}')
            st.code(STAGE.script, language='python')

            st.button(
                'Use This Stage', disabled=stage_version == stage_dict[stage_name],  # current version, don't publish
                on_click=on_click_publish_stage_btn, use_container_width=True
            )

# Configure Action Job
with detail_col:
    if st.session_state.add_job:
        with st.expander('', expanded=True):
            col1, col2 = st.columns([23, 1])
            with col1:
                st.subheader('Add Job to Actions')
            with col2:
                if st.button('✖', use_container_width=True, key='close_action_job_btn'):
                    ADD_ACTION = False
            st.write('Create a job (CI/CD stage) from Action Hub')
            st.caption(
                'You can also create a custom job by publish a job/stage to Action Hub'
            )
            use = st.selectbox('Job Name', ['data_qc', 'dc_bench'])
            stages = Stage.find(f'official.{use}@*')
