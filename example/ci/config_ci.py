#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 14, 2023
"""
from typing import TYPE_CHECKING

import networkx as nx
import streamlit as st
from streamlit_ace import st_ace

from dataci.models import Workflow

if TYPE_CHECKING:
    from pygraphviz import AGraph

if 'edit_state' not in st.session_state:
    st.session_state.edit_state = None
if 'action_state' not in st.session_state:
    st.session_state.action_state = {
        'ready': False,
        'name': None,
        'version': None,
    }
if 'action_job_state' not in st.session_state:
    st.session_state.action_job_state = {
        'ready': False,
        'name': None,
        'version': None,
    }

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
            version = st.selectbox('Version', [w.version for w in workflows])
        workflow = next(filter(lambda w: w.version == version, workflows))

    with st.container():
        st.subheader('Workflow DAG Graph')
        col1, col2, col3 = st.columns([15, 4, 5])
        with col2:
            edit_workflow_btn = st.button('Edit', use_container_width=True)
        with col3:
            add_action_btn = st.button('Add Action', type='primary', use_container_width=True)
        st.graphviz_chart(
            generate_workflow_dag(workflow.dict()['dag']),
            use_container_width=True,
        )
        if edit_workflow_btn:
            st.session_state.edit_state = workflow
        if add_action_btn:
            st.session_state.action_state['ready'] = True
            st.session_state.action_state['name'] = workflow.name
            st.session_state.action_state['version'] = workflow.version

    st.header('CI/CD Actions')
    # if add_action_btn:
    # Display CI Actions configuration
    col1, col2, col3 = st.columns([3, 15, 6])
    with col1:
        st.write('Actions apply to')
    with col2:
        st.text_input('Workflow', value=st.session_state.action_state['name'], disabled=True)
    with col3:
        st.text_input('Version', value=st.session_state.action_state['version'], disabled=True)

    st.multiselect(
        'Triggered Condition', ['Dataset Publish', 'Text_Aug Publish'],
        disabled=not st.session_state.action_state['ready'],
    )
    ci_cd_workflow_selection = st.selectbox(
        'CI/CD Workflow', ['', '➕ Create New Action', 'text_dataset_ci@latest'],
        disabled=not st.session_state.action_state['ready'],
    )
    if ci_cd_workflow_selection == '':
        st.stop()
    if ci_cd_workflow_selection == '➕ Create New Action':
        ci_cd_workflow = Workflow('ci_cd_workflow')
    else:
        ci_cd_workflow = Workflow.get(ci_cd_workflow_selection)
    with st.container():
        _, col2, col3 = st.columns([15, 4, 5])
        with col2:
            if st.button('Add Job', use_container_width=True):
                st.session_state.action_job_state['ready'] = True
                st.session_state.action_job_state['name'] = ci_cd_workflow.name
                st.session_state.action_job_state['version'] = ci_cd_workflow.version
        with col3:
            add_action_btn = st.button('Manual Run', type='primary', use_container_width=True)
        st.graphviz_chart(
            generate_workflow_dag(ci_cd_workflow.dict()['dag']),
            use_container_width=True,
        )
    # ci_run_result = pd.DataFrame([
    #     {'id': 'run1', 'Execute_Workflow': 'success', 'Benchmark_Dataset': 'success', 'Publish_Dataset': 'success',
    #      'time': '2021-05-14 10:00:00', 'Action': ''},
    # ])
    #
    # st.dataframe(ci_run_result)

# Configure Workflow Stage
with detail_col:
    if st.session_state.edit_state is not None:
        workflow = st.session_state.edit_state
        with st.expander('', expanded=True):
            col1, col2 = st.columns([23, 1])
            with col1:
                st.subheader('Edit Workflow')
            with col2:
                if st.button('✖', use_container_width=True):
                    st.session_state.edit_state = None
            stage_dict = dict()
            for stage in workflow.stages:
                stage_dict[f'{stage.name}@{stage.version}'] = stage
            stage_name = st.selectbox('Stage', stage_dict.keys())
            cur_stage = stage_dict[stage_name]
            new_code = st_ace(
                value=cur_stage.script,
                language='python',
                theme='tomorrow',
                keybinding='vscode',
                auto_update=True,
            )
            if cur_stage.script == new_code:
                stage_publish_btn_disabled = True

            if st.button('Publish', disabled=stage_publish_btn_disabled):
                stage._script = new_code
                workflow.patch(stage)
                # st.session_state.edit_state.publish()
                st.session_state.edit_state = None

# Configure Action Job
with detail_col:
    if st.session_state.action_job_state['ready']:
        with st.expander('', expanded=True):
            col1, col2 = st.columns([23, 1])
            with col1:
                st.subheader('Add Job to Actions')
            with col2:
                if st.button('✖', use_container_width=True):
                    st.session_state.action_job_state = {
                        'ready': False,
                        'name': None,
                        'version': None,
                    }
            use = st.selectbox('Use from Action Hub', ['data_qc', 'dc_bench'])
