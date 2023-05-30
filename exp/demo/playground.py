#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 14, 2023
"""
import traceback
import uuid
from datetime import datetime
from typing import TYPE_CHECKING

import networkx as nx
import streamlit as st

from dataci.models import Workflow, Stage
from exp.demo.tag_visualizer import tag

if TYPE_CHECKING:
    from pygraphviz import AGraph

if 'workflow_dict' not in st.session_state:
    st.session_state.workflow_dict = None
if 'workflow' not in st.session_state:
    st.session_state.workflow = None
if 'edit_workflow' not in st.session_state:
    st.session_state.edit_workflow = False

if 'ci_workflow_trigger' not in st.session_state:
    st.session_state.ci_workflow_trigger = {
        'status': False,
        'on': None,
        'params': dict(),
    }
if 'run_result' not in st.session_state:
    st.session_state.run_result = dict()

st.set_page_config(
    page_title='DataCI Demo Playground',
    page_icon='🧊',
    layout='wide',
)

st.title('DataCI Demo Playground')


@st.cache_data
def generate_workflow_dag(workflow_dag: dict):
    dag_edgelist = workflow_dag['edge']
    dag_nodes = workflow_dag['node']
    g = nx.DiGraph(dag_edgelist)
    # Add standalone nodes
    for node_id, node in dag_nodes.items():
        g.add_node(node_id)
    dag_agraph: 'AGraph' = nx.nx_agraph.to_agraph(g)
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
        version = dag_nodes[node_id]['version']
        if version and version.isdigit():
            version = f'v{version}'
        agraph_node.attr['label'] = dag_nodes[node_id]['name'] + '@' + version

    return dag_agraph.to_string()


# Info banner if workflow is triggered
if st.session_state.workflow is not None and st.session_state.ci_workflow_trigger['status']:
    st.info(
        f'DataCI Pipeline `{st.session_state.workflow.identifier}` is triggered by '
        f'{st.session_state.ci_workflow_trigger["on"]}.',
        icon='🔍',
    )

config_col, detail_col = st.columns([1, 1])


def on_change_workflow_name():
    if st.session_state.workflow_name == '➕ Create New Workflow':
        workflow = Workflow('default')
        workflow_dict = {
            'None': workflow,
        }
    else:
        # Get version
        workflow_dict = {w.version: w for w in Workflow.find(st.session_state.workflow_name)}
    st.session_state.workflow_dict = workflow_dict
    # Clear previous set workflow
    st.session_state.workflow = None


def on_click_input_data():
    st.session_state.input_data = True


def on_click_edit_workflow():
    st.session_state.edit_workflow = True


def on_click_add_action():
    st.session_state.add_action = True


def on_change_workflow():
    st.session_state.workflow = st.session_state.workflow_dict[st.session_state.workflow_version_select]


def on_click_workflow_manual_trigger():
    st.session_state.ci_workflow_trigger = {
        'status': True,
        'on': 'manual',
        'params': {
            'workflow': st.session_state.workflow,
        }
    }


def on_click_workflow_save():
    st.session_state.ci_workflow_trigger = {
        'status': True,
        'on': 'pipeline save',
        'params': {
            'workflow': st.session_state.workflow,
        }
    }


with config_col:
    # Set input streaming data range
    with st.container():
        col1, col2 = st.columns([18, 6])
        with col1:
            input_data = st.selectbox('Select streaming data name', ['Yelp Review'])
        with col2:
            input_data_range = st.selectbox('Select streaming data range',
                                            ['Last day', 'Last week', 'Last month', 'Last 3 months'])

    with st.container():
        col1, col2 = st.columns([18, 6])
        all_workflows = Workflow.find('*@latest')
        with col1:
            st.selectbox(
                'Select a DataCI Pipeline', [w.name for w in all_workflows],
                key='workflow_name', on_change=on_change_workflow_name,
            )

        if st.session_state.workflow_dict is None:
            on_change_workflow_name()

        with col2:
            st.selectbox(
                'Pipeline version', st.session_state.workflow_dict.keys(),
                key='workflow_version_select', index=len(st.session_state.workflow_dict) - 1,
                on_change=on_change_workflow,
            )
            # Init set
            if st.session_state.workflow is None:
                on_change_workflow()

    with st.container():
        col0, col1, col2, col3 = st.columns([11, 4, 5, 4])
        with col0:
            st.write('### Pipeline DAG')
        with col1:
            st.button('Edit', use_container_width=True, on_click=on_click_edit_workflow)
        with col2:
            st.button('Manual Run', use_container_width=True, on_click=on_click_workflow_manual_trigger)
        with col3:
            st.button('Save', type='primary', use_container_width=True, on_click=on_click_workflow_save)

        if st.session_state.workflow is not None:
            st.graphviz_chart(
                generate_workflow_dag(st.session_state.workflow.dict()['dag']),
                use_container_width=True,
            )

# Run CI/CD workflow, show CI/CD Runs
with config_col:
    if st.session_state.workflow is not None:
        st.write('## Workflow Runs')
        if st.session_state.ci_workflow_trigger['status']:
            # Run CI workflow
            st.session_state.run_result = {
                'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'input_data': f'{input_data} ({input_data_range})',
                'workflow': st.session_state.workflow.identifier,
                'triggered by': st.session_state.ci_workflow_trigger['on'],
                'status': 'pending',
                'stage_status': {
                    stage.name: 'pending' for stage in st.session_state.workflow.stages
                },
                'msg': '',
            }
            with st.spinner('Running CI/CD workflow...'):
                try:
                    st.session_state.workflow.params = st.session_state.ci_workflow_trigger['params']
                    # Run workflow (current not run, use a dummy result)
                    # st.session_state.workflow()
                    st.session_state.run_result['status'] = 'success'
                    st.session_state.run_result['msg'] = 'CI/CD workflow run successfully.'
                except Exception as e:
                    error_stack = traceback.format_exc()
                    st.session_state.run_result['status'] = 'failed'
                    st.session_state.run_result['msg'] = f'{e}\n' + error_stack
                finally:
                    # if a stage runs successfully, there's some output
                    # if a stage fails, output = ...
                    # if a stage not run, no output
                    # for name, output in st.session_state.workflow._outputs.items():
                    #     if output is ...:
                    #         st.session_state.run_result['stage_status'][name] = 'failed'
                    #     else:
                    #         st.session_state.run_result['stage_status'][name] = 'success'
                    # Force set all success
                    for stage_name in st.session_state.run_result['stage_status']:
                        st.session_state.run_result['stage_status'][stage_name] = 'success'
                    st.session_state.workflow._outputs = dict()
                    # Reset trigger
                    st.session_state.ci_workflow_trigger['status'] = False

            # Visualize run result
            col1, col2 = st.columns([1, 3])
            with col1:
                st.write(f'Run ID')
            with col2:
                st.code(uuid.uuid4().hex)
            st.write('Run time: ', st.session_state.run_result['time'])
            st.write('Input data: ', st.session_state.run_result['input_data'])
            st.write('Workflow: ', st.session_state.run_result['workflow'])
            st.write('Triggered by: ', st.session_state.run_result['triggered by'])
            st.markdown('Status: ' + tag(st.session_state.run_result['status']))
            with st.expander('Run Details', expanded=True):
                st.write('Message: ', st.session_state.run_result['msg'])
                st.write('Stage Status: ')
                for stage_name, stage_status in st.session_state.run_result['stage_status'].items():
                    st.write(f'- {stage_name}: {stage_status}')


# Configure Input Data
def on_click_close_edit_btn():
    st.session_state.edit_workflow = False


def on_click_use_stage_btn():
    st.session_state.workflow.patch(STAGE)
    st.session_state.ci_workflow_trigger.update({
        'status': True,
        'on': f'stage &nbsp;`{STAGE.name}`&nbsp; changed',
        'params': {
            'workflow': st.session_state.workflow,
        }
    })
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
                    'Stage Version', versions, index=preselect_idx, key='stage_version_select',
                    format_func=lambda v: v if v != stage_dict[stage_name] else f'{v} (current)',
                )
            STAGE = Stage.get(f'{stage_name}@{stage_version}')
            st.code(STAGE.script, language='python')

            st.button(
                'Use This Stage',
                disabled=stage_version == stage_dict[stage_name],  # current version, don't allow use
                on_click=on_click_use_stage_btn, use_container_width=True
            )
