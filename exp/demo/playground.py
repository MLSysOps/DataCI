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

import dateutil
import networkx as nx
import streamlit as st
from streamlit_extras.mandatory_date_range import date_range_picker
from streamlit_toggle import st_toggle_switch

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
        f'DataCI Pipeline `{st.session_state.workflow.name}@v{st.session_state.workflow.version}` is triggered by '
        f'{st.session_state.ci_workflow_trigger["on"]}.',
        icon='🔍',
    )

config_col, detail_col = st.columns([2, 1])


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
        'on': 'pipeline update',
        'params': {
            'workflow': st.session_state.workflow,
        }
    }


with config_col:
    # Set input streaming data range
    with st.container():
        col1, col2 = st.columns([2, 1])
        with col1:
            input_data = st.selectbox('Select Streaming Data Name', ['yelp_review'])
        with col2:
            # 3 months ago as default start date
            default_start_date = datetime.now() - dateutil.relativedelta.relativedelta(months=3)
            input_data_range = date_range_picker(
                'Data range', default_start=default_start_date, default_end=datetime.now(),
            )

    with st.container():
        col1, col2 = st.columns([2, 1])
        all_workflows = Workflow.find('*@latest')
        with col1:
            st.selectbox(
                'Select a Pipeline', [w.name for w in all_workflows],
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
        col0, col1, col2, col3, col4 = st.columns([6, 6, 4, 4, 4])
        with col0:
            st.write('### Pipeline DAG')
        with col1:
            st_toggle_switch(
                label="Enable DataCI",
                key="enable_dataci",
                default_value=True,
                # inactive_color="#D3D3D3",  # optional
                active_color="#FF4B4B",  # optional
                track_color="#FFCBCB",  # optional
            )
        with col2:
            st.button('Edit', use_container_width=True, on_click=on_click_edit_workflow)
        with col3:
            st.button('Manual Run', use_container_width=True, on_click=on_click_workflow_manual_trigger)
        with col4:
            st.button('Save', type='primary', use_container_width=True, on_click=on_click_workflow_save)

        if st.session_state.workflow is not None:
            st.graphviz_chart(
                generate_workflow_dag(st.session_state.workflow.dict()['dag']),
                use_container_width=True,
            )

with config_col:
    st.divider()

# Run CI/CD workflow, show CI/CD Runs
with config_col:
    if st.session_state.workflow is not None:
        col0, col1, col2, col3 = st.columns([9, 3, 5, 8])
        with col0:
            st.write('### DataCI Pipeline Run')
        with col2:
            st.button('Detailed Result')
        with col3:
            st.button('View in Experiment Tracker', type='primary')
        if st.session_state.ci_workflow_trigger['status']:
            # Run CI workflow
            st.session_state.run_result = {
                'time': datetime.now(),
                'input_data_name': input_data,
                'input_data_range': input_data_range,
                'workflow': f'{st.session_state.workflow.name}@v{st.session_state.workflow.version}',
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
            col1, col2, col3, col4 = st.columns([1, 3, 1, 2])
            with col1:
                st.write(f'Run ID:')
            with col2:
                st.write(tag(uuid.uuid4().hex, variation='blue'), unsafe_allow_html=True)
            with col3:
                st.write('Submit Time:')
            with col4:
                st.write(st.session_state.run_result['time'].strftime('%Y/%m/%d %H:%M:%S'))

            with col1:
                st.write('Triggered by:')
            with col2:
                st.write(tag(st.session_state.run_result['triggered by'], variation='gold'), unsafe_allow_html=True)
            with col3:
                st.write('Status:')
            with col4:
                st.markdown(tag(st.session_state.run_result['status']), unsafe_allow_html=True)

            with col1:
                st.write('Input Data:')
            with col2:
                st.markdown(
                    tag(st.session_state.run_result['input_data_name'], variation='orange') + ' ' + \
                    tag(st.session_state.run_result['input_data_range'][0].strftime('%Y/%m/%d') + ' - ' + \
                        st.session_state.run_result['input_data_range'][1].strftime('%Y/%m/%d'), variation='geekblue'),
                    unsafe_allow_html=True
                )

            with col3:
                st.write('Data Duration:')
            with col4:
                input_date_range_days = (st.session_state.run_result['input_data_range'][1] -
                                         st.session_state.run_result['input_data_range'][0]).days
                st.write(tag(f'{input_date_range_days} days', variation='volcano'), unsafe_allow_html=True)

            with col1:
                st.write('Workflow:')
            with col2:
                st.write(tag(st.session_state.run_result['workflow'], variation='cyan'), unsafe_allow_html=True)

            col1, col2, col3, col4 = st.columns([1, 3, 1, 2])


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
            header_col1, header_col2, header_col3 = st.columns([12, 10, 2])
            with header_col1:
                st.subheader('Edit Pipeline')
            with header_col3:
                st.button('x', use_container_width=True, key='close_edit_btn', on_click=on_click_close_edit_btn)
            # Record current stage version
            stage_dict = dict()
            for stage in st.session_state.workflow.stages:
                stage_dict[stage.name] = stage.version

            # Stage selection
            col1, col2 = st.columns([2, 1])
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

            with header_col2:
                # Set save button on the header
                st.button(
                    'Use This Stage',
                    disabled=stage_version == stage_dict[stage_name],  # current version, don't allow use
                    on_click=on_click_use_stage_btn, use_container_width=True
                )

            st.button(
                'Use This Stage',
                disabled=stage_version == stage_dict[stage_name],  # current version, don't allow use
                on_click=on_click_use_stage_btn, use_container_width=True
            )
