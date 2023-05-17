#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: May 14, 2023
"""
import traceback
from datetime import datetime
from typing import TYPE_CHECKING

import networkx as nx
import pandas as pd
import streamlit as st

from dataci.models import Workflow, Stage, Dataset

if TYPE_CHECKING:
    from pygraphviz import AGraph

if 'workflow' not in st.session_state:
    st.session_state.workflow = None
if 'input_data' not in st.session_state:
    st.session_state.input_data = False
if 'edit_workflow' not in st.session_state:
    st.session_state.edit_workflow = False
if 'add_action' not in st.session_state:
    st.session_state.add_action = False

if 'ci_workflow' not in st.session_state:
    st.session_state.ci_workflow = None
if 'ci_workflow_select' not in st.session_state:
    st.session_state.ci_workflow_select = None
if 'ci_workflow_trigger' not in st.session_state:
    st.session_state.ci_workflow_trigger = {
        'status': False,
        'on': None,
        'params': dict(),
    }
if 'add_job' not in st.session_state:
    st.session_state.add_job = False
if 'use_stage' not in st.session_state:
    st.session_state.use_stage = None
if 'ci_workflow_runs' not in st.session_state:
    st.session_state.ci_workflow_runs = list()

st.set_page_config(
    page_title='DataCI',
    page_icon='🧊',
    layout='wide',
)


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
        if version.isdigit():
            version = f'v{version}'
        agraph_node.attr['label'] = dag_nodes[node_id]['name'] + '@' + version

    return dag_agraph.to_string()


# Info banner if workflow is triggered
if st.session_state.ci_workflow is not None and st.session_state.ci_workflow_trigger['status']:
    st.info(
        f'CI/CD Action `{st.session_state.ci_workflow.identifier}` is triggered by '
        f'{st.session_state.ci_workflow_trigger["on"]}.',
        icon='🔍',
    )

config_col, detail_col = st.columns([1, 1])


def on_change_workflow_select():
    if st.session_state.workflow_select == '':
        return

    if st.session_state.workflow_select == '➕ Create New Workflow':
        workflow = Workflow('default')
    else:
        workflow = Workflow.get(st.session_state.workflow_select)

    st.session_state.workflow = workflow


def on_click_input_data():
    st.session_state.input_data = True


def on_click_edit_workflow():
    st.session_state.edit_workflow = True


def on_click_add_action():
    st.session_state.add_action = True


def on_change_workflow():
    st.session_state.workflow = workflow_dict[st.session_state.workflow_version_select]


with config_col:
    st.header('Data Processing Workflow')
    with st.container():
        col1, col2 = st.columns([18, 6])
        all_workflows = Workflow.find('*@latest')
        with col1:
            workflow_name = st.selectbox('Select a workflow', [w.name for w in all_workflows])

        # Get version
        workflow_dict = {w.version: w for w in Workflow.find(workflow_name)}
        with col2:
            st.selectbox(
                'Version', workflow_dict.keys(),
                key='workflow_version_select', index=len(workflow_dict) - 1,
                on_change=on_change_workflow,
            )
            # Init set
            if st.session_state.workflow is None:
                on_change_workflow()

    with st.container():
        col0, col1, col2, col3 = st.columns([10, 5, 4, 5])
        with col0:
            st.subheader('Workflow DAG Graph')
        with col1:
            st.button('Input Data', use_container_width=True, on_click=on_click_input_data)
        with col2:
            st.button('Edit', use_container_width=True, on_click=on_click_edit_workflow)
        with col3:
            st.button('Add Action', type='primary', use_container_width=True, on_click=on_click_add_action)

        if st.session_state.workflow is not None:
            st.graphviz_chart(
                generate_workflow_dag(st.session_state.workflow.dict()['dag']),
                use_container_width=True,
            )


def on_click_add_job():
    st.session_state.add_job = True


def on_change_ci_workflow_select():
    if st.session_state.ci_workflow_select == '':
        st.session_state.ci_workflow = None
    elif st.session_state.ci_workflow_select == '➕ Create New Action':
        st.session_state.ci_workflow = Workflow('default_ci')
    else:
        st.session_state.ci_workflow = workflow_dict[st.session_state.ci_workflow_select]


def on_click_ci_workflow_manual_trigger():
    st.session_state.ci_workflow_trigger = {
        'status': True,
        'on': 'manual',
        'params': {
            'workflow': st.session_state.workflow,
        }
    }


# Configure CI/CD Actions
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

        workflow_dict = {
            w.name: w for w in Workflow.find('*ci@latest')
        }
        st.selectbox(
            'Select a CI Workflow',
            ['', '➕ Create New Action'] + list(workflow_dict.keys()),
            index=0,
            key='ci_workflow_select',
            on_change=on_change_ci_workflow_select,
        )
        if st.session_state.ci_workflow is not None:
            with st.container():
                col1, col2, col3 = st.columns([15, 4, 5])
                with col1:
                    st.subheader('CI/CD DAG Graph')
                with col2:
                    st.button('Add Job', use_container_width=True, on_click=on_click_add_job)
                with col3:
                    st.button(
                        'Manual Run', type='primary', use_container_width=True,
                        on_click=on_click_ci_workflow_manual_trigger,
                    )
                st.graphviz_chart(
                    generate_workflow_dag(st.session_state.ci_workflow.dict()['dag']),
                    use_container_width=True,
                )

# Run CI/CD workflow, show CI/CD Runs
with config_col:
    if st.session_state.ci_workflow is not None and st.session_state.workflow is not None:
        st.subheader('CI/CD Runs')
        if st.session_state.ci_workflow_trigger['status']:
            # Run CI workflow
            with st.spinner('Running CI/CD workflow...'):
                ci_run_result = dict()
                try:
                    ci_run_result['time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ci_run_result['workflow'] = str(st.session_state.workflow.dict())
                    ci_run_result['triggered by'] = st.session_state.ci_workflow_trigger['on']
                    # Add each job status
                    for stage in st.session_state.ci_workflow.stages:
                        ci_run_result[stage.name] = 'pending'
                    st.session_state.ci_workflow.params = st.session_state.ci_workflow_trigger['params']
                    st.session_state.ci_workflow()
                    ci_run_result['status'] = 'success'
                    ci_run_result['msg'] = 'CI/CD workflow run successfully.'
                except Exception as e:
                    error_stack = traceback.format_exc()
                    ci_run_result['status'] = 'failed'
                    ci_run_result['msg'] = f'{e}\n' + error_stack
                finally:
                    # if a stage runs successfully, there's some output
                    # if a stage fails, output = ...
                    # if a stage not run, no output
                    for name, output in st.session_state.ci_workflow._outputs.items():
                        if output is ...:
                            ci_run_result[name] = 'failed'
                        else:
                            ci_run_result[name] = 'success'
                    st.session_state.ci_workflow._outputs = dict()
                    st.session_state.ci_workflow_runs.append(ci_run_result)
                    # Reset trigger
                    st.session_state.ci_workflow_trigger['status'] = False
        if st.session_state.ci_workflow_runs:
            st.dataframe(st.session_state.ci_workflow_runs)


# Configure Input Data
def on_click_close_input_btn():
    st.session_state.input_data = False


def on_click_use_input_data_btn():
    st.session_state.workflow.params['input_data'] = dataset.identifier
    # If CI workflow is setup, trigger CI workflow
    if st.session_state.ci_workflow:
        st.session_state.ci_workflow_trigger.update({
            'status': True,
            'on': 'Input Data Changed',
            'params': {
                'workflow': st.session_state.workflow,
            }
        })
    st.session_state.input_data = False


with detail_col:
    if st.session_state.input_data and st.session_state.workflow is not None:
        with st.expander('', expanded=True):
            col1, col2 = st.columns([23, 1])
            with col1:
                st.subheader('Input Data')
            with col2:
                st.button('✖', use_container_width=True, key='close_input_btn', on_click=on_click_close_input_btn)

            cur_dataset_str = st.session_state.workflow.params.get('input_data', None)
            cur_dataset = Dataset.find(cur_dataset_str)[0]
            col1, col2 = st.columns([18, 6])
            datasets = Dataset.find('*@latest')
            with col1:
                input_data = st.selectbox(
                    'Input Dataset', datasets,
                    format_func=lambda d: d.name if d.name != cur_dataset.name else f'{d.name} (current)',
                    index=list(map(lambda d: d.name, datasets)).index(cur_dataset.name)
                )
                versions = Dataset.find(f'{input_data.full_name}@*')
                with col2:
                    dataset = st.selectbox(
                        'Version', versions,
                        format_func=lambda
                            d: d.version if d.version != cur_dataset.version else f'{d.version} (current)',
                        index=list(map(lambda d: d.version, versions)).index(cur_dataset.version)
                    )
                st.dataframe(pd.read_csv(dataset.dataset_files, nrows=10))
                st.write(f'Size: {dataset.size}')
                st.button(
                    'Use This Dataset', use_container_width=True, on_click=on_click_use_input_data_btn,
                    disabled=dataset.identifier == cur_dataset.identifier,
                )


def on_click_close_edit_btn():
    st.session_state.edit_workflow = False


def on_click_use_stage_btn():
    st.session_state.workflow.patch(STAGE)
    if st.session_state.ci_workflow:
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
                    'Version', versions, index=preselect_idx, key='stage_version_select',
                    format_func=lambda v: v if v != stage_dict[stage_name] else f'{v} (current)',
                )
            STAGE = Stage.get(f'{stage_name}@{stage_version}')
            st.code(STAGE.script, language='python')

            st.button(
                'Use This Stage',
                disabled=stage_version == stage_dict[stage_name],  # current version, don't allow use
                on_click=on_click_use_stage_btn, use_container_width=True
            )


def on_click_close_action_btn():
    st.session_state.add_job = False


def on_change_use_stage_name():
    st.session_state.use_stage = stage_dict[st.session_state.use_stage_name]


def on_click_add_job_to_action():
    last_job = None
    # Iterate through stages to find the last job
    for s in st.session_state.ci_workflow.stages:
        last_job = s
    # If last job is None, add a new job to workflow
    with st.session_state.ci_workflow:
        if last_job is None:
            st.session_state.use_stage.add_self()
        else:
            last_job.add_downstream(st.session_state.use_stage)
    # Refresh ci workflow dag
    # st.session_state.ci_workflow_dag = st.session_state.ci_workflow.dict()['dag']
    # Close Add Job Modal
    st.session_state.add_job = False


# Configure Action Job
with detail_col:
    if st.session_state.add_job:
        with st.expander('', expanded=True):
            col1, col2 = st.columns([23, 1])
            with col1:
                st.subheader('Add Job to Actions')
            with col2:
                st.button(
                    '✖', use_container_width=True, key='close_action_job_btn',
                    on_click=on_click_close_action_btn,
                )
            st.write('Create a job (CI/CD stage) from Action Hub')
            st.caption(
                'You can also create a custom job by publish a job/stage to Action Hub'
            )
            stage_dict = {s.name: s for s in Stage.find(f'official.*@latest')}
            st.selectbox(
                'Job Name', stage_dict.keys(), index=0, key='use_stage_name',
                on_change=on_change_use_stage_name,
            )
            # Initialize use_stage
            if st.session_state.use_stage is None:
                st.session_state.use_stage = stage_dict[st.session_state.use_stage_name]

            st.code(st.session_state.use_stage.script, language='python')
            st.button('Add This Job to Actions', use_container_width=True, on_click=on_click_add_job_to_action)
