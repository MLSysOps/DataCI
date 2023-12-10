#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 20, 2023
"""
from typing import Union

from fastapi import APIRouter, FastAPI

from dataci.models import Run as RunModel, Lineage, Stage
from metadata.models import RunEvent, DatasetEvent, JobEvent, RunState

app = FastAPI()

api_router = APIRouter(prefix='/api/v1')


# Record lineage information as schema defined as OpenLineage (2-0-2)
# https://openlineage.io/apidocs/openapi/
@api_router.post('/lineage', summary='Send an event related to the state of a run')
def post_lineage(event: Union[RunEvent, DatasetEvent, JobEvent]):
    """Updates a run state for a job.
    """
    # Skip if event is a test event (event job name cannot parse to workspace, job name and version)
    name_parts = event.job.name.split('--')
    if len(name_parts) != 3:
        return {'status': 'skip'}

    # Parse job type
    if '.' in event.job.name:
        # Get job
        job_workspace, job_name, job_version = name_parts
        job_version, stage_name = job_version.split('.')
        job = Stage.get_by_workflow(stage_name, f'{job_workspace}.{job_name}@{job_version}')
    else:
        job_workspace, job_name, job_version = name_parts
        job = {
            'workspace': job_workspace,
            'type': 'workflow',
            'name': job_name,
            'version': job_version,
        }

    # If event type is START, create a new run
    if event.eventType == RunState.START:
        run = RunModel(
            name=str(event.run.runId),
            status=event.eventType.value,
            job=job,
            create_time=event.eventTime,
        )
        run.save()
    else:
        run = RunModel(
            name=str(event.run.runId),
            status=event.eventType.value,
            job=job,
            update_time=event.eventTime,
        )
        run.update()

    # get parent run if exists
    if 'parent' in event.run.facets:
        parent_run_config = {
            'name': str(event.run.facets['parent'].run['runId']),
            'type': 'run',
        }
    else:
        parent_run_config = None

    # Get input and output dataset
    # Inputs: event.run.facets['unknownSourceAttribute'].unknownItems[0]['properties']['input_table']
    # Outputs: event.run.facets['unknownSourceAttribute'].unknownItems[0]['properties']['output_table']
    unknown_src_attr = event.run.facets.get('unknownSourceAttribute', object())
    ops_props = (getattr(unknown_src_attr, 'unknownItems', None) or [dict()])[0].get('properties', dict())
    # Input tables and parent run are upstream
    inputs = list(ops_props.get('input_table', dict()).values())
    if parent_run_config is not None:
        inputs.append(parent_run_config)
    # Output tables are downstream
    outputs = list(ops_props.get('output_table', dict()).values())

    if len(inputs) > 0:
        upstream_lineage = Lineage(upstream=inputs, downstream=run)
        upstream_lineage.save()
    if len(outputs) > 0:
        downstream_lineage = Lineage(upstream=run, downstream=outputs)
        downstream_lineage.save()

    return {'status': 'success'}


app.include_router(api_router)

if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='localhost', port=8000)
