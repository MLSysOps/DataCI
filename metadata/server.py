#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 20, 2023
"""
from typing import Union

from fastapi import APIRouter, FastAPI

from dataci.models import Run as RunModel, Lineage
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
        job_type = 'stage'
        # Get job
        job_workspace, job_name, job_version = name_parts
        job_version, stage_name = job_version.split('.')
    else:
        job_type = 'workflow'
        job_workspace, job_name, job_version = name_parts
        stage_name = None

    # If event type is START, create a new run
    if event.eventType == RunState.START:
        run = RunModel(
            name=str(event.run.runId),
            status=event.eventType.value,
            job={
                'workspace': job_workspace,
                'type': job_type,
                'name': job_name,
                'version': job_version,
                'stage_name': stage_name,
            },
            create_time=event.eventTime,
        )
        run.save()
    else:
        run = RunModel(
            name=str(event.run.runId),
            status=event.eventType.value,
            job={
                'workspace': job_workspace,
                'type': job_type,
                'name': job_name,
                'version': job_version,
                'stage_name': stage_name,
            },
            update_time=event.eventTime,
        )
        run.update()

    # get parent run if exists
    if 'parent' in event.run.facets:
        parent_run_config = {
            'name': str(event.run.facets['parent'].run['runId']),
        }
    else:
        parent_run_config = None

    # Get input and output dataset
    # Inputs: event.run.facets['unknownSourceAttribute'].unknownItems[0]['properties']['input_table']
    # Outputs: event.run.facets['unknownSourceAttribute'].unknownItems[0]['properties']['output_table']
    unknown_src_attr = event.run.facets.get('unknownSourceAttribute', object())
    ops_props = (getattr(unknown_src_attr, 'unknownItems', None) or [dict()])[0].get('properties', dict())
    inputs = list(ops_props.get('input_table', dict()).values())
    outputs = list(ops_props.get('output_table', dict()).values())

    lineage = Lineage(
        run=run,
        parent_run=parent_run_config,
        inputs=inputs,
        outputs=outputs,
    )
    lineage.save()

    return {'status': 'success'}


app.include_router(api_router)


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='localhost', port=8000)
