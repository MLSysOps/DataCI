#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 20, 2023
"""
from typing import Union

from fastapi import APIRouter, FastAPI

from dataci.models import Run as RunModel
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

    # Get job
    job_workspace, job_name, job_version = name_parts
    # Parse job type
    if '.' in event.job.name:
        job_type = 'workflow'
    else:
        job_type = 'stage'

    # If event type is START, create a new run
    if event.eventType == RunState.START:
        run = RunModel(
            name=str(event.run.runId),
            status=event.eventType.value,
            job={
                'job_workspace': job_workspace,
                'job_type': job_type,
                'job_name': job_name,
                'job_version': job_version,
            },
            try_num=event.run.facets.get('airflow', {}).get('taskInstance', {}).get('try_number', None),
            create_time=event.eventTime,
        )
        run.save()
    else:
        run = RunModel.get(str(event.run.runId))
    print(run.dict())
    return {'status': 'success'}


app.include_router(api_router)


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='localhost', port=8000)
