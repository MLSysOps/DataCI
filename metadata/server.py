#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 20, 2023
"""
from typing import Union

from fastapi import APIRouter, FastAPI

from metadata.models import RunEvent, DatasetEvent, JobEvent

app = FastAPI()

api_router = APIRouter(prefix='/api/v1')


# Record lineage information as schema defined as OpenLineage (2-0-2)
# https://openlineage.io/apidocs/openapi/
@api_router.post('/lineage', summary='Send an event related to the state of a run')
def post_lineage(event: Union[RunEvent, DatasetEvent, JobEvent]):
    """Updates a run state for a job.
    """
    print(event.json())
    return {'status': 'success'}


app.include_router(api_router)


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='localhost', port=8000)
