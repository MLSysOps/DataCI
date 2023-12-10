#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Nov 20, 2023
"""
import re
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
from uuid import UUID

from packaging import version
from pydantic import BaseModel, Field, AnyUrl, Extra, root_validator, validator

SCHEMA_VERSION = "2-0-2"
SCHEMA_PATH_PATTERN = re.compile(r'^/spec/(\d+)-(\d+)-(\d+)/OpenLineage.json$')


def parse_schema_version(schema_url: AnyUrl) -> str:
    match = SCHEMA_PATH_PATTERN.match(schema_url.path)
    if match:
        major, minor, micro = match.groups()
        return f"{major}-{minor}-{micro}"
    raise ValueError(f"Invalid schema url: {schema_url}")


class BaseEvent(BaseModel):
    eventTime: datetime = Field(default_factory=datetime.utcnow, description="the time the event occurred at")
    producer: AnyUrl = Field(
        description="URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha",
        example="https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
    )
    schemaURL: AnyUrl = Field(
        description="The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this RunEvent",
        example="https://openlineage.io/spec/0-0-1/OpenLineage.json",
    )

    @validator("schemaURL")
    def check_schema_version(cls, v):
        schema_version = parse_schema_version(v)
        if version.parse(schema_version.replace('-', '.')) <= \
            version.parse(SCHEMA_VERSION.replace('-', '.')):
            return v
        raise ValueError(f"Invalid schema version: {v}")

class BaseFacet(BaseModel, extra=Extra.allow):
    """all fields of the base facet are prefixed with _ to avoid name conflicts in facets"""

    _producer: AnyUrl = Field(
        description="URI identifying the producer of this metadata. For example this could be a git url with a given tag or sha",
        example="https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client"
    )
    _schemaURL: AnyUrl = Field(
        description="The JSON Pointer (https://tools.ietf.org/html/rfc6901) URL to the corresponding version of the schema definition for this facet",
        example="https://openlineage.io/spec/1-0-2/OpenLineage.json#/$defs/BaseFacet"
    )

class RunFacet(BaseFacet):
    """A Run Facet"""

class Run(BaseModel):
    runId: UUID = Field(description="The globally unique ID of the run associated with the job.")
    facets: Optional[Dict[Any, RunFacet]] = Field(
        default_factory=dict,
        description="The run facets.",
    )


class JobFacet(BaseFacet):
    """A Job Facet"""
    _deleted: bool = Field(
        description="set to true to delete a facet",
    )


class DatasetFacet(BaseFacet):
    """A Dataset Facet"""
    _deleted: bool = Field(
        description="set to true to delete a facet",
    )


class InputDatasetFacet(DatasetFacet):
    """An Input Dataset Facet"""


class OutputDatasetFacet(DatasetFacet):
    """An Output Dataset Facet"""


class Job(BaseModel):
    namespace: str = Field(description="The namespace containing that job", example="my-scheduler-namespace")
    name: str = Field(description="The unique name for that job within that namespace", example="myjob.mytask")
    facets: Optional[Dict[Any, JobFacet]] = Field(
        default_factory=dict,
        description="The job facets.",
    )

class Dataset(BaseModel):
    namespace: str = Field(description="The namespace containing that dataset", example="my-datasource-namespace")
    name: str = Field(description="The unique name for that dataset within that namespace", example="instance.schema.table")
    facets: Optional[Dict[Any, Any]] = Field(
        default_factory=dict,
        description="The facets for this dataset",
    )


class StaticDataset(Dataset):
    """A Dataset sent within static metadata events"""


class InputDataset(Dataset):
    """An input dataset"""
    inputFacets: Optional[Dict[Any, InputDatasetFacet]] = Field(
        default_factory=dict,
        description="The input facets for this dataset.",
    )


class OutputDataset(Dataset):
    """An output dataset"""
    outputFacets: Optional[Dict[Any, OutputDatasetFacet]] = Field(
        default_factory=dict,
        description="The output facets for this dataset",
    )


class RunState(Enum):
    START = "START"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    ABORT = "ABORT"
    FAIL = "FAIL"
    OTHER = "OTHER"


class RunEvent(BaseEvent):
    eventType: RunState = Field(
        description="the current transition of the run state. It is required to issue 1 START event and 1 of [ COMPLETE, ABORT, FAIL ] event per run. Additional events with OTHER eventType can be added to the same run. For example to send additional metadata after the run is complete",
        example="START|RUNNING|COMPLETE|ABORT|FAIL|OTHER",
        # enum=["START", "RUNNING", "COMPLETE", "ABORT", "FAIL", "OTHER"],
    )
    run: Run
    job: Job
    inputs: Optional[List[InputDataset]] = Field(default_factory=list, description="The set of **input** datasets.")
    outputs: Optional[List[OutputDataset]] = Field(default_factory=list, description="The set of **output** datasets.")


class DatasetEvent(BaseEvent):
    dataset: StaticDataset

    @root_validator
    def check_not_required(cls, values):
        if "job" in values or "run" in values:
            raise ValueError("DatasetEvent should not contain `job` or `run`")
        return values

    class Config:
        schema_extra = {"not": {"required": ["job", "run"]}}


class JobEvent(BaseEvent):
    job: Job
    inputs: Optional[List[InputDataset]] = Field(default_factory=list, description="The set of **input** datasets.")
    outputs: Optional[List[OutputDataset]] = Field(default_factory=list, description="The set of **output** datasets.")

    @root_validator
    def check_not_required(cls, values):
        if "run" in values:
            raise ValueError("JobEvent should not contain `run`")
        return values

    class Config:
        schema_extra = {"not": {"required": ["run"]}}


if __name__ == '__main__':
    import rich
    import builtins

    builtins.print = rich.print

    print(RunEvent.schema_json(indent=2))
