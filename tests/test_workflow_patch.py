#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Oct 12, 2023
"""
import abc
import unittest
from pathlib import Path
from typing import TYPE_CHECKING

from dataci.utils import cwd
from tests.base import AbstractTestCase

if TYPE_CHECKING:
    from dataci.models import Workflow

TEST_DIR = Path(__file__).parent


class TestWorkflowPatchSingleFilePipelineBase(AbstractTestCase, abc.ABC):
    def setUp(self):
        """Set up test fixtures.
        1. Create a test workspace and set it as the current default workspace
        2. Save and publish a test dataset
        3. Save the test pipeline
        """
        from dataci.models import workspace
        from dataci.models import Dataset

        workspace.DEFAULT_WORKSPACE = 'test'
        self.test_dataset = Dataset('yelp_review_test', dataset_files=[
            {'date': '2020-10-05 00:44:08', 'review_id': 'HWRpzNHPqjA4pxN5863QUA', 'stars': 5.0,
             'text': "I called Anytime on Friday afternoon about the number pad lock on my front door. After several questions, the gentleman asked me if I had changed the battery.", },
            {'date': '2020-10-15 04:34:49', 'review_id': '01plHaNGM92IT0LLcHjovQ', 'stars': 5.0,
             'text': "Friend took me for lunch.  Ordered the Chicken Pecan Tart although it was like a piece quiche, was absolutely delicious!", },
            {'date': '2020-10-17 06:58:09', 'review_id': '7CDDSuzoxTr4H5N4lOi9zw', 'stars': 4.0,
             'text': "I love coming here for my fruit and vegetables. It is always fresh and a great variety. The bags of already diced veggies are a huge time saver.", },
        ])
        self.assertEqual(
            self.test_dataset.workspace.name,
            'test',
            "Failed to set the default workspace to `test`."
        )
        self.test_dataset.publish(version_tag='2020-10')
        self._workflow = None
        self.workflow.save()

    @property
    @abc.abstractmethod
    def workflow(self) -> 'Workflow':
        pass

    def test_patch_standalone_stage(self):
        from workflow_patch.single_file_stage_v2 import single_file_stage_v2

        # Patch the standalone stage
        new_workflow = self.workflow.patch(step0_standalone_stage=single_file_stage_v2)
        # Check if the pipeline is patched
        self.assertIn(
            single_file_stage_v2.full_name,
            new_workflow.stages.keys(),
            "Failed to patch the `step0_standalone_stage` stage."
        )
        new_workflow.test()

    def test_patch_intra_deps_stage(self):
        from workflow_patch.single_file_stage_v2 import single_file_stage_v2

        # Patch the standalone stage
        new_workflow = self.workflow.patch(step1_intra_deps_stage=single_file_stage_v2)
        # Check if the pipeline is patched
        self.assertIn(
            single_file_stage_v2.full_name,
            new_workflow.stages.keys(),
            "Failed to patch the `step1_intra_deps_stage` stage."
        )
        new_workflow.test()

    def test_patch_unused_stage(self):
        from workflow_patch.single_file_stage_v2 import single_file_stage_v2

        # Check if the pipeline is patched
        with self.assertRaises(ValueError) as context:
            # Patch the standalone stage
            new_workflow = self.workflow.patch(unused_stage=single_file_stage_v2)
        self.assertIn(
            f'Cannot find stage name=unused_stage in workflow',
            context.exception.args[0],
            "Failed to raise exception for patching `unused_stage` stage."
        )


class TestWorkflowPatchSingleFilePipelineNormal(TestWorkflowPatchSingleFilePipelineBase):
    @property
    def workflow(self):
        if self._workflow is None:
            from tests.workflow_patch.single_file_pipeline.normal_pipeline import text_process_ci_pipeline

            self._workflow = text_process_ci_pipeline
        return self._workflow


@unittest.skip("Skip the test cases for local defined stages.")
class TestWorkflowPatchSingleFilePipelineLocalDefStage(
    TestWorkflowPatchSingleFilePipelineBase
):
    @property
    def workflow(self):
        if self._workflow is None:
            from tests.workflow_patch.single_file_pipeline.local_def_stage_pipeline import text_process_ci_pipeline

            self._workflow = text_process_ci_pipeline
        return self._workflow


class TestWorkflowPatchMultiFilePipelineBase(AbstractTestCase, abc.ABC):
    """Test cases for patching the multi-file pipeline."""

    def setUp(self):
        """Set up test fixtures.
                1. Create a test workspace and set it as the current default workspace
                2. Save and publish a test dataset
                3. Save the test pipeline
                """
        from dataci.models import workspace
        from dataci.models import Dataset

        workspace.DEFAULT_WORKSPACE = 'test'
        self.test_dataset = Dataset('yelp_review_test', dataset_files=[
            {'date': '2020-10-05 00:44:08', 'review_id': 'HWRpzNHPqjA4pxN5863QUA', 'stars': 5.0,
             'text': "I called Anytime on Friday afternoon about the number pad lock on my front door. After several questions, the gentleman asked me if I had changed the battery.", },
            {'date': '2020-10-15 04:34:49', 'review_id': '01plHaNGM92IT0LLcHjovQ', 'stars': 5.0,
             'text': "Friend took me for lunch.  Ordered the Chicken Pecan Tart although it was like a piece quiche, was absolutely delicious!", },
            {'date': '2020-10-17 06:58:09', 'review_id': '7CDDSuzoxTr4H5N4lOi9zw', 'stars': 4.0,
             'text': "I love coming here for my fruit and vegetables. It is always fresh and a great variety. The bags of already diced veggies are a huge time saver.", },
        ])
        self.assertEqual(
            self.test_dataset.workspace.name,
            'test',
            "Failed to set the default workspace to `test`."
        )
        self.test_dataset.publish(version_tag='2020-10')
        self._workflow = None
        self.workflow.save()

        from tests.workflow_patch.single_file_stage_v2 import single_file_stage_v2
        self.new_stage = single_file_stage_v2

    @property
    @abc.abstractmethod
    def workflow(self) -> 'Workflow':
        pass

    def test_patch_unused_stage(self):
        # Check if the pipeline is patched
        with self.assertRaises(ValueError) as context:
            # Patch the standalone stage
            new_workflow = self.workflow.patch(unused_stage=self.new_stage)
        self.assertIn(
            f'Cannot find stage name=unused_stage in workflow',
            context.exception.args[0],
            "Failed to raise exception for patching `unused_stage` stage."
        )

    def test_unused_stage_w_util(self):
        # Check if the pipeline is patched
        with self.assertRaises(ValueError) as context:
            # Patch the standalone stage
            new_workflow = self.workflow.patch(unused_stage_w_util=self.new_stage)
        self.assertIn(
            f'Cannot find stage name=unused_stage_w_util in workflow',
            context.exception.args[0],
            "Failed to raise exception for patching `unused_stage_w_util` stage."
        )

    def test_unused_stage_w_util2(self):
        # Check if the pipeline is patched
        with self.assertRaises(ValueError) as context:
            # Patch the standalone stage
            new_workflow = self.workflow.patch(unused_stage_w_util2=self.new_stage)
        self.assertIn(
            f'Cannot find stage name=unused_stage_w_util2 in workflow',
            context.exception.args[0],
            "Failed to raise exception for patching `unused_stage_w_util2` stage."
        )

    def test_used_stage(self):
        # Patch the standalone stage
        new_workflow = self.workflow.patch(used_stage=self.new_stage)
        # Check if the pipeline is patched
        self.assertIn(
            self.new_stage.full_name,
            new_workflow.stages.keys(),
            "Failed to patch the `used_stage` stage."
        )
        new_workflow.test()

    def test_used_stage_w_util(self):
        # Patch the standalone stage
        new_workflow = self.workflow.patch(used_stage_w_util=self.new_stage)
        # Check if the pipeline is patched
        self.assertIn(
            self.new_stage.full_name,
            new_workflow.stages.keys(),
            "Failed to patch the `used_stage_w_util` stage."
        )
        new_workflow.test()

    def test_multi_file_stage(self):
        # Patch the standalone stage
        new_workflow = self.workflow.patch(multi_file_stage=self.new_stage)
        # Check if the pipeline is patched
        self.assertIn(
            self.new_stage.full_name,
            new_workflow.stages.keys(),
            "Failed to patch the `multi_file_stage` stage."
        )
        new_workflow.test()

    def test_multi_file_stage_w_util(self):
        # Patch the standalone stage
        new_workflow = self.workflow.patch(multi_file_stage_w_util=self.new_stage)
        # Check if the pipeline is patched
        self.assertIn(
            self.new_stage.full_name,
            new_workflow.stages.keys(),
            "Failed to patch the `multi_file_stage_w_util` stage."
        )
        new_workflow.test()


class TestWorkflowPatchMultiFilePipelineNormal(TestWorkflowPatchMultiFilePipelineBase):
    @property
    def workflow(self):
        if self._workflow is None:
            from dataci.models import Workflow
            self._workflow = Workflow.from_path(
                TEST_DIR / 'multi_file_pipeline/normal_import_pipeline.py',
                entry_path='normal_import_pipeline.py'
            )
        return self._workflow


class TestWorkflowPatchMultiFilePipelineImportAlias(TestWorkflowPatchMultiFilePipelineBase):
    @property
    def workflow(self):
        if self._workflow is None:
            with cwd('tests/workflow_patch/multi_file_pipeline'):
                from tests.workflow_patch.multi_file_pipeline.import_alias_pipeline import standard_import_pipeline

            self._workflow = standard_import_pipeline
        return self._workflow


class TestWorkflowPatchMultiFilePipelineImportAndAssignToVar(TestWorkflowPatchMultiFilePipelineBase):
    @property
    def workflow(self):
        if self._workflow is None:
            with cwd('tests/workflow_patch/multi_file_pipeline'):
                from tests.workflow_patch.multi_file_pipeline.import_and_assign_to_var_pipeline import \
                    standard_import_pipeline

            self._workflow = standard_import_pipeline
        return self._workflow


class TestWorkflowPatchMultiFilePipelineLocalImport(TestWorkflowPatchMultiFilePipelineBase):
    @property
    def workflow(self):
        if self._workflow is None:
            with cwd('tests/workflow_patch/multi_file_pipeline'):
                from tests.workflow_patch.multi_file_pipeline.local_import_pipeline import standard_import_pipeline

            self._workflow = standard_import_pipeline
        return self._workflow


class TestWorkflowPatchMultiFilePipelineMultilineImport(TestWorkflowPatchMultiFilePipelineBase):
    @property
    def workflow(self):
        if self._workflow is None:
            with cwd('tests/workflow_patch/multi_file_pipeline'):
                from tests.workflow_patch.multi_file_pipeline.multiline_import_pipeline import standard_import_pipeline

            self._workflow = standard_import_pipeline
        return self._workflow
