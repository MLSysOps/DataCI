#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Author: Li Yuanming
Email: yuanmingleee@gmail.com
Date: Oct 12, 2023
"""
import unittest


class TestWorkflowPatch(unittest.TestCase):

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

    def test_patch_standalone_stage(self):
        from tests.workflow_patch.single_file_pipeline.text_process_ci import text_process_ci_pipeline
        from workflow_patch.single_file_stage_v2 import single_file_stage_v2

        # Patch the standalone stage
        new_workflow = text_process_ci_pipeline.patch(step0_standalone_stage=single_file_stage_v2)
        # Check if the pipeline is patched
        self.assertIn(
            single_file_stage_v2.full_name,
            new_workflow.stages.keys(),
            "Failed to patch the `step0_standalone_stage` stage."
        )
        new_workflow.test()

    def test_patch_intra_deps_stage(self):
        from tests.workflow_patch.single_file_pipeline.text_process_ci import text_process_ci_pipeline
        from workflow_patch.single_file_stage_v2 import single_file_stage_v2

        # Patch the standalone stage
        new_workflow = text_process_ci_pipeline.patch(step1_intra_deps_stage=single_file_stage_v2)
        # Check if the pipeline is patched
        self.assertIn(
            single_file_stage_v2.full_name,
            new_workflow.stages.keys(),
            "Failed to patch the `step1_intra_deps_stage` stage."
        )
        new_workflow.test()

    def test_patch_unused_stage(self):
        from tests.workflow_patch.single_file_pipeline.text_process_ci import text_process_ci_pipeline
        from workflow_patch.single_file_stage_v2 import single_file_stage_v2

        # Patch the standalone stage
        new_workflow = text_process_ci_pipeline.patch(unused_stage=single_file_stage_v2)
        # Check if the pipeline is patched
        self.assertIn(
            single_file_stage_v2.full_name,
            new_workflow.stages.keys(),
            "Failed to patch `unused_stage` stage."
        )
        new_workflow.test()
