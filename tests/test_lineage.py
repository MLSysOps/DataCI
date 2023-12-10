import unittest
from pathlib import Path

TEST_DIR = Path(__file__).parent


class TestLineage(unittest.TestCase):
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

        from dataci.models import Workflow

        self.workflow = Workflow.from_path(
            TEST_DIR / 'lineage',
            entry_path='python_ops_pipeline.py'
        )
        self.workflow.publish()
        self.workflow.run()

    def test_lineage(self):
        self.assertEqual(True, True)  # add assertion here


if __name__ == '__main__':
    unittest.main()
