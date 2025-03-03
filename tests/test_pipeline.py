import unittest
from pipeline import run_pipeline

class TestPipeline(unittest.TestCase):

    def test_run_pipeline(self):
        try:
            # Run the full ETL pipeline
            run_pipeline()
            # If no exception occurs, assume it is successful
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    unittest.main()
