import unittest
from ingestion import load_old_lms_data, load_new_lms_data

class TestIngestion(unittest.TestCase):

    def test_load_old_lms_data(self):
        data = load_old_lms_data()
        self.assertIsNotNone(data)
        self.assertTrue('customer_id' in data.columns)

    def test_load_new_lms_data(self):
        data = load_new_lms_data()
        self.assertIsNotNone(data)
        self.assertTrue('customer_id' in data.columns)

if __name__ == "__main__":
    unittest.main()
