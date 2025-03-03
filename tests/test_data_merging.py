import unittest
import pandas as pd
from src.merging import merge_customer_data

class TestMerging(unittest.TestCase):

    def test_merge_customer_data(self):
        # Sample data for old LMS and new LMS
        old_data = {
            'customer_id': [1, 2],
            'first_name': ['John', 'Jane'],
            'last_name': ['Doe', 'Smith'],
            'address': ['123 Main St', '456 Oak Ave'],
            'street': ['123 Main', '456 Oak'],
            'city': ['City1', 'City2'],
            'state': ['State1', 'State2'],
            'zip_code': ['11111', '22222']
        }
        
        new_data = {
            'customer_id': [1, 3],
            'first_name': ['John', 'Sam'],
            'last_name': ['Doe', 'Brown'],
            'address': ['123 Main St', '789 Pine Blvd'],
            'street': ['123 Main', '789 Pine'],
            'city': ['City1', 'City3'],
            'state': ['State1', 'State3'],
            'zip_code': ['11111', '55555']
        }
        
        old_df = pd.DataFrame(old_data)
        new_df = pd.DataFrame(new_data)
        
        merged_data = merge_customer_data(old_df, new_df)
        
        # Check merged data
        self.assertEqual(merged_data.shape[0], 3)  # Should be 3 rows after merging
        self.assertTrue('customer_id' in merged_data.columns)
        self.assertTrue('first_name' in merged_data.columns)

if __name__ == "__main__":
    unittest.main()
