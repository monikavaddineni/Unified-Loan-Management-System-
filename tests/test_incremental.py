import unittest
import pandas as pd
from datetime import datetime
from incremental import get_last_run_time, save_last_run_time, extract_incremental_data

class TestIncremental(unittest.TestCase):

    def test_get_last_run_time(self):
        # Create a mock file with the last run time
        last_run_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open('last_run_timestamp.txt', 'w') as f:
            f.write(last_run_time)
        
        result = get_last_run_time()
        self.assertEqual(result, last_run_time)

    def test_save_last_run_time(self):
        # Save last run time
        save_last_run_time()
        
        # Read saved time
        with open('last_run_timestamp.txt', 'r') as f:
            saved_time = f.read().strip()
        
        self.assertIsNotNone(saved_time)

    def test_extract_incremental_data(self):
        # Sample data for new LMS borrowers
        data = {
            'transaction_date': ['2024-01-01', '2024-02-01', '2024-03-01'],
            'customer_id': [1, 2, 3]
        }
        df = pd.DataFrame(data)
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        
        last_run_time = '2024-01-15'
        incremental_data = extract_incremental_data(df, last_run_time)
        
        # Check the extracted incremental data
        self.assertEqual(incremental_data.shape[0], 2)  # 2 rows should be after the last run time
        self.assertTrue('transaction_date' in incremental_data.columns)

if __name__ == "__main__":
    unittest.main()
