import unittest
import pandas as pd
from transformation import transform_old_lms_data, transform_new_lms_data

class TestTransformation(unittest.TestCase):

    def test_transform_old_lms_data(self):
        # Sample old LMS customer data
        data = {
            'txtAddr1': ['123 Main St', '456 Oak Ave'],
            'txtCity': ['City1', 'City2'],
            'txtState': ['State1', 'State2'],
            'zipCode': ['11111', '22222']
        }
        df = pd.DataFrame(data)
        
        transformed_data = transform_old_lms_data(df)
        
        self.assertTrue('address' in transformed_data.columns)
        self.assertTrue('street' in transformed_data.columns)
        self.assertTrue('city' in transformed_data.columns)
        self.assertTrue('state' in transformed_data.columns)
        self.assertTrue('zip_code' in transformed_data.columns)

    def test_transform_new_lms_data(self):
        # Sample new LMS borrower data
        data = {
            'address_line1': ['789 Pine Blvd', '101 Maple Rd'],
            'city': ['City3', 'City4'],
            'state': ['State3', 'State4'],
            'zip_code': ['33333', '44444']
        }
        df = pd.DataFrame(data)
        
        transformed_data = transform_new_lms_data(df)
        
        self.assertTrue('address' in transformed_data.columns)
        self.assertTrue('street' in transformed_data.columns)
        self.assertTrue('city' in transformed_data.columns)
        self.assertTrue('state' in transformed_data.columns)
        self.assertTrue('zip_code' in transformed_data.columns)

if __name__ == "__main__":
    unittest.main()
