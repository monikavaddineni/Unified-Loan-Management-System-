import pandas as pd
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to load old LMS data with incremental updates
def load_old_lms_incremental(last_run_timestamp):
    try:
        # Load Old LMS customer data from CSV file
        old_lms_customers = pd.read_csv('data/homework_old_lms_customers.csv')
        
        # Filter for data updated since the last run
        old_lms_customers['updated_at'] = pd.to_datetime(old_lms_customers['updated_at'], errors='coerce')
        new_old_lms_customers = old_lms_customers[old_lms_customers['updated_at'] > last_run_timestamp]
        
        logging.info(f"Loaded {len(new_old_lms_customers)} new or updated records from Old LMS Customer data")

        # Load old LMS loan and transaction data similarly
        old_lms_loans = pd.read_csv('data/homework_old_lms_loans.csv')
        old_lms_loans['updated_at'] = pd.to_datetime(old_lms_loans['updated_at'], errors='coerce')
        new_old_lms_loans = old_lms_loans[old_lms_loans['updated_at'] > last_run_timestamp]

        old_lms_transactions = pd.read_csv('data/homework_old_lms_transactions.csv')
        old_lms_transactions['updated_at'] = pd.to_datetime(old_lms_transactions['updated_at'], errors='coerce')
        new_old_lms_transactions = old_lms_transactions[old_lms_transactions['updated_at'] > last_run_timestamp]
        
        return new_old_lms_customers, new_old_lms_loans, new_old_lms_transactions
    except Exception as e:
        logging.error(f"Error loading Old LMS data: {e}")
        raise

# Function to load new LMS data with incremental updates
def load_new_lms_incremental(last_run_timestamp):
    try:
        # Load New LMS borrower data from CSV file
        new_lms_borrowers = pd.read_csv('data/homework_new_lms_borrowers.csv')
        new_lms_borrowers['updated_at'] = pd.to_datetime(new_lms_borrowers['updated_at'], errors='coerce')
        new_new_lms_borrowers = new_lms_borrowers[new_lms_borrowers['updated_at'] > last_run_timestamp]
        
        logging.info(f"Loaded {len(new_new_lms_borrowers)} new or updated records from New LMS Borrowers data")

        # Load new LMS loan data
        new_lms_loans = pd.read_csv('data/homework_new_lms_loans.csv')
        new_lms_loans['updated_at'] = pd.to_datetime(new_lms_loans['updated_at'], errors='coerce')
        new_new_lms_loans = new_lms_loans[new_lms_loans['updated_at'] > last_run_timestamp]

        # Load new LMS transaction data
        new_lms_transactions = pd.read_csv('data/homework_new_lms_transactions.csv')
        new_lms_transactions['updated_at'] = pd.to_datetime(new_lms_transactions['updated_at'], errors='coerce')
        new_new_lms_transactions = new_lms_transactions[new_lms_transactions['updated_at'] > last_run_timestamp]
        
        # Load the loan-borrower cross-reference data
        new_lms_loan_borrower = pd.read_csv('data/homework_new_lms_loan_borrower.csv')
        new_lms_loan_borrower['updated_at'] = pd.to_datetime(new_lms_loan_borrower['updated_at'], errors='coerce')
        new_new_lms_loan_borrower = new_lms_loan_borrower[new_lms_loan_borrower['updated_at'] > last_run_timestamp]
        
        return new_new_lms_borrowers, new_new_lms_loans, new_new_lms_transactions, new_new_lms_loan_borrower
    except Exception as e:
        logging.error(f"Error loading New LMS data: {e}")
        raise
