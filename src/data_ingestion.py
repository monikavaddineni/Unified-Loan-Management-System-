import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_old_lms_data():
    try:
        # Load Old LMS customer data from CSV file
        old_lms_customers = pd.read_csv('data/homework_old_lms_customers.csv')
        logging.info("Old LMS Customer data loaded successfully")
        
        # Load additional datasets like loan and transaction data (if needed)
        old_lms_loans = pd.read_csv('data/homework_old_lms_loans.csv')
        old_lms_transactions = pd.read_csv('data/homework_old_lms_transactions.csv')
        
        logging.info("Old LMS Loan and Transaction data loaded successfully")

        # Ensure consistency in column names
        old_lms_customers.columns = old_lms_customers.columns.str.strip().str.lower()
        old_lms_loans.columns = old_lms_loans.columns.str.strip().str.lower()
        old_lms_transactions.columns = old_lms_transactions.columns.str.strip().str.lower()

        return old_lms_customers, old_lms_loans, old_lms_transactions
    except Exception as e:
        logging.error(f"Error loading Old LMS data: {e}")
        raise

def load_new_lms_data():
    try:
        # Load New LMS borrower data from CSV file
        new_lms_borrowers = pd.read_csv('data/homework_new_lms_borrowers.csv')
        new_lms_loans = pd.read_csv('data/homework_new_lms_loans.csv')
        new_lms_transactions = pd.read_csv('data/homework_new_lms_transactions.csv')
        new_lms_loan_borrower = pd.read_csv('data/homework_new_lms_loan_borrower.csv')  # Load the loan-borrower cross-reference data

        logging.info("New LMS Borrower, Loan, and Transaction data loaded successfully")

        # Ensure consistency in column names
        new_lms_borrowers.columns = new_lms_borrowers.columns.str.strip().str.lower()
        new_lms_loans.columns = new_lms_loans.columns.str.strip().str.lower()
        new_lms_transactions.columns = new_lms_transactions.columns.str.strip().str.lower()
        new_lms_loan_borrower.columns = new_lms_loan_borrower.columns.str.strip().str.lower()  # Handle loan-borrower file

        return new_lms_borrowers, new_lms_loans, new_lms_transactions, new_lms_loan_borrower
    except Exception as e:
        logging.error(f"Error loading New LMS data: {e}")
        raise
