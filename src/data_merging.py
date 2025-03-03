def merge_lms_data(old_lms_customers, new_lms_borrowers, old_lms_loans, new_lms_loans, old_lms_transactions, new_lms_transactions, new_lms_loan_borrower):
    try:
        # Merge customer data
        merged_customers = pd.concat([old_lms_customers[['customer_id', 'first_name', 'last_name', 'address', 'street', 'city', 'state', 'zip_code']],
                                      new_lms_borrowers[['customer_id', 'first_name', 'last_name', 'address', 'street', 'city', 'state', 'zip_code']]])

        # Remove duplicate records
        merged_customers.drop_duplicates(subset=['customer_id'], keep='last', inplace=True)

        # Merge loan data
        merged_loans = pd.concat([old_lms_loans[['loan_id', 'loan_amount', 'loan_date', 'loan_status']],
                                  new_lms_loans[['loan_id', 'loan_amount', 'loan_date', 'loan_status']]])

        # Remove duplicate loan records
        merged_loans.drop_duplicates(subset=['loan_id'], keep='last', inplace=True)

        # Merge transaction data
        merged_transactions = pd.concat([old_lms_transactions[['transaction_id', 'loan_id', 'transaction_type', 'amount_principal', 'amount_interest', 'transaction_date']],
                                         new_lms_transactions[['transaction_id', 'loan_id', 'transaction_type', 'amount_principal', 'amount_interest', 'transaction_date']]])

        # Remove duplicate transaction records
        merged_transactions.drop_duplicates(subset=['transaction_id'], keep='last', inplace=True)

        # Merge Loan-Borrower Data
        merged_loan_borrower = pd.merge(new_lms_loan_borrower, merged_customers[['customer_id']], on='customer_id', how='inner')

        logging.info("Data Merged Successfully")

        return merged_customers, merged_loans, merged_transactions, merged_loan_borrower
    except Exception as e:
        logging.error(f"Error merging LMS data: {e}")
        raise
