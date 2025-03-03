def clean_and_transform_old_lms_data(old_lms_customers, old_lms_loans, old_lms_transactions):
    try:
        # Convert relevant columns to strings to avoid errors in concatenation
        old_lms_customers['txtaddr1'] = old_lms_customers['txtaddr1'].astype(str)
        old_lms_customers['txtcity'] = old_lms_customers['txtcity'].astype(str)
        old_lms_customers['txtstate'] = old_lms_customers['txtstate'].astype(str)
        old_lms_customers['zipcode'] = old_lms_customers['zipcode'].astype(str)

        # Handle missing values in address-related columns
        old_lms_customers['address'] = old_lms_customers['txtaddr1'] + " " + old_lms_customers['txtcity'] + " " + old_lms_customers['txtstate'] + " " + old_lms_customers['zipcode']
        old_lms_customers['address'] = old_lms_customers['address'].fillna('Unknown')

        # Split address into components
        address_split = old_lms_customers['address'].str.split(' ', n=3, expand=True)
        old_lms_customers['street'] = address_split[0] + " " + address_split[1]
        old_lms_customers['city'] = address_split[2]
        old_lms_customers['state'] = address_split[3].str.split(' ', n=1, expand=True)[0]
        old_lms_customers['zip_code'] = address_split[3].str.split(' ', n=1, expand=True)[1]

        # Handle missing values in numerical columns
        old_lms_loans['loan_amount'] = old_lms_loans['loan_amount'].fillna(0)
        old_lms_loans['loan_date'] = pd.to_datetime(old_lms_loans['loan_date'], errors='coerce')

        # Ensure proper date conversion
        old_lms_transactions['transaction_date'] = pd.to_datetime(old_lms_transactions['transaction_date'], errors='coerce')

        logging.info("Old LMS Data Cleaned and Transformed Successfully")

        return old_lms_customers, old_lms_loans, old_lms_transactions
    except Exception as e:
        logging.error(f"Error cleaning and transforming Old LMS data: {e}")
        raise


def clean_and_transform_new_lms_data(new_lms_borrowers, new_lms_loans, new_lms_transactions):
    try:
        # Convert relevant columns to strings
        new_lms_borrowers['address_line1'] = new_lms_borrowers['address_line1'].astype(str)
        new_lms_borrowers['city'] = new_lms_borrowers['city'].astype(str)
        new_lms_borrowers['state'] = new_lms_borrowers['state'].astype(str)
        new_lms_borrowers['zip_code'] = new_lms_borrowers['zip_code'].astype(str)

        # Handle missing values in address-related columns
        new_lms_borrowers['address'] = new_lms_borrowers['address_line1'] + " " + new_lms_borrowers['city'] + " " + new_lms_borrowers['state'] + " " + new_lms_borrowers['zip_code']
        new_lms_borrowers['address'] = new_lms_borrowers['address'].fillna('Unknown')

        # Handle missing values in numerical columns
        new_lms_loans['loan_amount'] = new_lms_loans['loan_amount'].fillna(0)

        # Ensure proper date conversion
        new_lms_loans['loan_date'] = pd.to_datetime(new_lms_loans['loan_date'], errors='coerce')
        new_lms_transactions['transaction_date'] = pd.to_datetime(new_lms_transactions['transaction_date'], errors='coerce')

        logging.info("New LMS Data Cleaned and Transformed Successfully")

        return new_lms_borrowers, new_lms_loans, new_lms_transactions
    except Exception as e:
        logging.error(f"Error cleaning and transforming New LMS data: {e}")
        raise
