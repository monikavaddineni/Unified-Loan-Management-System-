import logging
from ingestion import load_old_lms_data, load_new_lms_data
from transformation import transform_old_lms_data, transform_new_lms_data
from loading import load_data_to_db
from incremental import get_last_run_time, save_last_run_time, extract_incremental_data

# Logging configuration
logging.basicConfig(level=logging.INFO)

def run_pipeline():
    # Step 1: Data Ingestion
    old_lms_data = load_old_lms_data()
    new_lms_data = load_new_lms_data()

    # Step 2: Check for Incremental Data
    last_run_time = get_last_run_time()

    # Step 3: Transform Data
    old_lms_data_transformed = transform_old_lms_data(old_lms_data)
    new_lms_data_transformed = transform_new_lms_data(new_lms_data)

    # Step 4: Extract Incremental Data (If Applicable)
    incremental_new_lms_data = extract_incremental_data(new_lms_data_transformed, last_run_time)

    # Step 5: Load Data into Destination (DB)
    load_data_to_db(old_lms_data_transformed, 'old_lms_customers')
    load_data_to_db(new_lms_data_transformed, 'new_lms_borrowers')
    load_data_to_db(incremental_new_lms_data, 'incremental_lms_borrowers')

    # Step 6: Save Last Run Time
    save_last_run_time()

if __name__ == "__main__":
    run_pipeline()