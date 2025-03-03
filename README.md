# Unified Loan Management System

This project involves the migration and unification of data from two Loan Management Systems (LMS). The goal is to process and merge loan data from an **Old LMS** and a **New LMS**, and create a unified dataset that can be used for analysis and reporting. The project leverages data engineering principles, including data ingestion, transformation, and loading (ETL), as well as integrating incremental data pipelines.


## Project Overview

The company has migrated from an old loan management system (LMS) to a new LMS, and is designing a data warehouse to unify and centralize loan data from both systems. The project includes:
- Extracting data from both systems.
- Cleaning and transforming data to ensure consistency in formats and structure.
- Merging data into a unified schema.
- Supporting incremental data updates in the new LMS system.

The primary goal is to enable analysts to query all loan data in a single place.

## Architecture

The architecture for this data pipeline involves:
1. **Data Ingestion:** Extracting loan data from CSV files for the old LMS and querying MySQL for the new LMS.
2. **Data Transformation:** Standardizing data formats, handling missing data, and transforming the data to a common schema.
3. **Data Merging:** Combining data from the old and new LMS into a unified dataset.
4. **Incremental Updates:** Handling new records and updates from the new LMS via incremental data pipelines.
5. **Data Warehouse Schema:** Storing the unified data in a star or snowflake schema for efficient querying and analysis.

![Data Architecture Diagram](https://github.com/monikavaddineni/Unified-Loan-Management-System-/blob/master/Project%20flow.png)

## Data Ingestion

The project begins by loading data from the following sources:

1. **Old LMS (CSV Files):**
   - `homework_old_lms_customers.csv`
   - `homework_old_lms_loans.csv`
   - `homework_old_lms_transactions.csv`

2. **New LMS (MySQL Database):**
   - `homework_new_lms_borrowers.csv`
   - `homework_new_lms_loan_borrower.csv`
   - `homework_new_lms_loans.csv`
   - `homework_new_lms_transactions.csv`

The data from these files and databases is ingested into the processing pipeline.

## Data Transformation

The following transformations are performed on the data:
- **Consistency in Data Types:** Ensuring all columns have the appropriate data types (e.g., strings, integers, dates).
- **Missing Data Handling:** Using techniques like filling missing values with defaults (e.g., `'Unknown'` for addresses).
- **Date Conversion:** Converting string dates to `datetime` objects.
- **Column Renaming and Standardization:** Aligning column names across both datasets to maintain consistency.

## Data Merging

Once the data is cleaned, the old and new LMS datasets are merged:
- Data from both systems is joined on the `customer_id` column (borrower identifier).
- A final unified dataset is created, combining the customer, loan, and transaction data.

## Incremental Data Pipeline

For handling the incremental data from the new LMS (which is constantly changing), an incremental data pipeline is implemented:
- **New Records:** The pipeline identifies and loads new records.
- **Updated Records:** The pipeline identifies and updates existing records based on the most recent data.

## Testing

The following testing steps are included:
- **Unit Tests:** Test cases for individual functions, like data transformations and file parsing.
- **Integration Tests:** Test the entire ETL process from ingestion through transformation to merging.
- **Incremental Updates Tests:** Ensure that new and updated data are handled correctly.

Testing is implemented using Python’s `unittest` framework.

---

## Assumptions

1. **Old LMS Data**:
   - The data from the old LMS is static and will not change.
   
2. **New LMS Data**:
   - The New LMS will continuously receive new data and updates. These updates need to be handled incrementally.

3. **Date Fields**:
   - All date fields in the data will be either in a valid date format or can be easily converted to `datetime` objects.

4. **Missing Data**:
   - Missing values in address-related fields (such as city, state, or zip code) will be handled by filling them with the value `'Unknown'`.

---

## Recommendations

1. **Incremental Updates**:
   - Ensure that the New LMS is set up to **automatically push incremental updates** to the data warehouse. This would ensure that the system can accommodate new and updated records from the New LMS continuously.

2. **Pipeline Auditing**:
   - Periodically audit the data pipeline to ensure it is **handling edge cases**, such as invalid or corrupted records. This can be done by checking for records that do not meet the expected data format or have missing fields.
   
3. **Logging and Error Handling**:
   - Consider adding **logging** at key points in the ETL pipeline to track progress and any errors. Logging can be particularly useful for monitoring the incremental updates, to ensure that new and updated records are being correctly processed.

---

## How to Run the Project

To run the ETL pipeline:

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/unified-loan-management-system.git

---
   
![Project Strucure](https://github.com/monikavaddineni/Unified-Loan-Management-System-/blob/master/Project%20Structure.txt)

---

## Conclusion

This project provides an automated **ETL (Extract, Transform, Load)** pipeline that extracts, transforms, and loads loan data from two different Loan Management Systems (LMS) into a unified format. The pipeline ensures that data from both the **Old LMS** and **New LMS** is processed, cleaned, and combined efficiently for analysis.

Key features include:

- **Data Extraction**: The pipeline extracts data from both the static **Old LMS** (CSV files) and the dynamic **New LMS** (MySQL database).
- **Data Transformation**: It ensures consistency in data types, handles missing values, and standardizes column names across datasets.
- **Data Merging**: The cleaned datasets from both systems are merged into a unified format.
- **Incremental Updates**: The pipeline supports incremental updates from the New LMS, ensuring that new and updated records are handled efficiently.
  
By following the **recommendations for logging**, **incremental updates**, and **auditing**, the system can be maintained and scaled to accommodate future growth, ensuring data integrity and system reliability.


