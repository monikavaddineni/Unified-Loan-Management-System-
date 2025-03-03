from your_etl_module import load_old_lms_incremental, load_new_lms_incremental

def test_error_handling_load():
    with patch('your_etl_module.pd.read_csv', side_effect=Exception("File not found")):
        try:
            load_old_lms_incremental('2024-01-01')
        except Exception as e:
            assert str(e) == "File not found"

def test_error_handling_transform():
    # Simulating an error during transformation
    with patch('your_etl_module.clean_and_transform_old_lms_data', side_effect=Exception("Transformation failed")):
        try:
            transform_incremental_data(mock_old_lms_customers, None, None, None, None, None)
        except Exception as e:
            assert str(e) == "Transformation failed"

