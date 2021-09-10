from source.icg_file_processing import handler
from source.utils import read_csv_data_to_df, read_lookup_file_to_df, validate_mandatory_cols, validate_codes, \
    validate_data_types, validate_active_flag
import os
import unittest
import filecmp
import pandas as pd
from datetime import datetime


class LambTests(unittest.TestCase):
    def test_validate_mandatory_cols(self):
        # Validate source data with all mandatory fields
        data = [[12.59, 'USA', 'USD', 'FB']]
        df = pd.DataFrame(data, columns=['D1', 'Country', 'Currency', 'Company'])
        assert validate_mandatory_cols(df) == 0

        # Validate source data with missing mandatory fields
        data = [[12.59, 'USD', 'FB']]
        df = pd.DataFrame(data, columns=['D1', 'Currency', 'Company'])
        assert validate_mandatory_cols(df) == 1

    def test_validate_codes(self):
        lookup_data = read_lookup_file_to_df("lookup", "Lookup.xlsx")
        # Validate source data with valid Country, Currency and Company code
        data = [[12.59, 'USA', 'USD', 'FB']]
        df = pd.DataFrame(data, columns=['D1', 'Country', 'Currency', 'Company'])
        assert validate_codes(df, lookup_data) == 0

        # Validate source data with invalid valid Country, Currency or Company code
        data = [[12.59, 'XYZ', 'USD', 'FB']]
        df = pd.DataFrame(data, columns=['D1', 'Country', 'Currency', 'Company'])
        assert validate_codes(df, lookup_data) == 1

        # Validate source data with invalid valid Country, Currency or Company code
        data = [[12.59, 'USA', 'ABC', 'FB']]
        df = pd.DataFrame(data, columns=['D1', 'Country', 'Currency', 'Company'])
        assert validate_codes(df, lookup_data) == 1

    def test_validate_data_type(self):
        lookup_data = read_lookup_file_to_df("lookup", "Lookup.xlsx")
        # Validate Data type with correct data types
        data = [[12.59, 'USA', 'USD', 'FB']]
        df = pd.DataFrame(data, columns=['D1', 'Country', 'Currency', 'Company'])
        assert validate_data_types(df, lookup_data) == 0

        # Validate Data type with incorrect data types
        data = [[12.59, 1234, 'USD', 'FB']]
        df = pd.DataFrame(data, columns=['D1', 'Country', 'Currency', 'Company'])
        assert validate_data_types(df, lookup_data) == 1

        data = [[12.59, 'USA', 'USD', True]]
        df = pd.DataFrame(data, columns=['D1', 'Country', 'Currency', 'Company'])
        assert validate_data_types(df, lookup_data) == 1

    def test_validate_activity_flag(self):
        # Although Activity flag is boolean but contains Yes or No values, replace function can be used to make it bool
        # Test with a valid Active Flag
        data = [['Yes']]
        df = pd.DataFrame(data, columns=['Active Flag'])
        assert validate_active_flag(df) == 0

        # Test with an invalid Active Flag
        data = [['Yo']]
        df = pd.DataFrame(data, columns=['Active Flag'])
        assert validate_active_flag(df) == 1

        # Test with an unavailable Active Flag / should pass the test as this is not a mandate field
        data = [[123.9]]
        df = pd.DataFrame(data, columns=['D2'])
        assert validate_active_flag(df) == 0

    def test_handler(self):
        # Test handler with correct data, making sure it reaches the correct output directory
        event = {
            "input_dir": "test/fixture",
            "input_file": "Data.csv",
            "lookup_dir": "lookup",
            "lookup_file": "Lookup.xlsx",
            "error_dir": "error_dir",
            "csv_output_dir": "output_file_csv/",
            "parquet_output_dir": "output_file_parquet/"
        }
        handler(event)
        columns = ['AsOfDate', 'Rowhash']
        # Dropping AsOfDate and Rowhash to avoid timestamp and hashvalue mismatch
        # Test the valid files from output file
        actual_output_file = "Data_" + str(datetime.now().strftime("%Y-%m-%d")) + ".csv"
        actual_output_csv_df = read_csv_data_to_df("output_file_csv/", actual_output_file).drop(columns,
                                                                                                inplace=True, axis=1)
        expected_output_csv_df = read_csv_data_to_df("test/fixture/expected/", "Data_2021-09-10.csv").drop(columns,
                                                                                                           inplace=True,
                                                                                                           axis=1)
        assert actual_output_csv_df == expected_output_csv_df

    def test_handler_mandatory_fields(self):
        event = {
            "input_dir": "test/fixture/expected",
            "input_file": "Data_error4_2021-09-09.csv",
            "lookup_dir": "lookup",
            "lookup_file": "Lookup.xlsx",
            "error_dir": "error_dir",
            "csv_output_dir": "output_file_csv/",
            "parquet_output_dir": "output_file_parquet/"
        }
        handler(event)
        # Check whether the file moves to error directory or not
        actual_output_csv_error_file = os.path.abspath("error_dir/Data_error4_2021-09-09.csv")
        expected_output_csv_error_file = os.path.abspath("test/fixture/expected/Data_error4_2021-09-09.csv")
        files_match = filecmp.cmp(actual_output_csv_error_file, expected_output_csv_error_file)
        assert files_match is True

    def test_handler_invalid_date_type(self):
        event = {
            "input_dir": "test/fixture/expected",
            "input_file": "Data_error1_2021-09-09.csv",
            "lookup_dir": "lookup",
            "lookup_file": "Lookup.xlsx",
            "error_dir": "error_dir",
            "csv_output_dir": "output_file_csv/",
            "parquet_output_dir": "output_file_parquet/"
        }
        handler(event)
        # Check whether the file moves to error directory or not
        actual_output_csv_error_file = os.path.abspath("error_dir/Data_error1_2021-09-09.csv")
        expected_output_csv_error_file = os.path.abspath("test/fixture/expected/Data_error1_2021-09-09.csv")
        files_match = filecmp.cmp(actual_output_csv_error_file, expected_output_csv_error_file)
        assert files_match is True

    def test_handler_invalid_county_code(self):
        event = {
            "input_dir": "test/fixture/expected",
            "input_file": "Data_error2_2021-09-09.csv",
            "lookup_dir": "lookup",
            "lookup_file": "Lookup.xlsx",
            "error_dir": "error_dir",
            "csv_output_dir": "output_file_csv/",
            "parquet_output_dir": "output_file_parquet/"
        }
        handler(event)
        # Check whether the file moves to error directory or not
        actual_output_csv_error_file = os.path.abspath("error_dir/Data_error2_2021-09-09.csv")
        expected_output_csv_error_file = os.path.abspath("test/fixture/expected/Data_error2_2021-09-09.csv")
        files_match = filecmp.cmp(actual_output_csv_error_file, expected_output_csv_error_file)
        assert files_match is True

    def test_handler_invalid_Activity_Flag(self):
        event = {
            "input_dir": "test/fixture/expected",
            "input_file": "Data_error3_2021-09-09.csv",
            "lookup_dir": "lookup",
            "lookup_file": "Lookup.xlsx",
            "error_dir": "error_dir",
            "csv_output_dir": "output_file_csv/",
            "parquet_output_dir": "output_file_parquet/"
        }
        handler(event)
        # Check whether the file moves to error directory or not
        actual_output_csv_error_file = os.path.abspath("error_dir/Data_error3_2021-09-09.csv")
        expected_output_csv_error_file = os.path.abspath("test/fixture/expected/Data_error3_2021-09-09.csv")
        files_match = filecmp.cmp(actual_output_csv_error_file, expected_output_csv_error_file)
        assert files_match is True
