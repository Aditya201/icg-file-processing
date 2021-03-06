import os
import shutil
import logging
import json
from source.utils import read_csv_data_to_df, read_lookup_file_to_df, validate_mandatory_cols, validate_codes, \
    validate_data_types, validate_active_flag, generate_output_file, upload_as_csv, upload_as_parquet

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def handler(event):
    try:
        input_dir = event["input_dir"]
        input_file = event["input_file"]
        lookup_dir = event["lookup_dir"]
        lookup_file = event["lookup_file"]
        error_dir = event["error_dir"]
        csv_output_dir = event["csv_output_dir"]
        parquet_output_dir = event["parquet_output_dir"]
    except KeyError as e:
        message = str(e).strip("\'") + ' is missing'
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': message
            })
        }
    csv_data = read_csv_data_to_df(input_dir, input_file)
    lookup_data = read_lookup_file_to_df(lookup_dir, lookup_file)
    # Create error and output directories if not exists
    for directory in [error_dir, csv_output_dir, parquet_output_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    val_mandatory_cols = validate_mandatory_cols(csv_data)
    # Move the file to error directory if mandatory fields doesn't exists
    if val_mandatory_cols:
        log.info(f"Data validation failed")
        shutil.copy(os.path.join(input_dir, input_file), error_dir)
        return 0

    val_data_type = validate_data_types(csv_data, lookup_data)
    # Move the file to error directory if the Data type validation fails
    if val_data_type:
        log.info(f"Data validation failed")
        shutil.copy(os.path.join(input_dir, input_file), error_dir)
        return 0

    val_codes = validate_codes(csv_data, lookup_data)
    # Move the file to error directory if Country, Company or Currency code doesn't exists in the lookup file
    if val_codes:
        log.info(f"Code validation failed")
        shutil.copy(os.path.join(input_dir, input_file), error_dir)
        return 0

    val_active_flag = validate_active_flag(csv_data)
    # Move the file to error directory if Country, Company or Currency code doesn't exists in the lookup file
    if val_active_flag:
        log.info(f"Code validation failed")
        shutil.copy(os.path.join(input_dir, input_file), error_dir)
        return 0
    csv_data = generate_output_file(csv_data)
    # Uploads the file as csv
    upload_as_csv(csv_output_dir, input_file, csv_data)
    # Uploads the file as parquet
    upload_as_parquet(parquet_output_dir, input_file, csv_data)

