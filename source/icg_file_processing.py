import os
import shutil
import logging
from source.utils import read_csv_data_to_df, read_lookup_file_to_df, validate_mandatory_cols, validate_codes, \
    validate_data_types, validate_active_flag, generate_output_file, upload_as_csv, upload_as_parquet

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def handler(event):
    dir = event["dir"]
    file = event["file"]
    lookup_dir = event["lookup_dir"]
    lookup_file = event["lookup_file"]
    error_file = event["error_file"]
    csv_data = read_csv_data_to_df(dir, file)
    lookup_data = read_lookup_file_to_df(lookup_dir, lookup_file)
    val_mandatory_cols = validate_mandatory_cols(csv_data)
    # Move the file to error directory if mandatory fields doesn't exists
    if val_mandatory_cols:
        log.info(f"Data validation failed")
        shutil.copy(os.path.join(dir, file), error_file)
        return 0

    val_data_type = validate_data_types(csv_data, lookup_data)
    # Move the file to error directory if the Data type validation fails
    if val_data_type:
        log.info(f"Data validation failed")
        shutil.copy(os.path.join(dir, file), error_file)
        return 0

    val_codes = validate_codes(csv_data, lookup_data)
    # Move the file to error directory if Country, Company or Currency code doesn't exists in the lookup file
    if val_codes:
        log.info(f"Code validation failed")
        shutil.copy(os.path.join(dir, file), error_file)
        return 0

    val_active_flag = validate_active_flag(csv_data)
    # Move the file to error directory if Country, Company or Currency code doesn't exists in the lookup file
    if val_active_flag:
        log.info(f"Code validation failed")
        shutil.copy(os.path.join(dir, file), error_file)
        return 0
    csv_data = generate_output_file(csv_data)
    # Uploads the file as csv
    upload_as_csv(event["csv_output_dir"], file, csv_data)
    # Uploads the file as parquet
    upload_as_parquet(event["parquet_output_dir"], file, csv_data)

