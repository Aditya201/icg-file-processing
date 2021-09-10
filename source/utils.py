import pandas as pd
import numpy as np
from pydoc import locate
from datetime import datetime
import os


def read_csv_data_to_df(dir, file):
    fname = os.path.join(dir, file)
    csv_data = pd.read_csv(fname)
    return csv_data


def read_lookup_file_to_df(lookup_dir, lookup_file):
    lookup_df = pd.ExcelFile(os.path.join(lookup_dir, lookup_file))
    return lookup_df


# Checks the availability of Mandatory fields
def validate_mandatory_cols(csv_data):
    mandatory_col = ['D1', 'Country', 'Currency', 'Company']
    for index, row in csv_data.iterrows():
        for col in mandatory_col:
            if row.get(col) is None:
                return 1
    return 0


# Checks the valid data types for the csv data
def validate_data_types(csv_data, lookup_data):
    cols = ['Deal Name', 'D1', 'D2', 'D3', 'D4', 'D5', 'Active Flag', 'Country', 'Currency', 'Company']
    for index, row in csv_data.iterrows():
        for col in cols:
            if row.get(col) is not None and not isinstance(row[col], locate(lookup_data.parse(0)[col][0])):
                return 1
    return 0


# Checks the valid code for Country, Currency, Company fields against the lookup file
def validate_codes(csv_data, lookup_data):
    codes_df = lookup_data.parse(1)
    cols = ['Country', 'Currency', 'Company']
    for index, row in csv_data.iterrows():
        for col in cols:
            # Check the code exists in the lookup or not
            if row[col] in codes_df.Code.unique():
                # Check if the Type(Country/ Currency / Company) matches or not
                lkp = codes_df[(codes_df.Code == row[col])]
                if col == lkp.Type.unique()[0]:
                    # Assign the country name from the lookup file
                    if col == 'Country':
                        csv_data.loc[index, 'Country name'] = lkp.Name.unique()[0]
                else:
                    return 1
            else:
                return 1
    return 0


def validate_active_flag(csv_data):
    for index, row in csv_data.iterrows():
        # Assuming Activity Flag is not a mandatory field
        if row.get("Active Flag") not in ["Yes", "No", None]:
            return 1
    return 0


def generate_output_file(csv_data):
    columns = ['RowNo', 'Deal Name', 'D1', 'D2', 'D3', 'D4', 'D5', 'Active Flag', 'Country', 'Currency', 'Company',
               'Country name', 'AsOfDate', 'ProcessIdentifier', 'Rowhash']
    csv_data['RowNo'] = np.arange(len(csv_data))
    csv_data['AsOfDate'] = pd.Timestamp(datetime.now())
    csv_data['ProcessIdentifier'] = 'ICG'
    csv_data['Rowhash'] = csv_data.apply(lambda x: hash(tuple(x)), axis=1)
    csv_data = csv_data.reindex(columns, axis='columns')
    return csv_data


def upload_as_csv(csv_dir, file, csv_data):
    filename = csv_dir + file.split('.')[0] + '_' + str(datetime.now().strftime("%Y-%m-%d")) + '.csv'
    csv_data.to_csv(filename, index = False)


def upload_as_parquet(parquet_output_dir, file, csv_data):
    filename = parquet_output_dir + file.split('.')[0] + '_' + str(datetime.now().strftime("%Y-%m-%d")) + '.parquet'
    csv_data.to_parquet(filename)
