import os
import pandas as pd
import chardet
import argparse
import numpy as np
import uuid
import re
import pyarrow as pa
import pyarrow.parquet as pq


def check_donor_id(x):
    if len(x) == 36:
        x = str(uuid.UUID(x))
    else:
        x = str(int(float(x.zfill(19))))
    return x

def check_post_code(x):
    try:
        x = re.search('[0-9]{4}', str(x)).group()
    except:
        x = None
    return x

def check_gender(x):
    if str(x).upper() not in ['M','F',None]:
        try:
            x = re.search('^MA|^FE',str(x).upper()).group()
        except:
            x = None
    return x


def check_birthdate(x):
    if x is not None:
        x = pd.to_datetime(x,errors = 'coerce')
    return x


class DataProcess:
     """
     Attributes:
         input filename: CSV file
     """
     def __init__(self, filename):
        if not os.path.isfile(filename):
            raise ValueError("invalid argument or file does not exist")
        self.filename = filename

     def validate_file(self):
        # Finding the encoding format of the file 
        with open(self.filename, 'rb') as rawdata:
            result_type = chardet.detect(rawdata.read(10000))
        encoding_type = result_type['encoding']
        df = pd.read_csv(self.filename, encoding=encoding_type)
        df = df.dropna(how='all')
        df = df.drop_duplicates() 
        df['donor_id'] = df["donor_id"].apply(lambda x: ''.join([" " if ord(i) < 32 or ord(i) > 126 else i for i in x]))
        df = df[df['donor_id']!= " "]
        df = df[df['donor_type'] != 'donor_type']
        df = df.replace({"^\s*|\s*$":""}, regex=True) 
        df = df.replace({"":np.nan})
        df['donor_id'] = df['donor_id'].apply(lambda x: check_donor_id(x)).astype(str)
        df['postcode'] = df['postcode'].apply(lambda x: check_post_code(x)).astype(str)
        df['gender'] = df['gender'].apply(lambda x: check_gender(x)).astype(str)
        df['birth_date'] = df['birth_date'].apply(lambda x: check_birthdate(x))
        df['donor_type'] = pd.to_numeric(df['donor_type'], errors='coerce')
        df['donor_type'] = df['donor_type'].fillna(-1)
        df['donor_type'] = df['donor_type'].astype(np.int32)
        paraquet_table = pa.Table.from_pandas(df)
        pq.write_table(paraquet_table, 'processed.parquet')


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Convert csv to praquet")
    parser.add_argument("-i", "--inputfile", required=True, help="Input file path for '.csv' file")
    args = parser.parse_args()
    input_path = args.inputfile
    dataprocess = DataProcess(input_path)
    dataprocess.validate_file()