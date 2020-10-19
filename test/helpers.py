import boto3
import csv
import pymysql

from io import StringIO
from typing import Dict, List


def dict_to_csv_string(data_dict_list: List[dict], ordered_fieldnames: List[str] or None = None,
                       delimiter: str = ',') -> str:
    """
    Convert data_dict to string representing csv with headers. If ordered_fieldnames is not None, use those rather than
    data_dict_list.keys() to determine order of columns in output.
    """
    fieldnames = ordered_fieldnames or data_dict_list[0].keys()
    with StringIO() as buf:
        writer = csv.DictWriter(buf, fieldnames, delimiter=delimiter)
        writer.writeheader()
        for entry in data_dict_list:
            writer.writerow(entry)
        return buf.getvalue()


def dict_to_s3_csv(data_dict_list: List[dict], bucket: str, s3_key: str, s3_resource: boto3.resource,
                   ordered_fieldnames: List[str] or None = None, delimiter: str = ',') -> None:
    """
    Write records in data_dict_list as csv rows to s3_key file in bucket. Creates bucket if it does not exist.
    """
    file_str = dict_to_csv_string(data_dict_list, ordered_fieldnames=ordered_fieldnames, delimiter=delimiter)
    s3_resource.Bucket(bucket).create()
    s3_resource.Object(bucket, s3_key).put(Body=file_str)


def get_event(bucket: str, s3_key: str):
    return {
        'Records': [
            {
                's3': {
                    'bucket': {
                        'name': bucket
                    },
                    'object': {
                        'key': s3_key
                    }
                }
            }
        ]
    }


def dict_to_s3_csv_return_event(data_dict_list: List[dict], bucket: str, s3_key: str, s3_resource: boto3.resource,
                                delimiter: str = ',', ordered_fieldnames: List[str] or None = None) -> dict:
    """
    Write records in data_dict_list as csv rows to s3_key file in bucket, and return details as dict mimicking AWS
    event from S3 monitor queue.
    """
    dict_to_s3_csv(data_dict_list, bucket, s3_key, s3_resource, ordered_fieldnames, delimiter)
    return get_event(bucket, s3_key)


def table_contents(con: pymysql.Connection, table: str) -> List[Dict[str, str]]:
    with con.cursor() as cur:
        cur.execute(f'SELECT * FROM {table}')
        fieldnames = [field[0] for field in cur.description]
        return [{fieldnames[i]: str(row[i]) for i in range(len(row))} for row in cur.fetchall()]
