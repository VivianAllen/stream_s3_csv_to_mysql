import boto3
import csv
import codecs
import functools
import logging
import os
import pymysql
import sys
import timeit

from contextlib import closing
from typing import Tuple


# logging config
LOG_LEVEL = os.environ.get('log_level', 'INFO')
logger = logging.getLogger(__name__)
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setFormatter(logging.Formatter(fmt='%(asctime)s %(levelname)s: %(message)s', datefmt="%Y-%m-%dT%H:%M:%S%z"))
logger.addHandler(out_hdlr)
logger.setLevel(LOG_LEVEL)

# s3
s3 = boto3.resource('s3')
CSV_DELIMITER = os.environ.get('csv_delimiter', ',')

# db
DB_HOST = os.environ['mysql_endpoint']
DB_PORT = int(os.environ['mysql_port'])
DB_USER = os.environ['mysql_username']
DB_PASSWD = os.environ['mysql_password']
DB_DB = os.environ['mysql_db']
DB_TABLE = os.environ['mysql_table']


def debug_log_execution_time(function):
    """
    Decorator to debug log execution time of decorated function.
    """
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        start_execution_time_s = timeit.default_timer()
        result = function(*args, **kwargs)
        execution_time_s = timeit.default_timer() - start_execution_time_s
        logger.debug(f'Function {function.__name__} executed in {execution_time_s}s.')
        return result
    return wrapper


# ==================================================== MAIN ========================================================== #


@debug_log_execution_time
def handler(event, context={}):
    bucket, key = get_s3_properties_from_event(event)
    stream_reader = attach_csv_stream(bucket, key)

    with closing(get_db_connection()) as conn:
        stream_to_db(conn, stream_reader, DB_TABLE)


# ==================================================== SUB =========================================================== #


def get_s3_properties_from_event(event) -> Tuple[str]:
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']
    return bucket, key


@debug_log_execution_time
def attach_csv_stream(bucket: str, key: str) -> csv.DictReader:
    obj = s3.Object(bucket, key)
    response = obj.get()
    stream = codecs.getreader('utf-8-sig')(response['Body'])
    return csv.DictReader(f=stream, delimiter=CSV_DELIMITER)


@debug_log_execution_time
def get_db_connection() -> pymysql.Connection:
    return pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWD, database=DB_DB, port=DB_PORT)


@debug_log_execution_time
def stream_to_db(conn: pymysql.Connection, stream_reader: csv.DictReader, table: str) -> None:
    fieldname_sql = ','.join(stream_reader.fieldnames)
    values_placeholder_str = ','.join(['%s'] * len(stream_reader.fieldnames))
    values_generator = ([record[fieldname] for fieldname in stream_reader.fieldnames] for record in stream_reader)

    with conn.cursor() as cur:
        cur.executemany(f'INSERT INTO {table}({fieldname_sql}) VALUES ({values_placeholder_str})', values_generator)

    conn.commit()
