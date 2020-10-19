import helpers


# ================================================== FIXTURES ======================================================== #


def get_test_records(n):
    return [{'this': str(i), 'that': str(i), 'the': str(i), 'other': str(i)} for i in range(n)]


# ==================================================== TEST ========================================================== #


def test_handler(mock_s3_w_resource, mysql_test_connection, test_buckets, mock_env_for_lambda_function, test_table, n):
    import handler
    # GIVEN a set of n test records written as a csv to s3
    test_records = get_test_records(n)
    test_key = 'some_file.csv'
    test_bucket = test_buckets['test_bucket']
    event = helpers.dict_to_s3_csv_return_event(test_records, test_bucket, test_key, mock_s3_w_resource)

    # WHEN we pass it to handler.handler
    handler.handler(event)

    # THEN we expect to see that content written into the test table
    returned_records = helpers.table_contents(mysql_test_connection, test_table)
    assert returned_records == test_records
