import boto3
import docker
import pymysql
import pytest
import time

from moto import mock_s3

MSQL_USER = 'root'
MSQL_PASSWD = 'password'
MYSQL_DB = 'test_db'
MYSQL_PORT = 3306
MAX_SECONDS_TO_WAIT_FOR_SERVER_TO_START = 30


def pytest_addoption(parser):
    parser.addoption(
        "--n", type='int', action="store", default=10, help="Scaling control for tests."
    )
    parser.addoption(
        "--keep-mysql-alive", action="store_true",
        help=(
            'Flag to keep mysql server alive after tests. If set, mysql server in docker container will remain '
            'running after test run finished (useful if you want to run tests quickly without waiting for '
            'mysql to start up again on each run). If not set (DEFAULT), mysql server in docker container will be '
            'killed and removed after test run. NB - if this option is used, the server container will have to be '
            'stopped manually! NOTE: This does not affect mysql servers that are already running. '
            'No Action is taken on those'
        )
    )


@pytest.fixture()
def n(request):
    return request.config.getoption("--n")


@pytest.fixture()
def mock_s3_w_resource(monkeypatch):
    # ensure we only have mock keys in the env before instantiating the resource!
    mock_env_vars = {
        'AWS_ACCESS_KEY_ID': 'test_AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY': 'test_AWS_SECRET_ACCESS_KEY',
        'AWS_SECURITY_TOKEN': 'test_AWS_SECURITY_TOKEN',
        'AWS_SESSION_TOKEN': 'test_AWS_SESSION_TOKEN',
    }
    for key, value in mock_env_vars.items():
        monkeypatch.setenv(key, value)

    with mock_s3():
        yield boto3.resource('s3')


@pytest.fixture()
def test_buckets(mock_s3_w_resource, test_bucket_name="test_bucket") -> dict:
    buckets = {test_bucket_name: test_bucket_name}
    for bucket in buckets.values():
        mock_s3_w_resource.Bucket(bucket).create()
    yield buckets


@pytest.fixture(scope='session')
def mysql_test_server(request):
    """
    Run mysql server in a docker container.
    Configured according to module-level constants above:
        port: MYSQL_PORT
        user: root (default, not explicitly configured but provided as MSQL_USER for convenience)
        password: MSQL_PASSWD
        db: MYSQL_DB
    Fixture will block until server is accepting connections (will throw exception if this does not happen within
    MAX_SECONDS_TO_WAIT_FOR_SERVER_TO_START) then yields a connection info dict to caller.
    Stops and prunes container after use.
    Runs at session-level scope (i.e. only starts / stops server once per session), so WILL NOT clean databases etc
    between test cases.
    """
    # setup
    server_already_running = is_server_running()

    if server_already_running:
        print(f'\nContactable mysql server already running on port {MYSQL_PORT}, test suite will use this.\n')
    else:
        print(
            f'\nCannot contact mysql server on port {MYSQL_PORT}, test suite fixture server (in docker) will be used.\n'
        )
        docker_client = docker.from_env()
        container = docker_client.containers.run('mysql:5.6',
                                                 detach=True,
                                                 ports={3306: MYSQL_PORT},
                                                 name='ingestion-lambda-rds-mysql',
                                                 environment={
                                                     'MYSQL_ROOT_PASSWORD': MSQL_PASSWD,
                                                     'MYSQL_DATABASE': MYSQL_DB
                                                 })
        assert_server_running(container.id)

    yield {
        'host': 'localhost',
        'port': MYSQL_PORT,
        'user': MSQL_USER,
        'passwd': MSQL_PASSWD,
        'db': MYSQL_DB
    }

    # teardown
    if request.config.getoption("--keep-mysql-alive") is True or server_already_running:
        print(f'\nMysql server in docker container on port {MYSQL_PORT} left alive.\n')
    else:
        container.kill()
        container.remove()
        docker_client.volumes.prune()


def is_server_running() -> bool:
    try:
        con = pymysql.connect(
            host='localhost',
            port=MYSQL_PORT,
            user=MSQL_USER,
            passwd=MSQL_PASSWD,
            db=MYSQL_DB,
            autocommit=False
        )
        con.close()
        return True
    except pymysql.err.OperationalError:
        return False


def assert_server_running(container_id: str) -> None:
    remaining_timeout = MAX_SECONDS_TO_WAIT_FOR_SERVER_TO_START

    while remaining_timeout > 0:
        if is_server_running():
            return

        time.sleep(1)
        remaining_timeout -= 1

    raise EnvironmentError(
        f'MYSQL server in docker container {container_id} did not start and accept connections within '
        f'{MAX_SECONDS_TO_WAIT_FOR_SERVER_TO_START} seconds!'
    )


@pytest.fixture()
def mysql_test_connection(mysql_test_server):
    con = pymysql.connect(
        host=mysql_test_server['host'],
        port=mysql_test_server['port'],
        user=mysql_test_server['user'],
        passwd=mysql_test_server['passwd'],
        db=mysql_test_server['db'],
        autocommit=False
    )

    yield con

    con.close()


@pytest.fixture()
def test_table(mysql_test_connection):
    test_table = 'test_table'
    with mysql_test_connection.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE {test_table} (
                this VARCHAR(100),
                that VARCHAR(100),
                the VARCHAR(100),
                other VARCHAR(100)
            )
            """
        )
    mysql_test_connection.commit()

    yield test_table

    with mysql_test_connection.cursor() as cur:
        cur.execute(f'DROP TABLE {test_table}')


@pytest.fixture()
def mock_env_for_lambda_function(mysql_test_server, monkeypatch, test_table):
    mock_env_vars = {
        'mysql_endpoint': mysql_test_server['host'],
        'mysql_port': str(mysql_test_server['port']),
        'mysql_username': mysql_test_server['user'],
        'mysql_password': mysql_test_server['passwd'],
        'mysql_db': mysql_test_server['db'],
        'mysql_table': test_table
    }

    for key, value in mock_env_vars.items():
        monkeypatch.setenv(key, value)

    yield
