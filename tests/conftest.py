from __future__ import annotations

from collections.abc import Callable, Generator
import os
import time
import uuid
from typing import TYPE_CHECKING, Protocol, cast

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from google.cloud import bigquery


class DynamoDBTestClient(Protocol):
    def create_table(self, **kwargs: object) -> object: ...

    def list_tables(self) -> object: ...

    def describe_table(self, *, TableName: str) -> dict[str, dict[str, str]]: ...

    def delete_table(self, *, TableName: str) -> object: ...


PostgresTableFactory = Callable[[str], str]
DynamoDBTableFactory = Callable[[str], str]


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: requires external services or credentials")
    config.addinivalue_line("markers", "postgres: requires PostgreSQL test database")
    config.addinivalue_line("markers", "dynamodb: requires LocalStack DynamoDB test service")
    config.addinivalue_line("markers", "bigquery: requires real Google BigQuery project credentials")


@pytest.fixture(scope="session")
def postgres_url() -> str:
    url = os.environ.get("TEST_POSTGRES_URL")
    if not url:
        pytest.skip("TEST_POSTGRES_URL missing — skipping PostgreSQL integration tests")
    return url


@pytest.fixture(scope="session")
def postgres_engine(postgres_url: str) -> Generator[Engine, None, None]:
    engine = create_engine(postgres_url)
    try:
        with engine.connect() as conn:
            _ = conn.execute(text("SELECT 1"))
    except Exception as e:
        pytest.skip(f"PostgreSQL 연결 실패 — {e}")
    yield engine
    engine.dispose()


@pytest.fixture
def postgres_table_factory(postgres_engine: Engine) -> Generator[PostgresTableFactory, None, None]:
    created_tables: list[str] = []

    def factory(prefix: str = "t") -> str:
        name = f"{prefix}_{uuid.uuid4().hex[:8]}"
        created_tables.append(name)
        return name

    yield factory

    with postgres_engine.begin() as conn:
        for table_name in reversed(created_tables):
            _ = conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE'))


@pytest.fixture(scope="session")
def dynamodb_client() -> DynamoDBTestClient:
    endpoint = os.environ.get("DYNAMODB_ENDPOINT_URL")
    if not endpoint:
        pytest.skip("DYNAMODB_ENDPOINT_URL 없음 — DynamoDB 통합 테스트 건너뜀")

    try:
        import boto3
    except ModuleNotFoundError as e:
        pytest.skip(f"boto3 없음 — {e}")

    client = cast(
        DynamoDBTestClient,
        boto3.client(
        "dynamodb",
        endpoint_url=endpoint,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        ),
    )

    try:
        _ = client.list_tables()
    except Exception as e:
        pytest.skip(f"DynamoDB(LocalStack) 연결 실패 — {e}")

    return client


@pytest.fixture
def dynamodb_table_factory(dynamodb_client: DynamoDBTestClient) -> Generator[DynamoDBTableFactory, None, None]:
    created_tables: list[str] = []

    def factory(prefix: str = "t") -> str:
        name = f"{prefix}_{uuid.uuid4().hex[:8]}"
        created_tables.append(name)
        return name

    yield factory

    for table_name in created_tables:
        try:
            _ = dynamodb_client.delete_table(TableName=table_name)
        except Exception:
            continue


@pytest.fixture(scope="session")
def bq_client() -> bigquery.Client:
    project = os.environ.get("TEST_BQ_PROJECT")
    if not project:
        pytest.skip("TEST_BQ_PROJECT 없음 — BigQuery 통합 테스트 건너뜀")

    try:
        from google.auth.exceptions import DefaultCredentialsError
        from google.cloud import bigquery
    except ModuleNotFoundError as e:
        pytest.skip(f"google-cloud-bigquery 없음 — {e}")

    try:
        client = bigquery.Client(project=project)
        _ = next(iter(client.list_projects(page_size=1)), None)
    except DefaultCredentialsError as e:
        pytest.skip(f"BigQuery 인증 없음 — {e}")
    except Exception as e:
        pytest.skip(f"BigQuery 클라이언트 준비 실패 — {e}")

    return client


@pytest.fixture(scope="session")
def bq_dataset(bq_client: bigquery.Client) -> Generator[str, None, None]:
    from google.cloud import bigquery

    dataset_id = os.environ.get("TEST_BQ_DATASET", f"datagen_it_{uuid.uuid4().hex[:8]}")
    dataset_ref = bigquery.Dataset(f"{bq_client.project}.{dataset_id}")
    _ = bq_client.create_dataset(dataset_ref, exists_ok=True)
    yield dataset_id
    bq_client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)


def wait_for_dynamodb_table(dynamodb_client: DynamoDBTestClient, table_name: str, *, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status = dynamodb_client.describe_table(TableName=table_name)["Table"]["TableStatus"]
            if status == "ACTIVE":
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise AssertionError(f"DynamoDB table did not become ACTIVE: {table_name}")
