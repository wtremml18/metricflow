from pathlib import Path

import pandas as pd
import pytest

from metricflow.object_utils import random_id
from metricflow.protocols.sql_client import SqlClient
from metricflow.test.compare_df import assert_dataframes_equal
from metricflow.test.table_snapshot.table_snapshots import SqlTableSnapshot, SqlTableColumnDefinition, \
    SqlTableColumnType, SqlTableSnapshotRestorer
from metricflow.test.test_utils import as_datetime


@pytest.fixture
def table_snapshot() -> SqlTableSnapshot:  # noqa: D
    rows = (
        ("true", "1", "1.0", "2020-01-02", "hi"),
        ("false", "1", "1.0", "2020-03-04 05:06:07", "bye"),
    )

    return SqlTableSnapshot(
        name="example_snapshot",
        column_definitions=(
            SqlTableColumnDefinition(name="col0", type=SqlTableColumnType.BOOLEAN),
            SqlTableColumnDefinition(name="col1", type=SqlTableColumnType.INT),
            SqlTableColumnDefinition(name="col2", type=SqlTableColumnType.FLOAT),
            SqlTableColumnDefinition(name="col3", type=SqlTableColumnType.TIME),
            SqlTableColumnDefinition(name="col4", type=SqlTableColumnType.STRING),
        ),
        rows=rows,
        file_path=Path("/a/b/c"),
    )


def test_as_df(table_snapshot: SqlTableSnapshot) -> None:
    """Check that SqlTableSnapshot.as_df works as expected."""

    assert_dataframes_equal(
        actual=table_snapshot.as_df,
        expected=pd.DataFrame(
            columns=[f"col{i}" for i in range(5)],
            data=(
                (True, 1, 1.0, as_datetime("2020-01-02"), "hi"),
                (False, 1, 1.0, as_datetime("2020-03-04 05:06:07"), "bye"),
            )
        )
    )


def test_restore(sql_client: SqlClient, table_snapshot: SqlTableSnapshot) -> None:  # noqa: D
    schema_name = f"mf_test_snapshot_schema_{random_id()}"

    try:
        sql_client.create_schema(schema_name)

        snapshot_restorer = SqlTableSnapshotRestorer(sql_client=sql_client, schema_name=schema_name)
        snapshot_restorer.restore(table_snapshot)

        actual = sql_client.query(f"SELECT * FROM {schema_name}.{table_snapshot.name}")
        assert_dataframes_equal(actual=actual, expected=table_snapshot.as_df)

    finally:
        sql_client.drop_schema(schema_name, cascade=True)
