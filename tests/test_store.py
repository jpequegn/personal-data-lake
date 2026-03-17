"""Tests for LakeStore — the Parquet + DuckDB storage layer."""

from datetime import date, timedelta

import pyarrow as pa

from lake.store import LakeStore


def _make_test_table(n: int = 100) -> pa.Table:
    """Create a simple Arrow table with n rows and a date column."""
    base_date = date(2026, 3, 1)
    return pa.table(
        {
            "id": list(range(n)),
            "title": [f"item-{i}" for i in range(n)],
            "date": [base_date + timedelta(days=i % 28) for i in range(n)],
        }
    )


def test_write_close_reopen_query(tmp_path):
    """Acceptance criteria: write 100 rows, close, reopen, query returns 100."""
    store = LakeStore(tmp_path)
    table = _make_test_table(100)
    rows_written = store.write("test_table", table)
    assert rows_written == 100
    store.close()

    # Reopen — views should be re-registered from existing Parquet
    store2 = LakeStore(tmp_path)
    result = store2.query("SELECT COUNT(*) AS cnt FROM test_table")
    assert result.column("cnt")[0].as_py() == 100
    store2.close()


def test_tables_listing(tmp_path):
    """LakeStore.tables() returns registered table names."""
    store = LakeStore(tmp_path)
    assert store.tables() == []

    store.write("podcasts", _make_test_table(10))
    assert "podcasts" in store.tables()
    store.close()


def test_hive_partitioning(tmp_path):
    """Data is partitioned by year/month and queryable."""
    store = LakeStore(tmp_path)
    store.write("events", _make_test_table(50))

    result = store.query("SELECT DISTINCT year, month FROM events ORDER BY year, month")
    assert len(result) >= 1  # at least one partition
    store.close()


def test_query_returns_correct_data(tmp_path):
    """Query filters work against Parquet-backed views."""
    store = LakeStore(tmp_path)
    store.write("items", _make_test_table(100))

    result = store.query("SELECT * FROM items WHERE id < 10")
    assert len(result) == 10
    store.close()
