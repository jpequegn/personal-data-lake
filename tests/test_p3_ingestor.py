"""Tests for the P3 podcasts ingestor."""

import json
from pathlib import Path

import duckdb

from lake.catalog import Catalog
from lake.ingestors.p3_podcasts import P3PodcastsIngestor
from lake.store import LakeStore


def _create_p3_db(db_path: Path, num_episodes: int = 10) -> None:
    """Create a mock P3 DuckDB database with test data."""
    conn = duckdb.connect(str(db_path))

    conn.execute("""
        CREATE TABLE podcasts (
            id INTEGER PRIMARY KEY,
            name VARCHAR
        )
    """)
    conn.execute("INSERT INTO podcasts VALUES (1, 'AI Daily'), (2, 'Data Talk')")

    conn.execute("""
        CREATE TABLE episodes (
            id INTEGER PRIMARY KEY,
            podcast_id INTEGER,
            title VARCHAR,
            date DATE
        )
    """)

    conn.execute("""
        CREATE TABLE summaries (
            episode_id INTEGER,
            key_topics VARCHAR,
            themes VARCHAR,
            full_summary VARCHAR,
            key_takeaways VARCHAR,
            companies_mentioned VARCHAR
        )
    """)

    for i in range(1, num_episodes + 1):
        podcast_id = 1 if i <= num_episodes // 2 else 2
        conn.execute(
            "INSERT INTO episodes VALUES (?, ?, ?, ?)",
            [i, podcast_id, f"Episode {i}: AI Topic {i}", f"2026-03-{i:02d}"],
        )
        conn.execute(
            "INSERT INTO summaries VALUES (?, ?, ?, ?, ?, ?)",
            [
                i,
                json.dumps(["AI", f"topic_{i}"]),
                json.dumps(["tech"]),
                f"Full summary of episode {i} about AI and data engineering.",
                json.dumps([f"takeaway_{i}"]),
                json.dumps(["Anthropic"] if i % 3 == 0 else []),
            ],
        )

    conn.close()


def test_fetch_returns_all_episodes(tmp_path):
    """fetch() returns records for all episodes."""
    db_path = tmp_path / "p3.duckdb"
    _create_p3_db(db_path, num_episodes=10)

    ingestor = P3PodcastsIngestor(db_path)
    records = ingestor.fetch()

    assert len(records) == 10
    assert all("id" in r for r in records)
    assert all("title" in r for r in records)
    assert all("podcast_name" in r for r in records)


def test_fetch_incremental(tmp_path):
    """fetch() with last_ingested_id only returns new rows."""
    db_path = tmp_path / "p3.duckdb"
    _create_p3_db(db_path, num_episodes=10)

    ingestor = P3PodcastsIngestor(db_path)

    # First fetch: all 10
    records = ingestor.fetch(last_ingested_id=None)
    assert len(records) == 10

    # Incremental: only rows after id=7
    records = ingestor.fetch(last_ingested_id=7)
    assert len(records) == 3
    assert all(r["id"] > 7 for r in records)


def test_full_ingest_and_query(tmp_path):
    """Full ingest pipeline writes to store and is queryable."""
    db_path = tmp_path / "p3.duckdb"
    _create_p3_db(db_path, num_episodes=10)

    lake_path = tmp_path / "lake"
    store = LakeStore(lake_path)
    catalog = Catalog(lake_path)

    ingestor = P3PodcastsIngestor(db_path)
    result = ingestor.ingest(store, catalog)

    assert result.rows_written == 10
    assert result.table_name == "podcasts"

    # Query
    count = store.query("SELECT COUNT(*) AS cnt FROM podcasts")
    assert count.column("cnt")[0].as_py() == 10

    # Catalog updated
    assert catalog.has_table("podcasts")
    assert catalog.get_table("podcasts").row_count == 10

    store.close()


def test_incremental_ingest(tmp_path):
    """Re-running ingest only fetches new rows."""
    db_path = tmp_path / "p3.duckdb"
    _create_p3_db(db_path, num_episodes=5)

    lake_path = tmp_path / "lake"
    store = LakeStore(lake_path)
    catalog = Catalog(lake_path)

    ingestor = P3PodcastsIngestor(db_path)

    # First ingest: 5 rows
    result1 = ingestor.ingest(store, catalog)
    assert result1.rows_written == 5

    # Re-run: 0 new rows (incremental)
    result2 = ingestor.ingest(store, catalog)
    assert result2.rows_written == 0

    # Add more episodes to the source DB
    conn = duckdb.connect(str(db_path))
    for i in range(6, 9):
        conn.execute(
            "INSERT INTO episodes VALUES (?, ?, ?, ?)",
            [i, 1, f"Episode {i}", f"2026-03-{i:02d}"],
        )
        conn.execute(
            "INSERT INTO summaries VALUES (?, ?, ?, ?, ?, ?)",
            [i, "[]", "[]", f"Summary {i}", "[]", "[]"],
        )
    conn.close()

    # Third ingest: 3 new rows
    result3 = ingestor.ingest(store, catalog)
    assert result3.rows_written == 3

    # Total should be 8
    total = store.query("SELECT COUNT(*) AS cnt FROM podcasts")
    assert total.column("cnt")[0].as_py() == 8

    store.close()


def test_json_columns_are_strings(tmp_path):
    """JSON columns are serialized as strings in the output."""
    db_path = tmp_path / "p3.duckdb"
    _create_p3_db(db_path, num_episodes=3)

    ingestor = P3PodcastsIngestor(db_path)
    records = ingestor.fetch()

    for r in records:
        for col in ("key_topics", "themes", "key_takeaways", "companies_mentioned"):
            assert isinstance(r[col], str), f"{col} should be a string"
            json.loads(r[col])  # Should be valid JSON
