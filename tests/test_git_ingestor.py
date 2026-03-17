"""Tests for the git commits ingestor."""

from pathlib import Path

from git import Repo

from lake.catalog import Catalog
from lake.ingestors.git_commits import GitCommitsIngestor
from lake.store import LakeStore


def _create_test_repo(path: Path, num_commits: int = 5) -> Repo:
    """Create a git repo with some commits for testing."""
    repo = Repo.init(path)
    repo.config_writer().set_value("user", "name", "Test User").release()
    repo.config_writer().set_value("user", "email", "test@test.com").release()

    for i in range(num_commits):
        f = path / f"file_{i}.txt"
        f.write_text(f"content {i}")
        repo.index.add([str(f)])
        repo.index.commit(f"Commit #{i}: add file_{i}.txt")

    return repo


def test_fetch_returns_records(tmp_path):
    """fetch() returns a list of dicts with expected fields."""
    repo_path = tmp_path / "repo"
    _create_test_repo(repo_path, num_commits=3)

    ingestor = GitCommitsIngestor(repo_path)
    records = ingestor.fetch()

    assert len(records) == 3
    assert all("hash" in r for r in records)
    assert all("message" in r for r in records)
    assert all("author" in r for r in records)
    assert records[0]["repo_name"] == "repo"


def test_full_ingest_writes_to_store(tmp_path):
    """Full ingest pipeline: fetch → normalize → write → catalog."""
    repo_path = tmp_path / "repo"
    _create_test_repo(repo_path, num_commits=5)

    lake_path = tmp_path / "lake"
    store = LakeStore(lake_path)
    catalog = Catalog(lake_path)

    ingestor = GitCommitsIngestor(repo_path)
    result = ingestor.ingest(store, catalog)

    assert result.rows_written == 5
    assert result.table_name == "commits"

    # Query via DuckDB
    query_result = store.query(
        "SELECT message FROM commits ORDER BY date DESC LIMIT 5"
    )
    messages = query_result.column("message").to_pylist()
    assert len(messages) == 5
    assert "Commit #4" in messages[0]

    # Catalog updated
    assert catalog.has_table("commits")
    assert catalog.get_table("commits").row_count == 5

    store.close()


def test_empty_repo_returns_zero(tmp_path):
    """Handle repos with no commits gracefully."""
    repo_path = tmp_path / "empty_repo"
    Repo.init(repo_path)

    lake_path = tmp_path / "lake"
    store = LakeStore(lake_path)
    catalog = Catalog(lake_path)

    ingestor = GitCommitsIngestor(repo_path)
    result = ingestor.ingest(store, catalog)

    assert result.rows_written == 0
    store.close()


def test_max_commits_limit(tmp_path):
    """max_commits parameter limits the number of ingested commits."""
    repo_path = tmp_path / "repo"
    _create_test_repo(repo_path, num_commits=10)

    ingestor = GitCommitsIngestor(repo_path, max_commits=3)
    records = ingestor.fetch()

    assert len(records) == 3
