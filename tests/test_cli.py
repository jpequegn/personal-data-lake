"""Tests for CLI commands."""

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
from click.testing import CliRunner

from lake.catalog import Catalog, ColumnDef
from lake.cli import cli
from lake.store import LakeStore


def _setup_lake_with_data(tmp_path: Path):
    """Create a lake with test data for CLI tests."""
    store = LakeStore(tmp_path)
    catalog = Catalog(tmp_path)

    table = pa.table(
        {
            "title": ["Note A", "Note B", "Note C"],
            "word_count": [100, 200, 50],
            "date": [date(2026, 3, 1), date(2026, 3, 5), date(2026, 3, 10)],
        }
    )
    store.write("notes", table)
    catalog.register_table(
        "notes",
        [
            ColumnDef(name="title", type="VARCHAR"),
            ColumnDef(name="word_count", type="INTEGER"),
            ColumnDef(name="date", type="DATE"),
        ],
        source="markdown",
    )
    catalog.update_row_count("notes", 3)
    store.close()


def test_tables_empty(tmp_path):
    """tables command shows message when no tables exist."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["tables"])
        assert result.exit_code == 0
        assert "No tables" in result.output


def test_tables_with_data(tmp_path):
    """tables command shows table info with rich formatting."""
    _setup_lake_with_data(tmp_path)
    runner = CliRunner()
    with patch("lake.cli._lake_root", return_value=tmp_path):
        result = runner.invoke(cli, ["tables"])
        assert result.exit_code == 0
        assert "notes" in result.output
        assert "3" in result.output


def test_stats_empty(tmp_path):
    """stats command shows message when lake is empty."""
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["stats"])
        assert result.exit_code == 0
        assert "empty" in result.output.lower()


def test_stats_with_data(tmp_path):
    """stats command shows disk usage and row counts."""
    _setup_lake_with_data(tmp_path)
    runner = CliRunner()
    with patch("lake.cli._lake_root", return_value=tmp_path):
        result = runner.invoke(cli, ["stats"])
        assert result.exit_code == 0
        assert "notes" in result.output
        assert "3" in result.output


def test_sql_command(tmp_path):
    """sql command executes raw SQL and shows results."""
    _setup_lake_with_data(tmp_path)
    runner = CliRunner()
    with patch("lake.cli._lake_root", return_value=tmp_path):
        result = runner.invoke(cli, ["sql", "SELECT COUNT(*) AS cnt FROM notes"])
        assert result.exit_code == 0
        assert "3" in result.output


def test_sql_error(tmp_path):
    """sql command shows error for bad SQL."""
    _setup_lake_with_data(tmp_path)
    runner = CliRunner()
    with patch("lake.cli._lake_root", return_value=tmp_path):
        result = runner.invoke(cli, ["sql", "SELECT * FROM nonexistent"])
        assert result.exit_code == 0
        assert "Error" in result.output


def test_schema_command(tmp_path):
    """schema command shows table schema."""
    _setup_lake_with_data(tmp_path)
    runner = CliRunner()
    with patch("lake.cli._lake_root", return_value=tmp_path):
        result = runner.invoke(cli, ["schema", "notes"])
        assert result.exit_code == 0
        assert "title" in result.output
        assert "word_count" in result.output


def test_ingest_all_no_config(tmp_path):
    """ingest --all shows help when no config file exists."""
    runner = CliRunner()
    with patch("lake.cli._lake_root", return_value=tmp_path):
        result = runner.invoke(cli, ["ingest", "--all"])
        assert result.exit_code == 0
        assert "lake.config.json" in result.output


def test_ingest_all_with_config(tmp_path):
    """ingest --all reads config and ingests configured sources."""
    # Create a test repo
    from git import Repo

    repo_path = tmp_path / "test-repo"
    repo_path.mkdir()
    repo = Repo.init(repo_path)
    repo.config_writer().set_value("user", "name", "Test").release()
    repo.config_writer().set_value("user", "email", "t@t.com").release()
    (repo_path / "f.txt").write_text("hi")
    repo.index.add(["f.txt"])
    repo.index.commit("init")

    # Create config
    lake_path = tmp_path / "lake"
    lake_path.mkdir()
    config = {"sources": [{"type": "git", "path": str(repo_path)}]}
    (lake_path / "lake.config.json").write_text(json.dumps(config))

    runner = CliRunner()
    with patch("lake.cli._lake_root", return_value=lake_path):
        result = runner.invoke(cli, ["ingest", "--all"])
        assert result.exit_code == 0
        assert "1 rows" in result.output


def test_ingest_requires_source_or_all(tmp_path):
    """ingest without --source or --all shows error."""
    runner = CliRunner()
    with patch("lake.cli._lake_root", return_value=tmp_path):
        result = runner.invoke(cli, ["ingest"])
        assert result.exit_code != 0 or "required" in result.output.lower()
