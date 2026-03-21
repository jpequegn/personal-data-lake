"""Tests for the BriefEngine — AI-generated intelligence reports."""

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
from click.testing import CliRunner

from lake.brief import BriefEngine
from lake.catalog import Catalog, ColumnDef
from lake.cli import cli
from lake.store import LakeStore


def _seed_lake(tmp_path):
    """Create a lake with multi-source test data."""
    store = LakeStore(tmp_path)
    catalog = Catalog(tmp_path)

    # Podcasts
    podcasts = pa.table({
        "id": [1, 2, 3],
        "podcast_name": ["AI Daily", "Data Talk", "AI Daily"],
        "title": ["Embeddings Deep Dive", "DuckDB Revolution", "Agents at Scale"],
        "date": [date(2026, 3, 15), date(2026, 3, 16), date(2026, 3, 17)],
        "key_topics": [
            json.dumps(["embeddings", "vectors"]),
            json.dumps(["duckdb", "parquet"]),
            json.dumps(["agents", "embeddings"]),
        ],
        "full_summary": [
            "Deep dive into embedding models and vector search.",
            "How DuckDB is changing local analytics.",
            "Building AI agents at scale with embeddings.",
        ],
        "companies_mentioned": [
            json.dumps(["Anthropic"]),
            json.dumps(["DuckDB Labs"]),
            json.dumps(["Anthropic", "OpenAI"]),
        ],
    })
    store.write("podcasts", podcasts)
    catalog.register_table("podcasts", [
        ColumnDef(name="id", type="INTEGER"),
        ColumnDef(name="podcast_name", type="VARCHAR"),
        ColumnDef(name="title", type="VARCHAR"),
        ColumnDef(name="date", type="DATE"),
        ColumnDef(name="key_topics", type="VARCHAR"),
        ColumnDef(name="full_summary", type="VARCHAR"),
        ColumnDef(name="companies_mentioned", type="VARCHAR"),
    ], source="p3_podcasts")
    catalog.update_row_count("podcasts", 3)

    # Commits
    commits = pa.table({
        "hash": ["aaa", "bbb", "ccc", "ddd"],
        "repo_name": ["embeddings-lib", "embeddings-lib", "data-lake", "data-lake"],
        "author": ["Julien", "Julien", "Julien", "Julien"],
        "date": ["2026-03-16 10:00:00", "2026-03-17 11:00:00", "2026-03-17 14:00:00", "2026-03-17 16:00:00"],
        "message": [
            "feat: add vector search module",
            "feat: implement cosine similarity",
            "feat: add parquet writer",
            "fix: schema evolution edge case",
        ],
        "insertions": [120, 80, 200, 15],
        "deletions": [10, 5, 30, 3],
    })
    store.write("commits", commits)
    catalog.register_table("commits", [
        ColumnDef(name="hash", type="VARCHAR"),
        ColumnDef(name="repo_name", type="VARCHAR"),
        ColumnDef(name="author", type="VARCHAR"),
        ColumnDef(name="date", type="TIMESTAMP"),
        ColumnDef(name="message", type="VARCHAR"),
        ColumnDef(name="insertions", type="INTEGER"),
        ColumnDef(name="deletions", type="INTEGER"),
    ], source="git")
    catalog.update_row_count("commits", 4)

    # Notes
    notes = pa.table({
        "title": ["Embeddings Research", "Data Lake Architecture"],
        "content": [
            "Notes on embedding models, vector search, and retrieval patterns.",
            "Architecture decisions for the personal data lake project.",
        ],
        "word_count": [150, 200],
        "tags": [json.dumps(["ai", "embeddings"]), json.dumps(["data", "architecture"])],
        "date": [date(2026, 3, 15), date(2026, 3, 16)],
    })
    store.write("notes", notes)
    catalog.register_table("notes", [
        ColumnDef(name="title", type="VARCHAR"),
        ColumnDef(name="content", type="VARCHAR"),
        ColumnDef(name="word_count", type="INTEGER"),
        ColumnDef(name="tags", type="VARCHAR"),
        ColumnDef(name="date", type="DATE"),
    ], source="markdown")
    catalog.update_row_count("notes", 2)

    return store, catalog


@patch("lake.brief.anthropic.Anthropic")
def test_build_context_includes_all_tables(mock_anthropic_cls, tmp_path):
    """Context includes data from all available tables."""
    store, catalog = _seed_lake(tmp_path)

    mock_client = MagicMock()
    mock_anthropic_cls.return_value = mock_client

    engine = BriefEngine(store, catalog)
    context, tables_used, total_rows = engine._build_context(days=7)

    assert "podcasts" in tables_used
    assert "commits" in tables_used
    assert "notes" in tables_used
    assert total_rows == 9  # 3 + 4 + 2
    assert "Embeddings Deep Dive" in context
    assert "vector search module" in context
    assert "Embeddings Research" in context

    store.close()


@patch("lake.brief.anthropic.Anthropic")
def test_generate_calls_claude_with_context(mock_anthropic_cls, tmp_path):
    """generate() sends cross-source context to Claude."""
    store, catalog = _seed_lake(tmp_path)

    mock_client = MagicMock()
    mock_anthropic_cls.return_value = mock_client
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="## Activity Summary\nYou were busy.\n\n## Cross-Source Connections\nEmbeddings everywhere.")]
    )

    engine = BriefEngine(store, catalog)
    brief = engine.generate(days=7)

    assert brief.tables_used == ["commits", "notes", "podcasts"]
    assert brief.total_rows_analyzed == 9
    assert "Activity Summary" in brief.content
    assert "Cross-Source Connections" in brief.content

    # Verify Claude was called with data from all sources
    call_args = mock_client.messages.create.call_args
    prompt = call_args.kwargs["messages"][0]["content"]
    assert "podcasts" in prompt
    assert "commits" in prompt
    assert "notes" in prompt

    store.close()


@patch("lake.brief.anthropic.Anthropic")
def test_save_creates_dated_file(mock_anthropic_cls, tmp_path):
    """save() writes a markdown file to data/briefs/."""
    store, catalog = _seed_lake(tmp_path)

    mock_client = MagicMock()
    mock_anthropic_cls.return_value = mock_client
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="## Activity Summary\nTest brief content.")]
    )

    engine = BriefEngine(store, catalog)
    brief = engine.generate(days=7)
    filepath = engine.save(brief, tmp_path)

    assert filepath.exists()
    assert filepath.suffix == ".md"
    content = filepath.read_text()
    assert "Daily Brief" in content
    assert "Test brief content" in content
    assert "commits" in content  # tables_used in header

    store.close()


@patch("lake.brief.anthropic.Anthropic")
def test_list_briefs(mock_anthropic_cls, tmp_path):
    """list_briefs() returns saved briefs sorted most recent first."""
    store, catalog = _seed_lake(tmp_path)

    mock_client = MagicMock()
    mock_anthropic_cls.return_value = mock_client
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="Brief content.")]
    )

    engine = BriefEngine(store, catalog)

    # No briefs yet
    assert engine.list_briefs(tmp_path) == []

    # Generate and save one
    brief = engine.generate(days=7)
    engine.save(brief, tmp_path)

    briefs = engine.list_briefs(tmp_path)
    assert len(briefs) == 1
    assert briefs[0].suffix == ".md"

    store.close()


def test_generate_with_empty_lake(tmp_path):
    """generate() handles empty lake gracefully."""
    store = LakeStore(tmp_path)
    catalog = Catalog(tmp_path)

    # No mock needed — shouldn't call Claude
    engine = BriefEngine.__new__(BriefEngine)
    engine.store = store
    engine.catalog = catalog

    context, tables_used, total_rows = engine._build_context(days=7)
    assert tables_used == []
    assert total_rows == 0

    store.close()


@patch("lake.brief.anthropic.Anthropic")
def test_cli_brief_history_empty(mock_anthropic_cls, tmp_path):
    """CLI --history shows message when no briefs exist."""
    runner = CliRunner()
    with patch("lake.cli._lake_root", return_value=tmp_path):
        result = runner.invoke(cli, ["brief", "--history"])
        assert result.exit_code == 0
        assert "No briefs" in result.output
