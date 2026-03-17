"""Tests for the NL → SQL query engine."""

from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa

from lake.catalog import Catalog, ColumnDef
from lake.query import QueryEngine
from lake.store import LakeStore


def _setup_lake(tmp_path):
    """Create a lake with test data."""
    store = LakeStore(tmp_path)
    catalog = Catalog(tmp_path)

    # Create a notes table
    table = pa.table(
        {
            "title": ["AI Research", "Data Lakes", "Embeddings Deep Dive"],
            "content": ["About AI and agents", "DuckDB and Parquet", "Vector embeddings"],
            "word_count": [100, 200, 150],
            "date": [date(2026, 3, 1), date(2026, 3, 5), date(2026, 3, 10)],
        }
    )
    store.write("notes", table)
    catalog.register_table(
        "notes",
        [
            ColumnDef(name="title", type="VARCHAR"),
            ColumnDef(name="content", type="VARCHAR"),
            ColumnDef(name="word_count", type="INTEGER"),
            ColumnDef(name="date", type="DATE"),
        ],
        source="markdown",
    )
    catalog.update_row_count("notes", 3)

    return store, catalog


def _mock_response(text: str):
    """Create a mock Anthropic API response."""
    msg = MagicMock()
    msg.content = [MagicMock(text=text)]
    return msg


@patch("lake.query.anthropic.Anthropic")
def test_generate_sql(mock_anthropic_cls, tmp_path):
    """generate_sql sends schema context to Claude and returns SQL."""
    store, catalog = _setup_lake(tmp_path)

    mock_client = MagicMock()
    mock_anthropic_cls.return_value = mock_client
    mock_client.messages.create.return_value = _mock_response(
        "SELECT title, word_count FROM notes ORDER BY word_count DESC"
    )

    engine = QueryEngine(store, catalog)
    sql = engine.generate_sql("which notes are the longest?")

    assert "SELECT" in sql
    assert "notes" in sql
    # Verify Claude was called with schema context
    call_args = mock_client.messages.create.call_args
    prompt = call_args.kwargs["messages"][0]["content"]
    assert "notes" in prompt
    assert "word_count" in prompt

    store.close()


@patch("lake.query.anthropic.Anthropic")
def test_ask_full_pipeline(mock_anthropic_cls, tmp_path):
    """ask() runs the full NL → SQL → execute → format pipeline."""
    store, catalog = _setup_lake(tmp_path)

    mock_client = MagicMock()
    mock_anthropic_cls.return_value = mock_client

    # First call: generate SQL. Second call: format results
    mock_client.messages.create.side_effect = [
        _mock_response("SELECT title, word_count FROM notes ORDER BY word_count DESC LIMIT 3"),
        _mock_response("Here are your longest notes:\n1. Data Lakes (200 words)\n2. Embeddings Deep Dive (150 words)\n3. AI Research (100 words)"),
    ]

    engine = QueryEngine(store, catalog)
    result = engine.ask("which notes are the longest?")

    assert result.row_count == 3
    assert result.sql == "SELECT title, word_count FROM notes ORDER BY word_count DESC LIMIT 3"
    assert "Data Lakes" in result.formatted
    assert mock_client.messages.create.call_count == 2

    store.close()


@patch("lake.query.anthropic.Anthropic")
def test_error_recovery_retries_once(mock_anthropic_cls, tmp_path):
    """If SQL fails, engine retries with Claude once."""
    store, catalog = _setup_lake(tmp_path)

    mock_client = MagicMock()
    mock_anthropic_cls.return_value = mock_client

    # First: bad SQL, second: corrected SQL, third: format
    mock_client.messages.create.side_effect = [
        _mock_response("SELECT title FROM nonexistent_table"),
        _mock_response("SELECT title FROM notes LIMIT 3"),
        _mock_response("Here are some notes."),
    ]

    engine = QueryEngine(store, catalog)
    result = engine.ask("show me some notes")

    assert result.row_count == 3
    # 3 calls: generate, retry, format
    assert mock_client.messages.create.call_count == 3

    store.close()


@patch("lake.query.anthropic.Anthropic")
def test_empty_results(mock_anthropic_cls, tmp_path):
    """Empty results return 'No results found.'"""
    store, catalog = _setup_lake(tmp_path)

    mock_client = MagicMock()
    mock_anthropic_cls.return_value = mock_client

    mock_client.messages.create.side_effect = [
        _mock_response("SELECT title FROM notes WHERE word_count > 9999"),
    ]

    engine = QueryEngine(store, catalog)
    result = engine.ask("find notes with more than 9999 words")

    assert result.row_count == 0
    assert result.formatted == "No results found."

    store.close()


@patch("lake.query.anthropic.Anthropic")
def test_strips_markdown_fences(mock_anthropic_cls, tmp_path):
    """SQL extraction strips markdown code fences."""
    store, catalog = _setup_lake(tmp_path)

    mock_client = MagicMock()
    mock_anthropic_cls.return_value = mock_client

    mock_client.messages.create.side_effect = [
        _mock_response("```sql\nSELECT COUNT(*) AS cnt FROM notes\n```"),
        _mock_response("You have 3 notes."),
    ]

    engine = QueryEngine(store, catalog)
    result = engine.ask("how many notes do I have?")

    assert result.sql == "SELECT COUNT(*) AS cnt FROM notes"
    assert result.row_count == 1

    store.close()


@patch("lake.query.anthropic.Anthropic")
def test_schema_context_includes_all_tables(mock_anthropic_cls, tmp_path):
    """Schema context includes all catalog tables and sample data."""
    store, catalog = _setup_lake(tmp_path)

    # Add a second table
    commits_table = pa.table(
        {
            "hash": ["abc123"],
            "message": ["initial commit"],
            "date": [date(2026, 3, 1)],
        }
    )
    store.write("commits", commits_table)
    catalog.register_table(
        "commits",
        [
            ColumnDef(name="hash", type="VARCHAR"),
            ColumnDef(name="message", type="VARCHAR"),
            ColumnDef(name="date", type="DATE"),
        ],
        source="git",
    )

    mock_client = MagicMock()
    mock_anthropic_cls.return_value = mock_client

    engine = QueryEngine(store, catalog)
    context = engine._build_schema_context()

    assert "notes" in context
    assert "commits" in context
    assert "word_count" in context
    assert "hash" in context

    store.close()
