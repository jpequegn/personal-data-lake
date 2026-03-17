"""Tests for the markdown notes ingestor."""

import json
from pathlib import Path

from lake.catalog import Catalog
from lake.ingestors.markdown import MarkdownIngestor
from lake.store import LakeStore


def _create_notes(path: Path, count: int = 10) -> None:
    """Create test markdown files with various formats."""
    path.mkdir(parents=True, exist_ok=True)

    # File with frontmatter and H1
    (path / "ai-notes.md").write_text(
        "---\ntags: [ai, agents, embeddings]\n---\n\n# AI Research Notes\n\nSome content about embeddings and LLMs.\nMore words here for counting.\n"
    )

    # File with H1 but no frontmatter
    (path / "meeting.md").write_text(
        "# Weekly Meeting\n\nDiscussed roadmap and priorities.\n"
    )

    # File with no H1 (title falls back to filename)
    (path / "quick-thought.md").write_text(
        "Just a quick thought about data lakes.\n"
    )

    # File in subdirectory
    sub = path / "projects"
    sub.mkdir()
    (sub / "data-lake.md").write_text(
        "---\ntags: [data, duckdb]\n---\n\n# Data Lake Project\n\nBuilding a personal data lake with DuckDB and Parquet.\nThis is the main project document.\n"
    )

    # Files that should be skipped
    git_dir = path / ".git"
    git_dir.mkdir()
    (git_dir / "ignored.md").write_text("should be ignored")

    node_dir = path / "node_modules"
    node_dir.mkdir()
    (node_dir / "also-ignored.md").write_text("should be ignored too")

    # Fill remaining count with simple files
    for i in range(count - 4):
        (path / f"note-{i}.md").write_text(
            f"# Note {i}\n\nThis is note number {i} with some content.\n"
        )


def test_fetch_finds_md_files(tmp_path):
    """fetch() finds .md files and skips .git/node_modules."""
    notes_dir = tmp_path / "notes"
    _create_notes(notes_dir, count=10)

    ingestor = MarkdownIngestor(notes_dir)
    records = ingestor.fetch()

    assert len(records) == 10
    paths = [r["path"] for r in records]
    assert not any(".git" in p for p in paths)
    assert not any("node_modules" in p for p in paths)


def test_title_extraction(tmp_path):
    """Extracts H1 as title, falls back to filename."""
    notes_dir = tmp_path / "notes"
    _create_notes(notes_dir, count=4)

    ingestor = MarkdownIngestor(notes_dir)
    records = ingestor.fetch()

    by_filename = {r["filename"]: r for r in records}
    assert by_filename["ai-notes.md"]["title"] == "AI Research Notes"
    assert by_filename["meeting.md"]["title"] == "Weekly Meeting"
    assert by_filename["quick-thought.md"]["title"] == "quick-thought"  # fallback


def test_frontmatter_tags(tmp_path):
    """Extracts tags from YAML frontmatter."""
    notes_dir = tmp_path / "notes"
    _create_notes(notes_dir, count=4)

    ingestor = MarkdownIngestor(notes_dir)
    records = ingestor.fetch()

    by_filename = {r["filename"]: r for r in records}
    tags = json.loads(by_filename["ai-notes.md"]["tags"])
    assert "ai" in tags
    assert "embeddings" in tags

    # No frontmatter → empty tags
    tags_none = json.loads(by_filename["meeting.md"]["tags"])
    assert tags_none == []


def test_word_count(tmp_path):
    """word_count is calculated correctly."""
    notes_dir = tmp_path / "notes"
    _create_notes(notes_dir, count=4)

    ingestor = MarkdownIngestor(notes_dir)
    records = ingestor.fetch()

    by_filename = {r["filename"]: r for r in records}
    # "Just a quick thought about data lakes." — no frontmatter to strip
    assert by_filename["quick-thought.md"]["word_count"] == 7


def test_full_ingest_and_query(tmp_path):
    """Acceptance criteria: ingest 10 files, query returns correct results."""
    notes_dir = tmp_path / "notes"
    _create_notes(notes_dir, count=10)

    lake_path = tmp_path / "lake"
    store = LakeStore(lake_path)
    catalog = Catalog(lake_path)

    ingestor = MarkdownIngestor(notes_dir)
    result = ingestor.ingest(store, catalog)

    assert result.rows_written == 10
    assert result.table_name == "notes"

    # Acceptance criteria query
    query_result = store.query(
        "SELECT title, word_count FROM notes ORDER BY word_count DESC"
    )
    assert len(query_result) == 10
    # Highest word count should be first
    word_counts = query_result.column("word_count").to_pylist()
    assert word_counts == sorted(word_counts, reverse=True)

    # Catalog updated
    assert catalog.has_table("notes")
    assert catalog.get_table("notes").row_count == 10

    store.close()
