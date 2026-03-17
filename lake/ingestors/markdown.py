"""Markdown notes ingestor: recursive .md file scanner with frontmatter parsing."""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from lake.catalog import ColumnDef
from lake.ingestors.base import BaseIngestor

SKIP_DIRS = {".git", "node_modules", "__pycache__", ".venv", "venv"}

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)
_TAGS_RE = re.compile(r"tags:\s*\[([^\]]*)\]")
_H1_RE = re.compile(r"^#\s+(.+)", re.MULTILINE)


class MarkdownIngestor(BaseIngestor):
    """Ingest markdown files from a directory tree."""

    source_name = "markdown"
    table_name = "notes"

    def __init__(self, root_path: str | Path) -> None:
        self.root_path = Path(root_path)

    def fetch(self) -> list[dict]:
        records = []
        for md_file in self._find_md_files():
            try:
                content = md_file.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue

            title = self._extract_title(content, md_file.stem)
            tags = self._extract_tags(content)
            body = self._strip_frontmatter(content)
            stat = md_file.stat()

            records.append(
                {
                    "path": str(md_file),
                    "filename": md_file.name,
                    "title": title,
                    "content": body,
                    "word_count": len(body.split()),
                    "tags": json.dumps(tags),
                    "file_created_at": datetime.fromtimestamp(
                        stat.st_birthtime, tz=timezone.utc
                    ).isoformat(),
                    "file_modified_at": datetime.fromtimestamp(
                        stat.st_mtime, tz=timezone.utc
                    ).isoformat(),
                    "date": datetime.fromtimestamp(
                        stat.st_mtime, tz=timezone.utc
                    ).strftime("%Y-%m-%d"),
                }
            )

        return records

    def schema_columns(self) -> list[ColumnDef]:
        return [
            ColumnDef(name="path", type="VARCHAR"),
            ColumnDef(name="filename", type="VARCHAR"),
            ColumnDef(name="title", type="VARCHAR"),
            ColumnDef(name="content", type="VARCHAR"),
            ColumnDef(name="word_count", type="INTEGER"),
            ColumnDef(name="tags", type="VARCHAR"),
            ColumnDef(name="file_created_at", type="VARCHAR"),
            ColumnDef(name="file_modified_at", type="VARCHAR"),
            ColumnDef(name="date", type="DATE"),
            ColumnDef(name="ingested_at", type="VARCHAR"),
        ]

    def _find_md_files(self) -> list[Path]:
        """Recursively find .md files, skipping ignored directories."""
        results = []
        for item in sorted(self.root_path.rglob("*.md")):
            if any(part in SKIP_DIRS for part in item.parts):
                continue
            if item.is_file():
                results.append(item)
        return results

    @staticmethod
    def _extract_title(content: str, fallback: str) -> str:
        """Extract H1 header as title, falling back to filename stem."""
        match = _H1_RE.search(content)
        return match.group(1).strip() if match else fallback

    @staticmethod
    def _extract_tags(content: str) -> list[str]:
        """Extract tags from YAML frontmatter."""
        fm = _FRONTMATTER_RE.match(content)
        if not fm:
            return []
        tags_match = _TAGS_RE.search(fm.group(1))
        if not tags_match:
            return []
        return [t.strip().strip("'\"") for t in tags_match.group(1).split(",") if t.strip()]

    @staticmethod
    def _strip_frontmatter(content: str) -> str:
        """Remove YAML frontmatter from content."""
        return _FRONTMATTER_RE.sub("", content).strip()
