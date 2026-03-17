"""Git commits ingestor: extract commit history from any local repo."""

import json
from datetime import datetime, timezone
from pathlib import Path

from git import Repo

from lake.catalog import ColumnDef
from lake.ingestors.base import BaseIngestor


class GitCommitsIngestor(BaseIngestor):
    """Ingest git commit history from a local repository."""

    source_name = "git"
    table_name = "commits"

    def __init__(self, repo_path: str | Path, max_commits: int = 1000) -> None:
        self.repo_path = Path(repo_path)
        self.max_commits = max_commits

    def fetch(self) -> list[dict]:
        repo = Repo(self.repo_path)

        if repo.head.is_detached or not repo.heads:
            return []

        records = []
        for commit in repo.iter_commits(max_count=self.max_commits):
            stats = commit.stats.total
            # Get list of changed files
            try:
                files = list(commit.stats.files.keys())
            except Exception:
                files = []

            records.append(
                {
                    "hash": commit.hexsha,
                    "repo_name": self.repo_path.name,
                    "repo_path": str(self.repo_path),
                    "author": str(commit.author),
                    "date": datetime.fromtimestamp(
                        commit.committed_date, tz=timezone.utc
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    "message": commit.message.strip(),
                    "files_changed": json.dumps(files),
                    "insertions": stats.get("insertions", 0),
                    "deletions": stats.get("deletions", 0),
                }
            )

        return records

    def schema_columns(self) -> list[ColumnDef]:
        return [
            ColumnDef(name="hash", type="VARCHAR"),
            ColumnDef(name="repo_name", type="VARCHAR"),
            ColumnDef(name="repo_path", type="VARCHAR"),
            ColumnDef(name="author", type="VARCHAR"),
            ColumnDef(name="date", type="TIMESTAMP"),
            ColumnDef(name="message", type="VARCHAR"),
            ColumnDef(name="files_changed", type="VARCHAR"),
            ColumnDef(name="insertions", type="INTEGER"),
            ColumnDef(name="deletions", type="INTEGER"),
            ColumnDef(name="ingested_at", type="VARCHAR"),
        ]
