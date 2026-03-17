"""P3 podcasts ingestor: pull episodes from the Parakeet Podcast Processor DuckDB."""

import json
from pathlib import Path

import duckdb

from lake.catalog import Catalog, ColumnDef
from lake.ingestors.base import BaseIngestor, IngestResult
from lake.store import LakeStore


class P3PodcastsIngestor(BaseIngestor):
    """Ingest podcast episodes from a P3 DuckDB database."""

    source_name = "p3_podcasts"
    table_name = "podcasts"

    def __init__(self, p3_db_path: str | Path) -> None:
        self.p3_db_path = Path(p3_db_path)

    def fetch(self, last_ingested_id: int | None = None) -> list[dict]:
        """Open P3 DB read-only, join episodes + podcasts + summaries."""
        conn = duckdb.connect(str(self.p3_db_path), read_only=True)

        try:
            # Discover available tables
            tables = [
                row[0]
                for row in conn.execute(
                    "SELECT table_name FROM information_schema.tables"
                ).fetchall()
            ]

            # Build query based on available schema
            query = self._build_query(conn, tables, last_ingested_id)
            rows = conn.execute(query).fetchall()
            columns = [desc[0] for desc in conn.description]
        finally:
            conn.close()

        records = []
        for row in rows:
            record = dict(zip(columns, row))
            # Ensure JSON columns are strings
            for json_col in ("key_topics", "themes", "key_takeaways", "companies_mentioned"):
                val = record.get(json_col)
                if val is not None and not isinstance(val, str):
                    record[json_col] = json.dumps(val)
                elif val is None:
                    record[json_col] = "[]"
            records.append(record)

        return records

    def _build_query(
        self, conn: duckdb.DuckDBPyConnection, tables: list[str], last_id: int | None
    ) -> str:
        """Build the appropriate SQL based on what tables/columns exist."""
        # Try the common P3 schema patterns
        if "episodes" in tables:
            # Get column names for episodes table
            episode_cols = {
                row[0]
                for row in conn.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name = 'episodes'"
                ).fetchall()
            }

            select_parts = ["e.id"]

            # podcast_name from join or direct column
            if "podcasts" in tables and "podcast_id" in episode_cols:
                join = "LEFT JOIN podcasts p ON e.podcast_id = p.id"
                p_cols = {
                    row[0]
                    for row in conn.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_name = 'podcasts'"
                    ).fetchall()
                }
                if "name" in p_cols:
                    select_parts.append("p.name AS podcast_name")
                elif "title" in p_cols:
                    select_parts.append("p.title AS podcast_name")
                else:
                    select_parts.append("'Unknown' AS podcast_name")
            elif "podcast_name" in episode_cols:
                join = ""
                select_parts.append("e.podcast_name")
            else:
                join = ""
                select_parts.append("'Unknown' AS podcast_name")

            # Standard columns
            for col in ("title", "date"):
                if col in episode_cols:
                    select_parts.append(f"e.{col}")

            # Summary columns — might be in episodes or a separate summaries table
            summary_cols = ("key_topics", "themes", "full_summary", "key_takeaways", "companies_mentioned")
            if "summaries" in tables:
                join += " LEFT JOIN summaries s ON e.id = s.episode_id"
                for col in summary_cols:
                    s_cols = {
                        row[0]
                        for row in conn.execute(
                            "SELECT column_name FROM information_schema.columns WHERE table_name = 'summaries'"
                        ).fetchall()
                    }
                    if col in s_cols:
                        select_parts.append(f"s.{col}")
            else:
                for col in summary_cols:
                    if col in episode_cols:
                        select_parts.append(f"e.{col}")

            where = f"WHERE e.id > {last_id}" if last_id is not None else ""

            return f"SELECT {', '.join(select_parts)} FROM episodes e {join} {where} ORDER BY e.id"

        # Fallback: single table with all columns
        main_table = tables[0] if tables else "episodes"
        where = f"WHERE id > {last_id}" if last_id is not None else ""
        return f"SELECT * FROM {main_table} {where} ORDER BY id"

    def schema_columns(self) -> list[ColumnDef]:
        return [
            ColumnDef(name="id", type="INTEGER"),
            ColumnDef(name="podcast_name", type="VARCHAR"),
            ColumnDef(name="title", type="VARCHAR"),
            ColumnDef(name="date", type="DATE"),
            ColumnDef(name="key_topics", type="VARCHAR"),
            ColumnDef(name="themes", type="VARCHAR"),
            ColumnDef(name="full_summary", type="VARCHAR"),
            ColumnDef(name="key_takeaways", type="VARCHAR"),
            ColumnDef(name="companies_mentioned", type="VARCHAR"),
            ColumnDef(name="ingested_at", type="VARCHAR"),
        ]

    def ingest(self, store: LakeStore, catalog: Catalog) -> IngestResult:
        """Fetch with incremental support, normalize, write, update catalog."""
        last_id = catalog.get_metadata(self.table_name, "last_ingested_id")

        records = self.fetch(last_ingested_id=last_id)
        if not records:
            return IngestResult(
                source=self.source_name, rows_written=0, table_name=self.table_name
            )

        table = self.normalize(records)
        rows = store.write(self.table_name, table)

        if not catalog.has_table(self.table_name):
            catalog.register_table(
                self.table_name, self.schema_columns(), source=self.source_name
            )

        # Track last ingested ID for incremental
        max_id = max(r["id"] for r in records)
        catalog.set_metadata(self.table_name, "last_ingested_id", max_id)

        total = store.query(f"SELECT COUNT(*) AS cnt FROM {self.table_name}")
        catalog.update_row_count(self.table_name, total.column("cnt")[0].as_py())

        return IngestResult(
            source=self.source_name, rows_written=rows, table_name=self.table_name
        )
