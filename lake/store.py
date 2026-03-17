"""LakeStore: write Arrow tables to Parquet, query via DuckDB."""

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


class LakeStore:
    """Storage layer: Parquet files for data, DuckDB for queries."""

    def __init__(self, base_path: str | Path) -> None:
        self.base_path = Path(base_path)
        self.raw_path = self.base_path / "data" / "raw"
        self.db_path = self.base_path / "data" / "lake.duckdb"

        self.raw_path.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))

        self._register_existing_views()

    def write(self, table_name: str, table: pa.Table) -> int:
        """Write an Arrow table to hive-partitioned Parquet and register a DuckDB view.

        Returns the number of rows written.
        """
        if "year" not in table.column_names or "month" not in table.column_names:
            table = self._add_partition_columns(table)

        dest = self.raw_path / table_name
        pq.write_to_dataset(
            table,
            root_path=str(dest),
            partition_cols=["year", "month"],
        )

        self._register_view(table_name)
        return len(table)

    def query(self, sql: str) -> pa.Table:
        """Execute SQL against DuckDB and return an Arrow table."""
        return self.conn.execute(sql).to_arrow_table()

    def query_df(self, sql: str) -> "duckdb.DuckDBPyRelation":
        """Execute SQL and return a DuckDB relation (for display)."""
        return self.conn.execute(sql)

    def tables(self) -> list[str]:
        """List all registered table names."""
        result = self.conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'"
        ).fetchall()
        return [row[0] for row in result]

    def close(self) -> None:
        """Close the DuckDB connection."""
        self.conn.close()

    def _register_view(self, table_name: str) -> None:
        """Create or replace a DuckDB view over a table's Parquet files."""
        parquet_glob = self.raw_path / table_name / "**" / "*.parquet"
        self.conn.execute(
            f"CREATE OR REPLACE VIEW {table_name} AS "
            f"SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true, union_by_name=true)"
        )

    def _register_existing_views(self) -> None:
        """Scan raw/ for existing Parquet directories and register views."""
        if not self.raw_path.exists():
            return
        for child in sorted(self.raw_path.iterdir()):
            if child.is_dir() and any(child.rglob("*.parquet")):
                self._register_view(child.name)

    @staticmethod
    def _add_partition_columns(table: pa.Table) -> pa.Table:
        """Add year/month partition columns derived from a 'date' column or ingested_at."""
        date_col = None
        for col_name in ("date", "ingested_at"):
            if col_name in table.column_names:
                date_col = col_name
                break

        if date_col is None:
            import pyarrow.compute as pc
            from datetime import datetime, timezone

            now = datetime.now(timezone.utc)
            n = len(table)
            table = table.append_column("year", pa.array([now.year] * n, type=pa.int32()))
            table = table.append_column("month", pa.array([now.month] * n, type=pa.int32()))
            return table

        import pyarrow.compute as pc

        col = table.column(date_col)
        # Cast to timestamp if it's a date or string type
        if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
            col = pc.cast(col, pa.timestamp("us"))
        elif pa.types.is_date(col.type):
            col = pc.cast(col, pa.timestamp("us"))

        years = pc.year(col)
        months = pc.month(col)
        table = table.append_column("year", pc.cast(years, pa.int32()))
        table = table.append_column("month", pc.cast(months, pa.int32()))
        return table
