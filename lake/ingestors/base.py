"""Base ingestor interface for all data sources."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone

import pyarrow as pa

from lake.catalog import Catalog, ColumnDef
from lake.store import LakeStore


@dataclass
class IngestResult:
    """Result of an ingestion run."""

    source: str
    rows_written: int
    table_name: str


class BaseIngestor(ABC):
    """Abstract base for all data source ingestors."""

    source_name: str
    table_name: str

    @abstractmethod
    def fetch(self) -> list[dict]:
        """Pull raw records from the data source."""

    def normalize(self, records: list[dict]) -> pa.Table:
        """Convert raw records to an Arrow table with standard fields."""
        if not records:
            return pa.table({})

        # Add ingested_at timestamp to every record
        now = datetime.now(timezone.utc).isoformat()
        for record in records:
            record["ingested_at"] = now

        return pa.Table.from_pylist(records)

    def schema_columns(self) -> list[ColumnDef]:
        """Return the column definitions for this ingestor's table."""
        raise NotImplementedError

    def ingest(self, store: LakeStore, catalog: Catalog) -> IngestResult:
        """Fetch, normalize, write to store, and update catalog."""
        records = self.fetch()
        if not records:
            return IngestResult(
                source=self.source_name, rows_written=0, table_name=self.table_name
            )

        table = self.normalize(records)
        rows = store.write(self.table_name, table)

        # Register or update catalog
        if not catalog.has_table(self.table_name):
            catalog.register_table(
                self.table_name, self.schema_columns(), source=self.source_name
            )

        # Update row count from DuckDB (total across all ingestions)
        total = store.query(f"SELECT COUNT(*) AS cnt FROM {self.table_name}")
        catalog.update_row_count(self.table_name, total.column("cnt")[0].as_py())

        return IngestResult(
            source=self.source_name, rows_written=rows, table_name=self.table_name
        )
