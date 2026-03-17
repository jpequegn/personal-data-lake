"""Metadata catalog: track table schemas and versions (Iceberg-inspired)."""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class ColumnDef:
    """A column definition in the catalog."""

    name: str
    type: str
    added_in: int = 1


@dataclass
class SchemaVersion:
    """A snapshot of a table's schema at a given version."""

    version: int
    columns: list[ColumnDef]
    added_at: str  # ISO format


@dataclass
class TableEntry:
    """Catalog entry for a single table."""

    current_version: int = 1
    row_count: int = 0
    last_updated: str = ""
    source: str = ""
    versions: list[SchemaVersion] = field(default_factory=list)


class Catalog:
    """Load/save catalog.json and manage table schemas."""

    def __init__(self, base_path: str | Path) -> None:
        self.base_path = Path(base_path)
        self.catalog_path = self.base_path / "data" / "catalog.json"
        self._tables: dict[str, TableEntry] = {}
        self._load()

    def _load(self) -> None:
        if not self.catalog_path.exists():
            return
        raw = json.loads(self.catalog_path.read_text())
        for name, entry_data in raw.items():
            versions = [
                SchemaVersion(
                    version=v["version"],
                    columns=[ColumnDef(**c) for c in v["columns"]],
                    added_at=v["added_at"],
                )
                for v in entry_data.get("versions", [])
            ]
            self._tables[name] = TableEntry(
                current_version=entry_data["current_version"],
                row_count=entry_data.get("row_count", 0),
                last_updated=entry_data.get("last_updated", ""),
                source=entry_data.get("source", ""),
                versions=versions,
            )

    def _save(self) -> None:
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        data = {}
        for name, entry in self._tables.items():
            data[name] = {
                "current_version": entry.current_version,
                "row_count": entry.row_count,
                "last_updated": entry.last_updated,
                "source": entry.source,
                "versions": [
                    {
                        "version": v.version,
                        "columns": [asdict(c) for c in v.columns],
                        "added_at": v.added_at,
                    }
                    for v in entry.versions
                ],
            }
        tmp = self.catalog_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2) + "\n")
        tmp.replace(self.catalog_path)

    def register_table(
        self, name: str, columns: list[ColumnDef], source: str = ""
    ) -> None:
        """Add a new table to the catalog."""
        now = datetime.now(timezone.utc).isoformat()
        version = SchemaVersion(version=1, columns=columns, added_at=now)
        self._tables[name] = TableEntry(
            current_version=1,
            row_count=0,
            last_updated=now,
            source=source,
            versions=[version],
        )
        self._save()

    def update_schema(self, name: str, new_columns: list[ColumnDef]) -> int:
        """Bump schema version with new columns added. Returns new version number."""
        entry = self._tables[name]
        new_version = entry.current_version + 1

        # Carry forward existing columns, add new ones
        current_cols = self.get_schema(name)
        current_names = {c.name for c in current_cols}
        merged = list(current_cols)
        for col in new_columns:
            if col.name not in current_names:
                merged.append(ColumnDef(name=col.name, type=col.type, added_in=new_version))

        now = datetime.now(timezone.utc).isoformat()
        entry.versions.append(
            SchemaVersion(version=new_version, columns=merged, added_at=now)
        )
        entry.current_version = new_version
        entry.last_updated = now
        self._save()
        return new_version

    def update_row_count(self, name: str, row_count: int) -> None:
        """Update the row count for a table."""
        entry = self._tables[name]
        entry.row_count = row_count
        entry.last_updated = datetime.now(timezone.utc).isoformat()
        self._save()

    def get_schema(self, name: str, version: int | None = None) -> list[ColumnDef]:
        """Return the schema at a given version (default: current)."""
        entry = self._tables[name]
        target = version or entry.current_version
        for v in entry.versions:
            if v.version == target:
                return v.columns
        raise ValueError(f"Version {target} not found for table '{name}'")

    def get_table(self, name: str) -> TableEntry:
        """Return the full catalog entry for a table."""
        return self._tables[name]

    def list_tables(self) -> list[str]:
        """Return all table names in the catalog."""
        return sorted(self._tables.keys())

    def has_table(self, name: str) -> bool:
        """Check if a table exists in the catalog."""
        return name in self._tables

    def schema_diff(self, name: str) -> list[tuple[int, list[ColumnDef]]]:
        """Return columns added in each version (beyond v1)."""
        entry = self._tables[name]
        diffs: list[tuple[int, list[ColumnDef]]] = []
        for i, ver in enumerate(entry.versions):
            if i == 0:
                continue
            prev_names = {c.name for c in entry.versions[i - 1].columns}
            added = [c for c in ver.columns if c.name not in prev_names]
            if added:
                diffs.append((ver.version, added))
        return diffs
