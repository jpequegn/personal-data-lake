"""Tests for the metadata catalog."""

from datetime import date, timedelta

import pyarrow as pa

from lake.catalog import Catalog, ColumnDef
from lake.store import LakeStore


def _base_columns() -> list[ColumnDef]:
    return [
        ColumnDef(name="id", type="VARCHAR"),
        ColumnDef(name="title", type="VARCHAR"),
        ColumnDef(name="date", type="DATE"),
    ]


def test_register_and_get_schema(tmp_path):
    """Register a table and retrieve its schema."""
    catalog = Catalog(tmp_path)
    catalog.register_table("podcasts", _base_columns(), source="p3_podcasts")

    schema = catalog.get_schema("podcasts")
    assert len(schema) == 3
    assert schema[0].name == "id"
    assert catalog.get_table("podcasts").source == "p3_podcasts"


def test_update_schema_bumps_version(tmp_path):
    """Adding columns bumps the schema version."""
    catalog = Catalog(tmp_path)
    catalog.register_table("podcasts", _base_columns())

    new_ver = catalog.update_schema(
        "podcasts",
        [ColumnDef(name="companies_mentioned", type="VARCHAR")],
    )
    assert new_ver == 2

    schema = catalog.get_schema("podcasts")
    assert len(schema) == 4
    added_col = [c for c in schema if c.name == "companies_mentioned"][0]
    assert added_col.added_in == 2


def test_schema_diff(tmp_path):
    """schema_diff returns columns added in each version."""
    catalog = Catalog(tmp_path)
    catalog.register_table("podcasts", _base_columns())
    catalog.update_schema("podcasts", [ColumnDef(name="tags", type="VARCHAR")])
    catalog.update_schema("podcasts", [ColumnDef(name="rating", type="DOUBLE")])

    diffs = catalog.schema_diff("podcasts")
    assert len(diffs) == 2
    assert diffs[0][0] == 2  # version
    assert diffs[0][1][0].name == "tags"
    assert diffs[1][0] == 3
    assert diffs[1][1][0].name == "rating"


def test_catalog_persists_across_reload(tmp_path):
    """Catalog survives save/load cycle."""
    catalog = Catalog(tmp_path)
    catalog.register_table("notes", _base_columns(), source="markdown")
    catalog.update_schema("notes", [ColumnDef(name="word_count", type="INT64")])

    # Reload from disk
    catalog2 = Catalog(tmp_path)
    assert catalog2.has_table("notes")
    assert catalog2.get_table("notes").current_version == 2
    schema = catalog2.get_schema("notes")
    assert len(schema) == 4


def test_old_parquet_new_schema_returns_null(tmp_path):
    """Acceptance criteria: add column, old Parquet still queryable (NULL for missing)."""
    store = LakeStore(tmp_path)
    catalog = Catalog(tmp_path)

    # Write data with original 3 columns
    table_v1 = pa.table(
        {
            "id": ["a", "b", "c"],
            "title": ["one", "two", "three"],
            "date": [date(2026, 3, 1), date(2026, 3, 2), date(2026, 3, 3)],
        }
    )
    store.write("podcasts", table_v1)
    catalog.register_table(
        "podcasts",
        [
            ColumnDef(name="id", type="VARCHAR"),
            ColumnDef(name="title", type="VARCHAR"),
            ColumnDef(name="date", type="DATE"),
        ],
        source="p3_podcasts",
    )

    # Evolve schema — add a column
    catalog.update_schema(
        "podcasts",
        [ColumnDef(name="companies_mentioned", type="VARCHAR")],
    )

    # Write new data WITH the extra column
    table_v2 = pa.table(
        {
            "id": ["d"],
            "title": ["four"],
            "date": [date(2026, 3, 4)],
            "companies_mentioned": ["Anthropic"],
        }
    )
    store.write("podcasts", table_v2)

    # Query all rows — old rows should have NULL for companies_mentioned
    result = store.query("SELECT id, companies_mentioned FROM podcasts ORDER BY id")
    assert len(result) == 4
    vals = result.column("companies_mentioned").to_pylist()
    assert vals[0] is None  # old row
    assert vals[3] == "Anthropic"  # new row

    store.close()
