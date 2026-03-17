"""CLI entrypoint for the personal data lake."""

from pathlib import Path

import click

from lake.catalog import Catalog


def _lake_root() -> Path:
    """Resolve the lake root directory (cwd by default)."""
    return Path.cwd()


@click.group()
@click.version_option(version="0.1.0")
def cli() -> None:
    """Personal Data Lake — ingest, store, and query your data."""


@cli.command()
def tables() -> None:
    """List all tables in the lake."""
    click.echo("No tables yet. Run `lake ingest` to add data.")


@cli.command()
def stats() -> None:
    """Show lake statistics."""
    click.echo("Lake is empty. Ingest some data first.")


@cli.command()
@click.argument("table_name")
@click.option("--diff", is_flag=True, help="Show what changed between schema versions.")
def schema(table_name: str, diff: bool) -> None:
    """Show schema for a table, optionally with version diff."""
    catalog = Catalog(_lake_root())

    if not catalog.has_table(table_name):
        click.echo(f"Table '{table_name}' not found in catalog.")
        raise SystemExit(1)

    entry = catalog.get_table(table_name)

    if diff:
        diffs = catalog.schema_diff(table_name)
        if not diffs:
            click.echo(f"Table '{table_name}' has only one schema version.")
            return
        for version, added_cols in diffs:
            click.echo(f"\n  Version {version}:")
            for col in added_cols:
                click.echo(f"    + {col.name} ({col.type})")
        return

    click.echo(f"Table: {table_name}  (v{entry.current_version}, {entry.row_count} rows)")
    click.echo(f"Source: {entry.source or 'unknown'}")
    click.echo(f"Last updated: {entry.last_updated or 'never'}")
    click.echo()
    columns = catalog.get_schema(table_name)
    for col in columns:
        added = f"  [added in v{col.added_in}]" if col.added_in > 1 else ""
        click.echo(f"  {col.name:30s} {col.type}{added}")


@cli.command()
@click.option("--source", required=True, type=click.Choice(["git", "markdown"]), help="Data source to ingest.")
@click.option("--path", type=click.Path(exists=True), help="Path to the data source (e.g., repo path for git).")
@click.option("--max-commits", default=1000, help="Max commits to ingest (git only).")
def ingest(source: str, path: str | None, max_commits: int) -> None:
    """Ingest data from a source into the lake."""
    from lake.catalog import Catalog
    from lake.store import LakeStore

    root = _lake_root()
    store = LakeStore(root)
    catalog = Catalog(root)

    if not path:
        click.echo(f"Error: --path is required for {source} source.")
        raise SystemExit(1)

    if source == "git":
        from lake.ingestors.git_commits import GitCommitsIngestor

        ingestor = GitCommitsIngestor(path, max_commits=max_commits)
    elif source == "markdown":
        from lake.ingestors.markdown import MarkdownIngestor

        ingestor = MarkdownIngestor(path)

    result = ingestor.ingest(store, catalog)
    click.echo(f"Ingested {result.rows_written} rows into '{result.table_name}' from {result.source}.")
    store.close()


@cli.command("query")
@click.argument("question")
@click.option("--show-sql", is_flag=True, help="Show the generated SQL query.")
def query_cmd(question: str, show_sql: bool) -> None:
    """Ask a natural language question about your data."""
    from lake.query import QueryEngine
    from lake.store import LakeStore

    root = _lake_root()
    store = LakeStore(root)
    catalog = Catalog(root)

    engine = QueryEngine(store, catalog)
    result = engine.ask(question)

    if show_sql:
        click.echo(f"SQL: {result.sql}\n")

    click.echo(result.formatted)
    store.close()


@cli.command()
@click.argument("sql")
def sql(sql: str) -> None:
    """Execute raw SQL against the lake."""
    from lake.store import LakeStore

    root = _lake_root()
    store = LakeStore(root)

    try:
        result = store.query(sql)
        if len(result) == 0:
            click.echo("No results.")
        else:
            # Print as a simple table
            rows = result.to_pylist()
            cols = result.column_names
            click.echo("  ".join(cols))
            click.echo("  ".join("-" * len(c) for c in cols))
            for row in rows:
                click.echo("  ".join(str(row.get(c, "")) for c in cols))
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        store.close()


if __name__ == "__main__":
    cli()
