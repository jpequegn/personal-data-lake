"""CLI entrypoint for the personal data lake."""

from datetime import datetime, timezone
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from lake.catalog import Catalog

console = Console()


def _lake_root() -> Path:
    """Resolve the lake root directory (cwd by default)."""
    return Path.cwd()


@click.group()
@click.version_option(version="0.1.0")
def cli() -> None:
    """Personal Data Lake — ingest, store, and query your data."""


@cli.command()
def tables() -> None:
    """List all tables with row counts and freshness."""
    catalog = Catalog(_lake_root())
    table_names = catalog.list_tables()

    if not table_names:
        click.echo("No tables yet. Run `lake ingest` to add data.")
        return

    now = datetime.now(timezone.utc)
    t = Table(title="Lake Tables")
    t.add_column("Table", style="bold")
    t.add_column("Rows", justify="right")
    t.add_column("Source")
    t.add_column("Schema Version", justify="center")
    t.add_column("Last Updated")

    for name in table_names:
        entry = catalog.get_table(name)
        # Color freshness: green if <7d, yellow if <30d, red if older
        freshness_style = "dim"
        if entry.last_updated:
            try:
                updated = datetime.fromisoformat(entry.last_updated)
                age_days = (now - updated).days
                if age_days < 7:
                    freshness_style = "green"
                elif age_days < 30:
                    freshness_style = "yellow"
                else:
                    freshness_style = "red"
            except ValueError:
                pass

        t.add_row(
            name,
            str(entry.row_count),
            entry.source or "—",
            f"v{entry.current_version}",
            f"[{freshness_style}]{entry.last_updated[:19] if entry.last_updated else 'never'}[/{freshness_style}]",
        )

    console.print(t)


@cli.command()
def stats() -> None:
    """Show lake statistics: disk usage, row counts, freshness."""
    from lake.store import LakeStore

    root = _lake_root()
    catalog = Catalog(root)
    store = LakeStore(root)

    table_names = catalog.list_tables()
    if not table_names:
        click.echo("Lake is empty. Ingest some data first.")
        store.close()
        return

    total_rows = 0
    total_bytes = 0

    t = Table(title="Lake Statistics")
    t.add_column("Table", style="bold")
    t.add_column("Rows", justify="right")
    t.add_column("Disk", justify="right")
    t.add_column("Parquet Files", justify="right")
    t.add_column("Last Updated")

    for name in table_names:
        entry = catalog.get_table(name)
        total_rows += entry.row_count

        # Calculate disk usage for this table's Parquet files
        table_dir = store.raw_path / name
        parquet_files = list(table_dir.rglob("*.parquet")) if table_dir.exists() else []
        table_bytes = sum(f.stat().st_size for f in parquet_files)
        total_bytes += table_bytes

        t.add_row(
            name,
            f"{entry.row_count:,}",
            _format_bytes(table_bytes),
            str(len(parquet_files)),
            entry.last_updated[:19] if entry.last_updated else "never",
        )

    console.print(t)
    console.print(f"\n  Total: {total_rows:,} rows, {_format_bytes(total_bytes)} on disk")
    store.close()


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
@click.option("--source", type=click.Choice(["git", "markdown", "p3"]), help="Data source to ingest.")
@click.option("--path", type=click.Path(exists=True), help="Path to the data source.")
@click.option("--max-commits", default=1000, help="Max commits to ingest (git only).")
@click.option("--all", "ingest_all", is_flag=True, help="Run all configured sources.")
def ingest(source: str | None, path: str | None, max_commits: int, ingest_all: bool) -> None:
    """Ingest data from a source into the lake."""
    from lake.store import LakeStore

    root = _lake_root()
    store = LakeStore(root)
    catalog = Catalog(root)

    if ingest_all:
        _ingest_all(store, catalog, max_commits)
        store.close()
        return

    if not source:
        click.echo("Error: --source or --all is required.")
        raise SystemExit(1)

    if not path:
        click.echo(f"Error: --path is required for {source} source.")
        raise SystemExit(1)

    if source == "git":
        from lake.ingestors.git_commits import GitCommitsIngestor

        ingestor = GitCommitsIngestor(path, max_commits=max_commits)
    elif source == "markdown":
        from lake.ingestors.markdown import MarkdownIngestor

        ingestor = MarkdownIngestor(path)
    elif source == "p3":
        from lake.ingestors.p3_podcasts import P3PodcastsIngestor

        ingestor = P3PodcastsIngestor(path)

    result = ingestor.ingest(store, catalog)
    click.echo(f"Ingested {result.rows_written} rows into '{result.table_name}' from {result.source}.")
    store.close()


@cli.command("query")
@click.argument("question")
@click.option("--show-sql", is_flag=True, help="Show the generated SQL query.")
@click.option("--format", "fmt", type=click.Choice(["prose", "table", "list"]), default="prose", help="Output format.")
def query_cmd(question: str, show_sql: bool, fmt: str) -> None:
    """Ask a natural language question about your data."""
    from lake.query import QueryEngine
    from lake.store import LakeStore

    root = _lake_root()
    store = LakeStore(root)
    catalog = Catalog(root)

    engine = QueryEngine(store, catalog)

    if fmt == "prose":
        result = engine.ask(question)
        if show_sql:
            console.print(f"[dim]SQL: {result.sql}[/dim]\n")
        console.print(result.formatted)
    else:
        # For table/list formats, generate SQL and execute without prose formatting
        sql = engine.generate_sql(question)
        if show_sql:
            console.print(f"[dim]SQL: {sql}[/dim]\n")
        results = store.query(sql)
        if len(results) == 0:
            click.echo("No results found.")
        elif fmt == "table":
            _print_rich_table(results)
        elif fmt == "list":
            _print_list(results)

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
            _print_rich_table(result)
    except Exception as e:
        click.echo(f"Error: {e}")
    finally:
        store.close()


@cli.command()
@click.option("--days", default=7, help="Number of days to look back.")
@click.option("--history", is_flag=True, help="List past briefs.")
def brief(days: int, history: bool) -> None:
    """Generate an AI intelligence brief across all data sources."""
    from lake.brief import BriefEngine
    from lake.store import LakeStore

    root = _lake_root()

    if history:
        engine = BriefEngine.__new__(BriefEngine)
        briefs = engine.list_briefs(root)
        if not briefs:
            click.echo("No briefs yet. Run `lake brief` to generate one.")
            return
        console.print("[bold]Past Briefs[/bold]\n")
        for path in briefs:
            console.print(f"  {path.stem}  ({_format_bytes(path.stat().st_size)})")
        return

    store = LakeStore(root)
    catalog = Catalog(root)
    engine = BriefEngine(store, catalog)

    console.print(f"[dim]Generating {days}-day brief...[/dim]\n")
    result = engine.generate(days=days)

    console.print(result.content)

    # Save to disk
    filepath = engine.save(result, root)
    console.print(f"\n[dim]Brief saved to {filepath}[/dim]")

    store.close()


# --- Helpers ---


def _ingest_all(store, catalog, max_commits: int) -> None:
    """Run all configured ingestors from a config file or sensible defaults."""
    config_path = _lake_root() / "lake.config.json"
    if config_path.exists():
        import json

        config = json.loads(config_path.read_text())
        sources = config.get("sources", [])
    else:
        click.echo("No lake.config.json found. Create one to configure --all sources.")
        click.echo("Example:")
        click.echo('  {"sources": [')
        click.echo('    {"type": "git", "path": "~/Code/my-repo"},')
        click.echo('    {"type": "markdown", "path": "~/Documents/Notes"}')
        click.echo("  ]}")
        return

    for src in sources:
        src_type = src["type"]
        src_path = Path(src["path"]).expanduser()
        if not src_path.exists():
            click.echo(f"Skipping {src_type} ({src_path}): path not found.")
            continue

        if src_type == "git":
            from lake.ingestors.git_commits import GitCommitsIngestor

            ingestor = GitCommitsIngestor(src_path, max_commits=max_commits)
        elif src_type == "markdown":
            from lake.ingestors.markdown import MarkdownIngestor

            ingestor = MarkdownIngestor(src_path)
        elif src_type == "p3":
            from lake.ingestors.p3_podcasts import P3PodcastsIngestor

            ingestor = P3PodcastsIngestor(src_path)
        else:
            click.echo(f"Unknown source type: {src_type}")
            continue

        result = ingestor.ingest(store, catalog)
        click.echo(f"  {result.source}: {result.rows_written} rows → {result.table_name}")


def _print_rich_table(results) -> None:
    """Print an Arrow table as a rich formatted table."""
    t = Table()
    for col in results.column_names:
        t.add_column(col)

    for row in results.to_pylist():
        t.add_row(*[_truncate(str(v)) for v in row.values()])

    console.print(t)


def _print_list(results) -> None:
    """Print results as a numbered list."""
    rows = results.to_pylist()
    for i, row in enumerate(rows, 1):
        parts = [f"{k}: {_truncate(str(v))}" for k, v in row.items()]
        click.echo(f"{i}. {', '.join(parts)}")


def _truncate(s: str, max_len: int = 80) -> str:
    """Truncate a string for display."""
    return s[:max_len] + "..." if len(s) > max_len else s


def _format_bytes(size: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}" if unit != "B" else f"{size} B"
        size /= 1024
    return f"{size:.1f} TB"


if __name__ == "__main__":
    cli()
