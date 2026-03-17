"""CLI entrypoint for the personal data lake."""

import click


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


if __name__ == "__main__":
    cli()
