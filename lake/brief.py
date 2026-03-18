"""BriefEngine: AI-generated intelligence reports across all data sources."""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import anthropic

from lake.catalog import Catalog
from lake.store import LakeStore


@dataclass
class Brief:
    """A generated intelligence brief."""

    date: str
    days: int
    content: str
    tables_used: list[str]
    total_rows_analyzed: int


class BriefEngine:
    """Generate daily intelligence briefs by cross-referencing all lake data."""

    def __init__(
        self, store: LakeStore, catalog: Catalog, model: str = "claude-sonnet-4-20250514"
    ) -> None:
        self.store = store
        self.catalog = catalog
        self.model = model
        self.client = anthropic.Anthropic()

    def generate(self, days: int = 7) -> Brief:
        """Pull recent data from all tables, send to Claude, return structured brief."""
        context, tables_used, total_rows = self._build_context(days)

        if not tables_used:
            return Brief(
                date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                days=days,
                content="No data found in the lake. Run `lake ingest` to add data first.",
                tables_used=[],
                total_rows_analyzed=0,
            )

        content = self._generate_brief(context, days)

        return Brief(
            date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            days=days,
            content=content,
            tables_used=tables_used,
            total_rows_analyzed=total_rows,
        )

    def save(self, brief: Brief, base_path: Path) -> Path:
        """Save a brief as a dated markdown file."""
        briefs_dir = base_path / "data" / "briefs"
        briefs_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{brief.date}.md"
        filepath = briefs_dir / filename

        md = (
            f"# Daily Brief — {brief.date}\n\n"
            f"*{brief.days}-day lookback | "
            f"{brief.total_rows_analyzed} rows analyzed across "
            f"{', '.join(brief.tables_used)}*\n\n"
            f"{brief.content}\n"
        )
        filepath.write_text(md)
        return filepath

    def list_briefs(self, base_path: Path) -> list[Path]:
        """List all saved briefs, most recent first."""
        briefs_dir = base_path / "data" / "briefs"
        if not briefs_dir.exists():
            return []
        return sorted(briefs_dir.glob("*.md"), reverse=True)

    def _build_context(self, days: int) -> tuple[str, list[str], int]:
        """Pull recent rows from every table and format as context."""
        available_tables = self.store.tables()
        if not available_tables:
            return "", [], 0

        parts = []
        tables_used = []
        total_rows = 0

        for table_name in sorted(available_tables):
            # Try to get recent data with a date filter
            try:
                rows = self._query_recent(table_name, days)
            except Exception:
                # Fallback: just get the latest N rows
                try:
                    rows = self.store.query(
                        f"SELECT * FROM {table_name} LIMIT 100"
                    ).to_pylist()
                except Exception:
                    continue

            if not rows:
                continue

            tables_used.append(table_name)
            total_rows += len(rows)

            # Build schema description
            if self.catalog.has_table(table_name):
                cols = self.catalog.get_schema(table_name)
                schema_desc = ", ".join(f"{c.name} ({c.type})" for c in cols)
            else:
                schema_desc = ", ".join(rows[0].keys()) if rows else "unknown"

            # Truncate large text fields for context efficiency
            truncated_rows = [self._truncate_row(r) for r in rows[:50]]

            parts.append(
                f"## {table_name} ({len(rows)} rows in last {days} days)\n"
                f"Schema: {schema_desc}\n\n"
                f"Data:\n{self._format_rows(truncated_rows)}"
            )

        return "\n\n".join(parts), tables_used, total_rows

    def _query_recent(self, table_name: str, days: int) -> list[dict]:
        """Query recent rows from a table using date column."""
        result = self.store.query(
            f"SELECT * FROM {table_name} "
            f"WHERE date >= CURRENT_DATE - INTERVAL '{days} days' "
            f"ORDER BY date DESC"
        )
        return result.to_pylist()

    def _generate_brief(self, context: str, days: int) -> str:
        """Send data context to Claude and get a structured brief."""
        response = self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            messages=[
                {
                    "role": "user",
                    "content": f"""You are an intelligence analyst for a personal data lake. Generate a daily brief based on the data below.

This data spans the last {days} days across multiple personal data sources (code commits, podcast episodes, markdown notes, etc.).

Your brief should have these sections:

## Activity Summary
What happened across all sources. Highlight volume, key items, and notable activity.

## Cross-Source Connections
Identify concepts, topics, technologies, or themes that appear across MULTIPLE data sources. These are the most valuable insights — things the user wouldn't notice without cross-referencing. Be specific: cite actual titles, commit messages, and note names.

## Temporal Patterns
Any patterns over time: bursts of activity, quiet periods, learning-then-doing sequences, or correlations between sources.

## Emerging Interests
Topics that are trending up — appearing more frequently or across more sources recently.

## Actionable Nudges
2-3 specific, actionable suggestions based on the data. Examples:
- "You've been reading about X but haven't started building — consider a spike."
- "Your commit frequency dropped this week — might be worth checking if you're blocked."
- "Topic Y appeared in 3 podcasts — might be worth writing a note to consolidate your thinking."

Be concise, specific, and reference actual data points. No generic advice. If you don't have enough data for a section, say so briefly rather than fabricating insights.

---

DATA:

{context}""",
                }
            ],
        )

        return response.content[0].text.strip()

    @staticmethod
    def _truncate_row(row: dict, max_field_len: int = 200) -> dict:
        """Truncate long string fields in a row for context efficiency."""
        return {
            k: (v[:max_field_len] + "..." if isinstance(v, str) and len(v) > max_field_len else v)
            for k, v in row.items()
        }

    @staticmethod
    def _format_rows(rows: list[dict]) -> str:
        """Format rows as a readable string for the prompt."""
        if not rows:
            return "(no data)"
        lines = []
        for r in rows:
            parts = [f"{k}: {v}" for k, v in r.items() if v is not None]
            lines.append("  - " + " | ".join(parts))
        return "\n".join(lines)
