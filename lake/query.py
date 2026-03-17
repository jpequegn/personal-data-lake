"""NL → SQL query engine: Claude generates SQL, DuckDB executes it."""

import re
from dataclasses import dataclass

import anthropic
import pyarrow as pa

from lake.catalog import Catalog
from lake.store import LakeStore


@dataclass
class QueryResult:
    """Result of a natural language query."""

    question: str
    sql: str
    raw_results: pa.Table
    formatted: str
    row_count: int


class QueryEngine:
    """Two-stage query engine: NL → SQL → formatted results."""

    def __init__(self, store: LakeStore, catalog: Catalog, model: str = "claude-sonnet-4-20250514") -> None:
        self.store = store
        self.catalog = catalog
        self.model = model
        self.client = anthropic.Anthropic()

    def ask(self, question: str) -> QueryResult:
        """Full pipeline: NL → SQL → execute → format."""
        schema_context = self._build_schema_context()
        sql = self._generate_sql(question, schema_context)
        results, sql = self._execute_with_retry(question, sql, schema_context)

        formatted = self._format_results(question, sql, results)

        return QueryResult(
            question=question,
            sql=sql,
            raw_results=results,
            formatted=formatted,
            row_count=len(results),
        )

    def generate_sql(self, question: str) -> str:
        """Generate SQL from a natural language question (exposed for testing)."""
        schema_context = self._build_schema_context()
        return self._generate_sql(question, schema_context)

    def _build_schema_context(self) -> str:
        """Build a schema description string for all tables in the catalog."""
        tables = self.catalog.list_tables()
        if not tables:
            return "No tables in the lake yet."

        parts = []
        for table_name in tables:
            entry = self.catalog.get_table(table_name)
            columns = self.catalog.get_schema(table_name)
            col_lines = [f"  - {c.name} ({c.type})" for c in columns]
            parts.append(
                f"Table: {table_name} ({entry.row_count} rows)\n"
                + "\n".join(col_lines)
            )

            # Add sample rows if data exists
            if table_name in self.store.tables():
                try:
                    sample = self.store.query(
                        f"SELECT * FROM {table_name} LIMIT 3"
                    )
                    if len(sample) > 0:
                        rows = sample.to_pylist()
                        parts.append(f"  Sample rows: {rows[:3]}")
                except Exception:
                    pass

        return "\n\n".join(parts)

    def _generate_sql(self, question: str, schema_context: str) -> str:
        """Ask Claude to generate DuckDB-compatible SQL."""
        response = self.client.messages.create(
            model=self.model,
            max_tokens=1024,
            messages=[
                {
                    "role": "user",
                    "content": f"""You are a SQL expert. Generate a DuckDB-compatible SQL query to answer the user's question.

Available tables and schemas:

{schema_context}

Important:
- Use DuckDB SQL syntax
- Only reference tables and columns that exist in the schema above
- For JSON string columns, use json_extract or LIKE for filtering
- Return ONLY the SQL query, no explanation, no markdown fences

Question: {question}""",
                }
            ],
        )

        sql = response.content[0].text.strip()
        # Strip markdown code fences if Claude included them anyway
        sql = re.sub(r"^```(?:sql)?\s*\n?", "", sql)
        sql = re.sub(r"\n?```\s*$", "", sql)
        return sql.strip()

    def _execute_with_retry(
        self, question: str, sql: str, schema_context: str
    ) -> tuple[pa.Table, str]:
        """Execute SQL, retry once with Claude if it fails."""
        try:
            return self.store.query(sql), sql
        except Exception as e:
            # One retry: send the error back to Claude
            corrected_sql = self._retry_sql(question, sql, str(e), schema_context)
            return self.store.query(corrected_sql), corrected_sql

    def _retry_sql(
        self, question: str, failed_sql: str, error: str, schema_context: str
    ) -> str:
        """Ask Claude to fix a failed SQL query."""
        response = self.client.messages.create(
            model=self.model,
            max_tokens=1024,
            messages=[
                {
                    "role": "user",
                    "content": f"""The following DuckDB SQL query failed. Fix it.

Schema:
{schema_context}

Original question: {question}

Failed SQL:
{failed_sql}

Error:
{error}

Return ONLY the corrected SQL query, no explanation, no markdown fences.""",
                }
            ],
        )

        sql = response.content[0].text.strip()
        sql = re.sub(r"^```(?:sql)?\s*\n?", "", sql)
        sql = re.sub(r"\n?```\s*$", "", sql)
        return sql.strip()

    def _format_results(self, question: str, sql: str, results: pa.Table) -> str:
        """Ask Claude to format raw query results as readable prose."""
        if len(results) == 0:
            return "No results found."

        rows = results.to_pylist()
        # Limit to 50 rows for formatting to avoid token limits
        display_rows = rows[:50]
        total = len(rows)

        response = self.client.messages.create(
            model=self.model,
            max_tokens=2048,
            messages=[
                {
                    "role": "user",
                    "content": f"""Format these query results as a clear, readable answer to the original question.

Question: {question}
SQL used: {sql}
Total rows: {total}

Results:
{display_rows}

Provide a concise, well-formatted answer. Use bullet points or numbered lists where appropriate. Do not include the SQL query in your response.""",
                }
            ],
        )

        return response.content[0].text.strip()
