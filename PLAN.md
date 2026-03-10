# Personal Data Lake — Implementation Plan

## What We're Building

A personal data lake: ingest your own data sources (notes, podcast summaries, bookmarks, git commits, browser history) into a local DuckDB + Parquet store, then query it with natural language powered by Claude.

No cloud. No SaaS. All your data, fully queryable, on your machine.

## Why This Matters

Two converging signals: Joe Reis's episode "Software Engineers are the New Data Engineers" (personal data lakes, local LLMs, owning your own pipeline) and the Iceberg ecosystem episode (metadata management, schema evolution, interoperability). The pattern is clear: the tools that big data teams use at scale are now accessible to individuals. You can run a proper lakehouse on your laptop.

## Architecture

```
lake/
├── __init__.py
├── ingestors/
│   ├── base.py             # BaseIngestor interface
│   ├── markdown.py         # Markdown files / Obsidian notes
│   ├── p3_podcasts.py      # P³ DuckDB → podcast summaries
│   ├── git_commits.py      # Git log from any repo
│   ├── browser_history.py  # Chrome/Safari history SQLite
│   └── json_files.py       # Generic JSON / JSONL
├── store.py                # LakeStore: write Parquet, manage catalog
├── catalog.py              # Iceberg-inspired metadata catalog
├── query.py                # NL → SQL via Claude → DuckDB execute
├── schema.py               # Schema registry and evolution
└── cli.py                  # `lake ingest`, `lake query`, `lake tables`

data/                       # gitignored
├── raw/                    # source-specific Parquet partitions
│   ├── podcasts/
│   ├── notes/
│   ├── commits/
│   └── browser/
├── catalog.json            # table metadata and schema versions
└── lake.duckdb             # DuckDB catalog + views over Parquet

pyproject.toml
README.md
```

## Core Concepts

### The Catalog (Iceberg-inspired)
Every table in the lake has metadata:
```json
{
  "table": "podcasts",
  "schema_version": 3,
  "columns": [...],
  "partition_by": "date",
  "row_count": 964,
  "last_updated": "2026-03-10T07:00:00",
  "source": "p3_podcasts",
  "parquet_files": ["data/raw/podcasts/2026-03/*.parquet"]
}
```

Schema evolution: adding a column to the schema creates a new `schema_version` entry. Old Parquet files still readable (DuckDB handles missing columns gracefully).

### The Query Flow
```
User: "what podcasts mentioned Temporal or durable computing last month?"
          ↓
query.py: send NL + table schemas to Claude
          ↓
Claude generates SQL:
    SELECT title, podcast, full_summary, date
    FROM podcasts
    WHERE date >= '2026-02-10'
    AND (lower(full_summary) LIKE '%temporal%'
      OR lower(key_topics) LIKE '%durable%')
    ORDER BY date DESC
          ↓
DuckDB executes against Parquet files
          ↓
Claude formats results as readable prose
```

## Implementation Phases

### Phase 1: Base ingestor interface (ingestors/base.py)

```python
class BaseIngestor(ABC):
    source_name: str

    @abstractmethod
    def fetch(self) -> list[dict]:
        """Pull raw records from source."""

    def normalize(self, records: list[dict]) -> pa.Table:
        """Convert to Arrow table with standard fields."""
        # Every table has: id, source, ingested_at, date
        ...

    def ingest(self, store: LakeStore) -> IngestResult:
        records = self.fetch()
        table = self.normalize(records)
        return store.write(self.source_name, table)
```

### Phase 2: P³ podcasts ingestor (ingestors/p3_podcasts.py)

The most immediately useful source — pull from your existing P³ DuckDB.

Columns: `id, title, podcast_name, date, key_topics (JSON), themes (JSON), full_summary, key_takeaways (JSON), companies_mentioned (JSON), ingested_at`

Incremental: track `last_ingested_id` so re-running doesn't re-ingest everything.

### Phase 3: Git commits ingestor (ingestors/git_commits.py)

For any git repo path, extract commit history:

Columns: `hash, repo, author, date, message, files_changed, insertions, deletions, ingested_at`

```bash
lake ingest --source git --path ~/Code/parakeet-podcast-processor
lake ingest --source git --path ~/Code/nano-agent
```

### Phase 4: Markdown notes ingestor (ingestors/markdown.py)

Recursively scan a directory for `.md` files. Extract:

Columns: `path, filename, title (H1 header), content, word_count, tags (frontmatter), created_at (file mtime), ingested_at`

Supports Obsidian-style frontmatter: `---\ntags: [ai, agents]\n---`

### Phase 5: Lake store (store.py)

Write Arrow tables to partitioned Parquet files. Read them back with DuckDB.

```python
store = LakeStore("~/data-lake")
store.write("podcasts", arrow_table)
# → writes data/raw/podcasts/2026-03/part-0001.parquet
# → updates catalog.json

store.query("SELECT * FROM podcasts WHERE date > '2026-03-01'")
# → DuckDB reads Parquet via glob pattern
```

Key: use `pyarrow.parquet.write_to_dataset` with hive partitioning by date.

### Phase 6: Metadata catalog (catalog.py)

Track table schemas and versions in `catalog.json`. When a new ingestor adds a column that didn't exist before:
1. Increment schema_version
2. Add column to catalog with `added_in_version` and `nullable: true`
3. Log the migration

```bash
lake schema podcasts        # show current schema
lake schema podcasts --diff # show what changed between versions
```

### Phase 7: Natural language query engine (query.py)

Two-stage: NL → SQL → format results.

**Stage 1: NL → SQL**
Send Claude: the user's query + the table schemas (column names, types, sample values). Ask for a DuckDB-compatible SQL query. Validate the SQL before executing (check table/column names exist).

**Stage 2: Execute + format**
Run SQL against DuckDB. If results are raw rows, send them back to Claude to format as a readable answer with the original question as context.

Handle errors gracefully: if DuckDB raises, send the error back to Claude and ask for a corrected SQL.

### Phase 8: CLI

```bash
# Ingest sources
lake ingest --source p3                          # podcasts from P³ DB
lake ingest --source git --path ~/Code/nano-agent
lake ingest --source markdown --path ~/Documents/Notes
lake ingest --all                                # run all configured sources

# Query
lake query "what AI topics came up most in March?"
lake query "which repos did I commit to most last week?"
lake query "find all notes about embeddings"
lake query "show me podcast episodes that mentioned Anthropic" --format table

# Inspect
lake tables                  # list all tables with row counts
lake schema <table>          # show schema and version history
lake stats                   # total rows, disk usage, last update per source

# Raw SQL (escape hatch)
lake sql "SELECT podcast_name, COUNT(*) FROM podcasts GROUP BY 1 ORDER BY 2 DESC"
```

### Phase 9: Eval — 20 natural language queries

Write 20 queries. For each:
1. Write the intended SQL manually (ground truth)
2. Run `lake query` and capture generated SQL
3. Compare: does generated SQL match intent? Do results match?
4. Grade: exact match / semantically correct / wrong

Target: ≥15/20 semantically correct on first try.

Document failure patterns in `RESULTS.md`.

## Key Design Decisions

**Why DuckDB + Parquet instead of just DuckDB?**
Parquet is the lingua franca of the data ecosystem. Files are portable, readable by pandas/Spark/BigQuery. DuckDB is the query engine, not the storage format. This separation is the Iceberg insight: storage and compute are decoupled.

**Why Iceberg-inspired catalog instead of actual Iceberg?**
Real Apache Iceberg adds significant complexity (REST catalog, JVM dependency, Avro manifests). We build a simplified version to understand the *why* — schema evolution, partition pruning, snapshot isolation. Follow-on: swap in real Iceberg.

**Why not just use a vector DB for NL queries?**
For structured queries (filter by date, group by podcast, count commits), SQL is far more precise and reliable than semantic search. NL→SQL is the right abstraction here. We keep embeddings for the `personal-memory` project (different use case: "what do I know about X?").

**Why not LangChain for the NL→SQL pipeline?**
Raw API calls. You need to feel the prompt engineering directly. LangChain's SQL agent is a black box — you won't understand why it fails.

**What we're NOT building**
- Remote/cloud storage
- Real-time streaming ingestion
- Multi-user / shared lake
- Full Iceberg compliance

## Acceptance Criteria

1. `lake ingest --source p3` ingests all 964 P³ episodes into Parquet
2. `lake query "what topics came up most in March?"` returns correct, formatted results
3. Schema evolution: add a new column to P³ ingestor, re-ingest, old + new queries both work
4. 20-query eval: ≥15/20 semantically correct SQL
5. `lake stats` shows correct row counts matching source data
6. Total disk usage for 964 podcast episodes < 50MB (Parquet compression)

## Learning Outcomes

After building this you will understand:
- Why Parquet + DuckDB is the minimum viable lakehouse
- What schema evolution actually looks like at the file level (the core Iceberg problem)
- The precision/recall tradeoff in NL→SQL (when it works, when it fails)
- Why "personal data infrastructure" is a real concept, not just a buzzword
- The difference between a data lake (storage) and a lakehouse (storage + catalog + query)
