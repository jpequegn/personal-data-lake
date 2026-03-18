# Eval Results: NL → SQL Query Accuracy

**Status: Pending** — Run `uv run python eval/run.py` with `ANTHROPIC_API_KEY` set to generate results.

**Target: ≥15/20 semantically correct on first try**

## Eval Infrastructure

- **20 queries** across 10 categories covering all three tables (podcasts, commits, notes)
- **Automated grading**: exact match, semantically correct (same row count + values), or wrong
- **Seeded test data**: 20 podcast episodes, 30 commits, 10 notes
- **Error recovery**: eval captures and reports SQL generation/execution failures

## Query Categories

| Category | Count | Description |
|----------|-------|-------------|
| simple_count | 2 | Basic COUNT(*) queries |
| group_by | 3 | GROUP BY with ORDER BY |
| text_filter | 1 | WHERE with text matching |
| text_search | 2 | LIKE-based content search |
| sort_limit | 3 | ORDER BY + LIMIT |
| date_filter | 1 | Date range filtering |
| date_filter_group | 1 | Date filter + GROUP BY |
| date_range | 1 | Specific date range |
| aggregation | 3 | SUM, AVG aggregations |
| json_filter | 1 | Filtering on JSON string columns |
| json_complex | 1 | Complex JSON field analysis |
| multi_table | 1 | UNION across multiple tables |

## How to Run

```bash
# Set your API key
export ANTHROPIC_API_KEY=sk-ant-...

# Run against seeded test data (default)
uv run python eval/run.py

# Run against your actual lake data
uv run python eval/run.py --live
```

Results will be written to this file automatically.

## Expected Patterns

Queries likely to succeed:
- Simple counts and aggregations (straightforward SQL)
- ORDER BY + LIMIT patterns (common and unambiguous)
- Text search with LIKE (direct mapping)

Queries that may struggle:
- JSON field parsing (Claude needs to know values are JSON strings, not native JSON)
- Complex date range boundaries (off-by-one on inclusive/exclusive)
- Multi-table UNION queries (unusual pattern for NL→SQL)
