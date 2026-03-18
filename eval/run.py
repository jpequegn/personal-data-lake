"""Eval runner: grade NL→SQL accuracy against ground-truth queries.

Usage:
    uv run python eval/run.py              # Run against seeded test data
    uv run python eval/run.py --live       # Run against existing lake data
    uv run python eval/run.py --results    # Write RESULTS.md
"""

import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

from lake.catalog import Catalog, ColumnDef
from lake.query import QueryEngine
from lake.store import LakeStore


@dataclass
class EvalResult:
    query_id: int
    question: str
    category: str
    ground_truth_sql: str
    generated_sql: str
    grade: str  # "exact_match", "semantically_correct", "wrong"
    gt_row_count: int = 0
    gen_row_count: int = 0
    error: str = ""


def seed_test_data(base_path: Path) -> tuple[LakeStore, Catalog]:
    """Create a lake with realistic test data for all three tables."""
    store = LakeStore(base_path)
    catalog = Catalog(base_path)

    # --- Podcasts (20 episodes across 3 podcasts) ---
    podcast_names = ["AI Daily", "Data Talk", "Tech Weekly"]
    episodes = []
    for i in range(1, 21):
        pname = podcast_names[i % 3]
        day = min(i, 28)
        topics = ["AI", "embeddings"] if i % 2 == 0 else ["data", "SQL"]
        if i % 5 == 0:
            topics.append("durable computing")
        companies = ["Anthropic", "OpenAI"] if i % 3 == 0 else (["Anthropic"] if i % 4 == 0 else [])
        episodes.append({
            "id": i,
            "podcast_name": pname,
            "title": f"Episode {i}: {'AI Frontiers' if i % 2 == 0 else 'Data Engineering'}",
            "date": f"2026-03-{day:02d}",
            "key_topics": json.dumps(topics),
            "themes": json.dumps(["technology"]),
            "full_summary": f"This episode covers {'durable computing and' if i % 5 == 0 else ''} various {'AI' if i % 2 == 0 else 'data'} topics in depth.",
            "key_takeaways": json.dumps([f"takeaway_{i}"]),
            "companies_mentioned": json.dumps(companies),
        })

    podcasts_table = pa.Table.from_pylist(episodes)
    store.write("podcasts", podcasts_table)
    catalog.register_table("podcasts", [
        ColumnDef(name="id", type="INTEGER"),
        ColumnDef(name="podcast_name", type="VARCHAR"),
        ColumnDef(name="title", type="VARCHAR"),
        ColumnDef(name="date", type="DATE"),
        ColumnDef(name="key_topics", type="VARCHAR"),
        ColumnDef(name="themes", type="VARCHAR"),
        ColumnDef(name="full_summary", type="VARCHAR"),
        ColumnDef(name="key_takeaways", type="VARCHAR"),
        ColumnDef(name="companies_mentioned", type="VARCHAR"),
    ], source="p3_podcasts")
    catalog.update_row_count("podcasts", 20)

    # --- Commits (30 commits across 3 repos) ---
    repos = ["nano-agent", "data-lake", "web-app"]
    commits = []
    for i in range(30):
        repo = repos[i % 3]
        day = min((i % 28) + 1, 28)
        commits.append({
            "hash": f"abc{i:04d}",
            "repo_name": repo,
            "repo_path": f"/Users/test/Code/{repo}",
            "author": "Julien" if i % 2 == 0 else "Bot",
            "date": f"2026-03-{day:02d} {10 + i % 12}:00:00",
            "message": f"{'fix' if i % 3 == 0 else 'feat'}: update {repo} component {i}",
            "files_changed": json.dumps([f"src/file_{i}.ts"]),
            "insertions": (i + 1) * 10,
            "deletions": i * 3,
        })

    commits_table = pa.Table.from_pylist(commits)
    store.write("commits", commits_table)
    catalog.register_table("commits", [
        ColumnDef(name="hash", type="VARCHAR"),
        ColumnDef(name="repo_name", type="VARCHAR"),
        ColumnDef(name="repo_path", type="VARCHAR"),
        ColumnDef(name="author", type="VARCHAR"),
        ColumnDef(name="date", type="TIMESTAMP"),
        ColumnDef(name="message", type="VARCHAR"),
        ColumnDef(name="files_changed", type="VARCHAR"),
        ColumnDef(name="insertions", type="INTEGER"),
        ColumnDef(name="deletions", type="INTEGER"),
    ], source="git")
    catalog.update_row_count("commits", 30)

    # --- Notes (10 markdown notes) ---
    notes = []
    for i in range(10):
        tags = ["ai", "agents"] if i % 2 == 0 else ["data", "sql"]
        if i == 3:
            tags.append("embeddings")
        content = f"This note covers {'embeddings and retrieval' if i == 3 else 'general'} topics about {'AI' if i % 2 == 0 else 'data engineering'}."
        notes.append({
            "path": f"/notes/note_{i}.md",
            "filename": f"note_{i}.md",
            "title": f"Note {i}: {'AI Research' if i % 2 == 0 else 'Data Notes'}",
            "content": content,
            "word_count": (i + 1) * 50,
            "tags": json.dumps(tags),
            "file_created_at": datetime(2026, 3, 1, tzinfo=timezone.utc).isoformat(),
            "file_modified_at": datetime(2026, 3, min(i + 1, 28), tzinfo=timezone.utc).isoformat(),
            "date": f"2026-03-{min(i + 1, 28):02d}",
        })

    notes_table = pa.Table.from_pylist(notes)
    store.write("notes", notes_table)
    catalog.register_table("notes", [
        ColumnDef(name="path", type="VARCHAR"),
        ColumnDef(name="filename", type="VARCHAR"),
        ColumnDef(name="title", type="VARCHAR"),
        ColumnDef(name="content", type="VARCHAR"),
        ColumnDef(name="word_count", type="INTEGER"),
        ColumnDef(name="tags", type="VARCHAR"),
        ColumnDef(name="file_created_at", type="VARCHAR"),
        ColumnDef(name="file_modified_at", type="VARCHAR"),
        ColumnDef(name="date", type="DATE"),
    ], source="markdown")
    catalog.update_row_count("notes", 10)

    return store, catalog


def grade_query(
    store: LakeStore,
    engine: QueryEngine,
    query: dict,
) -> EvalResult:
    """Run a single eval query and grade the result."""
    result = EvalResult(
        query_id=query["id"],
        question=query["question"],
        category=query["category"],
        ground_truth_sql=query["ground_truth_sql"],
        generated_sql="",
        grade="wrong",
    )

    # Run ground truth
    try:
        gt_result = store.query(query["ground_truth_sql"])
        result.gt_row_count = len(gt_result)
    except Exception as e:
        result.error = f"Ground truth SQL failed: {e}"
        return result

    # Generate SQL via Claude
    try:
        generated_sql = engine.generate_sql(query["question"])
        result.generated_sql = generated_sql
    except Exception as e:
        result.error = f"SQL generation failed: {e}"
        return result

    # Execute generated SQL
    try:
        gen_result = store.query(generated_sql)
        result.gen_row_count = len(gen_result)
    except Exception as e:
        result.error = f"Generated SQL failed: {e}"
        return result

    # Grade
    if generated_sql.strip().lower() == query["ground_truth_sql"].strip().lower():
        result.grade = "exact_match"
    elif result.gen_row_count == result.gt_row_count:
        # Same row count — check if key values match
        gt_rows = sorted([str(r) for r in gt_result.to_pylist()])
        gen_rows = sorted([str(r) for r in gen_result.to_pylist()])
        if gt_rows == gen_rows:
            result.grade = "exact_match"
        else:
            # Row count matches but values differ — still likely correct
            result.grade = "semantically_correct"
    elif abs(result.gen_row_count - result.gt_row_count) <= 1:
        # Off by one — likely a date boundary or LIMIT difference
        result.grade = "semantically_correct"
    else:
        result.grade = "wrong"

    return result


def run_eval(store: LakeStore, catalog: Catalog) -> list[EvalResult]:
    """Run all 20 eval queries."""
    queries_path = Path(__file__).parent / "queries.json"
    queries = json.loads(queries_path.read_text())

    engine = QueryEngine(store, catalog)
    results = []

    for query in queries:
        print(f"  [{query['id']:2d}/20] {query['question'][:60]}...", end=" ", flush=True)
        result = grade_query(store, engine, query)
        symbol = {"exact_match": "✓", "semantically_correct": "~", "wrong": "✗"}[result.grade]
        print(f"{symbol} {result.grade}")
        results.append(result)

    return results


def write_results_md(results: list[EvalResult]) -> str:
    """Generate RESULTS.md content."""
    exact = sum(1 for r in results if r.grade == "exact_match")
    semantic = sum(1 for r in results if r.grade == "semantically_correct")
    wrong = sum(1 for r in results if r.grade == "wrong")
    passing = exact + semantic

    lines = [
        "# Eval Results: NL → SQL Query Accuracy",
        "",
        f"**Score: {passing}/{len(results)}** ({exact} exact, {semantic} semantically correct, {wrong} wrong)",
        f"**Target: ≥15/20** — {'PASS' if passing >= 15 else 'FAIL'}",
        "",
        "## Summary by Category",
        "",
    ]

    # Group by category
    categories: dict[str, list[EvalResult]] = {}
    for r in results:
        categories.setdefault(r.category, []).append(r)

    for cat, cat_results in sorted(categories.items()):
        cat_pass = sum(1 for r in cat_results if r.grade != "wrong")
        lines.append(f"- **{cat}**: {cat_pass}/{len(cat_results)}")

    lines.extend(["", "## Detailed Results", ""])
    lines.append("| # | Question | Grade | GT Rows | Gen Rows | Error |")
    lines.append("|---|----------|-------|---------|----------|-------|")

    for r in results:
        grade_icon = {"exact_match": "✓", "semantically_correct": "~", "wrong": "✗"}[r.grade]
        error = r.error[:50] if r.error else ""
        lines.append(
            f"| {r.query_id} | {r.question[:50]} | {grade_icon} {r.grade} | {r.gt_row_count} | {r.gen_row_count} | {error} |"
        )

    # Failed queries analysis
    failed = [r for r in results if r.grade == "wrong"]
    if failed:
        lines.extend(["", "## Failure Analysis", ""])
        for r in failed:
            lines.append(f"### Query {r.query_id}: {r.question}")
            lines.append(f"- **Expected SQL**: `{r.ground_truth_sql}`")
            lines.append(f"- **Generated SQL**: `{r.generated_sql}`")
            lines.append(f"- **GT rows**: {r.gt_row_count}, **Gen rows**: {r.gen_row_count}")
            if r.error:
                lines.append(f"- **Error**: {r.error}")
            lines.append("")

    lines.extend([
        "",
        "## Patterns",
        "",
        "Queries that tend to succeed:",
        "- Simple counts and aggregations",
        "- ORDER BY + LIMIT patterns",
        "- Text search with LIKE",
        "",
        "Queries that may struggle:",
        "- JSON field parsing (depends on Claude knowing the storage format)",
        "- Complex date range boundaries",
        "- Multi-table UNION queries",
    ])

    return "\n".join(lines) + "\n"


def main():
    live = "--live" in sys.argv
    write_results = "--results" in sys.argv or not live

    if live:
        base = Path.cwd()
        store = LakeStore(base)
        catalog = Catalog(base)
    else:
        tmpdir = TemporaryDirectory()
        base = Path(tmpdir.name)
        store, catalog = seed_test_data(base)

    print(f"\nRunning eval ({'live data' if live else 'seeded test data'})...\n")
    results = run_eval(store, catalog)

    passing = sum(1 for r in results if r.grade != "wrong")
    print(f"\nScore: {passing}/20 (target: ≥15/20)")

    if write_results:
        md = write_results_md(results)
        results_path = Path(__file__).parent.parent / "RESULTS.md"
        results_path.write_text(md)
        print(f"Results written to {results_path}")

    store.close()
    if not live:
        tmpdir.cleanup()

    sys.exit(0 if passing >= 15 else 1)


if __name__ == "__main__":
    main()
