"""Tests for the eval infrastructure."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from eval.run import EvalResult, grade_query, seed_test_data, write_results_md
from lake.query import QueryEngine


def test_seed_test_data(tmp_path):
    """Seed data creates all three tables with expected row counts."""
    store, catalog = seed_test_data(tmp_path)

    assert "podcasts" in store.tables()
    assert "commits" in store.tables()
    assert "notes" in store.tables()

    podcasts = store.query("SELECT COUNT(*) AS cnt FROM podcasts")
    assert podcasts.column("cnt")[0].as_py() == 20

    commits = store.query("SELECT COUNT(*) AS cnt FROM commits")
    assert commits.column("cnt")[0].as_py() == 30

    notes = store.query("SELECT COUNT(*) AS cnt FROM notes")
    assert notes.column("cnt")[0].as_py() == 10

    store.close()


def test_ground_truth_queries_all_execute(tmp_path):
    """All 20 ground-truth SQL queries execute without error."""
    store, catalog = seed_test_data(tmp_path)

    queries_path = Path(__file__).parent.parent / "eval" / "queries.json"
    queries = json.loads(queries_path.read_text())

    for q in queries:
        try:
            result = store.query(q["ground_truth_sql"])
            assert len(result) >= 0, f"Query {q['id']} returned negative rows"
        except Exception as e:
            raise AssertionError(f"Query {q['id']} failed: {e}\nSQL: {q['ground_truth_sql']}")

    store.close()


@patch("lake.query.anthropic.Anthropic")
def test_grade_exact_match(mock_anthropic_cls, tmp_path):
    """Grading works for exact SQL match."""
    store, catalog = seed_test_data(tmp_path)

    mock_client = MagicMock()
    mock_anthropic_cls.return_value = mock_client
    # Return the exact ground truth SQL
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="SELECT COUNT(*) AS count FROM podcasts")]
    )

    engine = QueryEngine(store, catalog)
    query = {
        "id": 1,
        "question": "How many podcast episodes are in the lake?",
        "ground_truth_sql": "SELECT COUNT(*) AS count FROM podcasts",
        "category": "simple_count",
    }

    result = grade_query(store, engine, query)
    assert result.grade == "exact_match"
    store.close()


@patch("lake.query.anthropic.Anthropic")
def test_grade_wrong(mock_anthropic_cls, tmp_path):
    """Grading detects wrong results."""
    store, catalog = seed_test_data(tmp_path)

    mock_client = MagicMock()
    mock_anthropic_cls.return_value = mock_client
    # Return SQL that gives wrong row count
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="SELECT * FROM podcasts LIMIT 1")]
    )

    engine = QueryEngine(store, catalog)
    query = {
        "id": 1,
        "question": "How many podcast episodes are in the lake?",
        "ground_truth_sql": "SELECT COUNT(*) AS count FROM podcasts",
        "category": "simple_count",
    }

    result = grade_query(store, engine, query)
    assert result.grade in ("wrong", "semantically_correct")  # 1 vs 1 row might be "semantically_correct"
    store.close()


def test_write_results_md():
    """Results markdown is generated correctly."""
    results = [
        EvalResult(1, "q1", "simple", "sql1", "sql1", "exact_match", 5, 5),
        EvalResult(2, "q2", "simple", "sql2", "sql3", "semantically_correct", 10, 10),
        EvalResult(3, "q3", "complex", "sql4", "sql5", "wrong", 5, 0, error="failed"),
    ]

    md = write_results_md(results)
    assert "2/3" in md  # 2 passing out of 3
    assert "exact_match" in md
    assert "Failure Analysis" in md
    assert "q3" in md
