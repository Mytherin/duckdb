# fmt: off

import pytest
import subprocess
import sys
from typing import List
from conftest import ShellTest
import os

def test_invalid_explain(shell):
    test = (
        ShellTest(shell)
        .statement("EXPLAIN SELECT 'any_string' IN ?;")
    )
    result = test.run()

def test_pretty_explain(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE integers AS SELECT range i FROM range(1000);")
        .statement("EXPLAIN SELECT sum(i) FROM integers WHERE i > 42;")
    )
    result = test.run()
    # operator boxes with rounded corners
    result.check_stdout("Physical Plan")
    result.check_stdout("╭─ ")
    result.check_stdout("UNGROUPED AGGREGATE")
    result.check_stdout("integers")
    # estimated cardinality is rendered as a metrics line
    result.check_stdout("rows")

def test_pretty_explain_join(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE t1 AS SELECT range i FROM range(1000);")
        .statement("CREATE TABLE t2 AS SELECT range j FROM range(100);")
        .statement("EXPLAIN SELECT count(*) FROM t1 JOIN t2 ON i = j;")
    )
    result = test.run()
    result.check_stdout("HASH JOIN")
    # the children of the join are connected to the join box
    result.check_stdout("┬")

def test_pretty_explain_subtree(shell):
    # an extremely wide plan (many joins) is broken into separate "[Subtree #N]" sections
    setup = [".maxwidth 120", "CREATE TABLE t0 AS SELECT range i FROM range(1000);"]
    setup += [f"CREATE TABLE t{n} AS SELECT range j{n} FROM range(100);" for n in range(1, 24)]
    joins = " ".join(f"JOIN t{n} ON i = j{n}" for n in range(1, 24))
    test = ShellTest(shell)
    for stmt in setup:
        test = test.statement(stmt)
    test = test.statement(f"EXPLAIN SELECT count(*) FROM t0 {joins};")
    result = test.run()
    # the over-wide subtree is referenced inline and rendered separately below
    result.check_stdout("[Subtree #1]")
    result.check_stdout("HASH JOIN")

def test_pretty_explain_flatten(shell):
    # a large EXPLAIN ANALYZE plan collapses each low-impact sub-tree into a single compact grouped box, one row per
    # operator. collapsing only happens for large plans, so use enough joins to exceed the node threshold.
    setup = ["CREATE TABLE big AS SELECT range i, range % 10 k FROM range(1000000);"]
    setup += [f"CREATE TABLE d{n} AS SELECT range j{n} FROM range(10);" for n in range(1, 11)]
    joins = " ".join(f"JOIN (SELECT j{n} FROM d{n} WHERE j{n} >= 0 GROUP BY j{n}) s{n} ON k = s{n}.j{n}"
                      for n in range(1, 11))
    test = ShellTest(shell)
    for stmt in setup:
        test = test.statement(stmt)
    test = test.statement(f"EXPLAIN ANALYZE SELECT count(*) FROM big {joins};")
    result = test.run()
    # each collapsed dimension sub-tree is a grouped box whose rows include its scan (named after the table)
    result.check_stdout("rows · ")
    result.check_stdout("SCAN d")

def test_pretty_explain_condensed(shell):
    # individual operators that take a small share of the total time are condensed (name + metrics, no detail lines).
    # this happens even for small plans - the dominant sort below keeps full detail, the tiny aggregate is condensed.
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE big AS SELECT range i FROM range(1000000);")
        .statement("CREATE TABLE tiny AS SELECT range j FROM range(5);")
        .statement("EXPLAIN ANALYZE SELECT count(*) FROM (SELECT i FROM big ORDER BY i) t JOIN tiny ON i = j;")
    )
    result = test.run()
    # the tiny aggregate is still shown as its own box, but its detail line is dropped
    result.check_stdout("UNGROUPED AGGREGATE")
    result.check_not_exist("Aggregates: count_star")
    # the dominant sort keeps its full detail
    result.check_stdout("Order By:")
    # a condensed scan still keeps its table name (the tiny table's scan is negligible, so it is condensed)
    result.check_stdout("Table: memory.main.tiny")
    # a small plan never merges operators into a "⋯" node
    result.check_not_exist("⋯")

def test_pretty_explain_grouped(shell):
    # a run of consecutive low-significance operators is drawn as one compact box, a row per operator. The dominant
    # sort makes the surrounding operators unambiguously insignificant, so they group together reliably.
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE big AS SELECT range i FROM range(1000000);")
        .statement("EXPLAIN ANALYZE SELECT c + 1 AS a FROM (SELECT count(*) c FROM (SELECT i FROM big ORDER BY i) o);")
    )
    result = test.run()
    # the grouped box shows each operator as "<name>   N rows · <time>" (this " rows · " marker is unique to it)
    result.check_stdout("rows · ")
    # the dominant sort still gets its own full box
    result.check_stdout("Order By:")

def test_pretty_explain_analyze(shell):
    test = (
        ShellTest(shell)
        .statement("CREATE TABLE integers AS SELECT range i FROM range(1000);")
        .statement("EXPLAIN ANALYZE SELECT sum(i) FROM integers;")
    )
    result = test.run()
    result.check_stdout("Query Profile")
    result.check_stdout("total time:")
    result.check_stdout("UNGROUPED AGGREGATE")
    result.check_stdout("1,000 rows")

def test_pretty_explain_all(shell):
    test = (
        ShellTest(shell)
        .statement("SET explain_output='all';")
        .statement("EXPLAIN SELECT 42;")
    )
    result = test.run()
    result.check_stdout("Unoptimized Logical Plan")
    result.check_stdout("Optimized Logical Plan")
    result.check_stdout("Physical Plan")

def test_explain_explicit_format(shell):
    # when the user explicitly requests a format the output is printed as-is
    test = (
        ShellTest(shell)
        .statement("EXPLAIN (FORMAT json) SELECT 42;")
    )
    result = test.run()
    result.check_stdout('"name"')
    result.check_stdout('"children"')

def test_explain_text_format(shell):
    # the old text rendering remains available through FORMAT text
    test = (
        ShellTest(shell)
        .statement("EXPLAIN (FORMAT text) SELECT 42;")
    )
    result = test.run()
    result.check_stdout("┌───────────────────────────┐")

def test_explain_analyze_custom_profiler_format(shell):
    # a custom profiling format is rendered as-is
    test = (
        ShellTest(shell)
        .statement("PRAGMA enable_profiling='json';")
        .statement("EXPLAIN ANALYZE SELECT 42;")
    )
    result = test.run()
    result.check_stdout('"operator"')
