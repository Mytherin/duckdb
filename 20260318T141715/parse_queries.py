#!/usr/bin/env python3
"""
Parse jepsen.duckdb stdout.log and extract query events into TSV.

Columns: timestamp, connection, query, params
"""

import re
import sys

# Matches lines like:
#   14:17:17.821 [virtual-87] INFO jepsen.duckdb.local-node -- next.jdbc/execute! query [...]
LOG_RE = re.compile(
    r'^(\d{2}:\d{2}:\d{2}\.\d+) \[([^\]]+)\] \S+ \S+ -- next\.jdbc/execute! query \[(.+)\]$'
)

# Tokenize the vector body (everything inside the outer [...]):
#   "some string"   -> string token
#   123             -> integer token
#   -123            -> negative integer token
TOKEN_RE = re.compile(r'"((?:[^"\\]|\\.)*)"|(-?\d+(?:\.\d+)?)')


SELECT_RE = re.compile(r'select\b.+\bfrom\s+(\w+)\b', re.IGNORECASE)
MERGE_RE  = re.compile(r'merge\s+into\s+(\w+)\b', re.IGNORECASE)


def decompose_sql(sql: str) -> str:
    """Return a short descriptor for the SQL statement."""
    m = SELECT_RE.match(sql.strip())
    if m:
        return f'SELECT\t{m.group(1)}'
    m = MERGE_RE.match(sql.strip())
    if m:
        return f'MERGE\t{m.group(1)}'
    return f'{sql}\t'


def parse_vector(body: str):
    """Return (query_desc, params_str) from the bracket contents of a query vector."""
    tokens = TOKEN_RE.findall(body)
    # Each match is (string_content, number_content); one of the two is non-empty.
    parts = []
    for s, n in tokens:
        if s != '' or (s == '' and n == ''):
            # string token (n is empty)
            parts.append(f'"{s}"')
        else:
            parts.append(n)

    if not parts:
        return f'{body}\t', ''

    sql = parts[0][1:-1]  # strip the outer "..."
    query_desc = decompose_sql(sql)
    params = '\t'.join(parts[1:]) if len(parts) > 1 else ''
    return query_desc, params


def main(log_path: str, out_path: str):
    with open(log_path, 'r', errors='replace') as f_in, \
         open(out_path, 'w') as f_out:

        f_out.write('connection\top\ttable\tparams\n')

        for line in f_in:
            line = line.rstrip('\n')
            m = LOG_RE.match(line)
            if not m:
                continue
            timestamp, connection, vector_body = m.group(1), m.group(2), m.group(3)
            query_desc, params = parse_vector(vector_body)
            op = query_desc.split('\t')[0]
            if op not in ('SELECT', 'MERGE'):
                continue
            f_out.write(f'{connection}\t{query_desc}\t{params}\n')

    print(f'Written to {out_path}')


if __name__ == '__main__':
    log = sys.argv[1] if len(sys.argv) > 1 else 'l1/stdout.log'
    out = sys.argv[2] if len(sys.argv) > 2 else 'queries.tsv'
    main(log, out)
