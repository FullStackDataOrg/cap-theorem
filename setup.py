"""
setup.py — Project 03: CAP Theorem
Fullstack Data — Kenneth

Responsibility:
  - Connect to both PostgreSQL nodes
  - Create the payments table on each node
  - Verify both nodes are healthy and in sync before any experiment begins
  - Print a clear pre-flight status table

Run this first before any other script.
"""

import psycopg2
from psycopg2 import OperationalError
from tabulate import tabulate
from datetime import datetime

# ── Connection config ────────────────────────────────────────────────────────

NODE1 = {
    "host": "localhost",
    "port": 5432,
    "dbname": "payments",
    "user": "engineer",
    "password": "engineer",
    "connect_timeout": 5,
}

NODE2 = {
    "host": "localhost",
    "port": 5433,
    "dbname": "payments",
    "user": "engineer",
    "password": "engineer",
    "connect_timeout": 5,
}

# ── DDL ───────────────────────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS payment_events (
    event_id     TEXT        PRIMARY KEY,
    user_id      TEXT        NOT NULL,
    merchant_id  TEXT        NOT NULL,
    amount       NUMERIC     NOT NULL,
    currency     TEXT        NOT NULL,
    status       TEXT        NOT NULL,
    event_ts     BIGINT      NOT NULL,
    is_flagged   BOOLEAN     NOT NULL,
    written_to   TEXT        NOT NULL,   -- which node accepted this write
    written_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

DROP_TABLE_SQL = "DROP TABLE IF EXISTS payment_events;"

# ── Helpers ───────────────────────────────────────────────────────────────────

def connect(config: dict, label: str):
    """Return a connection or None, printing status."""
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = True
        print(f"  ✓  {label} connected  ({config['host']}:{config['port']})")
        return conn
    except OperationalError as e:
        print(f"  ✗  {label} FAILED — {e}")
        return None


def setup_node(conn, label: str, fresh: bool = True):
    """
    Create the payments table on a node.
    fresh=True drops the table first — ensures a clean slate each run.
    """
    with conn.cursor() as cur:
        if fresh:
            cur.execute(DROP_TABLE_SQL)
            print(f"  ↻  {label} — existing table dropped")
        cur.execute(CREATE_TABLE_SQL)
        print(f"  ✓  {label} — payment_events table created")


def node_status(conn, label: str) -> dict:
    """Return a status dict for the pre-flight table."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM payment_events;")
        row_count = cur.fetchone()[0]

        cur.execute("SELECT version();")
        pg_version = cur.fetchone()[0].split(",")[0]   # trim noise

        cur.execute("SELECT now();")
        server_time = cur.fetchone()[0].strftime("%H:%M:%S UTC")

    return {
        "node":        label,
        "status":      "HEALTHY",
        "rows":        row_count,
        "pg_version":  pg_version,
        "server_time": server_time,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "═" * 60)
    print("  PROJECT 03 — CAP THEOREM  |  setup.py")
    print("  Fullstack Data — Kenneth")
    print("═" * 60)

    print("\n[1/3] Connecting to nodes …\n")
    conn1 = connect(NODE1, "Node 1 (port 5432)")
    conn2 = connect(NODE2, "Node 2 (port 5433)")

    if not conn1 or not conn2:
        print("\n✗  One or more nodes unreachable. Run:")
        print("   docker compose up postgres_node1 postgres_node2 -d")
        print("   Then retry setup.py\n")
        raise SystemExit(1)

    print("\n[2/3] Creating tables (fresh) …\n")
    setup_node(conn1, "Node 1", fresh=True)
    setup_node(conn2, "Node 2", fresh=True)

    print("\n[3/3] Pre-flight status check …\n")
    rows = [node_status(conn1, "Node 1"), node_status(conn2, "Node 2")]
    print(tabulate(rows, headers="keys", tablefmt="rounded_outline"))

    print("\n✓  Both nodes healthy, tables empty, schemas identical.")
    print("   Next step: python simulate_partition.py\n")

    conn1.close()
    conn2.close()


if __name__ == "__main__":
    main()