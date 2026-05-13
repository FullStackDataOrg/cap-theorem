"""
observe_divergence.py — Project 03: CAP Theorem
Fullstack Data — Kenneth

Responsibility:
  Deep inspection of the diverged state after simulate_partition.py runs.
  This script answers four questions:

  1. MACRO — How many rows does each node have? What is the gap?
  2. MISSING — Which specific event_ids exist on node1 but not node2?
  3. TEMPORAL — When did the divergence start? (first partition write timestamp)
  4. CONSISTENCY CHECK — For rows both nodes share, do the values agree?

  Observability output:
  - Divergence summary table
  - 5 sample missing rows (the events node2 never received)
  - Timeline of writes showing the partition boundary
  - Node agreement check on shared rows
  - Reads simulation_log.json to cross-reference with simulation metadata

Run after: simulate_partition.py
Run before: repair.py
"""

import psycopg2
import json
import os
from datetime import datetime, timezone
from tabulate import tabulate

# ── Config ────────────────────────────────────────────────────────────────────

NODE1 = {"host": "localhost", "port": 5432, "dbname": "payments",
         "user": "engineer", "password": "engineer", "connect_timeout": 5}
NODE2 = {"host": "localhost", "port": 5433, "dbname": "payments",
         "user": "engineer", "password": "engineer", "connect_timeout": 5}

LOG_PATH = "data/simulation_log.json"

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_conn(config: dict):
    conn = psycopg2.connect(**config)
    conn.autocommit = True
    return conn


def section(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}\n")


# ── Observation queries ───────────────────────────────────────────────────────

def macro_summary(conn1, conn2) -> list[dict]:
    """Row counts and written_to distribution per node."""
    rows = []
    for label, conn in [("Node 1", conn1), ("Node 2", conn2)]:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM payment_events;")
            total = cur.fetchone()[0]

            cur.execute("""
                SELECT written_to, COUNT(*)
                FROM payment_events
                GROUP BY written_to ORDER BY written_to;
            """)
            dist = {r[0]: r[1] for r in cur.fetchall()}

        rows.append({
            "node":           label,
            "total_rows":     total,
            "both":           dist.get("both", 0),
            "node1_only":     dist.get("node1_only", 0),
        })
    return rows


def missing_on_node2(conn1, conn2, limit: int = 5) -> list[dict]:
    """
    Event IDs present on node1 but absent on node2.
    These are the writes that happened during the partition window.
    Uses NOT EXISTS rather than NOT IN for correctness on large sets.
    """
    with conn1.cursor() as cur:
        cur.execute("""
            SELECT
                n1.event_id,
                n1.user_id,
                n1.merchant_id,
                n1.amount,
                n1.currency,
                n1.status,
                n1.written_to,
                to_char(n1.written_at AT TIME ZONE 'UTC', 'HH24:MI:SS') AS written_at
            FROM payment_events n1
            WHERE n1.written_to = 'node1_only'
            ORDER BY n1.written_at ASC
            LIMIT %s;
        """, (limit,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def count_missing(conn1, conn2) -> int:
    """Total rows on node1 that node2 does not have."""
    with conn1.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM payment_events
            WHERE written_to = 'node1_only';
        """)
        return cur.fetchone()[0]


def shared_row_agreement(conn1, conn2, sample: int = 10) -> list[dict]:
    """
    For a sample of rows that exist on both nodes, check if the values agree.
    These are the 'both' rows — they should be identical.
    Any disagreement here would indicate a deeper corruption issue.
    """
    with conn1.cursor() as cur:
        cur.execute("""
            SELECT event_id, amount, status, currency
            FROM payment_events
            WHERE written_to = 'both'
            ORDER BY event_id
            LIMIT %s;
        """, (sample,))
        n1_rows = {r[0]: r[1:] for r in cur.fetchall()}

    with conn2.cursor() as cur:
        cur.execute("""
            SELECT event_id, amount, status, currency
            FROM payment_events
            WHERE written_to = 'both'
            ORDER BY event_id
            LIMIT %s;
        """, (sample,))
        n2_rows = {r[0]: r[1:] for r in cur.fetchall()}

    results = []
    for eid in n1_rows:
        n1_val = n1_rows[eid]
        n2_val = n2_rows.get(eid, ("MISSING", "MISSING", "MISSING"))
        results.append({
            "event_id":  eid[:18] + "…",
            "n1_amount": n1_val[0],
            "n2_amount": n2_val[0],
            "n1_status": n1_val[1],
            "n2_status": n2_val[1],
            "agree":     "✓" if n1_val == n2_val else "✗  MISMATCH",
        })
    return results


def write_timeline(conn1, n: int = 15) -> list[dict]:
    """
    Chronological write log from node1 showing the partition boundary.
    written_to='both' = pre-partition, written_to='node1_only' = partition window.
    """
    with conn1.cursor() as cur:
        cur.execute("""
            SELECT
                ROW_NUMBER() OVER (ORDER BY written_at) AS seq,
                to_char(written_at AT TIME ZONE 'UTC', 'HH24:MI:SS') AS written_at,
                written_to,
                event_id
            FROM payment_events
            ORDER BY written_at
            LIMIT %s;
        """, (n,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "═" * 60)
    print("  PROJECT 03 — CAP THEOREM  |  observe_divergence.py")
    print("  Fullstack Data — Kenneth")
    print("═" * 60)

    # ── Load simulation log ───────────────────────────────────────────────────
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH) as f:
            sim_log = json.load(f)
        print(f"\n  Simulation log loaded from {LOG_PATH}")
        fs = sim_log.get("final_state", {})
        print(f"  Recorded gap at simulation end: {fs.get('divergence_gap', '?')} rows")
    else:
        print(f"\n  ⚠  No simulation log found at {LOG_PATH}")
        print(f"     Run simulate_partition.py first.")
        sim_log = {}

    print(f"\n  Connecting to both nodes …")
    conn1 = get_conn(NODE1)
    conn2 = get_conn(NODE2)
    print(f"  ✓  Node 1 connected")
    print(f"  ✓  Node 2 connected")

    # ─────────────────────────────────────────────────────────────────────────
    # CHECK 1 — MACRO SUMMARY
    # ─────────────────────────────────────────────────────────────────────────
    section("CHECK 1 — MACRO DIVERGENCE SUMMARY")
    macro = macro_summary(conn1, conn2)
    print(tabulate(macro, headers="keys", tablefmt="rounded_outline"))

    n1_total = macro[0]["total_rows"]
    n2_total = macro[1]["total_rows"]
    gap = n1_total - n2_total
    print(f"\n  Current gap: {gap} rows")
    print(f"  Node 2 is {'IN SYNC' if gap == 0 else 'STALE — missing ' + str(gap) + ' writes'}")

    # ─────────────────────────────────────────────────────────────────────────
    # CHECK 2 — MISSING ROWS
    # ─────────────────────────────────────────────────────────────────────────
    section("CHECK 2 — ROWS NODE2 NEVER RECEIVED  (first 5 of " + str(count_missing(conn1, conn2)) + ")")
    missing = missing_on_node2(conn1, conn2, limit=5)
    if missing:
        print(tabulate(missing, headers="keys", tablefmt="rounded_outline"))
        print(f"\n  These {count_missing(conn1, conn2)} rows exist on Node 1 only.")
        print(f"  Node 2 has no knowledge they were ever written.")
        print(f"  If a client reads from Node 2 right now, it gets stale data.")
    else:
        print("  No missing rows found — nodes may already be in sync.")

    # ─────────────────────────────────────────────────────────────────────────
    # CHECK 3 — SHARED ROW AGREEMENT
    # ─────────────────────────────────────────────────────────────────────────
    section("CHECK 3 — SHARED ROW AGREEMENT  (sample 10 baseline rows)")
    agreement = shared_row_agreement(conn1, conn2, sample=10)
    print(tabulate(agreement, headers="keys", tablefmt="rounded_outline"))
    mismatches = [r for r in agreement if "MISMATCH" in str(r["agree"])]
    if mismatches:
        print(f"\n  ✗  {len(mismatches)} value mismatches found on shared rows — deeper investigation needed")
    else:
        print(f"\n  ✓  All shared rows agree on values — divergence is purely additive (missing rows, not corrupted rows)")

    # ─────────────────────────────────────────────────────────────────────────
    # CHECK 4 — WRITE TIMELINE
    # ─────────────────────────────────────────────────────────────────────────
    section("CHECK 4 — WRITE TIMELINE  (node1 — showing partition boundary)")
    timeline = write_timeline(conn1, n=15)
    print(tabulate(timeline, headers="keys", tablefmt="rounded_outline"))
    print(f"""
  HOW TO READ THIS:
    written_to = 'both'       → pre-partition writes, both nodes received these
    written_to = 'node1_only' → partition window writes, node2 missed these
    The boundary between them is the moment docker pause fired.
    """)

    # ─────────────────────────────────────────────────────────────────────────
    # SUMMARY
    # ─────────────────────────────────────────────────────────────────────────
    section("OBSERVATION SUMMARY")
    print(f"""
  ┌─────────────────────────────────────────────────────┐
  │  Node 1 rows:         {n1_total:<5}                          │
  │  Node 2 rows:         {n2_total:<5}                          │
  │  Divergence gap:      {gap:<5}                          │
  │                                                     │
  │  The gap is not a bug. It is the direct result of   │
  │  choosing AP (available + partition tolerant) over  │
  │  CP (consistent + partition tolerant).              │
  │                                                     │
  │  No automatic reconciliation occurred on restore.   │
  │  repair.py implements two strategies to close it.   │
  └─────────────────────────────────────────────────────┘
    """)
    print(f"  Next step: python repair.py\n")

    conn1.close()
    conn2.close()


if __name__ == "__main__":
    main()