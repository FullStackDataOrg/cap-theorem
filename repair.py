"""
repair.py — Project 03: CAP Theorem
Fullstack Data — Kenneth

Responsibility:
  Reconcile the diverged state across both nodes using two strategies.

  STRATEGY 1 — LAST-WRITE-WINS (LWW)
    Node1's version is authoritative because it has the most recent writes.
    Copy all node1_only rows from node1 → node2.
    After this, node2 is fully caught up.
    Trade-off: any write that happened on node2 during the partition
    (in a real active-active system) would be silently discarded.

  STRATEGY 2 — FIRST-WRITE-WINS (FWW)
    The baseline (first write) is authoritative.
    Partition writes are discarded — node1 is rolled back to match node2.
    Trade-off: 30 real events are lost. Appropriate when the partition
    writes were speculative or unverified.

  Both strategies are demonstrated sequentially with a reset between them.

  Observability:
    - Pre-repair state printed
    - Post-repair state printed for each strategy
    - Row counts verified on both nodes after each strategy
    - Strategy comparison table printed at end
    - Results written to data/repair_log.json

Run after: observe_divergence.py
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

LOG_PATH = "data/repair_log.json"

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_conn(config: dict):
    conn = psycopg2.connect(**config)
    conn.autocommit = True
    return conn


def row_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM payment_events;")
        return cur.fetchone()[0]


def written_to_counts(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT written_to, COUNT(*)
            FROM payment_events GROUP BY written_to;
        """)
        return {r[0]: r[1] for r in cur.fetchall()}


def section(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}\n")


def state_table(conn1, conn2, label1="Node 1", label2="Node 2") -> list[dict]:
    d1 = written_to_counts(conn1)
    d2 = written_to_counts(conn2)
    return [
        {"node": label1,
         "total": row_count(conn1),
         "both": d1.get("both", 0),
         "node1_only": d1.get("node1_only", 0)},
        {"node": label2,
         "total": row_count(conn2),
         "both": d2.get("both", 0),
         "node1_only": d2.get("node1_only", 0)},
    ]


def reset_to_diverged_state(conn1, conn2):
    """
    Restore both nodes to the post-partition diverged state so we can
    demonstrate Strategy 2 cleanly after Strategy 1 has already run.

    Node1: keep all rows (both + node1_only)
    Node2: remove node1_only rows so it is back to baseline-only
    """
    with conn2.cursor() as cur:
        cur.execute("DELETE FROM payment_events WHERE written_to = 'node1_only';")
    print(f"  ↻  Nodes reset to diverged state for Strategy 2 demonstration")


# ── Strategy 1 — Last-Write-Wins ──────────────────────────────────────────────

def strategy_lww(conn1, conn2) -> dict:
    """
    Copy all node1_only rows from node1 → node2.
    Node1 is the source of truth. Its state wins.
    After this: node2 == node1.
    """
    # Fetch missing rows from node1
    with conn1.cursor() as cur:
        cur.execute("""
            SELECT event_id, user_id, merchant_id, amount, currency,
                   status, event_ts, is_flagged, written_to
            FROM payment_events
            WHERE written_to = 'node1_only';
        """)
        cols = [d[0] for d in cur.description]
        missing_rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    rows_to_sync = len(missing_rows)

    # Insert into node2
    insert_sql = """
        INSERT INTO payment_events
          (event_id, user_id, merchant_id, amount, currency,
           status, event_ts, is_flagged, written_to)
        VALUES
          (%(event_id)s, %(user_id)s, %(merchant_id)s, %(amount)s, %(currency)s,
           %(status)s, %(event_ts)s, %(is_flagged)s, %(written_to)s)
        ON CONFLICT (event_id) DO NOTHING;
    """
    with conn2.cursor() as cur:
        cur.executemany(insert_sql, missing_rows)

    n1_after = row_count(conn1)
    n2_after = row_count(conn2)

    return {
        "strategy":        "Last-Write-Wins (LWW)",
        "rows_synced":     rows_to_sync,
        "node1_after":     n1_after,
        "node2_after":     n2_after,
        "in_sync":         n1_after == n2_after,
        "data_lost":       0,
        "description": (
            "Node1 is authoritative. All partition writes copied to Node2. "
            "Zero data loss. Any hypothetical Node2 writes during partition would be overwritten."
        ),
    }


# ── Strategy 2 — First-Write-Wins ────────────────────────────────────────────

def strategy_fww(conn1, conn2) -> dict:
    """
    The baseline (first write) wins. Partition writes are discarded.
    Node1 is rolled back to match node2's state.
    After this: node1 == node2 == baseline only.
    """
    rows_before_n1 = row_count(conn1)

    with conn1.cursor() as cur:
        cur.execute("DELETE FROM payment_events WHERE written_to = 'node1_only';")

    rows_after_n1 = row_count(conn1)
    rows_deleted = rows_before_n1 - rows_after_n1

    n1_after = row_count(conn1)
    n2_after = row_count(conn2)

    return {
        "strategy":    "First-Write-Wins (FWW)",
        "rows_synced": 0,
        "node1_after": n1_after,
        "node2_after": n2_after,
        "in_sync":     n1_after == n2_after,
        "data_lost":   rows_deleted,
        "description": (
            f"Baseline is authoritative. {rows_deleted} partition writes deleted from Node1. "
            "Both nodes agree — at the cost of losing those writes permanently."
        ),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "═" * 60)
    print("  PROJECT 03 — CAP THEOREM  |  repair.py")
    print("  Fullstack Data — Kenneth")
    print("═" * 60)

    conn1 = get_conn(NODE1)
    conn2 = get_conn(NODE2)

    # ── Pre-repair state ──────────────────────────────────────────────────────
    section("PRE-REPAIR STATE  (diverged)")
    pre = state_table(conn1, conn2)
    print(tabulate(pre, headers="keys", tablefmt="rounded_outline"))
    pre_gap = pre[0]["total"] - pre[1]["total"]
    print(f"\n  Current gap: {pre_gap} rows  ← this is what both strategies will close")

    results = []

    # ─────────────────────────────────────────────────────────────────────────
    # STRATEGY 1 — LAST-WRITE-WINS
    # ─────────────────────────────────────────────────────────────────────────
    section("STRATEGY 1 — LAST-WRITE-WINS  (node1 is authoritative)")
    print(f"""
  How it works:
    All rows that node1 has and node2 is missing are copied to node2.
    Node1 is treated as the source of truth.
    After repair: node2 == node1.

  When to use in production:
    Analytics pipelines, metrics, event logs — workloads where
    recency matters more than a specific write being authoritative.
    """)

    lww_result = strategy_lww(conn1, conn2)
    results.append(lww_result)

    post_lww = state_table(conn1, conn2)
    print(tabulate(post_lww, headers="keys", tablefmt="rounded_outline"))
    print(f"\n  ✓  LWW complete — {lww_result['rows_synced']} rows synced to Node 2")
    print(f"  ✓  Node 1: {lww_result['node1_after']} rows | Node 2: {lww_result['node2_after']} rows")
    print(f"  ✓  In sync: {lww_result['in_sync']} | Data lost: {lww_result['data_lost']} rows")

    # ── Reset for Strategy 2 ──────────────────────────────────────────────────
    print(f"\n  Resetting to diverged state to demonstrate Strategy 2 …")
    reset_to_diverged_state(conn1, conn2)

    # ─────────────────────────────────────────────────────────────────────────
    # STRATEGY 2 — FIRST-WRITE-WINS
    # ─────────────────────────────────────────────────────────────────────────
    section("STRATEGY 2 — FIRST-WRITE-WINS  (baseline is authoritative)")
    print(f"""
  How it works:
    The partition writes on node1 are deleted. Node1 is rolled back.
    The baseline (first agreed state) is treated as authoritative.
    After repair: node1 == node2 == baseline only.

  When to use in production:
    Financial ledgers, inventory counts — workloads where a
    speculative write during an outage must not be treated as real
    until it has been verified against the primary state.
    """)

    fww_result = strategy_fww(conn1, conn2)
    results.append(fww_result)

    post_fww = state_table(conn1, conn2)
    print(tabulate(post_fww, headers="keys", tablefmt="rounded_outline"))
    print(f"\n  ✓  FWW complete — {fww_result['data_lost']} partition writes deleted from Node 1")
    print(f"  ✓  Node 1: {fww_result['node1_after']} rows | Node 2: {fww_result['node2_after']} rows")
    print(f"  ✓  In sync: {fww_result['in_sync']} | Data lost: {fww_result['data_lost']} rows")

    # ─────────────────────────────────────────────────────────────────────────
    # STRATEGY COMPARISON
    # ─────────────────────────────────────────────────────────────────────────
    section("STRATEGY COMPARISON")
    comparison = [
        {
            "strategy":       r["strategy"],
            "rows_synced":    r["rows_synced"],
            "data_lost":      r["data_lost"],
            "in_sync_after":  "✓" if r["in_sync"] else "✗",
            "cost":           "Zero loss — forward repair" if r["data_lost"] == 0 else f"{r['data_lost']} writes permanently deleted",
        }
        for r in results
    ]
    print(tabulate(comparison, headers="keys", tablefmt="rounded_outline"))

    print(f"""
  KEY INSIGHT:
  ┌─────────────────────────────────────────────────────┐
  │  Both strategies achieve consistency.               │
  │  They differ in what they sacrifice to get there.   │
  │                                                     │
  │  LWW  — recency wins. No data lost. Node2 catches   │
  │         up fully. Used when availability > safety.  │
  │                                                     │
  │  FWW  — safety wins. Partition writes discarded.    │
  │         Node1 rolled back. Used when a write must   │
  │         be verified before it is treated as real.   │
  │                                                     │
  │  Neither is automatic. You write the repair logic.  │
  │  That is the actual cost of eventual consistency.   │
  └─────────────────────────────────────────────────────┘
    """)

    # ── Write repair log ──────────────────────────────────────────────────────
    os.makedirs("data", exist_ok=True)
    with open(LOG_PATH, "w") as f:
        json.dump({"strategies": results}, f, indent=2, default=str)
    print(f"  Repair log written → {LOG_PATH}")

    conn1.close()
    conn2.close()


if __name__ == "__main__":
    main()