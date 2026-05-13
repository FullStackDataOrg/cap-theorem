# Teardown — Project 03: CAP Theorem
### Fullstack Data — Kenneth

---

## What I Built

A two-node PostgreSQL experiment that makes the CAP theorem observable at the data level. I simulated a network partition using `docker pause`, forced a divergence by continuing to write to the live node, then implemented Last-Write-Wins and First-Write-Wins as the two reconciliation strategies to close the gap. The output is not a diagram or a definition — it is a live row count gap in your terminal that you caused and then repaired with code you wrote yourself.

---

## Decisions Made During the Build

**Using `docker pause` instead of dropping a network rule.**
`iptables` rules or Docker network disconnection would have worked, but `docker pause` is simpler, reversible in one command, and more honest — it suspends the container's processes without crashing it, which is exactly what a real network partition looks like from the other node's perspective. The node is alive. It is just unreachable.

**Adding `written_to` and `written_at` columns to the schema.**
The standard 8-column payment schema has no way to distinguish a baseline write from a partition write. Without `written_to`, `observe_divergence.py` can only show a row count gap — it cannot identify which specific rows caused it, show you their values, or produce a write timeline. These two columns are pure observability. They have no business meaning. They are the instrumentation that makes the experiment legible.

**Using two different seeds for baseline and partition events (42 and 99).**
If both batches used the same seed they would generate overlapping `event_id` values and the `ON CONFLICT DO NOTHING` clause would silently deduplicate them, making the partition writes invisible in the counts. Different seeds guarantee the two batches are fully distinct.

**Taking the AP path explicitly during the partition.**
The experiment could have demonstrated CP behaviour — stopping writes when node2 becomes unreachable. AP was the more instructive choice because it produces visible data: a gap you can query, sample, and repair. CP produces an error message. The error is important conceptually but teaches less about what divergence actually looks like in a database.

**Implementing the reset between repair strategies.**
After LWW runs, both nodes are in sync — there is nothing left for FWW to demonstrate. The reset function in `repair.py` deliberately removes node1_only rows from node2 to restore the diverged state. Without it, FWW would find no gap and delete nothing, which would look like a no-op rather than a deliberate strategy with real data loss consequences.

**Writing `simulation_log.json` and `repair_log.json` to `data/`.**
JSON logs make the experiment reproducible and inspectable outside the terminal. `observe_divergence.py` cross-references the simulation log to confirm the gap it sees matches what was recorded at simulation time. This is the pattern used in production observability pipelines — structured event logs that downstream processes can read independently of the system that produced them.

---

## What Broke During the Build

**`docker pause` requires the container name, not the service name.**
`docker pause postgres_node2` works. `docker pause cap_postgres_node2` — the name Docker Compose generates when it prefixes the project directory name — does not, unless you set `container_name` explicitly in the compose file. The fix was adding `container_name: postgres_node2` to both service definitions. Without this, `simulate_partition.py` throws a non-zero exit code from `subprocess.run` and the partition never fires.

**Node 2 hanging on query after `docker pause`.**
In an early draft of `simulate_partition.py`, the script attempted to query node2 immediately after pausing it to record its final row count. The query hung indefinitely because the connection was open before the pause and the socket remained half-open — psycopg2 waited for a response that never came. The fix was to record node2's last known state before issuing the pause, and skip querying it until after unpause.

**`ON CONFLICT DO NOTHING` masking seed collisions.**
During testing with identical seeds on both batches, the insert counts looked correct but the partition events were being silently deduplicated against baseline events that happened to share a UUID. The actual row count gap was smaller than expected. Switching partition events to seed 99 resolved this and the gap matched the configured `PARTITION_ROWS` exactly.

**psycopg2 `executemany` not committing without `autocommit = True`.**
The first pass of `repair.py` used the default transaction mode. The inserts executed without error but nothing appeared in node2 after the LWW sync because the transaction was never committed and the connection was closed at the end of `main()`, rolling everything back. Setting `conn.autocommit = True` on connection open resolved this across all scripts.

---

## What the Results Clarified

**Eventual consistency is not a guarantee — it is a promise with no deadline and no mechanism.**
The phrase "nodes will eventually converge" sounds passive, as if the system handles it. This experiment showed that on restore, node2 had no idea it missed anything. It did not request the missing rows. It did not flag itself as stale. It just sat there with 50 rows, confident. Convergence required `repair.py` — code written by the engineer, implementing a deliberate strategy, making an explicit choice about which write wins.

**The divergence is additive, not corrupting.**
The shared rows — the 50 baseline events — were identical on both nodes after restore. `observe_divergence.py`'s agreement check confirmed this across a 10-row sample. The divergence was not a case of node2 having wrong values. It was a case of node2 having fewer rows. This distinction matters in production: additive divergence is repairable by copying. Value-level divergence — two nodes with different values for the same primary key — requires conflict resolution logic and is significantly more complex.

**LWW and FWW are not symmetric.**
LWW costs nothing in data. It brings the stale node forward. FWW costs exactly the partition window's writes. In this experiment that is 30 rows — a small number. At production scale, a 60-second partition on a high-volume pipeline could mean tens of thousands of events discarded by FWW. The choice between them is not a technical preference — it is a product decision about what the business considers authoritative.

**`docker pause` is a better partition simulation than it looks.**
A paused container is not a crashed one. The process is suspended, the socket is half-open, and the OS is not aware of a failure. This is closer to real network partition behaviour — where the node is up and the link is down — than a container crash, which sends a TCP RST and causes an immediate connection error. The hanging query issue during development was actually evidence that the simulation was accurate.

---

## What I Would Do Differently

**Introduce a third node and demonstrate quorum.**
With two nodes the only reconciliation options are "one node wins" or "one node loses." A third node would allow a majority vote — the core mechanism behind Cassandra, MongoDB, and DynamoDB. Node 1 and Node 3 could agree on the partition writes, outvoting the stale Node 2. This would make the 3-node repair strategies (quorum, vector clocks) observable rather than just conceptual.

**Add a continuous reader during the partition.**
`simulate_partition.py` shows what happens to writes during a partition. It does not show what happens to reads. A reader polling both nodes every second during the partition window would produce a concrete record of stale reads — timestamps where Node 2 returned data that Node 1 had already superseded. That is the user-visible cost of AP systems that this experiment only implies.

**Measure repair time as partition window grows.**
The experiment uses a fixed 30-row partition gap. In production the gap is a function of write volume and partition duration. A parameterised version — `--partition-duration 60 --write-rate 1000` — would produce a repair time vs gap size curve that is directly applicable to SLA reasoning: if a partition lasts N seconds, how long does repair take, and what is the replication lag ceiling before repair becomes impractical?