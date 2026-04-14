"""
MLFQ Benchmark Script
Compares vanilla DuckDB vs MLFQ-DuckDB under mixed workloads.

Usage:
    python mlfq_benchmark.py --sf 1 --runs 5 --label "MLFQ"
    python mlfq_benchmark.py --sf 1 --runs 5 --label "Vanilla"

Metrics: avg latency, P50, P95, P99 for short queries; throughput.
"""

import duckdb
import threading
import time
import statistics
import argparse
import json
import os

# ── TPC-H Queries ────────────────────────────────────────────────────────────

# Long analytical query: TPC-H Q1
LONG_QUERY = """
SELECT
    l_returnflag, l_linestatus,
    sum(l_quantity)                                       AS sum_qty,
    sum(l_extendedprice)                                  AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity)                                       AS avg_qty,
    avg(l_extendedprice)                                  AS avg_price,
    avg(l_discount)                                       AS avg_disc,
    count(*)                                              AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
"""

# Short interactive queries
SHORT_QUERIES = [
    "SELECT count(*) FROM orders WHERE o_orderstatus = 'O'",
    "SELECT avg(l_discount) FROM lineitem WHERE l_quantity < 10",
    "SELECT count(*) FROM customer WHERE c_mktsegment = 'BUILDING'",
    "SELECT sum(o_totalprice) FROM orders WHERE o_orderdate >= DATE '1995-01-01'",
    "SELECT count(*) FROM lineitem WHERE l_shipmode = 'AIR'",
]

# ── Setup ─────────────────────────────────────────────────────────────────────

def setup_database(db_path: str, sf: int):
    """Generate TPC-H data at the given scale factor."""
    if os.path.exists(db_path):
        print(f"[setup] Database already exists at {db_path}, skipping generation.")
        return
    print(f"[setup] Generating TPC-H SF={sf} data into {db_path} ...")
    con = duckdb.connect(db_path)
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    con.execute(f"CALL dbgen(sf={sf})")
    con.close()
    print("[setup] Done.")

# ── Workers ───────────────────────────────────────────────────────────────────

def run_query_worker(db_path: str, query: str, results: list, idx: int):
    """Run a single query and store its latency (ms) in results[idx]."""
    con = duckdb.connect(db_path, read_only=True)
    start = time.perf_counter()
    con.execute(query).fetchall()
    elapsed_ms = (time.perf_counter() - start) * 1000
    results[idx] = elapsed_ms
    con.close()

# ── Benchmark ─────────────────────────────────────────────────────────────────

def run_benchmark(db_path: str, num_short: int, num_runs: int):
    """
    Each run launches 1 long query + num_short short queries simultaneously.
    Returns list of short-query latencies (ms) across all runs.
    """
    all_short_latencies = []
    long_latencies = []

    for run in range(1, num_runs + 1):
        results = [None] * (1 + num_short)   # index 0 = long query
        threads = []

        # Long query
        threads.append(threading.Thread(
            target=run_query_worker,
            args=(db_path, LONG_QUERY, results, 0)
        ))

        # Short queries
        for i in range(num_short):
            query = SHORT_QUERIES[i % len(SHORT_QUERIES)]
            threads.append(threading.Thread(
                target=run_query_worker,
                args=(db_path, query, results, i + 1)
            ))

        start_wall = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        wall_ms = (time.perf_counter() - start_wall) * 1000

        long_latencies.append(results[0])
        short_run = results[1:]
        all_short_latencies.extend(short_run)

        print(f"  Run {run:>2}: long={results[0]:7.1f}ms  "
              f"short_avg={statistics.mean(short_run):7.1f}ms  "
              f"wall={wall_ms:7.1f}ms")

    return all_short_latencies, long_latencies

# ── Metrics ───────────────────────────────────────────────────────────────────

def compute_metrics(latencies: list) -> dict:
    s = sorted(latencies)
    n = len(s)
    return {
        "mean":  round(statistics.mean(s), 2),
        "p50":   round(s[int(n * 0.50)], 2),
        "p95":   round(s[int(n * 0.95)], 2),
        "p99":   round(s[min(int(n * 0.99), n - 1)], 2),
        "min":   round(s[0], 2),
        "max":   round(s[-1], 2),
        "throughput_qps": round(n / (sum(latencies) / 1000), 2),
    }

def print_metrics(label: str, short_metrics: dict, long_metrics: dict):
    print(f"\n{'='*55}")
    print(f"  Results — {label}")
    print(f"{'='*55}")
    print(f"  SHORT QUERIES (latency in ms):")
    print(f"    Mean  : {short_metrics['mean']}")
    print(f"    P50   : {short_metrics['p50']}")
    print(f"    P95   : {short_metrics['p95']}")
    print(f"    P99   : {short_metrics['p99']}")
    print(f"    Min   : {short_metrics['min']}")
    print(f"    Max   : {short_metrics['max']}")
    print(f"    Throughput: {short_metrics['throughput_qps']} qps")
    print(f"  LONG QUERY (TPC-H Q1):")
    print(f"    Mean  : {long_metrics['mean']}")
    print(f"    P95   : {long_metrics['p95']}")
    print(f"{'='*55}\n")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="MLFQ vs Vanilla DuckDB Benchmark")
    parser.add_argument("--sf",        type=int, default=1,        help="TPC-H scale factor (1 or 10)")
    parser.add_argument("--runs",      type=int, default=10,       help="Number of benchmark runs")
    parser.add_argument("--short",     type=int, default=5,        help="Number of concurrent short queries per run")
    parser.add_argument("--label",     type=str, default="DuckDB", help="Label for this run (e.g. Vanilla or MLFQ)")
    parser.add_argument("--db",        type=str, default=None,     help="Path to DuckDB database file (auto-generated if absent)")
    parser.add_argument("--out",       type=str, default=None,     help="Optional JSON output file for results")
    args = parser.parse_args()

    db_path = args.db or f"tpch_sf{args.sf}.duckdb"

    setup_database(db_path, args.sf)

    print(f"\n[benchmark] Label={args.label}  SF={args.sf}  runs={args.runs}  short_queries={args.short}")
    print(f"[benchmark] Starting mixed workload (1 long + {args.short} short queries per run)...\n")

    short_latencies, long_latencies = run_benchmark(db_path, args.short, args.runs)

    short_metrics = compute_metrics(short_latencies)
    long_metrics  = compute_metrics(long_latencies)

    print_metrics(args.label, short_metrics, long_metrics)

    if args.out:
        output = {
            "label": args.label,
            "sf": args.sf,
            "runs": args.runs,
            "short_queries_per_run": args.short,
            "short_query_metrics": short_metrics,
            "long_query_metrics": long_metrics,
        }
        with open(args.out, "w") as f:
            json.dump(output, f, indent=2)
        print(f"[output] Results saved to {args.out}")

if __name__ == "__main__":
    main()
