"""
Writes-Follow-Reads (WFR) Consistency Experiment - Operational Proxy
Investigates causal lineage preservation, stale reads, and LWW reconciliation
under tunable consistency levels in Apache Cassandra.
"""

import argparse
import csv
import os
import random
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

# ============================================================
# EXPERIMENT SETTINGS
# ============================================================

KEYSPACE = "consistency_experiment"
TABLE = "consistency_test"
NETWORK_DESCRIPTION = "netem delay 25ms 25ms"
SETUP_RETRIES = 3

CONSISTENCY_LEVELS = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
}

NODE_PORTS = {
    "cassandra1": 9042,
    "cassandra2": 9043,
    "cassandra3": 9044,
}

# Default representative validation pairs: (CL_W1, CL_R)
VALIDATION_CONFIGS = [
    ("ONE", "ONE"),
    ("ONE", "QUORUM"),
    ("QUORUM", "ONE"),
    ("QUORUM", "QUORUM"),
    ("ALL", "ONE"),
]


def init_sessions():
    """Connect to each exposed Cassandra node using pinned local policy."""
    sessions = {}
    clusters = {}
    print("=" * 70)
    print("CONNECTING TO CASSANDRA NODES")
    print("=" * 70)
    for node_name, port in NODE_PORTS.items():
        cluster = Cluster(
            ["127.0.0.1"],
            port=port,
            load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
            connect_timeout=10.0,
        )
        session = cluster.connect(KEYSPACE)
        clusters[node_name] = cluster
        sessions[node_name] = session
        print(f"  {node_name}: localhost:{port} connected")
    return clusters, sessions


def run_experiment(trials=5, configs=None, dependent_write_cl_name="QUORUM"):
    """
    Run WFR operational proxy experiment.
    trials: number of trials per configuration (default 5 for validation)
    configs: list of (CL_W1, CL_R) tuples to test
    dependent_write_cl_name: consistency level used for Client B's dependent write
    """
    if configs is None:
        configs = VALIDATION_CONFIGS

    dep_write_cl = CONSISTENCY_LEVELS[dependent_write_cl_name]

    clusters, sessions = init_sessions()
    node_names = list(sessions.keys())

    # Statements
    setup_stmt = SimpleStatement(
        f"""
        INSERT INTO {TABLE} (key, value, version, client, updated_at)
        VALUES (%s, %s, %s, %s, toTimestamp(now()))
        """,
        consistency_level=ConsistencyLevel.ALL,
    )

    probe_stmt = SimpleStatement(
        f"""
        SELECT version, value, client, WRITETIME(version) AS wtime
        FROM {TABLE} WHERE key = %s
        """,
        consistency_level=ConsistencyLevel.ONE,
    )

    reconcile_stmt = SimpleStatement(
        f"""
        SELECT version, value, client, WRITETIME(version) AS wtime
        FROM {TABLE} WHERE key = %s
        """,
        consistency_level=ConsistencyLevel.ALL,
    )

    trial_records = []
    probe_records = []

    print("\n" + "=" * 70)
    print(f"STARTING WFR EXPERIMENT ({trials} trials/config, W2_CL={dependent_write_cl_name})")
    print("=" * 70)

    for w1_name, r_name in configs:
        w1_cl = CONSISTENCY_LEVELS[w1_name]
        r_cl = CONSISTENCY_LEVELS[r_name]

        print(f"\n---> Configuration: W1={w1_name} | READ={r_name} | W2={dependent_write_cl_name}")

        w1_stmt = SimpleStatement(
            f"""
            INSERT INTO {TABLE} (key, value, version, client, updated_at)
            VALUES (%s, %s, %s, %s, toTimestamp(now()))
            """,
            consistency_level=w1_cl,
        )

        read_stmt = SimpleStatement(
            f"""
            SELECT version, value, client, WRITETIME(version) AS wtime
            FROM {TABLE} WHERE key = %s
            """,
            consistency_level=r_cl,
        )

        w2_stmt = SimpleStatement(
            f"""
            INSERT INTO {TABLE} (key, value, version, client, updated_at)
            VALUES (%s, %s, %s, %s, toTimestamp(now()))
            """,
            consistency_level=dep_write_cl,
        )

        for trial in range(1, trials + 1):
            unique_id = f"{time.time_ns()}_{random.randint(1000, 9999)}"
            key = f"wfr_{w1_name.lower()}_{r_name.lower()}_{trial}_{unique_id}"

            # Step 0: Baseline Setup (v1, CL=ALL)
            setup_success = False
            setup_attempts = 0
            setup_error = ""

            while not setup_success and setup_attempts < SETUP_RETRIES:
                setup_attempts += 1
                setup_node = random.choice(node_names)
                try:
                    sessions[setup_node].execute(
                        setup_stmt,
                        (key, "v1_init_parent_0", 1, "setup"),
                    )
                    setup_success = True
                except Exception as e:
                    setup_error = f"{type(e).__name__}: {str(e)}"

            if not setup_success:
                trial_records.append({
                    "trial": trial,
                    "property": "WRITES_FOLLOW_READS",
                    "scenario": "normal",
                    "w1_cl": w1_name,
                    "read_cl": r_name,
                    "w2_cl": dependent_write_cl_name,
                    "key": key,
                    "setup_attempts": setup_attempts,
                    "w1_completed": False,
                    "observed_version": None,
                    "observed_parent": None,
                    "read_type": "NONE",
                    "w2_completed": False,
                    "dependent_version": None,
                    "dependent_parent": None,
                    "final_version": None,
                    "final_value": None,
                    "final_client": None,
                    "classification": "SETUP_ERROR",
                    "error": setup_error,
                })
                print(f"  Trial {trial:2d} | SETUP_ERROR: {setup_error}")
                continue

            # Measured Experiment
            w1_completed = False
            observed_version = None
            observed_parent = None
            observed_wtime = None
            read_type = "NONE"
            w2_completed = False
            dep_version = None
            dep_parent = None
            final_version = None
            final_value = None
            final_client = None
            final_wtime = None
            classification = "INCONCLUSIVE"
            error_msg = ""

            try:
                # Step 1: Prerequisite Write W1 (Writer A -> v2)
                w1_node = random.choice(node_names)
                sessions[w1_node].execute(
                    w1_stmt,
                    (key, "v2_writerA_parent_1", 2, "writer_A"),
                )
                w1_completed = True

                # Step 2: Client B Read (reads key X at CL_R)
                r_node = random.choice(node_names)
                r_row = sessions[r_node].execute(read_stmt, (key,)).one()

                if r_row is not None:
                    observed_version = r_row.version
                    observed_value = r_row.value
                    observed_wtime = r_row.wtime
                    if "_parent_" in (observed_value or ""):
                        try:
                            observed_parent = int(observed_value.split("_parent_")[-1])
                        except ValueError:
                            observed_parent = None

                    if observed_version == 2:
                        read_type = "FRESH_READ"
                    elif observed_version == 1:
                        read_type = "STALE_READ"
                    else:
                        read_type = f"UNEXPECTED_v{observed_version}"
                else:
                    read_type = "EMPTY_ROW"

                # Step 3: Causally Dependent Write W2 (Client B)
                # Client B carries causal lineage explicitly
                if observed_version is not None:
                    dep_version = observed_version + 1
                    dep_parent = observed_version
                    dep_value = f"v{dep_version}_clientB_parent_{dep_parent}"
                else:
                    dep_version = 2
                    dep_parent = 0
                    dep_value = "v2_clientB_parent_0"

                w2_node = random.choice(node_names)
                sessions[w2_node].execute(
                    w2_stmt,
                    (key, dep_value, dep_version, "client_B"),
                )
                w2_completed = True

                # Step 4a: Diagnostic Probes (CL=ONE on each node)
                probes_diverged = False
                for p_node in node_names:
                    p_row = sessions[p_node].execute(probe_stmt, (key,)).one()
                    p_ver = p_row.version if p_row else None
                    p_val = p_row.value if p_row else None
                    probe_records.append({
                        "trial": trial,
                        "w1_cl": w1_name,
                        "read_cl": r_name,
                        "w2_cl": dependent_write_cl_name,
                        "probe_node": p_node,
                        "observed_version": p_ver,
                        "observed_value": p_val,
                    })
                    if p_ver != dep_version:
                        probes_diverged = True

                # Step 4b: Reconciled Read (CL=ALL)
                rec_node = random.choice(node_names)
                rec_row = sessions[rec_node].execute(reconcile_stmt, (key,)).one()
                if rec_row is not None:
                    final_version = rec_row.version
                    final_value = rec_row.value
                    final_client = rec_row.client
                    final_wtime = rec_row.wtime

                # Classification Logic
                if read_type == "FRESH_READ":
                    # Client B observed v2, issued v3 (parent 2)
                    if final_version == 3 and final_client == "client_B":
                        classification = "CAUSAL_LINEAGE_PRESERVED"
                    else:
                        classification = "CAUSAL_LINEAGE_NOT_OBSERVED"
                elif read_type == "STALE_READ":
                    # Client B observed v1, issued v2 (parent 1)
                    if final_version == 2 and final_client == "client_B":
                        classification = "STALE_READ_LOST_UPDATE"
                    else:
                        classification = "STALE_READ"
                else:
                    classification = "INCONCLUSIVE"

            except Exception as e:
                classification = "OPERATION_FAILURE"
                error_msg = f"{type(e).__name__}: {str(e)}"

            trial_records.append({
                "trial": trial,
                "property": "WRITES_FOLLOW_READS",
                "scenario": "normal",
                "w1_cl": w1_name,
                "read_cl": r_name,
                "w2_cl": dependent_write_cl_name,
                "key": key,
                "setup_attempts": setup_attempts,
                "w1_completed": w1_completed,
                "observed_version": observed_version,
                "observed_parent": observed_parent,
                "read_type": read_type,
                "w2_completed": w2_completed,
                "dependent_version": dep_version,
                "dependent_parent": dep_parent,
                "final_version": final_version,
                "final_value": final_value,
                "final_client": final_client,
                "classification": classification,
                "error": error_msg,
            })

            print(
                f"  Trial {trial:2d} | Read_v={observed_version} ({read_type}) | "
                f"W2_v={dep_version} (parent={dep_parent}) | "
                f"Final_v={final_version} (client={final_client}) | "
                f"Result: {classification}"
            )

    # Save Results
    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(os.path.dirname(script_dir), "results")
    os.makedirs(results_dir, exist_ok=True)

    trials_csv_path = os.path.join(results_dir, "normal.csv")
    with open(trials_csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=trial_records[0].keys())
        writer.writeheader()
        writer.writerows(trial_records)
    print(f"\nTrial records saved to: {trials_csv_path}")

    # Generate and Save Summary
    summary_dict = {}
    for r in trial_records:
        cfg = (r["w1_cl"], r["read_cl"], r["w2_cl"])
        if cfg not in summary_dict:
            summary_dict[cfg] = {
                "w1_cl": r["w1_cl"],
                "read_cl": r["read_cl"],
                "w2_cl": r["w2_cl"],
                "total_trials": 0,
                "fresh_reads": 0,
                "stale_reads": 0,
                "lineage_preserved": 0,
                "lineage_not_observed": 0,
                "stale_read_lost_updates": 0,
                "operation_failures": 0,
                "setup_errors": 0,
            }
        s = summary_dict[cfg]
        s["total_trials"] += 1
        if r["read_type"] == "FRESH_READ":
            s["fresh_reads"] += 1
        elif r["read_type"] == "STALE_READ":
            s["stale_reads"] += 1

        cls = r["classification"]
        if cls == "CAUSAL_LINEAGE_PRESERVED":
            s["lineage_preserved"] += 1
        elif cls == "CAUSAL_LINEAGE_NOT_OBSERVED":
            s["lineage_not_observed"] += 1
        elif cls == "STALE_READ_LOST_UPDATE":
            s["stale_read_lost_updates"] += 1
        elif cls == "OPERATION_FAILURE":
            s["operation_failures"] += 1
        elif cls == "SETUP_ERROR":
            s["setup_errors"] += 1

    summary_rows = list(summary_dict.values())
    summary_csv_path = os.path.join(results_dir, "normal_summary.csv")
    with open(summary_csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
        writer.writeheader()
        writer.writerows(summary_rows)
    print(f"Summary saved to: {summary_csv_path}")

    # Shutdown
    for c in clusters.values():
        c.shutdown()
    print("\nCluster connections closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WFR Normal Consistency Experiment")
    parser.add_argument("--trials", type=int, default=5, help="Number of trials per configuration (default 5)")
    parser.add_argument("--w2_cl", type=str, default="QUORUM", choices=["ONE", "QUORUM", "ALL"], help="Consistency level for dependent write W2")
    args = parser.parse_args()

    run_experiment(trials=args.trials, dependent_write_cl_name=args.w2_cl)