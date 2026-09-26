"""
Writes-Follow-Reads (WFR) Consistency Experiment - Node Failure Scenario
Evaluates causal lineage preservation, stale reads, and availability under
single-node crash-stop failure in Apache Cassandra 4.1.12.

Methodology:
- Independent per-trial failure/recovery lifecycle:
  1. Verify all 3 nodes (cassandra1, cassandra2, cassandra3) report UN.
  2. Step 0 (Baseline Setup): Written with CL=ALL while all three nodes are
     healthy, ensuring that the baseline write is acknowledged by all three
     replicas before the failure is injected.
  3. Node Failure: Stop target node (cassandra3); verify DN in nodetool status.
  4. Degraded Operations (Surviving nodes cassandra1, cassandra2):
     - Step 1: Writer A writes v2 at CL_W1
     - Step 2: Reader B reads key at CL_R
     - Step 3: Reader B writes dependent mutation at CL_W2 (QUORUM)
     - Step 4: Degraded diagnostic probe at CL=ONE on surviving nodes
  5. Node Recovery: Restart cassandra3; poll until all 3 nodes report UN and
     schema agreement is restored.
  6. Step 5 (Reconciliation): Query key at CL=QUORUM across healed cluster.
"""

import argparse
import csv
import os
import random
import re
import socket
import subprocess
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
SETUP_RETRIES = 3

CONSISTENCY_LEVELS = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
}

SURVIVING_NODES = {
    "cassandra1": 9042,
    "cassandra2": 9043,
}

TARGET_FAILED_NODE = "cassandra3"
TARGET_FAILED_PORT = 9044

# 5 representative validation configurations: (CL_W1, CL_R)
VALIDATION_CONFIGS = [
    ("ONE", "ONE"),
    ("ONE", "QUORUM"),
    ("QUORUM", "ONE"),
    ("QUORUM", "QUORUM"),
    ("ALL", "ONE"),
]

# Full 9 configurations matrix: (CL_W1, CL_R)
ALL_CONFIGS = [
    ("ONE", "ONE"),
    ("ONE", "QUORUM"),
    ("ONE", "ALL"),
    ("QUORUM", "ONE"),
    ("QUORUM", "QUORUM"),
    ("QUORUM", "ALL"),
    ("ALL", "ONE"),
    ("ALL", "QUORUM"),
    ("ALL", "ALL"),
]


# ============================================================
# DOCKER & CLUSTER HEALTH HELPERS
# ============================================================

def run_cmd(cmd_list, timeout=15):
    """Execute a system command and return stdout string."""
    res = subprocess.run(cmd_list, capture_output=True, text=True, timeout=timeout)
    return res.stdout.strip(), res.stderr.strip(), res.returncode


def is_port_open(port, host="127.0.0.1", timeout=2.0):
    """Check whether a local TCP port is open."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def get_nodetool_statuses():
    """
    Query nodetool status from cassandra1.
    Returns a dict mapping IP address -> status string ('UN', 'DN', etc.).
    """
    stdout, stderr, code = run_cmd(["docker", "exec", "cassandra1", "nodetool", "status"], timeout=15)
    if code != 0:
        return {}
    statuses = {}
    pattern = re.compile(r"^([UD][NLJM])\s+([0-9.]+)", re.MULTILINE)
    for match in pattern.finditer(stdout):
        status, ip = match.groups()
        statuses[ip] = status
    return statuses


def stop_node(node_name=TARGET_FAILED_NODE):
    """Stop the specified Cassandra container."""
    run_cmd(["docker", "stop", "-t", "2", node_name], timeout=15)


def start_node(node_name=TARGET_FAILED_NODE):
    """Start the specified Cassandra container."""
    run_cmd(["docker", "start", node_name], timeout=15)


def wait_for_node_down(node_name=TARGET_FAILED_NODE, timeout=20):
    """
    Wait until target container is stopped and gossiped as down.
    Returns True if stopped, False on timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        stdout, _, _ = run_cmd(["docker", "inspect", "-f", "{{.State.Running}}", node_name], timeout=5)
        if stdout == "false":
            # Give surviving nodes a brief moment to update gossip state
            time.sleep(2.0)
            return True
        time.sleep(1.0)
    return False


def check_schema_and_cluster_status():
    """
    Query nodetool describecluster from cassandra1.
    Verifies that:
    1. nodetool describecluster succeeds (returncode 0).
    2. 'Unreachable: 0' is reported.
    3. Exactly one schema version is reported under 'Schema versions:'.
    4. 'Live: 3' is reported in cluster stats.
    """
    stdout, stderr, code = run_cmd(["docker", "exec", "cassandra1", "nodetool", "describecluster"], timeout=10)
    if code != 0:
        return False, f"nodetool describecluster failed with code {code}: {stderr}"

    if "Unreachable: 0" not in stdout:
        return False, "Non-zero unreachable nodes detected in describecluster"

    if "Live: 3" not in stdout:
        return False, "Fewer than 3 live nodes detected in describecluster"

    # Match lines indented under 'Schema versions:' formatted as '<uuid>: [<ips>]'
    schema_section_match = re.search(
        r"Schema versions:\s*\n((?:\s+[0-9a-fA-F-]+:\s*\[[^\]]+\]\s*\n?)+)",
        stdout,
    )
    if not schema_section_match:
        return False, "Could not parse Schema versions section from describecluster"

    schema_lines = [line.strip() for line in schema_section_match.group(1).strip().splitlines() if line.strip()]
    if len(schema_lines) != 1:
        return False, f"Multiple schema versions detected ({len(schema_lines)} versions): {schema_lines}"

    return True, f"Schema agreed: {schema_lines[0]}"


def wait_for_cluster_health(surviving_clusters=None, timeout=60):
    """
    Wait until all 3 nodes report UN in nodetool status, native CQL ports
    (9042, 9043, 9044) are open, and nodetool describecluster confirms exactly
    one schema version across all 3 live nodes with 0 unreachable nodes.
    Raises RuntimeError if health is not restored within timeout.
    """
    deadline = time.time() + timeout
    last_reason = ""
    while time.time() < deadline:
        statuses = get_nodetool_statuses()
        un_count = sum(1 for s in statuses.values() if s == "UN")
        cql_ports_open = all(is_port_open(port) for port in [9042, 9043, 9044])

        if un_count == 3 and cql_ports_open:
            agreed, reason = check_schema_and_cluster_status()
            last_reason = reason
            if agreed:
                # Stabilization buffer for gossip settling and hinted handoffs
                time.sleep(3.0)
                return True
        else:
            last_reason = f"un_count={un_count}/3, cql_ports_open={cql_ports_open}"
        time.sleep(2.0)

    # Health check failed
    current_statuses = get_nodetool_statuses()
    raise RuntimeError(
        f"Cluster failed to recover to 3 x UN within {timeout}s. "
        f"Current statuses: {current_statuses}. Reason: {last_reason}"
    )


# ============================================================
# EXPERIMENT EXECUTION
# ============================================================

def init_surviving_sessions():
    """Connect to surviving nodes cassandra1 (9042) and cassandra2 (9043)."""
    sessions = {}
    clusters = {}
    print("=" * 70)
    print("CONNECTING TO SURVIVING CASSANDRA NODES")
    print("=" * 70)
    for node_name, port in SURVIVING_NODES.items():
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


def run_experiment(trials=3, configs=None, dependent_write_cl_name="QUORUM", recovery_timeout=60):
    """
    Run WFR node-failure experiment with per-trial failure/recovery lifecycle.
    """
    if configs is None:
        configs = VALIDATION_CONFIGS

    dep_write_cl = CONSISTENCY_LEVELS[dependent_write_cl_name]
    surviving_clusters, surviving_sessions = init_surviving_sessions()
    surviving_node_names = list(surviving_sessions.keys())

    # Pre-compiled statements
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
        consistency_level=ConsistencyLevel.QUORUM,
    )

    trial_records = []
    probe_records = []

    print("\n" + "=" * 70)
    print(f"STARTING WFR NODE-FAILURE EXPERIMENT")
    print(f"Target Failed Node: {TARGET_FAILED_NODE} (port {TARGET_FAILED_PORT})")
    print(f"Trials per Config : {trials}")
    print(f"Dependent Write CL: {dependent_write_cl_name}")
    print(f"Configurations    : {len(configs)}")
    print("=" * 70)

    try:
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
                key = f"wfr_fail_{w1_name.lower()}_{r_name.lower()}_{trial}_{unique_id}"

                # ----------------------------------------------------
                # PHASE 1: Pre-trial Health Verification & Baseline
                # ----------------------------------------------------
                try:
                    wait_for_cluster_health(surviving_clusters, timeout=recovery_timeout)
                except RuntimeError as he:
                    print(f"  Trial {trial:2d} | FATAL HEALTH ERROR before baseline: {he}")
                    raise

                # Step 0: Written with CL=ALL while all three nodes are healthy,
                # ensuring that the baseline write is acknowledged by all three replicas
                # before the failure is injected.
                setup_success = False
                setup_attempts = 0
                setup_error = ""

                while not setup_success and setup_attempts < SETUP_RETRIES:
                    setup_attempts += 1
                    setup_node = random.choice(surviving_node_names)
                    try:
                        surviving_sessions[setup_node].execute(
                            setup_stmt,
                            (key, "v1_init_parent_0", 1, "setup"),
                        )
                        setup_success = True
                    except Exception as e:
                        setup_error = f"{type(e).__name__}: {str(e)}"
                        time.sleep(1.0)

                if not setup_success:
                    trial_records.append({
                        "trial": trial,
                        "property": "WRITES_FOLLOW_READS",
                        "scenario": "node_failure",
                        "failed_node": TARGET_FAILED_NODE,
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

                # ----------------------------------------------------
                # PHASE 2: Inject Node Failure
                # ----------------------------------------------------
                stop_node(TARGET_FAILED_NODE)
                if not wait_for_node_down(TARGET_FAILED_NODE, timeout=20):
                    print(f"  Trial {trial:2d} | WARNING: Node {TARGET_FAILED_NODE} down verification timed out.")

                # ----------------------------------------------------
                # PHASE 3: Degraded State Operations (Nodes 1 & 2)
                # ----------------------------------------------------
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
                classification = "INCONCLUSIVE"
                error_msg = ""

                try:
                    # Step 1: Prerequisite Write W1 (Writer A -> v2)
                    w1_node = random.choice(surviving_node_names)
                    try:
                        surviving_sessions[w1_node].execute(
                            w1_stmt,
                            (key, "v2_writerA_parent_1", 2, "writer_A"),
                        )
                        w1_completed = True
                    except Exception as we:
                        # Write W1 failed (e.g. UNAVAILABLE_ALL or Timeout)
                        err_cls = type(we).__name__
                        if w1_name == "ALL":
                            error_msg = f"UNAVAILABLE_ALL: {err_cls}: {str(we)}"
                        else:
                            error_msg = f"{err_cls}: {str(we)}"
                        classification = "OPERATION_FAILURE"
                        raise

                    # Step 2: Client B Read (reads key at CL_R)
                    r_node = random.choice(surviving_node_names)
                    try:
                        r_row = surviving_sessions[r_node].execute(read_stmt, (key,)).one()
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
                    except Exception as re_err:
                        err_cls = type(re_err).__name__
                        if r_name == "ALL":
                            error_msg = f"UNAVAILABLE_ALL: {err_cls}: {str(re_err)}"
                        else:
                            error_msg = f"{err_cls}: {str(re_err)}"
                        classification = "OPERATION_FAILURE"
                        raise

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

                    w2_node = random.choice(surviving_node_names)
                    try:
                        surviving_sessions[w2_node].execute(
                            w2_stmt,
                            (key, dep_value, dep_version, "client_B"),
                        )
                        w2_completed = True
                    except Exception as w2e:
                        err_cls = type(w2e).__name__
                        if dependent_write_cl_name == "ALL":
                            error_msg = f"UNAVAILABLE_ALL: {err_cls}: {str(w2e)}"
                        else:
                            error_msg = f"{err_cls}: {str(w2e)}"
                        classification = "OPERATION_FAILURE"
                        raise

                    # Step 4: Degraded Diagnostic Probe (CL=ONE on surviving nodes)
                    for p_node in surviving_node_names:
                        p_row = surviving_sessions[p_node].execute(probe_stmt, (key,)).one()
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

                except Exception:
                    # Operation failure occurred during degraded execution
                    pass

                finally:
                    # ----------------------------------------------------
                    # PHASE 4: Recover Node & Verify Health
                    # ----------------------------------------------------
                    start_node(TARGET_FAILED_NODE)
                    try:
                        wait_for_cluster_health(surviving_clusters, timeout=recovery_timeout)
                    except RuntimeError as re_fail:
                        print(f"  Trial {trial:2d} | FATAL HEALTH ERROR during recovery: {re_fail}")
                        raise

                # ----------------------------------------------------
                # PHASE 5: Post-Recovery Reconciliation Probe
                # ----------------------------------------------------
                if classification != "OPERATION_FAILURE":
                    try:
                        rec_node = random.choice(surviving_node_names)
                        rec_row = surviving_sessions[rec_node].execute(reconcile_stmt, (key,)).one()
                        if rec_row is not None:
                            final_version = rec_row.version
                            final_value = rec_row.value
                            final_client = rec_row.client

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
                                classification = "STALE_READ_BRANCH_CONVERGED"
                        else:
                            classification = "INCONCLUSIVE"
                    except Exception as fe:
                        classification = "OPERATION_FAILURE"
                        error_msg = f"RECONCILIATION_ERROR: {type(fe).__name__}: {str(fe)}"

                trial_records.append({
                    "trial": trial,
                    "property": "WRITES_FOLLOW_READS",
                    "scenario": "node_failure",
                    "failed_node": TARGET_FAILED_NODE,
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
                    f"Result: {classification} {f'({error_msg})' if error_msg else ''}"
                )

    finally:
        # Guarantee cassandra3 is started before shutdown
        start_node(TARGET_FAILED_NODE)
        for c in surviving_clusters.values():
            c.shutdown()
        print("\nSurviving cluster connections closed.")

    # ----------------------------------------------------
    # Save Results
    # ----------------------------------------------------
    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(os.path.dirname(script_dir), "results")
    os.makedirs(results_dir, exist_ok=True)

    trials_csv_path = os.path.join(results_dir, "node_failure.csv")
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
                "stale_read_branch_converged": 0,
                "operation_failures": 0,
                "unavailable_all_failures": 0,
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
        elif cls == "STALE_READ_BRANCH_CONVERGED":
            s["stale_read_branch_converged"] += 1
        elif cls == "OPERATION_FAILURE":
            s["operation_failures"] += 1
            if "UNAVAILABLE_ALL" in r.get("error", ""):
                s["unavailable_all_failures"] += 1
        elif cls == "SETUP_ERROR":
            s["setup_errors"] += 1

    summary_rows = list(summary_dict.values())
    summary_csv_path = os.path.join(results_dir, "node_failure_summary.csv")
    with open(summary_csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
        writer.writeheader()
        writer.writerows(summary_rows)
    print(f"Summary saved to: {summary_csv_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WFR Node Failure Consistency Experiment")
    parser.add_argument("--trials", type=int, default=3, help="Number of trials per configuration (default 3)")
    parser.add_argument("--w2_cl", type=str, default="QUORUM", choices=["ONE", "QUORUM", "ALL"], help="Consistency level for dependent write W2 (default QUORUM)")
    parser.add_argument("--all-configs", action="store_true", help="Run full 9 configurations matrix instead of 5 validation configurations")
    parser.add_argument("--timeout", type=int, default=60, help="Node recovery timeout in seconds (default 60)")
    args = parser.parse_args()

    selected_configs = ALL_CONFIGS if args.all_configs else VALIDATION_CONFIGS
    run_experiment(
        trials=args.trials,
        configs=selected_configs,
        dependent_write_cl_name=args.w2_cl,
        recovery_timeout=args.timeout,
    )
