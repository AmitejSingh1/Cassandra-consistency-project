"""
Monotonic Writes (MW) Consistency Experiment - Node Failure Scenario
Evaluates monotonic-write consistency, write-order preservation (W1 -> W2),
and availability boundaries under single-node crash-stop failure in Apache Cassandra 4.1.12 (RF=3).

Topology:
  - Cluster: cassandra1 (9042), cassandra2 (9043), cassandra3 (9044)
  - Target failed node: cassandra3 (stopped during degraded write phase)
  - Surviving nodes: cassandra1, cassandra2

Methodology:
  1. Pre-trial Health: All 3 nodes report UN, Live=3, Unreachable=0, schema agreed.
  2. Baseline Setup (v1): Written at CL=ALL while cluster is healthy via surviving coordinator.
  3. Client Write 1 (v2): Issued at tested write_cl across healthy cluster with timestamp T1.
     Coordinator selected from all 3 healthy nodes (cassandra1, cassandra2, cassandra3).
  4. Inject Node Failure: Stop cassandra3 ('docker stop -t 2 cassandra3'); verify container down.
  5. Degraded Client Write 2 (v3): Issued by the same client at tested write_cl with timestamp T2 > T1.
     Coordinator selected from surviving nodes (cassandra1, cassandra2).
     If write_cl == ALL: Cassandra cannot satisfy ALL (requires 3, only 2 alive) -> OPERATION_FAILURE.
  6. Node Recovery (finally block): Restart cassandra3; poll until UN x 3, Live=3, Unreachable=0,
     and schema agreement is restored.
  7. Post-Recovery Reconciliation Probe: Read key at CL=QUORUM across healed cluster.
  8. Classification:
     - PASS: w1 and w2 completed, and reconciled_version == 3 (latest write preserved).
     - MW_VIOLATION: w1 and w2 completed, but reconciled_version < 3 (earlier write overwrote later).
     - OPERATION_FAILURE: driver exception during write (e.g. UNAVAILABLE_ALL or WRITE_TIMEOUT for CL=ALL).
     - SETUP_ERROR: pre-trial baseline write failure.
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

WRITE_LEVELS = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
}

ALL_WRITE_LEVELS = ["ONE", "QUORUM", "ALL"]

SURVIVING_NODES = {
    "cassandra1": 9042,
    "cassandra2": 9043,
}

TARGET_FAILED_NODE = "cassandra3"
TARGET_FAILED_PORT = 9044

ALL_NODES = {
    "cassandra1": 9042,
    "cassandra2": 9043,
    "cassandra3": 9044,
}

MAJORITY_NODES = ["cassandra1", "cassandra2"]

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(BASE_DIR, "results")
RAW_CSV_PATH = os.path.join(RESULTS_DIR, "node_failure.csv")
SUMMARY_CSV_PATH = os.path.join(RESULTS_DIR, "node_failure_summary.csv")


# ============================================================
# DOCKER & CLUSTER HEALTH HELPERS
# ============================================================

def run_cmd(cmd_list, timeout=15):
    """Execute a system command and return stdout, stderr, returncode."""
    res = subprocess.run(cmd_list, capture_output=True, text=True, timeout=timeout)
    return res.stdout.strip(), res.stderr.strip(), res.returncode


def is_port_open(port, host="127.0.0.1", timeout=2.0):
    """Check whether a local TCP port is open."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def is_node_running(node_name=TARGET_FAILED_NODE):
    """Check if the docker container is running."""
    stdout, stderr, code = run_cmd(["docker", "inspect", "-f", "{{.State.Running}}", node_name])
    return code == 0 and stdout.lower() == "true"


def stop_node(node_name=TARGET_FAILED_NODE, timeout=2):
    """Stop target Cassandra container."""
    run_cmd(["docker", "stop", "-t", str(timeout), node_name], timeout=30)
    time.sleep(1.0)


def start_node(node_name=TARGET_FAILED_NODE):
    """Start target Cassandra container."""
    run_cmd(["docker", "start", node_name], timeout=30)
    time.sleep(2.0)


def get_nodetool_statuses():
    """Query nodetool status from cassandra1."""
    stdout, stderr, code = run_cmd(["docker", "exec", "cassandra1", "nodetool", "status"], timeout=15)
    if code != 0:
        return {}
    statuses = {}
    pattern = re.compile(r"^([UD][NLJM])\s+([0-9.]+)", re.MULTILINE)
    for match in pattern.finditer(stdout):
        status, ip = match.groups()
        statuses[ip] = status
    return statuses


def check_schema_and_cluster_status():
    """Query nodetool describecluster: verifies Live: 3, Unreachable: 0, schema agreement."""
    stdout, stderr, code = run_cmd(["docker", "exec", "cassandra1", "nodetool", "describecluster"], timeout=10)
    if code != 0:
        return False, f"nodetool describecluster failed: {stderr}"

    if "Unreachable: 0" not in stdout:
        return False, "Non-zero unreachable nodes detected in describecluster"

    if "Live: 3" not in stdout:
        return False, "Fewer than 3 live nodes detected in describecluster"

    schema_match = re.search(r"Schema versions:\s*\n((?:\s+[0-9a-fA-F-]+:\s*\[[^\]]+\]\s*\n?)+)", stdout)
    if not schema_match:
        return False, "Could not parse Schema versions section"

    schema_lines = [line.strip() for line in schema_match.group(1).strip().splitlines() if line.strip()]
    if len(schema_lines) != 1:
        return False, f"Multiple schema versions detected ({len(schema_lines)} versions): {schema_lines}"

    return True, f"Schema agreed: {schema_lines[0]}"


def wait_for_cluster_health(timeout=60):
    """Wait until all 3 nodes report UN, ports open, and schema agreed."""
    deadline = time.time() + timeout
    last_reason = ""
    while time.time() < deadline:
        statuses = get_nodetool_statuses()
        un_count = sum(1 for s in statuses.values() if s == "UN")
        cql_ports_open = all(is_port_open(port) for port in ALL_NODES.values())

        if un_count == 3 and cql_ports_open:
            agreed, reason = check_schema_and_cluster_status()
            last_reason = reason
            if agreed:
                time.sleep(3.0)
                return True
        else:
            last_reason = f"un_count={un_count}/3, cql_ports_open={cql_ports_open}"
        time.sleep(2.0)

    current_statuses = get_nodetool_statuses()
    raise RuntimeError(
        f"Cluster failed to recover within {timeout}s. Current statuses: {current_statuses}. Reason: {last_reason}"
    )


# ============================================================
# DRIVER SESSIONS MANAGEMENT
# ============================================================

def init_surviving_sessions():
    """Connect dedicated sessions to surviving nodes (cassandra1: 9042, cassandra2: 9043)."""
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


def connect_target_node_session():
    """Connect dedicated single-node session to cassandra3 (9044)."""
    cluster = Cluster(
        ["127.0.0.1"],
        port=TARGET_FAILED_PORT,
        load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
        connect_timeout=10.0,
    )
    session = cluster.connect(KEYSPACE)
    return cluster, session


def classify_failure(exc, cl_name, op_type="OPERATION"):
    """Extract standard failure classification from driver exception."""
    err_cls = type(exc).__name__
    err_str = str(exc)
    if "Unavailable" in err_cls or "unavailable" in err_str.lower():
        if cl_name == "ALL":
            return f"UNAVAILABLE_ALL: {err_cls}: {err_str}"
        elif cl_name == "QUORUM":
            return f"UNAVAILABLE_QUORUM: {err_cls}: {err_str}"
        else:
            return f"UNAVAILABLE_{cl_name}: {err_cls}: {err_str}"
    elif "WriteTimeout" in err_cls or "write timeout" in err_str.lower():
        return f"WRITE_TIMEOUT: {err_cls}: {err_str}"
    elif "ReadTimeout" in err_cls or "read timeout" in err_str.lower():
        return f"READ_TIMEOUT: {err_cls}: {err_str}"
    elif "OperationTimedOut" in err_cls or "timed out" in err_str.lower():
        return f"OPERATION_TIMEOUT: {err_cls}: {err_str}"
    else:
        return f"{err_cls}: {err_str}"


# ============================================================
# EXPERIMENT EXECUTION
# ============================================================

def run_experiment(trials=60, write_levels=None, recovery_timeout=60):
    """
    Run Monotonic Writes node-failure experiment.
    Per-trial crash-stop lifecycle:
      - Establish baseline v1 (CL=ALL) while healthy
      - Write 1 (v2) with timestamp T1 while healthy
      - Stop cassandra3
      - Write 2 (v3) with timestamp T2 > T1 while cassandra3 is stopped
      - Restart cassandra3 (in finally block)
      - Read reconciled version at CL=QUORUM
    """
    if write_levels is None:
        write_levels = ALL_WRITE_LEVELS

    surviving_clusters, surviving_sessions = init_surviving_sessions()
    surviving_node_names = list(SURVIVING_NODES.keys())

    os.makedirs(RESULTS_DIR, exist_ok=True)

    raw_fields = [
        "trial",
        "key",
        "property",
        "scenario",
        "failed_node",
        "write_cl",
        "setup_coordinator",
        "w1_coordinator",
        "w2_coordinator",
        "w1_timestamp",
        "w2_timestamp",
        "initial_version",
        "w1_version",
        "w2_version",
        "w1_completed",
        "w2_completed",
        "reconciled_version",
        "regression",
        "classification",
        "failure_mechanism",
        "error",
    ]

    setup_stmt = SimpleStatement(
        f"""
        INSERT INTO {TABLE} (key, value, version, client, updated_at)
        VALUES (%s, %s, %s, %s, toTimestamp(now()))
        """,
        consistency_level=ConsistencyLevel.ALL,
    )

    reconcile_stmt = SimpleStatement(
        f"""
        SELECT version, value, client
        FROM {TABLE} WHERE key = %s
        """,
        consistency_level=ConsistencyLevel.QUORUM,
    )

    trial_records = []

    print("\n" + "=" * 70)
    print("STARTING MONOTONIC WRITES NODE-FAILURE EXPERIMENT")
    print(f"Trials per Write CL: {trials}")
    print(f"Write CLs to Run   : {write_levels}")
    print(f"Target Failed Node : {TARGET_FAILED_NODE}")
    print("=" * 70)

    try:
        # Pre-experiment check: Ensure target node is running and cluster healthy
        if not is_node_running(TARGET_FAILED_NODE):
            start_node(TARGET_FAILED_NODE)
        wait_for_cluster_health(timeout=recovery_timeout)

        for write_cl_name in write_levels:
            write_cl = WRITE_LEVELS[write_cl_name]

            print(f"\n---> WRITE CONSISTENCY: {write_cl_name}")

            write_query = SimpleStatement(
                f"""
                UPDATE {TABLE}
                USING TIMESTAMP %s
                SET
                    value = %s,
                    version = %s,
                    client = %s,
                    updated_at = toTimestamp(now())
                WHERE key = %s
                """,
                consistency_level=write_cl,
            )

            for trial in range(1, trials + 1):
                unique_id = f"{time.time_ns()}_{random.randint(1000, 9999)}"
                key = f"mw_fail_{write_cl_name.lower()}_{trial}_{unique_id}"

                # ----------------------------------------------------
                # PHASE 1: Pre-trial Health Verification
                # ----------------------------------------------------
                try:
                    if not is_node_running(TARGET_FAILED_NODE):
                        start_node(TARGET_FAILED_NODE)
                    wait_for_cluster_health(timeout=recovery_timeout)
                except RuntimeError as he:
                    print(f"  Trial {trial:2d} | FATAL HEALTH ERROR before baseline: {he}")
                    raise

                # Connect dedicated session to cassandra3 for healthy phase
                c3_cluster, c3_session = connect_target_node_session()
                all_sessions = {
                    **surviving_sessions,
                    TARGET_FAILED_NODE: c3_session,
                }
                all_node_names = list(all_sessions.keys())

                # ----------------------------------------------------
                # PHASE 2: Baseline Setup (v1, CL=ALL)
                # Coordinator is re-randomized from surviving majority
                # nodes inside the retry loop.
                # ----------------------------------------------------
                setup_success = False
                setup_attempts = 0
                setup_error = ""
                setup_node = None

                while not setup_success and setup_attempts < SETUP_RETRIES:
                    setup_attempts += 1
                    setup_node = random.choice(MAJORITY_NODES)
                    try:
                        surviving_sessions[setup_node].execute(
                            setup_stmt,
                            (key, "v1_init", 1, "setup"),
                        )
                        setup_success = True
                    except Exception as e:
                        setup_error = f"{type(e).__name__}: {str(e)}"
                        time.sleep(1.0)

                if not setup_success:
                    try:
                        c3_cluster.shutdown()
                    except Exception:
                        pass
                    trial_records.append({
                        "trial": trial,
                        "key": key,
                        "property": "MONOTONIC_WRITES",
                        "scenario": "node_failure",
                        "failed_node": TARGET_FAILED_NODE,
                        "write_cl": write_cl_name,
                        "setup_coordinator": setup_node,
                        "w1_coordinator": None,
                        "w2_coordinator": None,
                        "w1_timestamp": None,
                        "w2_timestamp": None,
                        "initial_version": 1,
                        "w1_version": 2,
                        "w2_version": 3,
                        "w1_completed": False,
                        "w2_completed": False,
                        "reconciled_version": None,
                        "regression": False,
                        "classification": "SETUP_ERROR",
                        "failure_mechanism": "SETUP_ERROR",
                        "error": setup_error,
                    })
                    print(f"  Trial {trial:2d} | SETUP_ERROR: {setup_error}")
                    continue

                # ----------------------------------------------------
                # PHASE 3: Write 1 (v2) while Cluster is Healthy
                # Base timestamp in microseconds.
                # ----------------------------------------------------
                base_timestamp = time.time_ns() // 1000
                t1 = base_timestamp + 1000
                t2 = base_timestamp + 2000

                w1_node = random.choice(all_node_names)
                w1_completed = False
                w2_completed = False
                w2_node = None
                regression = False
                classification = "INCONCLUSIVE"
                failure_mechanism = ""
                error_str = ""

                try:
                    # Write 1 (v2)
                    try:
                        all_sessions[w1_node].execute(
                            write_query,
                            (t1, "v2_val", 2, "client1", key),
                        )
                        w1_completed = True
                    except Exception as we1:
                        fail_msg = classify_failure(we1, write_cl_name, op_type="WRITE")
                        classification = "OPERATION_FAILURE"
                        failure_mechanism = fail_msg.split(":")[0]
                        error_str = fail_msg

                    # ------------------------------------------------
                    # PHASE 4: Inject Node Failure (Stop cassandra3)
                    # ------------------------------------------------
                    if w1_completed:
                        try:
                            c3_cluster.shutdown()
                        except Exception:
                            pass
                        stop_node(TARGET_FAILED_NODE, timeout=2)
                        if is_node_running(TARGET_FAILED_NODE):
                            raise RuntimeError(f"Failed to stop container {TARGET_FAILED_NODE}!")

                        # --------------------------------------------
                        # PHASE 5: Write 2 (v3) while cassandra3 is Down
                        # Coordinator chosen from surviving nodes.
                        # Timestamp T2 > T1.
                        # --------------------------------------------
                        w2_node = random.choice(surviving_node_names)
                        try:
                            surviving_sessions[w2_node].execute(
                                write_query,
                                (t2, "v3_val", 3, "client1", key),
                            )
                            w2_completed = True
                        except Exception as we2:
                            fail_msg = classify_failure(we2, write_cl_name, op_type="WRITE")
                            classification = "OPERATION_FAILURE"
                            failure_mechanism = fail_msg.split(":")[0]
                            error_str = fail_msg

                finally:
                    try:
                        c3_cluster.shutdown()
                    except Exception:
                        pass
                    # ------------------------------------------------
                    # PHASE 6: Restore Failed Node (always in finally)
                    # ------------------------------------------------
                    if not is_node_running(TARGET_FAILED_NODE):
                        start_node(TARGET_FAILED_NODE)
                    try:
                        wait_for_cluster_health(timeout=recovery_timeout)
                    except RuntimeError as re_fail:
                        print(f"  Trial {trial:2d} | FATAL HEALTH ERROR during recovery: {re_fail}")
                        raise

                # ----------------------------------------------------
                # PHASE 7: Post-Recovery Reconciliation Probe
                # Read at CL=QUORUM across healed cluster via surviving session
                # ----------------------------------------------------
                reconciled_version = None
                if classification != "SETUP_ERROR":
                    try:
                        rec_node = random.choice(surviving_node_names)
                        rec_row = surviving_sessions[rec_node].execute(reconcile_stmt, (key,)).one()
                        if rec_row is not None:
                            reconciled_version = rec_row.version
                    except Exception as rec_err:
                        print(f"  Trial {trial:2d} | Reconciliation probe error: {rec_err}")

                # ----------------------------------------------------
                # PHASE 8: Monotonic Writes Evaluation
                # ----------------------------------------------------
                if w1_completed and w2_completed:
                    if reconciled_version == 3:
                        classification = "PASS"
                        regression = False
                    elif reconciled_version is not None and reconciled_version < 3:
                        # Version regression: e.g. reconciled_version == 2 (w1 overwrote w2)
                        classification = "MW_VIOLATION"
                        regression = True
                    else:
                        classification = "OPERATION_FAILURE"
                        failure_mechanism = "EMPTY_ROW"

                record = {
                    "trial": trial,
                    "key": key,
                    "property": "MONOTONIC_WRITES",
                    "scenario": "node_failure",
                    "failed_node": TARGET_FAILED_NODE,
                    "write_cl": write_cl_name,
                    "setup_coordinator": setup_node,
                    "w1_coordinator": w1_node,
                    "w2_coordinator": w2_node,
                    "w1_timestamp": t1,
                    "w2_timestamp": t2,
                    "initial_version": 1,
                    "w1_version": 2,
                    "w2_version": 3,
                    "w1_completed": w1_completed,
                    "w2_completed": w2_completed,
                    "reconciled_version": reconciled_version,
                    "regression": regression,
                    "classification": classification,
                    "failure_mechanism": failure_mechanism,
                    "error": error_str,
                }
                trial_records.append(record)

                obs_str = f"w1=v2, w2=v3, rec=v{reconciled_version}" if (w1_completed and w2_completed) else (f"w1=v2, w2=FAIL, rec=v{reconciled_version}" if w1_completed else "w1=FAIL")
                print(
                    f"  Trial {trial:2d} | "
                    f"W1_coord: {w1_node:<10} | "
                    f"W2_coord: {str(w2_node):<10} | "
                    f"Observed: {obs_str:<26} | "
                    f"Class: {classification:<17} | "
                    f"Mech: {failure_mechanism or 'N/A'}"
                )

    finally:
        if not is_node_running(TARGET_FAILED_NODE):
            start_node(TARGET_FAILED_NODE)
        for c in surviving_clusters.values():
            try:
                c.shutdown()
            except Exception:
                pass

    # ============================================================
    # WRITE RAW RESULTS CSV
    # ============================================================
    with open(RAW_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=raw_fields)
        writer.writeheader()
        writer.writerows(trial_records)
    print(f"\n[OK] Raw results written to {RAW_CSV_PATH} ({len(trial_records)} rows)")

    # ============================================================
    # WRITE SUMMARY CSV
    # ============================================================
    summary_fields = [
        "write_cl",
        "trials",
        "passes",
        "violations",
        "operation_failures",
        "setup_errors",
        "availability",
        "mw_success_rate",
        "unavailable_all_failures",
        "unavailable_quorum_failures",
        "timeout_failures",
        "other_failures",
    ]

    summary_records = []
    for write_cl_name in write_levels:
        matched = [r for r in trial_records if r["write_cl"] == write_cl_name]
        cnt = len(matched)
        passes = sum(1 for r in matched if r["classification"] == "PASS")
        violations = sum(1 for r in matched if r["classification"] == "MW_VIOLATION")
        op_failures = sum(1 for r in matched if r["classification"] == "OPERATION_FAILURE")
        setup_errs = sum(1 for r in matched if r["classification"] == "SETUP_ERROR")

        unavail_all = sum(1 for r in matched if "UNAVAILABLE_ALL" in (r["failure_mechanism"] or ""))
        unavail_quorum = sum(1 for r in matched if "UNAVAILABLE_QUORUM" in (r["failure_mechanism"] or ""))
        timeouts = sum(
            1 for r in matched
            if "WRITE_TIMEOUT" in (r["failure_mechanism"] or "")
            or "READ_TIMEOUT" in (r["failure_mechanism"] or "")
            or "OPERATION_TIMEOUT" in (r["failure_mechanism"] or "")
        )
        other_fail = op_failures - (unavail_all + unavail_quorum + timeouts)

        valid_completed = passes + violations
        avail = (valid_completed / cnt) if cnt > 0 else 0.0
        success_rate = (passes / valid_completed) if valid_completed > 0 else 0.0

        summary_records.append({
            "write_cl": write_cl_name,
            "trials": cnt,
            "passes": passes,
            "violations": violations,
            "operation_failures": op_failures,
            "setup_errors": setup_errs,
            "availability": round(avail, 4),
            "mw_success_rate": round(success_rate, 4),
            "unavailable_all_failures": unavail_all,
            "unavailable_quorum_failures": unavail_quorum,
            "timeout_failures": timeouts,
            "other_failures": other_fail,
        })

    with open(SUMMARY_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(summary_records)
    print(f"[OK] Summary results written to {SUMMARY_CSV_PATH} ({len(summary_records)} write levels)")


# ============================================================
# CLI ENTRY POINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Run Cassandra Monotonic Writes Node-Failure Experiment"
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=60,
        help="Number of trials per write CL (default: 60, total 180)",
    )
    parser.add_argument(
        "--cls",
        nargs="+",
        default=ALL_WRITE_LEVELS,
        help="List of write CLs to run (default: ONE QUORUM ALL)",
    )
    parser.add_argument(
        "--recovery-timeout",
        type=int,
        default=60,
        help="Timeout in seconds for cluster health recovery (default: 60)",
    )

    args = parser.parse_args()
    run_experiment(
        trials=args.trials,
        write_levels=args.cls,
        recovery_timeout=args.recovery_timeout,
    )


if __name__ == "__main__":
    main()
