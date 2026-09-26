"""
Monotonic Reads (MR) Consistency Experiment - Node Failure Scenario
Evaluates monotonic-read consistency, regressions (v2 -> v1), and availability
under single-node crash-stop failure in Apache Cassandra 4.1.12 (RF=3).

Methodology:
- Independent per-trial failure/recovery lifecycle:
  1. Pre-trial Health: Verify all 3 nodes (cassandra1, cassandra2, cassandra3)
     report UN in nodetool status, Live=3, Unreachable=0, and schema agreement.
  2. Baseline Setup (v1): Written with CL=ALL while all 3 nodes are healthy.
     Setup coordinator is chosen from surviving majority nodes and re-randomized
     inside the retry loop.
  3. Writer Write (v2): Writer updates key to version 2 at tested write_cl.
  4. Inject Failure: Stop target node (cassandra3); verify container down.
  5. Degraded Sequential Reads (Read 1 & Read 2):
     The same logical client issues TWO sequential reads at tested read_cl through
     coordinators independently selected from surviving nodes (cassandra1, cassandra2).
     Read 2 is executed ONLY after Read 1 completes.
  6. Recovery (finally block): Restart cassandra3; poll until all 3 nodes report
     UN, Live=3, Unreachable=0, and schema agreement is restored.
  7. Post-Recovery Reconciliation: Read key at CL=QUORUM across healed cluster.
  8. Classification:
     - PASS: read1_version <= read2_version (e.g. 1->1, 1->2, 2->2)
     - MR_VIOLATION: read1_version == 2 and read2_version == 1 (regression v2 -> v1)
     - OPERATION_FAILURE: driver exception during write or reads (e.g. UNAVAILABLE_ALL for CL=ALL read)
     - SETUP_ERROR: pre-trial baseline write failure
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

# All 9 configurations: (write_cl, read_cl)
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


def stop_node(node_name=TARGET_FAILED_NODE):
    """Stop the specified Cassandra container."""
    run_cmd(["docker", "stop", "-t", "2", node_name], timeout=15)


def start_node(node_name=TARGET_FAILED_NODE):
    """Start the specified Cassandra container."""
    run_cmd(["docker", "start", node_name], timeout=15)


def wait_for_node_down(node_name=TARGET_FAILED_NODE, timeout=20):
    """Wait until target container is stopped."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        stdout, _, _ = run_cmd(["docker", "inspect", "-f", "{{.State.Running}}", node_name], timeout=5)
        if stdout == "false":
            time.sleep(2.0)
            return True
        time.sleep(1.0)
    return False


def check_schema_and_cluster_status():
    """Query nodetool describecluster: verifies Live: 3, Unreachable: 0, schema agreement."""
    stdout, stderr, code = run_cmd(["docker", "exec", "cassandra1", "nodetool", "describecluster"], timeout=10)
    if code != 0:
        return False, f"nodetool describecluster failed with code {code}: {stderr}"

    if "Unreachable: 0" not in stdout:
        return False, "Non-zero unreachable nodes detected in describecluster"

    if "Live: 3" not in stdout:
        return False, "Fewer than 3 live nodes detected in describecluster"

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


def wait_for_cluster_health(timeout=60):
    """Wait until all 3 nodes report UN, ports open, and schema agreed."""
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
                time.sleep(3.0)
                return True
        else:
            last_reason = f"un_count={un_count}/3, cql_ports_open={cql_ports_open}"
        time.sleep(2.0)

    current_statuses = get_nodetool_statuses()
    raise RuntimeError(
        f"Cluster failed to recover to 3 x UN within {timeout}s. Current statuses: {current_statuses}. Reason: {last_reason}"
    )


# ============================================================
# DRIVER SESSIONS MANAGEMENT
# ============================================================

def init_surviving_sessions():
    """Connect persistent sessions to surviving nodes cassandra1 and cassandra2."""
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

def run_experiment(trials=20, configs=None, recovery_timeout=60):
    """
    Run MR node-failure experiment with per-trial failure/recovery lifecycle.
    """
    if configs is None:
        configs = ALL_CONFIGS

    surviving_clusters, surviving_sessions = init_surviving_sessions()
    surviving_node_names = list(surviving_sessions.keys())

    os.makedirs(RESULTS_DIR, exist_ok=True)

    raw_fields = [
        "trial",
        "key",
        "property",
        "scenario",
        "failed_node",
        "write_cl",
        "read_cl",
        "setup_coordinator",
        "write_coordinator",
        "read1_coordinator",
        "read2_coordinator",
        "same_read_coordinators",
        "initial_version",
        "written_version",
        "read1_version",
        "read2_version",
        "write_completed",
        "read1_completed",
        "read2_completed",
        "regression",
        "classification",
        "failure_mechanism",
        "reconciled_version",
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
    print("STARTING MONOTONIC READS NODE-FAILURE EXPERIMENT")
    print(f"Target Failed Node: {TARGET_FAILED_NODE} (port {TARGET_FAILED_PORT})")
    print(f"Trials per Config : {trials}")
    print(f"Configurations    : {len(configs)}")
    print("=" * 70)

    try:
        for write_cl_name, read_cl_name in configs:
            write_cl = CONSISTENCY_LEVELS[write_cl_name]
            read_cl = CONSISTENCY_LEVELS[read_cl_name]

            print(f"\n---> Configuration: WRITE={write_cl_name} | READ={read_cl_name}")

            write_stmt = SimpleStatement(
                f"""
                INSERT INTO {TABLE} (key, value, version, client, updated_at)
                VALUES (%s, %s, %s, %s, toTimestamp(now()))
                """,
                consistency_level=write_cl,
            )

            read_stmt = SimpleStatement(
                f"""
                SELECT version, value, client
                FROM {TABLE} WHERE key = %s
                """,
                consistency_level=read_cl,
            )

            for trial in range(1, trials + 1):
                unique_id = f"{time.time_ns()}_{random.randint(1000, 9999)}"
                key = f"mr_fail_{write_cl_name.lower()}_{read_cl_name.lower()}_{trial}_{unique_id}"

                # ----------------------------------------------------
                # PHASE 1: Pre-trial Health Verification
                # ----------------------------------------------------
                try:
                    wait_for_cluster_health(timeout=recovery_timeout)
                except RuntimeError as he:
                    print(f"  Trial {trial:2d} | FATAL HEALTH ERROR before baseline: {he}")
                    raise

                # Connect session to cassandra3 for healthy write phase
                c3_cluster, c3_session = connect_target_node_session()
                all_sessions = {
                    **surviving_sessions,
                    TARGET_FAILED_NODE: c3_session,
                }
                all_node_names = list(all_sessions.keys())

                # ----------------------------------------------------
                # PHASE 2: Establish Baseline State (v1, CL=ALL)
                # Coordinator is re-randomized from surviving nodes
                # inside the retry loop.
                # ----------------------------------------------------
                setup_success = False
                setup_attempts = 0
                setup_error = ""
                setup_node = None

                while not setup_success and setup_attempts < SETUP_RETRIES:
                    setup_attempts += 1
                    setup_node = random.choice(surviving_node_names)
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
                    c3_cluster.shutdown()
                    trial_records.append({
                        "trial": trial,
                        "key": key,
                        "property": "MONOTONIC_READS",
                        "scenario": "node_failure",
                        "failed_node": TARGET_FAILED_NODE,
                        "write_cl": write_cl_name,
                        "read_cl": read_cl_name,
                        "setup_coordinator": setup_node,
                        "write_coordinator": None,
                        "read1_coordinator": None,
                        "read2_coordinator": None,
                        "same_read_coordinators": False,
                        "initial_version": 1,
                        "written_version": 2,
                        "read1_version": None,
                        "read2_version": None,
                        "write_completed": False,
                        "read1_completed": False,
                        "read2_completed": False,
                        "regression": False,
                        "classification": "SETUP_ERROR",
                        "failure_mechanism": "SETUP_ERROR",
                        "reconciled_version": None,
                        "error": setup_error,
                    })
                    print(f"  Trial {trial:2d} | SETUP_ERROR: {setup_error}")
                    continue

                # ----------------------------------------------------
                # PHASE 3: Writer Write (v2) while 3 Nodes Healthy
                # Write coordinator selected from all 3 healthy nodes
                # ----------------------------------------------------
                write_completed = False
                write_node = random.choice(all_node_names)
                write_error = ""

                try:
                    all_sessions[write_node].execute(
                        write_stmt,
                        (key, "v2_new_value", 2, "writer"),
                    )
                    write_completed = True
                except Exception as we:
                    write_error = classify_failure(we, write_cl_name, op_type="WRITE")

                # Shutdown c3 session cleanly prior to stopping container
                try:
                    c3_cluster.shutdown()
                except Exception:
                    pass

                # ----------------------------------------------------
                # PHASE 4: Inject Node Failure (stop cassandra3)
                # ----------------------------------------------------
                stop_node(TARGET_FAILED_NODE)
                if not wait_for_node_down(TARGET_FAILED_NODE, timeout=20):
                    print(f"  Trial {trial:2d} | WARNING: Node {TARGET_FAILED_NODE} down verification timed out.")

                # ----------------------------------------------------
                # PHASE 5: Two Sequential Reads via Surviving Nodes
                # ----------------------------------------------------
                read1_completed = False
                read2_completed = False
                read1_version = None
                read2_version = None
                read1_node = random.choice(surviving_node_names)
                read2_node = random.choice(surviving_node_names)
                regression = False
                failure_mechanism = ""
                classification = "INCONCLUSIVE"
                error_str = ""

                try:
                    if not write_completed:
                        classification = "OPERATION_FAILURE"
                        failure_mechanism = write_error.split(":")[0]
                        error_str = write_error
                    else:
                        # Sequential Read 1
                        try:
                            row1 = surviving_sessions[read1_node].execute(read_stmt, (key,)).one()
                            read1_completed = True
                            read1_version = row1.version if row1 else None
                        except Exception as re1:
                            err_msg1 = classify_failure(re1, read_cl_name, op_type="READ")
                            classification = "OPERATION_FAILURE"
                            failure_mechanism = err_msg1.split(":")[0]
                            error_str = err_msg1

                        # Sequential Read 2 (ONLY after Read 1 completes successfully)
                        if read1_completed:
                            try:
                                row2 = surviving_sessions[read2_node].execute(read_stmt, (key,)).one()
                                read2_completed = True
                                read2_version = row2.version if row2 else None
                            except Exception as re2:
                                err_msg2 = classify_failure(re2, read_cl_name, op_type="READ")
                                classification = "OPERATION_FAILURE"
                                failure_mechanism = err_msg2.split(":")[0]
                                error_str = err_msg2

                        # Monotonic Reads Evaluation
                        if read1_completed and read2_completed:
                            if read1_version is not None and read2_version is not None:
                                if read1_version <= read2_version:
                                    classification = "PASS"
                                    regression = False
                                else:
                                    # Regression: e.g. read1 == 2, read2 == 1
                                    classification = "MR_VIOLATION"
                                    regression = True
                            else:
                                classification = "OPERATION_FAILURE"
                                failure_mechanism = "EMPTY_ROW"

                finally:
                    # ----------------------------------------------------
                    # PHASE 6: Recover Node & Verify Health
                    # ----------------------------------------------------
                    start_node(TARGET_FAILED_NODE)
                    try:
                        wait_for_cluster_health(timeout=recovery_timeout)
                    except RuntimeError as re_fail:
                        print(f"  Trial {trial:2d} | FATAL HEALTH ERROR during recovery: {re_fail}")
                        raise

                # ----------------------------------------------------
                # PHASE 7: Post-Recovery Reconciliation Probe
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

                same_coord = (read1_node == read2_node)

                record = {
                    "trial": trial,
                    "key": key,
                    "property": "MONOTONIC_READS",
                    "scenario": "node_failure",
                    "failed_node": TARGET_FAILED_NODE,
                    "write_cl": write_cl_name,
                    "read_cl": read_cl_name,
                    "setup_coordinator": setup_node,
                    "write_coordinator": write_node,
                    "read1_coordinator": read1_node,
                    "read2_coordinator": read2_node,
                    "same_read_coordinators": same_coord,
                    "initial_version": 1,
                    "written_version": 2,
                    "read1_version": read1_version,
                    "read2_version": read2_version,
                    "write_completed": write_completed,
                    "read1_completed": read1_completed,
                    "read2_completed": read2_completed,
                    "regression": regression,
                    "classification": classification,
                    "failure_mechanism": failure_mechanism,
                    "reconciled_version": reconciled_version,
                    "error": error_str,
                }
                trial_records.append(record)

                obs_seq = f"r1=v{read1_version}, r2=v{read2_version}" if (read1_completed and read2_completed) else (f"r1=v{read1_version}, r2=FAIL" if read1_completed else "r1=FAIL")
                print(
                    f"  Trial {trial:2d} | "
                    f"W_coord: {write_node:<10} | "
                    f"R1: {read1_node:<10} | "
                    f"R2: {read2_node:<10} | "
                    f"Observed: {obs_seq:<18} | "
                    f"Class: {classification:<17} | "
                    f"Mech: {failure_mechanism or 'N/A'}"
                )

    finally:
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
        "read_cl",
        "trials",
        "passes",
        "violations",
        "operation_failures",
        "setup_errors",
        "availability",
        "mr_success_rate",
        "different_coordinator_trials",
        "unavailable_all_failures",
        "unavailable_quorum_failures",
        "timeout_failures",
        "other_failures",
    ]

    summary_records = []
    for w_name, r_name in configs:
        matched = [r for r in trial_records if r["write_cl"] == w_name and r["read_cl"] == r_name]
        cnt = len(matched)
        passes = sum(1 for r in matched if r["classification"] == "PASS")
        violations = sum(1 for r in matched if r["classification"] == "MR_VIOLATION")
        op_failures = sum(1 for r in matched if r["classification"] == "OPERATION_FAILURE")
        setup_errs = sum(1 for r in matched if r["classification"] == "SETUP_ERROR")
        diff_coord = sum(1 for r in matched if not r["same_read_coordinators"])

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
            "write_cl": w_name,
            "read_cl": r_name,
            "trials": cnt,
            "passes": passes,
            "violations": violations,
            "operation_failures": op_failures,
            "setup_errors": setup_errs,
            "availability": round(avail, 4),
            "mr_success_rate": round(success_rate, 4),
            "different_coordinator_trials": diff_coord,
            "unavailable_all_failures": unavail_all,
            "unavailable_quorum_failures": unavail_quorum,
            "timeout_failures": timeouts,
            "other_failures": other_fail,
        })

    with open(SUMMARY_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(summary_records)
    print(f"[OK] Summary results written to {SUMMARY_CSV_PATH} ({len(summary_records)} configs)")


# ============================================================
# CLI ENTRY POINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Run Cassandra Monotonic Reads Node-Failure Experiment"
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=20,
        help="Number of trials per configuration (default: 20)",
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
        configs=ALL_CONFIGS,
        recovery_timeout=args.recovery_timeout,
    )


if __name__ == "__main__":
    main()
