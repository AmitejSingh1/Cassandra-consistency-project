"""
Read-Your-Writes (RYW) Consistency Experiment - Node Failure Scenario
Evaluates RYW preservation, stale reads, and operation/availability failures
under single-node crash-stop failure in Apache Cassandra 4.1.12 (RF=3).

Methodology:
- Independent per-trial failure/recovery lifecycle:
  1. Pre-trial Health: Verify all 3 nodes (cassandra1, cassandra2, cassandra3)
     report UN in nodetool status, Live=3, Unreachable=0, and schema agreement.
  2. Baseline Setup (v1): Written with CL=ALL while all 3 nodes are healthy.
  3. Client Write (v2): Client writes v2 at tested write_cl.
  4. Inject Failure: Stop target node (cassandra3); verify container down.
  5. Degraded RYW Read: Same client reads at tested read_cl through a surviving
     coordinator (cassandra1 or cassandra2).
  6. Recovery (finally block): Restart cassandra3; poll until all 3 nodes report
     UN, Live=3, Unreachable=0, and schema agreement is restored.
  7. Post-Recovery Reconciliation: Read key at CL=QUORUM across healed cluster.
  8. Classification:
     - PASS: observed_version == 2
     - RYW_VIOLATION: observed_version == 1 (stale read)
     - OPERATION_FAILURE: driver exception (e.g. UNAVAILABLE_ALL for CL=ALL read)
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

# 5 representative validation configurations: (write_cl, read_cl)
VALIDATION_CONFIGS = [
    ("ONE", "ONE"),
    ("ONE", "QUORUM"),
    ("ONE", "ALL"),
    ("QUORUM", "ONE"),
    ("ALL", "ONE"),
]

# Full 9 configurations matrix: (write_cl, read_cl)
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
    """
    Query nodetool status from cassandra1.
    Returns dict mapping IP address -> status string ('UN', 'DN', etc.).
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
    """Wait until target container is stopped and gossiped down."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        stdout, _, _ = run_cmd(["docker", "inspect", "-f", "{{.State.Running}}", node_name], timeout=5)
        if stdout == "false":
            time.sleep(2.0)
            return True
        time.sleep(1.0)
    return False


def check_schema_and_cluster_status():
    """
    Query nodetool describecluster from cassandra1.
    Verifies Live: 3, Unreachable: 0, and exactly 1 schema version.
    """
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
    """
    Wait until all 3 nodes report UN in nodetool status, native CQL ports
    (9042, 9043, 9044) are open, and nodetool describecluster confirms exactly
    one schema version across all 3 live nodes with 0 unreachable nodes.
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
                time.sleep(3.0)
                return True
        else:
            last_reason = f"un_count={un_count}/3, cql_ports_open={cql_ports_open}"
        time.sleep(2.0)

    current_statuses = get_nodetool_statuses()
    raise RuntimeError(
        f"Cluster failed to recover to 3 x UN within {timeout}s. "
        f"Current statuses: {current_statuses}. Reason: {last_reason}"
    )


# ============================================================
# DRIVER SESSIONS MANAGEMENT
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


def classify_failure(exc, cl_name):
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
    else:
        return f"{err_cls}: {err_str}"


# ============================================================
# EXPERIMENT EXECUTION
# ============================================================

def run_experiment(trials=20, configs=None, recovery_timeout=60):
    """
    Run RYW node-failure experiment with per-trial failure/recovery lifecycle.
    """
    if configs is None:
        configs = VALIDATION_CONFIGS

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
        "read_coordinator",
        "same_write_read_coordinator",
        "initial_version",
        "written_version",
        "observed_version",
        "write_completed",
        "read_completed",
        "classification",
        "failure_mechanism",
        "reconciled_version",
        "error",
    ]

    # Pre-compiled statements
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
    print("STARTING READ-YOUR-WRITES NODE-FAILURE EXPERIMENT")
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
                key = f"ryw_fail_{write_cl_name.lower()}_{read_cl_name.lower()}_{trial}_{unique_id}"

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
                # ----------------------------------------------------
                setup_success = False
                setup_attempts = 0
                setup_error = ""
                setup_node = random.choice(all_node_names)

                while not setup_success and setup_attempts < SETUP_RETRIES:
                    setup_attempts += 1
                    try:
                        all_sessions[setup_node].execute(
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
                        "property": "RYW",
                        "scenario": "node_failure",
                        "failed_node": TARGET_FAILED_NODE,
                        "write_cl": write_cl_name,
                        "read_cl": read_cl_name,
                        "setup_coordinator": setup_node,
                        "write_coordinator": None,
                        "read_coordinator": None,
                        "same_write_read_coordinator": False,
                        "initial_version": 1,
                        "written_version": 2,
                        "observed_version": None,
                        "write_completed": False,
                        "read_completed": False,
                        "classification": "SETUP_ERROR",
                        "failure_mechanism": "SETUP_ERROR",
                        "reconciled_version": None,
                        "error": setup_error,
                    })
                    print(f"  Trial {trial:2d} | SETUP_ERROR: {setup_error}")
                    continue

                # ----------------------------------------------------
                # PHASE 3: Client Write (v2) while 3 Nodes Healthy
                # ----------------------------------------------------
                write_completed = False
                write_node = random.choice(all_node_names)
                write_error = ""

                try:
                    all_sessions[write_node].execute(
                        write_stmt,
                        (key, "v2_new_value", 2, "client1"),
                    )
                    write_completed = True
                except Exception as we:
                    write_error = classify_failure(we, write_cl_name)

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
                # PHASE 5: Degraded RYW Read via Surviving Coordinator
                # ----------------------------------------------------
                read_completed = False
                observed_version = None
                read_node = random.choice(surviving_node_names)
                read_error = ""
                failure_mechanism = ""
                classification = "INCONCLUSIVE"
                error_str = ""

                try:
                    if not write_completed:
                        # Write failed prior to read
                        classification = "OPERATION_FAILURE"
                        failure_mechanism = write_error.split(":")[0]
                        error_str = write_error
                    else:
                        # Execute read through surviving coordinator
                        try:
                            row = surviving_sessions[read_node].execute(read_stmt, (key,)).one()
                            read_completed = True
                            if row is not None:
                                observed_version = row.version
                            else:
                                observed_version = None

                            # Classify RYW result
                            if observed_version == 2:
                                classification = "PASS"
                            elif observed_version == 1:
                                classification = "RYW_VIOLATION"
                            else:
                                classification = "RYW_VIOLATION"

                        except Exception as re_err:
                            read_error = classify_failure(re_err, read_cl_name)
                            classification = "OPERATION_FAILURE"
                            failure_mechanism = read_error.split(":")[0]
                            error_str = read_error

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

                same_coord = (write_node == read_node)

                record = {
                    "trial": trial,
                    "key": key,
                    "property": "RYW",
                    "scenario": "node_failure",
                    "failed_node": TARGET_FAILED_NODE,
                    "write_cl": write_cl_name,
                    "read_cl": read_cl_name,
                    "setup_coordinator": setup_node,
                    "write_coordinator": write_node,
                    "read_coordinator": read_node,
                    "same_write_read_coordinator": same_coord,
                    "initial_version": 1,
                    "written_version": 2,
                    "observed_version": observed_version,
                    "write_completed": write_completed,
                    "read_completed": read_completed,
                    "classification": classification,
                    "failure_mechanism": failure_mechanism,
                    "reconciled_version": reconciled_version,
                    "error": error_str,
                }
                trial_records.append(record)

                print(
                    f"  Trial {trial:2d} | "
                    f"W_coord: {write_node:<10} | "
                    f"R_coord: {read_node:<10} | "
                    f"Observed: v{str(observed_version):<4} | "
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
        "ryw_success_rate",
        "different_coordinator_trials",
        "unavailable_all_failures",
        "other_failures",
    ]

    summary_records = []
    for w_name, r_name in configs:
        matched = [r for r in trial_records if r["write_cl"] == w_name and r["read_cl"] == r_name]
        cnt = len(matched)
        passes = sum(1 for r in matched if r["classification"] == "PASS")
        violations = sum(1 for r in matched if r["classification"] == "RYW_VIOLATION")
        op_failures = sum(1 for r in matched if r["classification"] == "OPERATION_FAILURE")
        setup_errs = sum(1 for r in matched if r["classification"] == "SETUP_ERROR")
        diff_coord = sum(1 for r in matched if not r["same_write_read_coordinator"])
        unavail_all = sum(1 for r in matched if "UNAVAILABLE_ALL" in (r["failure_mechanism"] or ""))
        other_fail = op_failures - unavail_all

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
            "ryw_success_rate": round(success_rate, 4),
            "different_coordinator_trials": diff_coord,
            "unavailable_all_failures": unavail_all,
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
        description="Run Cassandra Read-Your-Writes Node-Failure Experiment"
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=20,
        help="Number of trials per configuration (default: 20)",
    )
    parser.add_argument(
        "--all-configs",
        action="store_true",
        help="Run all 9 configurations (default: 5 validation configs)",
    )
    parser.add_argument(
        "--recovery-timeout",
        type=int,
        default=60,
        help="Timeout in seconds for cluster health recovery (default: 60)",
    )

    args = parser.parse_args()
    configs_to_run = ALL_CONFIGS if args.all_configs else VALIDATION_CONFIGS
    run_experiment(
        trials=args.trials,
        configs=configs_to_run,
        recovery_timeout=args.recovery_timeout,
    )


if __name__ == "__main__":
    main()
