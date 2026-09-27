"""
Monotonic Writes (MW) Consistency Experiment - Network Partition Scenario
Evaluates monotonic-write consistency, write-order preservation (W1 -> W2),
and availability boundaries under symmetric network partition in Apache Cassandra 4.1.12 (RF=3).

Topology:
  - Majority component (M): cassandra1 (172.18.0.3), cassandra2 (172.18.0.4)
  - Minority component (I): cassandra3 (172.18.0.2)
  - Blocked links:
      cassandra1 <-> cassandra3 (blackhole)
      cassandra2 <-> cassandra3 (blackhole)
  - Available link:
      cassandra1 <-> cassandra2

Methodology:
  1. Pre-trial Health: All 3 nodes report UN, Live=3, Unreachable=0, schema agreed.
  2. Baseline Setup (v1): Written at CL=ALL while cluster is healthy via majority coordinator.
  3. Inject Partition: Blackhole routes installed on all 3 nodes.
  4. Client Write 1 (v2): Issued at assigned w1_comp and w1_cl with timestamp T1.
  5. Client Write 2 (v3): Issued by the same client at assigned w2_comp and w2_cl with timestamp T2 > T1.
     Executed ONLY if Write 1 completed successfully.
  6. Partition Healing (finally block): Remove blackholes, restore full connectivity.
  7. Post-Healing Health & Reconciliation: Wait for UN x 3 and query key at CL=QUORUM.
  8. Classification:
     - PASS: w1 and w2 completed, and reconciled_version == 3 (latest write preserved).
     - MW_VIOLATION: w1 and w2 completed, but reconciled_version < 3 (earlier write overwrote later).
     - OPERATION_FAILURE: driver exception during write (e.g. WRITE_TIMEOUT, UNAVAILABLE_ALL, UNAVAILABLE_QUORUM).
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
SETUP_RETRIES = 6

CONSISTENCY_LEVELS = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
}

NODE_INFO = {
    "cassandra1": {"port": 9042, "ip": "172.18.0.3", "comp": "M"},
    "cassandra2": {"port": 9043, "ip": "172.18.0.4", "comp": "M"},
    "cassandra3": {"port": 9044, "ip": "172.18.0.2", "comp": "I"},
}

MAJORITY_NODES = ["cassandra1", "cassandra2"]
MINORITY_NODES = ["cassandra3"]

# Core partition paths for Monotonic Writes
PATHS = {
    "P1": {
        "description": "Intra-majority quorum writes (M -> M, W1=QUORUM, W2=QUORUM)",
        "w1_comp": "M", "w1_cl": "QUORUM",
        "w2_comp": "M", "w2_cl": "QUORUM",
        "expected": "PASS",
    },
    "P2": {
        "description": "Intra-majority local writes (M -> M, W1=ONE, W2=ONE)",
        "w1_comp": "M", "w1_cl": "ONE",
        "w2_comp": "M", "w2_cl": "ONE",
        "expected": "PASS",
    },
    "P3": {
        "description": "Cross-partition forward traversal (M -> I, W1=ONE, W2=ONE)",
        "w1_comp": "M", "w1_cl": "ONE",
        "w2_comp": "I", "w2_cl": "ONE",
        "expected": "PASS",
    },
    "P4": {
        "description": "Cross-partition reverse traversal (I -> M, W1=ONE, W2=ONE)",
        "w1_comp": "I", "w1_cl": "ONE",
        "w2_comp": "M", "w2_cl": "ONE",
        "expected": "PASS",
    },
    "P5": {
        "description": "Intra-minority local writes (I -> I, W1=ONE, W2=ONE)",
        "w1_comp": "I", "w1_cl": "ONE",
        "w2_comp": "I", "w2_cl": "ONE",
        "expected": "PASS",
    },
    "P6": {
        "description": "Majority write availability boundary (M -> M, W1=ALL, W2=ONE)",
        "w1_comp": "M", "w1_cl": "ALL",
        "w2_comp": "M", "w2_cl": "ONE",
        "expected": "OPERATION_FAILURE",
    },
    "P7": {
        "description": "Minority write availability boundary (I -> I, W1=QUORUM, W2=ONE)",
        "w1_comp": "I", "w1_cl": "QUORUM",
        "w2_comp": "I", "w2_cl": "ONE",
        "expected": "OPERATION_FAILURE",
    },
}

ALL_PATH_KEYS = ["P1", "P2", "P3", "P4", "P5", "P6", "P7"]

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(BASE_DIR, "results")
RAW_CSV_PATH = os.path.join(RESULTS_DIR, "network_partition.csv")
SUMMARY_CSV_PATH = os.path.join(RESULTS_DIR, "network_partition_summary.csv")


# ============================================================
# DOCKER & NETWORK PARTITION CONTROL
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


def inject_partition():
    """
    Symmetric network partition:
    - Blocks traffic between cassandra1 and cassandra3
    - Blocks traffic between cassandra2 and cassandra3
    - Keeps traffic between cassandra1 and cassandra2 intact
    """
    run_cmd(["docker", "exec", "cassandra1", "ip", "route", "replace", "blackhole", NODE_INFO["cassandra3"]["ip"]])
    run_cmd(["docker", "exec", "cassandra2", "ip", "route", "replace", "blackhole", NODE_INFO["cassandra3"]["ip"]])
    run_cmd(["docker", "exec", "cassandra3", "ip", "route", "replace", "blackhole", NODE_INFO["cassandra1"]["ip"]])
    run_cmd(["docker", "exec", "cassandra3", "ip", "route", "replace", "blackhole", NODE_INFO["cassandra2"]["ip"]])


def remove_partition():
    """Remove all blackhole routes, restoring full connectivity."""
    run_cmd(["docker", "exec", "cassandra1", "ip", "route", "del", "blackhole", NODE_INFO["cassandra3"]["ip"]])
    run_cmd(["docker", "exec", "cassandra2", "ip", "route", "del", "blackhole", NODE_INFO["cassandra3"]["ip"]])
    run_cmd(["docker", "exec", "cassandra3", "ip", "route", "del", "blackhole", NODE_INFO["cassandra1"]["ip"]])
    run_cmd(["docker", "exec", "cassandra3", "ip", "route", "del", "blackhole", NODE_INFO["cassandra2"]["ip"]])


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
        cql_ports_open = all(is_port_open(info["port"]) for info in NODE_INFO.values())

        if un_count == 3 and cql_ports_open:
            agreed, reason = check_schema_and_cluster_status()
            last_reason = reason
            if agreed:
                time.sleep(5.0)
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

def init_node_sessions():
    """Connect one dedicated session to each Cassandra node via localhost port."""
    sessions = {}
    clusters = {}
    print("=" * 70)
    print("CONNECTING TO CASSANDRA NODES")
    print("=" * 70)
    for node_name, info in NODE_INFO.items():
        cluster = Cluster(
            ["127.0.0.1"],
            port=info["port"],
            load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
            connect_timeout=10.0,
        )
        session = cluster.connect(KEYSPACE)
        clusters[node_name] = cluster
        sessions[node_name] = session
        print(f"  {node_name} ({info['comp']}-comp): localhost:{info['port']} connected")
    return clusters, sessions


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

def run_experiment(trials=20, path_keys=None, recovery_timeout=60):
    """
    Run Monotonic Writes network-partition experiment with per-trial partition/heal lifecycle.
    """
    if path_keys is None:
        path_keys = ALL_PATH_KEYS

    clusters, sessions = init_node_sessions()
    all_node_names = list(sessions.keys())

    os.makedirs(RESULTS_DIR, exist_ok=True)

    raw_fields = [
        "trial",
        "key",
        "property",
        "scenario",
        "path",
        "w1_component",
        "w2_component",
        "w1_cl",
        "w2_cl",
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
    print("STARTING MONOTONIC WRITES NETWORK-PARTITION EXPERIMENT")
    print(f"Trials per Path: {trials}")
    print(f"Paths to Run   : {path_keys}")
    print("=" * 70)

    try:
        # Pre-experiment cleanup: remove partition and verify health
        remove_partition()
        wait_for_cluster_health(timeout=recovery_timeout)

        for path_key in path_keys:
            pinfo = PATHS[path_key]
            w1_comp = pinfo["w1_comp"]
            w2_comp = pinfo["w2_comp"]
            w1_cl_name = pinfo["w1_cl"]
            w2_cl_name = pinfo["w2_cl"]
            w1_cl = CONSISTENCY_LEVELS[w1_cl_name]
            w2_cl = CONSISTENCY_LEVELS[w2_cl_name]

            print(f"\n---> PATH {path_key}: {pinfo['description']}")

            w1_stmt = SimpleStatement(
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
                consistency_level=w1_cl,
            )

            w2_stmt = SimpleStatement(
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
                consistency_level=w2_cl,
            )

            for trial in range(1, trials + 1):
                unique_id = f"{time.time_ns()}_{random.randint(1000, 9999)}"
                key = f"mw_part_{path_key.lower()}_{trial}_{unique_id}"

                # ----------------------------------------------------
                # PHASE 1: Pre-trial Health Verification
                # ----------------------------------------------------
                try:
                    wait_for_cluster_health(timeout=recovery_timeout)
                except RuntimeError as he:
                    print(f"  Trial {trial:2d} | FATAL HEALTH ERROR before baseline: {he}")
                    raise

                # ----------------------------------------------------
                # PHASE 2: Baseline Setup (v1, CL=ALL)
                # Coordinator is re-randomized from majority nodes
                # inside the retry loop.
                # ----------------------------------------------------
                setup_success = False
                setup_attempts = 0
                setup_error = ""
                setup_node = None

                while not setup_success and setup_attempts < SETUP_RETRIES:
                    setup_attempts += 1
                    setup_node = random.choice(MAJORITY_NODES)
                    try:
                        sessions[setup_node].execute(
                            setup_stmt,
                            (key, "v1_init", 1, "setup"),
                        )
                        setup_success = True
                    except Exception as e:
                        setup_error = f"{type(e).__name__}: {str(e)}"
                        time.sleep(1.5 * setup_attempts)

                if not setup_success:
                    trial_records.append({
                        "trial": trial,
                        "key": key,
                        "property": "MONOTONIC_WRITES",
                        "scenario": "network_partition",
                        "path": path_key,
                        "w1_component": w1_comp,
                        "w2_component": w2_comp,
                        "w1_cl": w1_cl_name,
                        "w2_cl": w2_cl_name,
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
                # PHASE 3: Inject Symmetric Network Partition
                # ----------------------------------------------------
                inject_partition()
                time.sleep(1.0)

                # Select coordinators according to path definition
                if w1_comp == "M":
                    w1_node = random.choice(MAJORITY_NODES)
                else:
                    w1_node = random.choice(MINORITY_NODES)

                if w2_comp == "M":
                    w2_node = random.choice(MAJORITY_NODES)
                else:
                    w2_node = random.choice(MINORITY_NODES)

                base_timestamp = time.time_ns() // 1000
                t1 = base_timestamp + 1000
                t2 = base_timestamp + 2000

                w1_completed = False
                w2_completed = False
                regression = False
                classification = "INCONCLUSIVE"
                failure_mechanism = ""
                error_str = ""

                try:
                    # ------------------------------------------------
                    # PHASE 4: Write 1 (v2) under Partition
                    # ------------------------------------------------
                    try:
                        sessions[w1_node].execute(
                            w1_stmt,
                            (t1, "v2_val", 2, "client1", key),
                        )
                        w1_completed = True
                    except Exception as we1:
                        fail_msg = classify_failure(we1, w1_cl_name, op_type="WRITE")
                        classification = "OPERATION_FAILURE"
                        failure_mechanism = fail_msg.split(":")[0]
                        error_str = fail_msg

                    # ------------------------------------------------
                    # PHASE 5: Write 2 (v3) under Partition
                    # Executed ONLY if Write 1 completed
                    # ------------------------------------------------
                    if w1_completed:
                        try:
                            sessions[w2_node].execute(
                                w2_stmt,
                                (t2, "v3_val", 3, "client1", key),
                            )
                            w2_completed = True
                        except Exception as we2:
                            fail_msg = classify_failure(we2, w2_cl_name, op_type="WRITE")
                            classification = "OPERATION_FAILURE"
                            failure_mechanism = fail_msg.split(":")[0]
                            error_str = fail_msg

                finally:
                    # ------------------------------------------------
                    # PHASE 6: Heal Partition & Recover Health
                    # ------------------------------------------------
                    remove_partition()
                    try:
                        wait_for_cluster_health(timeout=recovery_timeout)
                    except RuntimeError as re_fail:
                        print(f"  Trial {trial:2d} | FATAL HEALTH ERROR during recovery: {re_fail}")
                        raise

                # ----------------------------------------------------
                # PHASE 7: Post-Recovery Reconciliation Probe
                # Read at CL=QUORUM across healed cluster
                # ----------------------------------------------------
                reconciled_version = None
                if classification != "SETUP_ERROR":
                    try:
                        rec_node = random.choice(all_node_names)
                        rec_row = sessions[rec_node].execute(reconcile_stmt, (key,)).one()
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
                    "scenario": "network_partition",
                    "path": path_key,
                    "w1_component": w1_comp,
                    "w2_component": w2_comp,
                    "w1_cl": w1_cl_name,
                    "w2_cl": w2_cl_name,
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
                    f"W2_coord: {w2_node:<10} | "
                    f"Observed: {obs_str:<26} | "
                    f"Class: {classification:<17} | "
                    f"Mech: {failure_mechanism or 'N/A'}"
                )

    finally:
        remove_partition()
        for c in clusters.values():
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
        "path",
        "description",
        "w1_component",
        "w2_component",
        "w1_cl",
        "w2_cl",
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
    for path_key in path_keys:
        pinfo = PATHS[path_key]
        matched = [r for r in trial_records if r["path"] == path_key]
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
            "path": path_key,
            "description": pinfo["description"],
            "w1_component": pinfo["w1_comp"],
            "w2_component": pinfo["w2_comp"],
            "w1_cl": pinfo["w1_cl"],
            "w2_cl": pinfo["w2_cl"],
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
    print(f"[OK] Summary results written to {SUMMARY_CSV_PATH} ({len(summary_records)} paths)")


# ============================================================
# CLI ENTRY POINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Run Cassandra Monotonic Writes Network-Partition Experiment"
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=20,
        help="Number of trials per path (default: 20)",
    )
    parser.add_argument(
        "--paths",
        nargs="+",
        default=ALL_PATH_KEYS,
        help="List of path keys to run (default: P1-P7)",
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
        path_keys=args.paths,
        recovery_timeout=args.recovery_timeout,
    )


if __name__ == "__main__":
    main()
