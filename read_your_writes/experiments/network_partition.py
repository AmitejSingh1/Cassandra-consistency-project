"""
Read-Your-Writes (RYW) Consistency Experiment - Network Partition Scenario
Evaluates RYW preservation, stale-read visibility, and availability boundaries
under symmetric network partition in Apache Cassandra 4.1.12 (RF=3).

Topology:
  - Majority component: cassandra1 (172.18.0.3), cassandra2 (172.18.0.4)
  - Minority component: cassandra3 (172.18.0.2)
  - Blocked links:
      cassandra1 <-> cassandra3 (blackhole)
      cassandra2 <-> cassandra3 (blackhole)
  - Available link:
      cassandra1 <-> cassandra2

Methodology:
  1. Pre-trial Health: All 3 nodes report UN, Live=3, Unreachable=0, schema agreed.
  2. Baseline Setup (v1): Written at CL=ALL while cluster is healthy.
  3. Inject Partition: Blackhole routes installed on all 3 nodes.
  4. Client Write (v2): Issued through assigned coordinator component & write_cl.
  5. Degraded RYW Read: Issued through assigned coordinator component & read_cl.
  6. Partition Healing (finally block): Remove blackholes, restore full connectivity.
  7. Post-Healing Health & Reconciliation: Wait for UN x 3 and query key at CL=QUORUM.
  8. Classification:
     - PASS: observed_version == 2
     - RYW_VIOLATION: observed_version == 1 (stale read after acknowledged write)
     - OPERATION_FAILURE: driver exception (UNAVAILABLE_ALL, UNAVAILABLE_QUORUM, WRITE_TIMEOUT, READ_TIMEOUT)
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

NODE_INFO = {
    "cassandra1": {"port": 9042, "ip": "172.18.0.3", "comp": "M"},
    "cassandra2": {"port": 9043, "ip": "172.18.0.4", "comp": "M"},
    "cassandra3": {"port": 9044, "ip": "172.18.0.2", "comp": "I"},
}

MAJORITY_NODES = ["cassandra1", "cassandra2"]
MINORITY_NODES = ["cassandra3"]

# 9 Explicit Coordinator Routing Paths for RYW
PATHS = {
    "P1": {
        "description": "Intra-majority quorum (M -> M, W=QUORUM, R=QUORUM)",
        "write_comp": "M", "write_cl": "QUORUM",
        "read_comp": "M", "read_cl": "QUORUM",
    },
    "P2": {
        "description": "Cross-partition stale read (M -> I, W=QUORUM, R=ONE)",
        "write_comp": "M", "write_cl": "QUORUM",
        "read_comp": "I", "read_cl": "ONE",
    },
    "P3": {
        "description": "Cross-partition stale read (M -> I, W=ONE, R=ONE)",
        "write_comp": "M", "write_cl": "ONE",
        "read_comp": "I", "read_cl": "ONE",
    },
    "P4": {
        "description": "Intra-minority local RYW (I -> I, W=ONE, R=ONE)",
        "write_comp": "I", "write_cl": "ONE",
        "read_comp": "I", "read_cl": "ONE",
    },
    "P5": {
        "description": "Cross-partition stale read (I -> M, W=ONE, R=ONE)",
        "write_comp": "I", "write_cl": "ONE",
        "read_comp": "M", "read_cl": "ONE",
    },
    "P6": {
        "description": "Intra-majority local RYW (M -> M, W=ONE, R=ONE)",
        "write_comp": "M", "write_cl": "ONE",
        "read_comp": "M", "read_cl": "ONE",
    },
    "P7": {
        "description": "Intra-majority quorum write / local read (M -> M, W=QUORUM, R=ONE)",
        "write_comp": "M", "write_cl": "QUORUM",
        "read_comp": "M", "read_cl": "ONE",
    },
    "P8": {
        "description": "Write availability boundary (M -> M, W=ALL, R=ONE)",
        "write_comp": "M", "write_cl": "ALL",
        "read_comp": "M", "read_cl": "ONE",
    },
    "P9": {
        "description": "Read availability boundary (M -> M, W=ONE, R=ALL)",
        "write_comp": "M", "write_cl": "ONE",
        "read_comp": "M", "read_cl": "ALL",
    },
}

VALIDATION_PATHS = ["P1", "P2", "P3", "P4", "P6"]
ALL_PATH_KEYS = ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9"]

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
    # cassandra1 <-> cassandra3
    run_cmd(["docker", "exec", "cassandra1", "ip", "route", "replace", "blackhole", NODE_INFO["cassandra3"]["ip"]])
    # cassandra2 <-> cassandra3
    run_cmd(["docker", "exec", "cassandra2", "ip", "route", "replace", "blackhole", NODE_INFO["cassandra3"]["ip"]])
    # cassandra3 <-> cassandra1, cassandra2
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
    """
    Query nodetool describecluster from cassandra1.
    Verifies Live: 3, Unreachable: 0, and exactly 1 schema version.
    """
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
    Run RYW network-partition experiment with per-trial partition/heal lifecycle.
    """
    if path_keys is None:
        path_keys = VALIDATION_PATHS

    clusters, sessions = init_node_sessions()
    all_node_names = list(sessions.keys())

    os.makedirs(RESULTS_DIR, exist_ok=True)

    raw_fields = [
        "trial",
        "key",
        "property",
        "scenario",
        "path",
        "write_component",
        "read_component",
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
    print("STARTING READ-YOUR-WRITES NETWORK-PARTITION EXPERIMENT")
    print(f"Trials per Path: {trials}")
    print(f"Paths to Run   : {path_keys}")
    print("=" * 70)

    try:
        # Startup cleanup: guarantee no leftover blackholes
        remove_partition()
        wait_for_cluster_health(timeout=recovery_timeout)

        for path_key in path_keys:
            pinfo = PATHS[path_key]
            w_comp = pinfo["write_comp"]
            r_comp = pinfo["read_comp"]
            w_cl_name = pinfo["write_cl"]
            r_cl_name = pinfo["read_cl"]
            w_cl = CONSISTENCY_LEVELS[w_cl_name]
            r_cl = CONSISTENCY_LEVELS[r_cl_name]

            print(f"\n---> PATH {path_key}: {pinfo['description']}")

            write_stmt = SimpleStatement(
                f"""
                INSERT INTO {TABLE} (key, value, version, client, updated_at)
                VALUES (%s, %s, %s, %s, toTimestamp(now()))
                """,
                consistency_level=w_cl,
            )

            read_stmt = SimpleStatement(
                f"""
                SELECT version, value, client
                FROM {TABLE} WHERE key = %s
                """,
                consistency_level=r_cl,
            )

            for trial in range(1, trials + 1):
                unique_id = f"{time.time_ns()}_{random.randint(1000, 9999)}"
                key = f"ryw_part_{path_key.lower()}_{trial}_{unique_id}"

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
                        time.sleep(1.0)

                if not setup_success:
                    trial_records.append({
                        "trial": trial,
                        "key": key,
                        "property": "RYW",
                        "scenario": "network_partition",
                        "path": path_key,
                        "write_component": w_comp,
                        "read_component": r_comp,
                        "write_cl": w_cl_name,
                        "read_cl": r_cl_name,
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
                # PHASE 3: Inject Symmetric Network Partition
                # ----------------------------------------------------
                inject_partition()
                time.sleep(1.0)  # Brief settling window for routing tables

                # Select coordinators according to path definition
                if w_comp == "M":
                    write_node = random.choice(MAJORITY_NODES)
                else:
                    write_node = random.choice(MINORITY_NODES)

                if r_comp == "M":
                    read_node = random.choice(MAJORITY_NODES)
                else:
                    read_node = random.choice(MINORITY_NODES)

                write_completed = False
                read_completed = False
                observed_version = None
                classification = "INCONCLUSIVE"
                failure_mechanism = ""
                error_str = ""

                try:
                    # ------------------------------------------------
                    # PHASE 4: Client Write (v2) under Partition
                    # ------------------------------------------------
                    try:
                        sessions[write_node].execute(
                            write_stmt,
                            (key, "v2_new_value", 2, "client1"),
                        )
                        write_completed = True
                    except Exception as we:
                        fail_msg = classify_failure(we, w_cl_name, op_type="WRITE")
                        classification = "OPERATION_FAILURE"
                        failure_mechanism = fail_msg.split(":")[0]
                        error_str = fail_msg

                    # ------------------------------------------------
                    # PHASE 5: Degraded RYW Read under Partition
                    # (Executed ONLY if write successfully completed)
                    # ------------------------------------------------
                    if write_completed:
                        try:
                            row = sessions[read_node].execute(read_stmt, (key,)).one()
                            read_completed = True
                            if row is not None:
                                observed_version = row.version
                            else:
                                observed_version = None

                            # Evaluate Read-Your-Writes consistency
                            if observed_version == 2:
                                classification = "PASS"
                            elif observed_version == 1:
                                classification = "RYW_VIOLATION"
                            else:
                                classification = "RYW_VIOLATION"

                        except Exception as re_err:
                            fail_msg = classify_failure(re_err, r_cl_name, op_type="READ")
                            classification = "OPERATION_FAILURE"
                            failure_mechanism = fail_msg.split(":")[0]
                            error_str = fail_msg

                finally:
                    # ------------------------------------------------
                    # PHASE 6: Heal Partition & Recover Cluster Health
                    # ------------------------------------------------
                    remove_partition()
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
                        rec_node = random.choice(all_node_names)
                        rec_row = sessions[rec_node].execute(reconcile_stmt, (key,)).one()
                        if rec_row is not None:
                            reconciled_version = rec_row.version
                    except Exception as rec_err:
                        print(f"  Trial {trial:2d} | Reconciliation probe error: {rec_err}")

                same_coord = (write_node == read_node)

                record = {
                    "trial": trial,
                    "key": key,
                    "property": "RYW",
                    "scenario": "network_partition",
                    "path": path_key,
                    "write_component": w_comp,
                    "read_component": r_comp,
                    "write_cl": w_cl_name,
                    "read_cl": r_cl_name,
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
        "write_component",
        "read_component",
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
        violations = sum(1 for r in matched if r["classification"] == "RYW_VIOLATION")
        op_failures = sum(1 for r in matched if r["classification"] == "OPERATION_FAILURE")
        setup_errs = sum(1 for r in matched if r["classification"] == "SETUP_ERROR")
        diff_coord = sum(1 for r in matched if not r["same_write_read_coordinator"])

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
            "write_component": pinfo["write_comp"],
            "read_component": pinfo["read_comp"],
            "write_cl": pinfo["write_cl"],
            "read_cl": pinfo["read_cl"],
            "trials": cnt,
            "passes": passes,
            "violations": violations,
            "operation_failures": op_failures,
            "setup_errors": setup_errs,
            "availability": round(avail, 4),
            "ryw_success_rate": round(success_rate, 4),
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
    print(f"[OK] Summary results written to {SUMMARY_CSV_PATH} ({len(summary_records)} paths)")


# ============================================================
# CLI ENTRY POINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Run Cassandra Read-Your-Writes Network-Partition Experiment"
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=20,
        help="Number of trials per path (default: 20)",
    )
    parser.add_argument(
        "--all-paths",
        action="store_true",
        help="Run all 9 paths (default: 5 validation paths)",
    )
    parser.add_argument(
        "--recovery-timeout",
        type=int,
        default=60,
        help="Timeout in seconds for cluster health recovery (default: 60)",
    )

    args = parser.parse_args()
    paths_to_run = ALL_PATH_KEYS if args.all_paths else VALIDATION_PATHS
    run_experiment(
        trials=args.trials,
        path_keys=paths_to_run,
        recovery_timeout=args.recovery_timeout,
    )


if __name__ == "__main__":
    main()
