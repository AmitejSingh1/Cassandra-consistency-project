"""
Writes-Follow-Reads (WFR) Consistency Experiment - Network Partition Scenario
Evaluates causal lineage preservation, cross-partition stale reads, LWW lost
updates, and availability boundaries under a symmetric 2-1 network partition
in Apache Cassandra 4.1.12.

Methodology:
- Topology:
  * Majority Component (M): cassandra1 (172.18.0.3), cassandra2 (172.18.0.4)
  * Minority Component (I): cassandra3 (172.18.0.2)
- Isolation:
  * Linux kernel blackhole routing (ip route add/del blackhole) blocks inter-node
    traffic (ports 7000/7001) bidirectionally between {cassandra1, cassandra2} and cassandra3.
  * cassandra1 <-> cassandra2 communication remains intact.
  * Host client CQL access (localhost:9042..9044) remains fully operational.
- Causal Sequence (Per-Trial Lifecycle):
  1. Pre-trial verification: 3 x UN, CQL ports open, schema agreed.
  2. Step 0 (Baseline Setup): Written with CL=ALL while all 3 nodes are healthy,
     ensuring all 3 replicas acknowledge v1 before partition injection.
  3. Inject Partition: Apply blackhole routes.
  4. Degraded Causal Operations (Step 1 W1 -> Step 2 Read -> Step 3 W2 -> Step 4 Probes).
  5. Heal Partition: Remove blackhole routes in guaranteed finally block.
  6. Post-Healing Health Check: Verify describecluster (Live=3, Unreachable=0, schema agreed).
  7. Step 5 (Final Reconciliation): Query key at CL=QUORUM across healed ring.
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
# EXPERIMENT SETTINGS & NETWORK TOPOLOGY
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
    "cassandra1": {"ip": "172.18.0.3", "port": 9042, "comp": "M"},
    "cassandra2": {"ip": "172.18.0.4", "port": 9043, "comp": "M"},
    "cassandra3": {"ip": "172.18.0.2", "port": 9044, "comp": "I"},
}

MAJORITY_NODES = ["cassandra1", "cassandra2"]
MINORITY_NODES = ["cassandra3"]

# P1 - P9 Coordinator-Routing & Causal Path Matrix
PATHS = {
    "P1": {
        "description": "Cross-Partition (I -> M, W1=ONE, R=ONE)",
        "w1_comp": "I", "w1_cl": "ONE",
        "read_comp": "M", "read_cl": "ONE",
        "w2_comp": "M", "w2_cl": "QUORUM",
    },
    "P2": {
        "description": "Cross-Partition (I -> M, W1=ONE, R=QUORUM)",
        "w1_comp": "I", "w1_cl": "ONE",
        "read_comp": "M", "read_cl": "QUORUM",
        "w2_comp": "M", "w2_cl": "QUORUM",
    },
    "P3": {
        "description": "Cross-Partition (M -> I, W1=QUORUM, R=ONE)",
        "w1_comp": "M", "w1_cl": "QUORUM",
        "read_comp": "I", "read_cl": "ONE",
        "w2_comp": "M", "w2_cl": "QUORUM",
    },
    "P4": {
        "description": "Intra-Majority (M -> M, W1=QUORUM, R=QUORUM)",
        "w1_comp": "M", "w1_cl": "QUORUM",
        "read_comp": "M", "read_cl": "QUORUM",
        "w2_comp": "M", "w2_cl": "QUORUM",
    },
    "P5": {
        "description": "Intra-Majority (M -> M, W1=ONE, R=ONE)",
        "w1_comp": "M", "w1_cl": "ONE",
        "read_comp": "M", "read_cl": "ONE",
        "w2_comp": "M", "w2_cl": "QUORUM",
    },
    "P6": {
        "description": "Availability Boundary (M, W1=ALL)",
        "w1_comp": "M", "w1_cl": "ALL",
        "read_comp": "M", "read_cl": "ONE",
        "w2_comp": "M", "w2_cl": "QUORUM",
    },
    "P7": {
        "description": "Availability Boundary (M, Read=ALL)",
        "w1_comp": "M", "w1_cl": "QUORUM",
        "read_comp": "M", "read_cl": "ALL",
        "w2_comp": "M", "w2_cl": "QUORUM",
    },
    "P8": {
        "description": "Minority Quorum Boundary (I, W1=QUORUM)",
        "w1_comp": "I", "w1_cl": "QUORUM",
        "read_comp": "M", "read_cl": "ONE",
        "w2_comp": "M", "w2_cl": "QUORUM",
    },
    "P9": {
        "description": "Minority Causal Trap (I, W2=QUORUM)",
        "w1_comp": "M", "w1_cl": "QUORUM",
        "read_comp": "I", "read_cl": "ONE",
        "w2_comp": "I", "w2_cl": "QUORUM",
    },
}

VALIDATION_PATHS = ["P1", "P2", "P3", "P4", "P6"]
ALL_PATH_KEYS = ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9"]


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
    # Isolate cassandra1 from cassandra3 (172.18.0.2)
    run_cmd(["docker", "exec", "cassandra1", "ip", "route", "replace", "blackhole", "172.18.0.2"])
    # Isolate cassandra2 from cassandra3 (172.18.0.2)
    run_cmd(["docker", "exec", "cassandra2", "ip", "route", "replace", "blackhole", "172.18.0.2"])
    # Isolate cassandra3 from cassandra1 (172.18.0.3) and cassandra2 (172.18.0.4)
    run_cmd(["docker", "exec", "cassandra3", "ip", "route", "replace", "blackhole", "172.18.0.3"])
    run_cmd(["docker", "exec", "cassandra3", "ip", "route", "replace", "blackhole", "172.18.0.4"])


def remove_partition():
    """Remove all blackhole routes, restoring full peer-to-peer connectivity."""
    run_cmd(["docker", "exec", "cassandra1", "ip", "route", "del", "blackhole", "172.18.0.2"])
    run_cmd(["docker", "exec", "cassandra2", "ip", "route", "del", "blackhole", "172.18.0.2"])
    run_cmd(["docker", "exec", "cassandra3", "ip", "route", "del", "blackhole", "172.18.0.3"])
    run_cmd(["docker", "exec", "cassandra3", "ip", "route", "del", "blackhole", "172.18.0.4"])


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
                time.sleep(2.0)
                return True
        else:
            last_reason = f"un_count={un_count}/3, cql_ports_open={cql_ports_open}"
        time.sleep(2.0)

    current_statuses = get_nodetool_statuses()
    raise RuntimeError(
        f"Cluster failed health verification within {timeout}s. "
        f"Statuses: {current_statuses}. Reason: {last_reason}"
    )


# ============================================================
# EXPERIMENT EXECUTION & COORDINATOR ROUTING
# ============================================================

def init_cluster_sessions():
    """Connect to each exposed Cassandra node using pinned local policy."""
    sessions = {}
    clusters = {}
    print("=" * 70)
    print("CONNECTING TO CASSANDRA NODES")
    print("=" * 70)
    for node_name, info in NODE_INFO.items():
        port = info["port"]
        cluster = Cluster(
            ["127.0.0.1"],
            port=port,
            load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
            connect_timeout=10.0,
        )
        session = cluster.connect(KEYSPACE)
        clusters[node_name] = cluster
        sessions[node_name] = session
        print(f"  {node_name} ({info['comp']}): localhost:{port} connected")
    return clusters, sessions


def select_coordinator(component):
    """Select coordinator node based on component ('M' for Majority, 'I' for Isolated)."""
    if component == "M":
        return random.choice(MAJORITY_NODES)
    elif component == "I":
        return random.choice(MINORITY_NODES)
    else:
        raise ValueError(f"Unknown component: {component}")


def parse_failure_mechanism(exception, requested_cl):
    """
    Extract the exact failure mechanism from a Cassandra driver exception:
    - UNAVAILABLE_ALL
    - UNAVAILABLE_QUORUM
    - WRITE_TIMEOUT
    - READ_TIMEOUT
    - OPERATION_TIMEOUT
    """
    exc_type = type(exception).__name__
    exc_str = str(exception)

    if "Unavailable" in exc_type or "Unavailable" in exc_str:
        if requested_cl == "ALL" or "'consistency': 'ALL'" in exc_str:
            return "UNAVAILABLE_ALL"
        elif requested_cl == "QUORUM" or "'consistency': 'QUORUM'" in exc_str:
            return "UNAVAILABLE_QUORUM"
        else:
            return f"UNAVAILABLE_{requested_cl}"
    elif "WriteTimeout" in exc_type or "WriteTimeout" in exc_str:
        return "WRITE_TIMEOUT"
    elif "ReadTimeout" in exc_type or "ReadTimeout" in exc_str:
        return "READ_TIMEOUT"
    elif "OperationTimedOut" in exc_type or "TimedOut" in exc_str:
        return "OPERATION_TIMEOUT"
    else:
        return exc_type


def run_experiment(trials=3, path_keys=None, recovery_timeout=60):
    """
    Execute WFR network partition experiment across specified paths.
    """
    if path_keys is None:
        path_keys = VALIDATION_PATHS

    # Startup cleanup: Ensure no lingering blackhole routes from previous runs
    print("\nEnsuring clean initial network state (removing any stale routes)...")
    remove_partition()

    clusters, sessions = init_cluster_sessions()

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
    print(f"STARTING WFR NETWORK-PARTITION EXPERIMENT")
    print(f"Paths to evaluate : {len(path_keys)} ({', '.join(path_keys)})")
    print(f"Trials per path   : {trials}")
    print("=" * 70)

    try:
        for p_id in path_keys:
            path_def = PATHS[p_id]
            w1_cl_name = path_def["w1_cl"]
            read_cl_name = path_def["read_cl"]
            w2_cl_name = path_def["w2_cl"]

            w1_cl = CONSISTENCY_LEVELS[w1_cl_name]
            read_cl = CONSISTENCY_LEVELS[read_cl_name]
            w2_cl = CONSISTENCY_LEVELS[w2_cl_name]

            print(f"\n---> Path {p_id}: {path_def['description']}")
            print(f"     Routing: W1({path_def['w1_comp']}, {w1_cl_name}) -> "
                  f"Read({path_def['read_comp']}, {read_cl_name}) -> "
                  f"W2({path_def['w2_comp']}, {w2_cl_name})")

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
                consistency_level=read_cl,
            )

            w2_stmt = SimpleStatement(
                f"""
                INSERT INTO {TABLE} (key, value, version, client, updated_at)
                VALUES (%s, %s, %s, %s, toTimestamp(now()))
                """,
                consistency_level=w2_cl,
            )

            for trial in range(1, trials + 1):
                unique_id = f"{time.time_ns()}_{random.randint(1000, 9999)}"
                key = f"wfr_part_{p_id.lower()}_{trial}_{unique_id}"

                # ----------------------------------------------------
                # PHASE 1: Pre-trial Health Verification & Baseline Setup
                # ----------------------------------------------------
                try:
                    wait_for_cluster_health(timeout=recovery_timeout)
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
                    setup_node = random.choice(MAJORITY_NODES)
                    try:
                        sessions[setup_node].execute(
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
                        "scenario": "network_partition",
                        "path_id": p_id,
                        "path_description": path_def["description"],
                        "w1_comp": path_def["w1_comp"],
                        "w1_node": None,
                        "w1_cl": w1_cl_name,
                        "read_comp": path_def["read_comp"],
                        "read_node": None,
                        "read_cl": read_cl_name,
                        "w2_comp": path_def["w2_comp"],
                        "w2_node": None,
                        "w2_cl": w2_cl_name,
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
                        "failure_mechanism": "NONE",
                        "classification": "SETUP_ERROR",
                        "error": setup_error,
                    })
                    print(f"  Trial {trial:2d} | SETUP_ERROR: {setup_error}")
                    continue

                # ----------------------------------------------------
                # PHASE 2: Inject Network Partition
                # ----------------------------------------------------
                inject_partition()
                time.sleep(1.0)  # Brief settling window for routing table

                # ----------------------------------------------------
                # PHASE 3: Degraded Causal Operations
                # ----------------------------------------------------
                w1_node = select_coordinator(path_def["w1_comp"])
                read_node = select_coordinator(path_def["read_comp"])
                w2_node = select_coordinator(path_def["w2_comp"])

                w1_completed = False
                observed_version = None
                observed_parent = None
                read_type = "NONE"
                w2_completed = False
                dep_version = None
                dep_parent = None
                final_version = None
                final_value = None
                final_client = None
                classification = "INCONCLUSIVE"
                failure_mechanism = "NONE"
                error_msg = ""

                try:
                    # Step 1: Write W1
                    try:
                        sessions[w1_node].execute(
                            w1_stmt,
                            (key, "v2_writerA_parent_1", 2, "writer_A"),
                        )
                        w1_completed = True
                    except Exception as we:
                        failure_mechanism = parse_failure_mechanism(we, w1_cl_name)
                        error_msg = f"{failure_mechanism}: {type(we).__name__}: {str(we)}"
                        classification = "OPERATION_FAILURE"
                        raise

                    # Step 2: Read R
                    try:
                        r_row = sessions[read_node].execute(read_stmt, (key,)).one()
                        if r_row is not None:
                            observed_version = r_row.version
                            observed_value = r_row.value
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
                        failure_mechanism = parse_failure_mechanism(re_err, read_cl_name)
                        error_msg = f"{failure_mechanism}: {type(re_err).__name__}: {str(re_err)}"
                        classification = "OPERATION_FAILURE"
                        raise

                    # Step 3: Dependent Write W2
                    if observed_version is not None:
                        dep_version = observed_version + 1
                        dep_parent = observed_version
                        dep_value = f"v{dep_version}_clientB_parent_{dep_parent}"
                    else:
                        dep_version = 2
                        dep_parent = 0
                        dep_value = "v2_clientB_parent_0"

                    try:
                        sessions[w2_node].execute(
                            w2_stmt,
                            (key, dep_value, dep_version, "client_B"),
                        )
                        w2_completed = True
                    except Exception as w2e:
                        failure_mechanism = parse_failure_mechanism(w2e, w2_cl_name)
                        error_msg = f"{failure_mechanism}: {type(w2e).__name__}: {str(w2e)}"
                        classification = "OPERATION_FAILURE"
                        raise

                    # Step 4: Degraded Diagnostic Probes (CL=ONE on each node)
                    for p_node in NODE_INFO.keys():
                        try:
                            p_row = sessions[p_node].execute(probe_stmt, (key,)).one()
                            p_ver = p_row.version if p_row else None
                            p_val = p_row.value if p_row else None
                            probe_records.append({
                                "trial": trial,
                                "path_id": p_id,
                                "probe_node": p_node,
                                "observed_version": p_ver,
                                "observed_value": p_val,
                            })
                        except Exception:
                            pass

                except Exception:
                    # Operation failure occurred during degraded execution
                    pass

                finally:
                    # ----------------------------------------------------
                    # PHASE 4: Heal Partition & Verify Health (Guaranteed)
                    # ----------------------------------------------------
                    remove_partition()
                    try:
                        wait_for_cluster_health(timeout=recovery_timeout)
                    except RuntimeError as re_fail:
                        print(f"  Trial {trial:2d} | FATAL HEALTH ERROR during healing: {re_fail}")
                        raise

                # ----------------------------------------------------
                # PHASE 5: Post-Healing Reconciliation Probe
                # ----------------------------------------------------
                if classification != "OPERATION_FAILURE":
                    try:
                        rec_node = random.choice(MAJORITY_NODES)
                        rec_row = sessions[rec_node].execute(reconcile_stmt, (key,)).one()
                        if rec_row is not None:
                            final_version = rec_row.version
                            final_value = rec_row.value
                            final_client = rec_row.client

                        # Classification Logic
                        if read_type == "FRESH_READ":
                            # Fresh read of v2 -> issued v3 (parent 2)
                            if final_version == 3 and final_client == "client_B":
                                classification = "CAUSAL_LINEAGE_PRESERVED"
                            else:
                                classification = "CAUSAL_LINEAGE_NOT_OBSERVED"
                        elif read_type == "STALE_READ":
                            # Stale read of v1 -> issued v2 (parent 1)
                            if final_version == 2 and final_client == "client_B":
                                classification = "STALE_READ_LOST_UPDATE"
                            else:
                                classification = "STALE_READ_BRANCH_CONVERGED"
                        else:
                            classification = "INCONCLUSIVE"
                    except Exception as fe:
                        failure_mechanism = "RECONCILIATION_ERROR"
                        error_msg = f"{failure_mechanism}: {type(fe).__name__}: {str(fe)}"
                        classification = "OPERATION_FAILURE"

                trial_records.append({
                    "trial": trial,
                    "property": "WRITES_FOLLOW_READS",
                    "scenario": "network_partition",
                    "path_id": p_id,
                    "path_description": path_def["description"],
                    "w1_comp": path_def["w1_comp"],
                    "w1_node": w1_node,
                    "w1_cl": w1_cl_name,
                    "read_comp": path_def["read_comp"],
                    "read_node": read_node,
                    "read_cl": read_cl_name,
                    "w2_comp": path_def["w2_comp"],
                    "w2_node": w2_node,
                    "w2_cl": w2_cl_name,
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
                    "failure_mechanism": failure_mechanism,
                    "classification": classification,
                    "error": error_msg,
                })

                print(
                    f"  Trial {trial:2d} | Read_v={observed_version} ({read_type}) | "
                    f"W2_v={dep_version} (parent={dep_parent}) | "
                    f"Final_v={final_version} (client={final_client}) | "
                    f"Result: {classification} {f'[{failure_mechanism}]' if failure_mechanism != 'NONE' else ''}"
                )

    finally:
        # Guarantee partition routes are removed before closing
        remove_partition()
        for c in clusters.values():
            c.shutdown()
        print("\nAll cluster connections closed.")

    # ----------------------------------------------------
    # Save Results
    # ----------------------------------------------------
    script_dir = os.path.dirname(os.path.abspath(__file__))
    results_dir = os.path.join(os.path.dirname(script_dir), "results")
    os.makedirs(results_dir, exist_ok=True)

    trials_csv_path = os.path.join(results_dir, "network_partition.csv")
    with open(trials_csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=trial_records[0].keys())
        writer.writeheader()
        writer.writerows(trial_records)
    print(f"\nTrial records saved to: {trials_csv_path}")

    # Generate and Save Summary
    summary_dict = {}
    for r in trial_records:
        pid = r["path_id"]
        if pid not in summary_dict:
            summary_dict[pid] = {
                "path_id": pid,
                "description": r["path_description"],
                "w1_comp": r["w1_comp"],
                "w1_cl": r["w1_cl"],
                "read_comp": r["read_comp"],
                "read_cl": r["read_cl"],
                "w2_comp": r["w2_comp"],
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
                "unavailable_quorum_failures": 0,
                "write_timeouts": 0,
                "read_timeouts": 0,
                "setup_errors": 0,
            }
        s = summary_dict[pid]
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
            fm = r.get("failure_mechanism", "")
            if fm == "UNAVAILABLE_ALL":
                s["unavailable_all_failures"] += 1
            elif fm == "UNAVAILABLE_QUORUM":
                s["unavailable_quorum_failures"] += 1
            elif fm == "WRITE_TIMEOUT":
                s["write_timeouts"] += 1
            elif fm == "READ_TIMEOUT":
                s["read_timeouts"] += 1
        elif cls == "SETUP_ERROR":
            s["setup_errors"] += 1

    summary_rows = list(summary_dict.values())
    summary_csv_path = os.path.join(results_dir, "network_partition_summary.csv")
    with open(summary_csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
        writer.writeheader()
        writer.writerows(summary_rows)
    print(f"Summary saved to: {summary_csv_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WFR Network Partition Consistency Experiment")
    parser.add_argument("--trials", type=int, default=3, help="Number of trials per path (default 3)")
    parser.add_argument("--all-paths", action="store_true", help="Run full 9 paths (P1-P9) instead of 5 validation paths")
    parser.add_argument("--timeout", type=int, default=60, help="Healing recovery timeout in seconds (default 60)")
    args = parser.parse_args()

    selected_paths = ALL_PATH_KEYS if args.all_paths else VALIDATION_PATHS
    run_experiment(
        trials=args.trials,
        path_keys=selected_paths,
        recovery_timeout=args.timeout,
    )
