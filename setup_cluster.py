"""
Cassandra Cluster Bootstrap and Schema Management Utility.

Provides idempotent schema creation, cluster status inspection, and safe table truncation
for the Cassandra consistency experiments.
"""

import argparse
import sys
import time

from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy

KEYSPACE = "consistency_experiment"
TABLE = "consistency_test"
DATACENTER = "datacenter1"

NODE_PORTS = {
    "cassandra1": 9042,
    "cassandra2": 9043,
    "cassandra3": 9044,
}

CREATE_KEYSPACE_CQL = f"""
CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
WITH replication = {{
    'class': 'NetworkTopologyStrategy',
    '{DATACENTER}': 3
}};
"""

CREATE_TABLE_CQL = f"""
CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (
    key text PRIMARY KEY,
    value text,
    version int,
    client text,
    updated_at timestamp
);
"""


def get_cluster_session(port=9042, keyspace=None, timeout=10.0):
    """Connect to a specific Cassandra node on localhost with pinned policy."""
    cluster = Cluster(
        ["127.0.0.1"],
        port=port,
        load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
        connect_timeout=timeout,
    )
    session = cluster.connect(keyspace)
    return cluster, session


def bootstrap():
    """Idempotently create the keyspace and table if they do not already exist."""
    print("=" * 70)
    print("BOOTSTRAPPING CASSANDRA SCHEMA (IDEMPOTENT)")
    print("=" * 70)
    print(f"Target Keyspace: {KEYSPACE} (NetworkTopologyStrategy, {DATACENTER}: 3)")
    print(f"Target Table:    {KEYSPACE}.{TABLE}")

    cluster = None
    session = None
    connected_port = None

    # Connect to primary node (cassandra1 at 9042), with fallback to other ports
    for node_name, port in NODE_PORTS.items():
        try:
            print(f"Attempting connection to {node_name} on localhost:{port}...")
            cluster, session = get_cluster_session(port=port)
            connected_port = port
            print(f"Successfully connected to {node_name} (localhost:{port}).")
            break
        except Exception as e:
            print(f"Could not connect to {node_name} (localhost:{port}): {type(e).__name__}: {e}")

    if session is None:
        print("\nERROR: Unable to connect to any Cassandra node. Is Docker running?")
        return False

    try:
        print(f"\n1. Ensuring keyspace '{KEYSPACE}' exists...")
        session.execute(CREATE_KEYSPACE_CQL)
        print(f"   Keyspace '{KEYSPACE}' verified/created.")

        print(f"2. Ensuring table '{KEYSPACE}.{TABLE}' exists...")
        session.execute(CREATE_TABLE_CQL)
        print(f"   Table '{KEYSPACE}.{TABLE}' verified/created.")

        print("\nBootstrap completed successfully.")
        return True
    except Exception as e:
        print(f"\nERROR executing schema CQL: {type(e).__name__}: {e}")
        return False
    finally:
        if cluster:
            cluster.shutdown()


def print_status():
    """Verify keyspace and table existence, and print cluster and schema information."""
    print("=" * 70)
    print("CASSANDRA CLUSTER AND SCHEMA STATUS")
    print("=" * 70)

    overall_healthy = True

    for node_name, port in NODE_PORTS.items():
        print(f"\nChecking node: {node_name} (localhost:{port})")
        cluster = None
        try:
            cluster, session = get_cluster_session(port=port)
            metadata = cluster.metadata
            print(f"  Connection:      ONLINE")
            print(f"  Cluster Name:    {metadata.cluster_name}")

            # Inspect hosts in cluster metadata
            hosts = list(metadata.all_hosts())
            print(f"  Discovered Nodes in Ring ({len(hosts)}):")
            for h in hosts:
                print(f"    - Address: {h.address} | DC: {h.datacenter} | Rack: {h.rack} | State: {'UP' if h.is_up else 'DOWN'}")

            # Verify keyspace
            if KEYSPACE in metadata.keyspaces:
                ks_meta = metadata.keyspaces[KEYSPACE]
                print(f"  Keyspace '{KEYSPACE}': FOUND")
                strat_obj = getattr(ks_meta, "replication_strategy", None)
                strategy = getattr(strat_obj, "name", type(strat_obj).__name__ if strat_obj else "N/A")
                options = getattr(strat_obj, "options", getattr(ks_meta, "strategy_options", {}))
                print(f"    Strategy: {strategy}")
                print(f"    Options:  {options}")

                # Verify table
                if TABLE in ks_meta.tables:
                    tbl_meta = ks_meta.tables[TABLE]
                    print(f"  Table '{TABLE}': FOUND")
                    print("    Columns:")
                    for col_name, col_meta in tbl_meta.columns.items():
                        print(f"      - {col_name} ({col_meta.cql_type})")

                    # Check current row count
                    session.set_keyspace(KEYSPACE)
                    row = session.execute(f"SELECT count(*) FROM {TABLE}").one()
                    row_count = row[0] if row else 0
                    print(f"    Current Row Count: {row_count}")
                else:
                    print(f"  Table '{TABLE}': MISSING")
                    overall_healthy = False
            else:
                print(f"  Keyspace '{KEYSPACE}': MISSING")
                overall_healthy = False

        except Exception as e:
            print(f"  Connection:      OFFLINE/ERROR ({type(e).__name__}: {e})")
            overall_healthy = False
        finally:
            if cluster:
                cluster.shutdown()

    print("\n" + "=" * 70)
    print(f"Overall Status: {'HEALTHY & READY' if overall_healthy else 'ISSUES DETECTED'}")
    print("=" * 70)
    return overall_healthy


def reset_table():
    """Safely truncate the experimental table."""
    print("=" * 70)
    print(f"RESETTING TABLE DATA: TRUNCATE {KEYSPACE}.{TABLE}")
    print("=" * 70)

    cluster = None
    try:
        cluster, session = get_cluster_session(port=9042, keyspace=KEYSPACE)
        session.execute(f"TRUNCATE {KEYSPACE}.{TABLE}")
        print(f"Table {KEYSPACE}.{TABLE} truncated successfully.")
        return True
    except Exception as e:
        print(f"Failed to truncate table: {type(e).__name__}: {e}")
        return False
    finally:
        if cluster:
            cluster.shutdown()


def main():
    parser = argparse.ArgumentParser(
        description="Idempotent Cassandra Cluster Bootstrap and Status Utility"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Inspect cluster nodes, ring metadata, keyspace, and table schema.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Truncate the consistency_test table to reset experimental data.",
    )
    args = parser.parse_args()

    if args.reset:
        success = reset_table()
        sys.exit(0 if success else 1)
    elif args.status:
        success = print_status()
        sys.exit(0 if success else 1)
    else:
        # Default action: idempotent bootstrap
        success = bootstrap()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
