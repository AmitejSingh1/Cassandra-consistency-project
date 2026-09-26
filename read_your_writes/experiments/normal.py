import csv
import os
import random
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.policies import WhiteListRoundRobinPolicy


# ============================================================
# EXPERIMENT SETTINGS
# ============================================================

TRIALS = 200

KEYSPACE = "consistency_experiment"


CONSISTENCY_LEVELS = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
}


# ============================================================
# CASSANDRA NODES
#
# Each Docker container is exposed separately:
#
# cassandra1 -> localhost:9042
# cassandra2 -> localhost:9043
# cassandra3 -> localhost:9044
#
# We maintain one pinned session per node.
# ============================================================

NODE_PORTS = {
    "cassandra1": 9042,
    "cassandra2": 9043,
    "cassandra3": 9044,
}


sessions = {}
clusters = {}


print("=" * 65)
print("CONNECTING TO CASSANDRA NODES")
print("=" * 65)


for node_name, port in NODE_PORTS.items():

    cluster = Cluster(
        ["127.0.0.1"],
        port=port,
        load_balancing_policy=WhiteListRoundRobinPolicy(
            ["127.0.0.1"]
        )
    )

    session = cluster.connect(KEYSPACE)

    clusters[node_name] = cluster
    sessions[node_name] = session

    print(
        f"{node_name}: localhost:{port} connected"
    )


# ============================================================
# BASELINE QUERY
#
# Every trial begins with version 1.
#
# CL=ALL means Cassandra must acknowledge the baseline write
# from all three replicas before the actual experiment begins.
# ============================================================

setup_query = SimpleStatement(
    """
    INSERT INTO consistency_test
        (key, value, version, client, updated_at)
    VALUES
        (%s, %s, %s, %s, toTimestamp(now()))
    """,
    consistency_level=ConsistencyLevel.ALL
)


results = []


# ============================================================
# EXPERIMENT
#
# Test all:
#
#       3 WRITE CL
#       x
#       3 READ CL
#
#       = 9 configurations
#
# For each trial:
#
# 1. Establish v1 using CL=ALL.
# 2. Randomly choose WRITE coordinator.
# 3. Write v2.
# 4. As soon as successful write returns,
#    randomly choose READ coordinator.
# 5. Immediately read.
# 6. v2 = PASS
#    v1 = FAIL
# ============================================================


for write_cl_name, write_cl in CONSISTENCY_LEVELS.items():

    for read_cl_name, read_cl in CONSISTENCY_LEVELS.items():

        print()
        print("=" * 65)

        print(
            f"WRITE={write_cl_name} | "
            f"READ={read_cl_name}"
        )

        print("=" * 65)


        write_query = SimpleStatement(
            """
            INSERT INTO consistency_test
                (key, value, version, client, updated_at)
            VALUES
                (%s, %s, %s, %s, toTimestamp(now()))
            """,
            consistency_level=write_cl
        )


        read_query = SimpleStatement(
            """
            SELECT version
            FROM consistency_test
            WHERE key = %s
            """,
            consistency_level=read_cl
        )


        for trial in range(1, TRIALS + 1):

            key = (
                f"ryw_normal_"
                f"{write_cl_name.lower()}_"
                f"{read_cl_name.lower()}_"
                f"{trial}_"
                f"{time.time_ns()}"
            )


            # ------------------------------------------------
            # Independently choose coordinators
            # ------------------------------------------------

            setup_node = random.choice(
                list(sessions.keys())
            )

            write_node = random.choice(
                list(sessions.keys())
            )

            read_node = random.choice(
                list(sessions.keys())
            )


            write_completed = False
            read_completed = False

            observed_version = None

            result = "ERROR"
            error = ""


            try:

                # ============================================
                # STEP 1
                # Establish OLD value = v1
                # ============================================

                sessions[setup_node].execute(
                    setup_query,
                    (
                        key,
                        "old_value",
                        1,
                        "setup"
                    )
                )


                # ============================================
                # STEP 2
                # Client writes NEW value = v2
                #
                # NO sleep after this operation.
                # ============================================

                sessions[write_node].execute(
                    write_query,
                    (
                        key,
                        "new_value",
                        2,
                        "client1"
                    )
                )

                write_completed = True


                # ============================================
                # STEP 3
                # IMMEDIATELY READ
                #
                # Read coordinator chosen independently.
                # ============================================

                row = sessions[read_node].execute(
                    read_query,
                    (key,)
                ).one()

                read_completed = True


                if row is not None:
                    observed_version = row.version


                # ============================================
                # STEP 4
                # TEST READ-YOUR-WRITES
                # ============================================

                if observed_version == 2:
                    result = "PASS"

                else:
                    result = "FAIL"


            except Exception as e:

                error = (
                    f"{type(e).__name__}: {str(e)}"
                )


            # ================================================
            # STORE RAW TRIAL
            # ================================================

            results.append({

                "trial": trial,

                "property": "RYW",

                "scenario": "normal",

                "network_delay":
                    "netem delay 25ms 25ms",

                "write_cl": write_cl_name,

                "read_cl": read_cl_name,

                "setup_coordinator": setup_node,

                "write_coordinator": write_node,

                "read_coordinator": read_node,

                "same_write_read_coordinator":
                    write_node == read_node,

                "initial_version": 1,

                "written_version": 2,

                "observed_version":
                    observed_version,

                "write_completed":
                    write_completed,

                "read_completed":
                    read_completed,

                "result": result,

                "error": error
            })


            print(
                f"Trial {trial:3} | "
                f"W-node={write_node} | "
                f"R-node={read_node} | "
                f"Observed={observed_version} | "
                f"{result}"
            )


# ============================================================
# OUTPUT PATHS
# ============================================================

experiment_directory = os.path.dirname(
    os.path.abspath(__file__)
)

ryw_directory = os.path.dirname(
    experiment_directory
)

results_directory = os.path.join(
    ryw_directory,
    "results"
)

os.makedirs(
    results_directory,
    exist_ok=True
)


raw_output_path = os.path.join(
    results_directory,
    "normal.csv"
)


summary_output_path = os.path.join(
    results_directory,
    "normal_summary.csv"
)


# ============================================================
# SAVE RAW RESULTS
# ============================================================

with open(
    raw_output_path,
    "w",
    newline=""
) as file:

    writer = csv.DictWriter(
        file,
        fieldnames=results[0].keys()
    )

    writer.writeheader()

    writer.writerows(results)


# ============================================================
# GENERATE SUMMARY
# ============================================================

summary_rows = []


print()
print()
print("=" * 75)
print("READ-YOUR-WRITES - NORMAL OPERATION")
print("=" * 75)


for write_cl_name in CONSISTENCY_LEVELS:

    for read_cl_name in CONSISTENCY_LEVELS:


        subset = [

            row

            for row in results

            if (
                row["write_cl"]
                == write_cl_name

                and

                row["read_cl"]
                == read_cl_name
            )
        ]


        passes = sum(
            row["result"] == "PASS"
            for row in subset
        )


        failures = sum(
            row["result"] == "FAIL"
            for row in subset
        )


        errors = sum(
            row["result"] == "ERROR"
            for row in subset
        )


        completed_sequences = (
            passes + failures
        )


        availability = (
            completed_sequences
            / len(subset)
        )


        if completed_sequences > 0:

            success_rate = (
                passes
                / completed_sequences
            )

        else:

            success_rate = None


        different_coordinator_trials = sum(

            row[
                "same_write_read_coordinator"
            ] is False

            for row in subset
        )


        summary_row = {

            "write_cl":
                write_cl_name,

            "read_cl":
                read_cl_name,

            "trials":
                len(subset),

            "passes":
                passes,

            "violations":
                failures,

            "errors":
                errors,

            "availability":
                availability,

            "ryw_success_rate":
                success_rate,

            "different_coordinator_trials":
                different_coordinator_trials
        }


        summary_rows.append(
            summary_row
        )


        print()

        print(
            f"WRITE={write_cl_name}, "
            f"READ={read_cl_name}"
        )

        print("-" * 50)

        print(
            f"Trials:                    "
            f"{len(subset)}"
        )

        print(
            f"RYW passes:                "
            f"{passes}"
        )

        print(
            f"RYW violations:            "
            f"{failures}"
        )

        print(
            f"Errors:                    "
            f"{errors}"
        )

        print(
            f"Availability:              "
            f"{availability:.4f}"
        )


        if success_rate is not None:

            print(
                f"RYW success rate:          "
                f"{success_rate:.4f}"
            )

        else:

            print(
                "RYW success rate:          N/A"
            )


        print(
            f"Different coordinators:    "
            f"{different_coordinator_trials}"
            f"/{len(subset)}"
        )


# ============================================================
# SAVE SUMMARY
# ============================================================

with open(
    summary_output_path,
    "w",
    newline=""
) as file:

    writer = csv.DictWriter(
        file,
        fieldnames=summary_rows[0].keys()
    )

    writer.writeheader()

    writer.writerows(
        summary_rows
    )


# ============================================================
# CLEANUP
# ============================================================

for cluster in clusters.values():
    cluster.shutdown()


print()
print("=" * 75)

print(
    f"Raw results saved to:\n"
    f"{raw_output_path}"
)

print()

print(
    f"Summary saved to:\n"
    f"{summary_output_path}"
)

print("=" * 75)