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
READS_PER_TRIAL = 5
SETUP_RETRIES = 3

KEYSPACE = "consistency_experiment"

NETWORK_DESCRIPTION = "netem delay 25ms 25ms"


CONSISTENCY_LEVELS = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
}


# ============================================================
# CASSANDRA NODES
#
# Each node is exposed separately so that the experiment can
# independently choose the coordinator for each operation.
# ============================================================

NODE_PORTS = {
    "cassandra1": 9042,
    "cassandra2": 9043,
    "cassandra3": 9044,
}


sessions = {}
clusters = {}


print("=" * 70)
print("CONNECTING TO CASSANDRA NODES")
print("=" * 70)


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


node_names = list(sessions.keys())


# ============================================================
# BASELINE SETUP QUERY
#
# Version 1 is established using ALL before each trial.
#
# This is experimental preparation, not part of the measured
# client history.
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


# ============================================================
# RESULT STORAGE
#
# read_results:
#     one row per read observation
#
# trial_results:
#     one row per complete trial
# ============================================================

read_results = []
trial_results = []


# ============================================================
# ALL 9 WRITE / READ CONFIGURATIONS
# ============================================================

for write_cl_name, write_cl in CONSISTENCY_LEVELS.items():

    for read_cl_name, read_cl in CONSISTENCY_LEVELS.items():

        print()
        print("=" * 70)

        print(
            f"WRITE={write_cl_name} | "
            f"READ={read_cl_name}"
        )

        print("=" * 70)


        # ----------------------------------------------------
        # Experimental write
        # ----------------------------------------------------

        write_query = SimpleStatement(
            """
            INSERT INTO consistency_test
                (key, value, version, client, updated_at)
            VALUES
                (%s, %s, %s, %s, toTimestamp(now()))
            """,
            consistency_level=write_cl
        )


        # ----------------------------------------------------
        # Experimental read
        # ----------------------------------------------------

        read_query = SimpleStatement(
            """
            SELECT version
            FROM consistency_test
            WHERE key = %s
            """,
            consistency_level=read_cl
        )


        # ====================================================
        # REPEATED TRIALS
        # ====================================================

        for trial in range(1, TRIALS + 1):

            key = (
                f"mr_normal_"
                f"{write_cl_name.lower()}_"
                f"{read_cl_name.lower()}_"
                f"{trial}_"
                f"{time.time_ns()}"
            )


            # =================================================
            # PHASE 1 — EXPERIMENTAL PREPARATION
            #
            # Establish v1 using ALL.
            #
            # A setup timeout does NOT count as an experimental
            # error because the measured trial has not begun.
            #
            # Retry up to SETUP_RETRIES times.
            # =================================================

            setup_success = False
            setup_attempts = 0
            setup_error = ""


            while (
                not setup_success
                and
                setup_attempts < SETUP_RETRIES
            ):

                setup_attempts += 1

                setup_node = random.choice(
                    node_names
                )

                try:

                    sessions[setup_node].execute(
                        setup_query,
                        (
                            key,
                            "value_1",
                            1,
                            "setup"
                        )
                    )

                    setup_success = True
                    setup_error = ""

                except Exception as e:

                    setup_error = (
                        f"{type(e).__name__}: {str(e)}"
                    )


            # -------------------------------------------------
            # If preparation repeatedly fails, skip this
            # attempted trial.
            #
            # It is recorded separately as SETUP_ERROR and is
            # NOT treated as an MR violation.
            # -------------------------------------------------

            if not setup_success:

                trial_results.append({

                    "trial":
                        trial,

                    "property":
                        "MONOTONIC_READS",

                    "scenario":
                        "normal",

                    "network_delay":
                        NETWORK_DESCRIPTION,

                    "write_cl":
                        write_cl_name,

                    "read_cl":
                        read_cl_name,

                    "setup_attempts":
                        setup_attempts,

                    "write_completed":
                        False,

                    "expected_reads":
                        READS_PER_TRIAL,

                    "completed_reads":
                        0,

                    "regressions":
                        0,

                    "observed_sequence":
                        "",

                    "result":
                        "SETUP_ERROR",

                    "error":
                        setup_error
                })


                print(
                    f"Trial {trial:3} | "
                    f"SETUP ERROR after "
                    f"{setup_attempts} attempts"
                )

                continue


            # =================================================
            # PHASE 2 — MEASURED EXPERIMENT BEGINS
            #
            # Write v2 ONCE.
            #
            # Once this operation begins, there are NO retries.
            # =================================================

            write_completed = False
            completed_reads = 0

            regression_count = 0
            trial_violation = False

            observed_sequence = []

            error = ""


            write_node = random.choice(
                node_names
            )


            try:

                sessions[write_node].execute(
                    write_query,
                    (
                        key,
                        "value_2",
                        2,
                        "writer"
                    )
                )

                write_completed = True


                # =============================================
                # PHASE 3 — FIVE IMMEDIATE READS
                #
                # No sleep is inserted.
                #
                # Every read independently chooses a
                # coordinator.
                # =============================================

                previous_observed_version = None


                for read_number in range(
                    1,
                    READS_PER_TRIAL + 1
                ):

                    read_node = random.choice(
                        node_names
                    )


                    row = sessions[read_node].execute(
                        read_query,
                        (key,)
                    ).one()


                    completed_reads += 1


                    if row is None:

                        observed_version = None

                    else:

                        observed_version = row.version


                    observed_sequence.append(
                        observed_version
                    )


                    # =========================================
                    # MONOTONIC-READ CHECK
                    #
                    # Regression:
                    #
                    # previous = 2
                    # current  = 1
                    #
                    # This is an MR violation.
                    # =========================================

                    regression = False


                    if (
                        previous_observed_version is not None
                        and
                        observed_version is not None
                        and
                        observed_version
                        < previous_observed_version
                    ):

                        regression = True
                        trial_violation = True
                        regression_count += 1


                    # =========================================
                    # SAVE INDIVIDUAL READ
                    # =========================================

                    read_results.append({

                        "trial":
                            trial,

                        "read_number":
                            read_number,

                        "property":
                            "MONOTONIC_READS",

                        "scenario":
                            "normal",

                        "network_delay":
                            NETWORK_DESCRIPTION,

                        "write_cl":
                            write_cl_name,

                        "read_cl":
                            read_cl_name,

                        "write_coordinator":
                            write_node,

                        "read_coordinator":
                            read_node,

                        "same_as_write_coordinator":
                            read_node == write_node,

                        "previous_observed_version":
                            previous_observed_version,

                        "observed_version":
                            observed_version,

                        "regression":
                            regression
                    })


                    # =========================================
                    # Update reader history
                    # =========================================

                    if observed_version is not None:

                        previous_observed_version = (
                            observed_version
                        )


            except Exception as e:

                error = (
                    f"{type(e).__name__}: {str(e)}"
                )


            # =================================================
            # CLASSIFY TRIAL
            # =================================================

            if not write_completed:

                result = "ERROR"

            elif completed_reads < READS_PER_TRIAL:

                result = "ERROR"

            elif trial_violation:

                result = "FAIL"

            else:

                result = "PASS"


            trial_results.append({

                "trial":
                    trial,

                "property":
                    "MONOTONIC_READS",

                "scenario":
                    "normal",

                "network_delay":
                    NETWORK_DESCRIPTION,

                "write_cl":
                    write_cl_name,

                "read_cl":
                    read_cl_name,

                "setup_attempts":
                    setup_attempts,

                "write_completed":
                    write_completed,

                "expected_reads":
                    READS_PER_TRIAL,

                "completed_reads":
                    completed_reads,

                "regressions":
                    regression_count,

                "observed_sequence":
                    "|".join(
                        str(v)
                        for v in observed_sequence
                    ),

                "result":
                    result,

                "error":
                    error
            })


            print(
                f"Trial {trial:3} | "
                f"Sequence={observed_sequence} | "
                f"Regressions={regression_count} | "
                f"{result}"
            )


# ============================================================
# OUTPUT DIRECTORIES
# ============================================================

experiment_directory = os.path.dirname(
    os.path.abspath(__file__)
)

mr_directory = os.path.dirname(
    experiment_directory
)

results_directory = os.path.join(
    mr_directory,
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

trial_output_path = os.path.join(
    results_directory,
    "normal_trials.csv"
)

summary_output_path = os.path.join(
    results_directory,
    "normal_summary.csv"
)


# ============================================================
# SAVE INDIVIDUAL READ OBSERVATIONS
# ============================================================

with open(
    raw_output_path,
    "w",
    newline=""
) as file:

    if read_results:

        writer = csv.DictWriter(
            file,
            fieldnames=read_results[0].keys()
        )

        writer.writeheader()
        writer.writerows(
            read_results
        )


# ============================================================
# SAVE TRIAL RESULTS
# ============================================================

with open(
    trial_output_path,
    "w",
    newline=""
) as file:

    writer = csv.DictWriter(
        file,
        fieldnames=trial_results[0].keys()
    )

    writer.writeheader()
    writer.writerows(
        trial_results
    )


# ============================================================
# GENERATE SUMMARY
# ============================================================

summary_rows = []


print()
print()
print("=" * 80)
print("MONOTONIC READS - NORMAL OPERATION")
print("=" * 80)


for write_cl_name in CONSISTENCY_LEVELS:

    for read_cl_name in CONSISTENCY_LEVELS:


        trial_subset = [

            row

            for row in trial_results

            if (
                row["write_cl"] == write_cl_name
                and
                row["read_cl"] == read_cl_name
            )
        ]


        read_subset = [

            row

            for row in read_results

            if (
                row["write_cl"] == write_cl_name
                and
                row["read_cl"] == read_cl_name
            )
        ]


        passes = sum(
            row["result"] == "PASS"
            for row in trial_subset
        )


        violations = sum(
            row["result"] == "FAIL"
            for row in trial_subset
        )


        errors = sum(
            row["result"] == "ERROR"
            for row in trial_subset
        )


        setup_errors = sum(
            row["result"] == "SETUP_ERROR"
            for row in trial_subset
        )


        # Only actual measured trials are used for
        # experimental availability.
        measured_trials = (
            passes
            + violations
            + errors
        )


        completed_trials = (
            passes
            + violations
        )


        if measured_trials > 0:

            availability = (
                completed_trials
                / measured_trials
            )

        else:

            availability = None


        if completed_trials > 0:

            mr_success_rate = (
                passes
                / completed_trials
            )

        else:

            mr_success_rate = None


        total_reads = len(
            read_subset
        )


        total_regressions = sum(
            row["regression"]
            for row in read_subset
        )


        if total_reads > 0:

            regression_rate = (
                total_regressions
                / total_reads
            )

        else:

            regression_rate = None


        summary_row = {

            "write_cl":
                write_cl_name,

            "read_cl":
                read_cl_name,

            "requested_trials":
                TRIALS,

            "measured_trials":
                measured_trials,

            "passes":
                passes,

            "violations":
                violations,

            "errors":
                errors,

            "setup_errors":
                setup_errors,

            "availability":
                availability,

            "mr_success_rate":
                mr_success_rate,

            "total_reads":
                total_reads,

            "total_regressions":
                total_regressions,

            "read_regression_rate":
                regression_rate
        }


        summary_rows.append(
            summary_row
        )


        print()

        print(
            f"WRITE={write_cl_name}, "
            f"READ={read_cl_name}"
        )

        print("-" * 55)


        print(
            f"Requested trials:          "
            f"{TRIALS}"
        )

        print(
            f"Measured trials:           "
            f"{measured_trials}"
        )

        print(
            f"MR passes:                 "
            f"{passes}"
        )

        print(
            f"Trials with violation:     "
            f"{violations}"
        )

        print(
            f"Experimental errors:       "
            f"{errors}"
        )

        print(
            f"Setup errors:              "
            f"{setup_errors}"
        )


        if availability is not None:

            print(
                f"Availability:              "
                f"{availability:.4f}"
            )

        else:

            print(
                "Availability:              N/A"
            )


        if mr_success_rate is not None:

            print(
                f"MR success rate:           "
                f"{mr_success_rate:.4f}"
            )

        else:

            print(
                "MR success rate:           N/A"
            )


        print(
            f"Total read observations:   "
            f"{total_reads}"
        )

        print(
            f"Total regressions:         "
            f"{total_regressions}"
        )


        if regression_rate is not None:

            print(
                f"Read regression rate:      "
                f"{regression_rate:.4f}"
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
print("=" * 80)

print(
    f"Read observations saved to:\n"
    f"{raw_output_path}"
)

print()

print(
    f"Trial results saved to:\n"
    f"{trial_output_path}"
)

print()

print(
    f"Summary saved to:\n"
    f"{summary_output_path}"
)

print("=" * 80)