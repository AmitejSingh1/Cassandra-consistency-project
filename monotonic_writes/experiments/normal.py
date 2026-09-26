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
WRITES_PER_TRIAL = 5
PROBES_PER_WRITE = 3
SETUP_RETRIES = 3

KEYSPACE = "consistency_experiment"

NETWORK_DESCRIPTION = "netem delay 25ms 25ms"


WRITE_LEVELS = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
}


# ============================================================
# CASSANDRA NODES
#
# cassandra1 -> localhost:9042
# cassandra2 -> localhost:9043
# cassandra3 -> localhost:9044
#
# Each operation can therefore independently choose a
# coordinator.
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
# BASELINE SETUP
#
# Establish v1 using ALL before the measured experiment.
#
# Setup may be retried because this is preparation rather than
# part of the client write history being measured.
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
# DIAGNOSTIC READ
#
# READ ONE is used after each write to observe temporary
# propagation behaviour.
#
# A stale diagnostic probe is NOT by itself classified as
# a monotonic-writes violation.
# ============================================================

probe_query = SimpleStatement(
    """
    SELECT version
    FROM consistency_test
    WHERE key = %s
    """,
    consistency_level=ConsistencyLevel.ONE
)


# ============================================================
# FINAL READ
#
# READ ALL is used after all five sequential writes to obtain
# the fully reconciled state.
#
# This is a measurement mechanism, not an MW configuration.
# ============================================================

final_read_query = SimpleStatement(
    """
    SELECT version
    FROM consistency_test
    WHERE key = %s
    """,
    consistency_level=ConsistencyLevel.ALL
)


# ============================================================
# RESULT STORAGE
#
# write_results:
#     one row per experimental write
#
# probe_results:
#     one row per diagnostic probe
#
# trial_results:
#     one row per complete trial
# ============================================================

write_results = []
probe_results = []
trial_results = []


# ============================================================
# RUN THREE WRITE CONSISTENCY CONFIGURATIONS
# ============================================================

for write_cl_name, write_cl in WRITE_LEVELS.items():

    print()
    print("=" * 70)
    print(f"WRITE CONSISTENCY = {write_cl_name}")
    print("=" * 70)


    # --------------------------------------------------------
    # Explicit timestamps are supplied.
    #
    # Later client writes always receive larger timestamps.
    # --------------------------------------------------------

    write_query = SimpleStatement(
        """
        UPDATE consistency_test
        USING TIMESTAMP %s
        SET
            value = %s,
            version = %s,
            client = %s,
            updated_at = toTimestamp(now())
        WHERE key = %s
        """,
        consistency_level=write_cl
    )


    # ========================================================
    # REPEATED TRIALS
    # ========================================================

    for trial in range(1, TRIALS + 1):

        key = (
            f"mw_normal_"
            f"{write_cl_name.lower()}_"
            f"{trial}_"
            f"{time.time_ns()}"
        )


        # ====================================================
        # PHASE 1 — PREPARE v1
        # ====================================================

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


        # ====================================================
        # SETUP FAILED
        #
        # The measured MW experiment never began.
        # ====================================================

        if not setup_success:

            trial_results.append({

                "trial":
                    trial,

                "property":
                    "MONOTONIC_WRITES",

                "scenario":
                    "normal",

                "network_delay":
                    NETWORK_DESCRIPTION,

                "write_cl":
                    write_cl_name,

                "setup_attempts":
                    setup_attempts,

                "expected_writes":
                    WRITES_PER_TRIAL,

                "completed_writes":
                    0,

                "expected_final_version":
                    WRITES_PER_TRIAL + 1,

                "observed_final_version":
                    None,

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


        # ====================================================
        # PHASE 2 — MEASURED EXPERIMENT
        # ====================================================

        completed_writes = 0

        expected_final_version = (
            WRITES_PER_TRIAL + 1
        )

        observed_final_version = None

        trial_error = ""

        write_failed = False


        # ----------------------------------------------------
        # Cassandra write timestamps use microseconds.
        #
        # Writes are separated by 1000 microseconds in logical
        # timestamp space so their intended order is explicit:
        #
        # v2 -> T + 1000
        # v3 -> T + 2000
        # ...
        # ----------------------------------------------------

        base_timestamp = (
            time.time_ns() // 1000
        )


        try:

            # =================================================
            # FIVE SEQUENTIAL WRITES
            # =================================================

            for step in range(
                1,
                WRITES_PER_TRIAL + 1
            ):

                version = step + 1

                timestamp = (
                    base_timestamp
                    + (step * 1000)
                )


                # ---------------------------------------------
                # Independently select write coordinator
                # ---------------------------------------------

                write_node = random.choice(
                    node_names
                )


                # ---------------------------------------------
                # Perform sequential client write
                #
                # No retry.
                # No sleep.
                # ---------------------------------------------

                try:

                    sessions[write_node].execute(
                        write_query,
                        (
                            timestamp,
                            f"value_{version}",
                            version,
                            "client1",
                            key
                        )
                    )

                    completed_writes += 1


                    write_results.append({

                        "trial":
                            trial,

                        "step":
                            step,

                        "property":
                            "MONOTONIC_WRITES",

                        "scenario":
                            "normal",

                        "network_delay":
                            NETWORK_DESCRIPTION,

                        "write_cl":
                            write_cl_name,

                        "write_coordinator":
                            write_node,

                        "version":
                            version,

                        "timestamp":
                            timestamp,

                        "write_completed":
                            True,

                        "error":
                            ""
                    })


                except Exception as e:

                    write_failed = True

                    write_error = (
                        f"{type(e).__name__}: {str(e)}"
                    )


                    write_results.append({

                        "trial":
                            trial,

                        "step":
                            step,

                        "property":
                            "MONOTONIC_WRITES",

                        "scenario":
                            "normal",

                        "network_delay":
                            NETWORK_DESCRIPTION,

                        "write_cl":
                            write_cl_name,

                        "write_coordinator":
                            write_node,

                        "version":
                            version,

                        "timestamp":
                            timestamp,

                        "write_completed":
                            False,

                        "error":
                            write_error
                    })


                    trial_error = write_error

                    # A sequential client history is broken if
                    # one of its writes does not complete.
                    break


                # =============================================
                # DIAGNOSTIC PROBES
                #
                # Three READ ONE probes immediately after
                # every successful write.
                #
                # These show temporary propagation/staleness.
                #
                # They do NOT determine MW PASS/FAIL.
                # =============================================

                for probe_number in range(
                    1,
                    PROBES_PER_WRITE + 1
                ):

                    probe_node = random.choice(
                        node_names
                    )


                    try:

                        row = sessions[
                            probe_node
                        ].execute(
                            probe_query,
                            (key,)
                        ).one()


                        observed_version = (
                            row.version
                            if row is not None
                            else None
                        )


                        # -------------------------------------
                        # Is the probe behind the latest
                        # successfully acknowledged client
                        # write?
                        #
                        # This is diagnostic staleness only.
                        # -------------------------------------

                        stale_probe = (
                            observed_version is not None
                            and
                            observed_version < version
                        )


                        probe_results.append({

                            "trial":
                                trial,

                            "write_step":
                                step,

                            "probe_number":
                                probe_number,

                            "property":
                                "MONOTONIC_WRITES",

                            "scenario":
                                "normal",

                            "network_delay":
                                NETWORK_DESCRIPTION,

                            "write_cl":
                                write_cl_name,

                            "latest_written_version":
                                version,

                            "write_coordinator":
                                write_node,

                            "probe_coordinator":
                                probe_node,

                            "observed_version":
                                observed_version,

                            "stale_probe":
                                stale_probe,

                            "probe_completed":
                                True,

                            "error":
                                ""
                        })


                    except Exception as e:

                        probe_results.append({

                            "trial":
                                trial,

                            "write_step":
                                step,

                            "probe_number":
                                probe_number,

                            "property":
                                "MONOTONIC_WRITES",

                            "scenario":
                                "normal",

                            "network_delay":
                                NETWORK_DESCRIPTION,

                            "write_cl":
                                write_cl_name,

                            "latest_written_version":
                                version,

                            "write_coordinator":
                                write_node,

                            "probe_coordinator":
                                probe_node,

                            "observed_version":
                                None,

                            "stale_probe":
                                False,

                            "probe_completed":
                                False,

                            "error":
                                (
                                    f"{type(e).__name__}: "
                                    f"{str(e)}"
                                )
                        })


            # =================================================
            # PHASE 3 — FINAL RECONCILED STATE
            #
            # Only meaningful if all five sequential writes
            # successfully completed.
            # =================================================

            if not write_failed:

                final_read_node = random.choice(
                    node_names
                )


                row = sessions[
                    final_read_node
                ].execute(
                    final_read_query,
                    (key,)
                ).one()


                observed_final_version = (
                    row.version
                    if row is not None
                    else None
                )


                # =============================================
                # MONOTONIC-WRITES RESULT
                #
                # The same client issued:
                #
                # v2 -> v3 -> v4 -> v5 -> v6
                #
                # with strictly increasing timestamps.
                #
                # After all writes completed, the reconciled
                # state should correspond to v6.
                # =============================================

                if (
                    observed_final_version
                    == expected_final_version
                ):

                    result = "PASS"

                else:

                    result = "FAIL"


            else:

                result = "ERROR"


        except Exception as e:

            result = "ERROR"

            trial_error = (
                f"{type(e).__name__}: {str(e)}"
            )


        # ====================================================
        # SAVE TRIAL RESULT
        # ====================================================

        trial_results.append({

            "trial":
                trial,

            "property":
                "MONOTONIC_WRITES",

            "scenario":
                "normal",

            "network_delay":
                NETWORK_DESCRIPTION,

            "write_cl":
                write_cl_name,

            "setup_attempts":
                setup_attempts,

            "expected_writes":
                WRITES_PER_TRIAL,

            "completed_writes":
                completed_writes,

            "expected_final_version":
                expected_final_version,

            "observed_final_version":
                observed_final_version,

            "result":
                result,

            "error":
                trial_error
        })


        print(
            f"Trial {trial:3} | "
            f"Writes="
            f"{completed_writes}/{WRITES_PER_TRIAL} | "
            f"Expected="
            f"{expected_final_version} | "
            f"Observed="
            f"{observed_final_version} | "
            f"{result}"
        )


# ============================================================
# OUTPUT DIRECTORIES
# ============================================================

experiment_directory = os.path.dirname(
    os.path.abspath(__file__)
)

mw_directory = os.path.dirname(
    experiment_directory
)

results_directory = os.path.join(
    mw_directory,
    "results"
)

os.makedirs(
    results_directory,
    exist_ok=True
)


write_output_path = os.path.join(
    results_directory,
    "normal.csv"
)

probe_output_path = os.path.join(
    results_directory,
    "normal_probes.csv"
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
# SAVE WRITE OBSERVATIONS
# ============================================================

with open(
    write_output_path,
    "w",
    newline=""
) as file:

    if write_results:

        writer = csv.DictWriter(
            file,
            fieldnames=write_results[0].keys()
        )

        writer.writeheader()
        writer.writerows(
            write_results
        )


# ============================================================
# SAVE DIAGNOSTIC PROBES
# ============================================================

with open(
    probe_output_path,
    "w",
    newline=""
) as file:

    if probe_results:

        writer = csv.DictWriter(
            file,
            fieldnames=probe_results[0].keys()
        )

        writer.writeheader()
        writer.writerows(
            probe_results
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
print("MONOTONIC WRITES - NORMAL OPERATION")
print("=" * 80)


for write_cl_name in WRITE_LEVELS:


    trial_subset = [

        row

        for row in trial_results

        if row["write_cl"] == write_cl_name
    ]


    probe_subset = [

        row

        for row in probe_results

        if row["write_cl"] == write_cl_name
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

        mw_success_rate = (
            passes
            / completed_trials
        )

    else:

        mw_success_rate = None


    completed_probes = sum(
        row["probe_completed"]
        for row in probe_subset
    )


    stale_probes = sum(
        (
            row["probe_completed"]
            and
            row["stale_probe"]
        )
        for row in probe_subset
    )


    if completed_probes > 0:

        stale_probe_rate = (
            stale_probes
            / completed_probes
        )

    else:

        stale_probe_rate = None


    summary_row = {

        "write_cl":
            write_cl_name,

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

        "mw_success_rate":
            mw_success_rate,

        "completed_diagnostic_probes":
            completed_probes,

        "stale_diagnostic_probes":
            stale_probes,

        "stale_probe_rate":
            stale_probe_rate
    }


    summary_rows.append(
        summary_row
    )


    print()

    print(
        f"WRITE={write_cl_name}"
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
        f"MW passes:                 "
        f"{passes}"
    )

    print(
        f"MW violations:             "
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


    if mw_success_rate is not None:

        print(
            f"MW success rate:           "
            f"{mw_success_rate:.4f}"
        )

    else:

        print(
            "MW success rate:           N/A"
        )


    print(
        f"Completed probes:          "
        f"{completed_probes}"
    )

    print(
        f"Stale diagnostic probes:   "
        f"{stale_probes}"
    )


    if stale_probe_rate is not None:

        print(
            f"Stale probe rate:          "
            f"{stale_probe_rate:.4f}"
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
    f"Write observations saved to:\n"
    f"{write_output_path}"
)

print()

print(
    f"Diagnostic probes saved to:\n"
    f"{probe_output_path}"
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