from cassandra.cluster import Cluster

cluster = Cluster(["127.0.0.1"], port=9042)

session = cluster.connect("consistency_experiment")

rows = session.execute("SELECT * FROM consistency_test")

for row in rows:
    print(
        f"key={row.key}, "
        f"value={row.value}, "
        f"version={row.version}, "
        f"client={row.client}"
    )

cluster.shutdown()