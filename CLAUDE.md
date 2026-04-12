# cryptoloader — Project Context for Claude Code

## Architecture overview
- PySpark driver container (`spark-firstattempt`) connects to a 4-node Raspberry Pi Spark/YARN/Hive cluster
- ETL container (`etl-firstattempt`) downloads CSV files from Binance and rsyncs them to all cluster nodes via GlusterFS
- Jobs are dispatched via RabbitMQ queues: SparkJob.py consumes `spark_job_assets`, LoadData.py consumes a separate ETL queue
- YARN queue used: `jupyter` (the `spring` queue is reserved for a separate Spring Boot Java job)
- Hive metastore backend: MySQL inside `hive-server` container; Hive 3.1.3 (NOT 4.x — Thrift API incompatible with Spark's embedded client)

## Spark session builder — complete reference (SparkJob.py)
```python
spark = SparkSession.builder \
    .appName("...") \
    .master("yarn") \
    .config("spark.submit.deployMode",                      "client") \
    .config("spark.driver.bindAddress",                     "0.0.0.0") \
    \
    .config("spark.yarn.queue",                             "jupyter") \
    .config("spark.yarn.stagingDir",                        "hdfs:///tmp/spark-staging") \
    .config("spark.yarn.jars",                              "local:///usr/spark-4.0.1-bin-hadoop3/jars/*") \
    .config("spark.driver.extraClassPath",                  "/opt/delta-jars/*") \
    .config("spark.yarn.am.memory",                         "256m") \
    .config("spark.yarn.am.livenessMonitor.interval-ms",    "10000") \
    \
    .config("spark.hadoop.fs.defaultFS",                    "hdfs://spark-master:9000") \
    .config("spark.hadoop.yarn.resourcemanager.address",    "spark-master:8032") \
    .config("spark.hadoop.yarn.resourcemanager.hostname",   "spark-master") \
    \
    .config("spark.sql.catalogImplementation",              "hive") \
    .config("spark.hadoop.hive.metastore.uris",             "thrift://hive-server:9083") \
    .config("spark.sql.warehouse.dir",                      "hdfs://spark-master:9000/user/hive/warehouse") \
    \
    .config("spark.executor.memory",                        "1g") \
    .config("spark.executor.memoryOverhead",                "384") \
    .config("spark.executor.cores",                         "4") \
    .config("spark.driver.memory",                          "1g") \
    \
    .config("spark.dynamicAllocation.enabled",                      "true") \
    .config("spark.dynamicAllocation.shuffleTracking.enabled",      "true") \
    .config("spark.dynamicAllocation.minExecutors",                 "1") \
    .config("spark.dynamicAllocation.maxExecutors",                 "3") \
    .config("spark.dynamicAllocation.executorIdleTimeout",          "30s") \
    .config("spark.dynamicAllocation.cachedExecutorIdleTimeout",    "60s") \
    \
    .config("spark.executor.heartbeatInterval",             "20s") \
    .config("spark.network.timeout",                        "300s") \
    \
    .config("spark.serializer",                             "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.referenceTracking",                 "false") \
    .config("spark.kryo.unsafe",                            "false") \
    \
    .config("spark.sql.adaptive.enabled",                   "false") \
    .config("spark.sql.hive.filesourcePartitionFileCacheSize", "209715200") \
    .config("spark.sql.hive.manageFilesourcePartitions",    "false") \
    .config("spark.hadoop.hive.metastore.client.socket.keepalive",    "true") \
    .config("spark.hadoop.hive.metastore.client.socket.timeout",      "300") \
    .config("spark.hadoop.hive.metastore.failure.retries",            "3") \
    .config("spark.hadoop.hive.metastore.client.connect.retry.delay", "2") \
    .config("spark.ui.proxyBase",                           "") \
    \
    .config("spark.eventLog.enabled",                       "true") \
    .config("spark.eventLog.dir",                           "hdfs://spark-master:9000/spark-logs") \
    .config("spark.eventLog.compress",                      "false") \
    \
    .config("spark.driver.extraJavaOptions",
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED ") \
    \
    .enableHiveSupport() \
    .getOrCreate()
```

## Known issues and fixes

| Error | Root cause | Fix applied |
|---|---|---|
| `SHOW PARTITIONS is not allowed ... manageFilesourcePartitions = false` | `SHOW PARTITIONS` requires the Hive partition registry, disabled on this cluster | Replace with Hadoop FileSystem API (`fs.listStatus`) to enumerate partition dirs directly from HDFS |
| `'Catalog' object has no attribute 'listPartitions'` | `spark.catalog.listPartitions` does not exist in PySpark 4 | Use Hadoop FileSystem API instead (see DAOHive pattern below) |
| `[NOT_COLUMN_OR_STR] Argument col should be a Column or str, got list` | `from pyspark.sql.functions import max` shadows Python's built-in `max` | Remove `max` from the PySpark import; use `F.max(...)` for Spark aggregates and plain `max()` for Python lists |
| `saveAsTable` listings 350+ paths taking 53-60s per write | With `manageFilesourcePartitions=false`, `saveAsTable` enumerates all partition dirs on every write to resolve the table | Replace `saveAsTable` with `write.parquet(hdfs_location)` — writes directly to path, no partition listing |
| `DESCRIBE FORMATTED` NullPointerException (`sparkSession is null`) | SparkSession died (DataNode failure killed YARN app); `_get_table_location` was calling SQL on a dead session | Derive HDFS path from known structure (`hdfs://spark-master:9000/datasets/{db}/{table}`) — zero SQL calls needed |
| `[PATH_NOT_FOUND] cuote_month=03` | HDFS partition dirs use unpadded month (`cuote_month=3`), code was building path with `{month:02d}` | Use `{month}` without zero-padding |
| Executor heartbeat timeout / 3MB+ task payloads | `manageFilesourcePartitions=true` + `MSCK REPAIR TABLE` or `RECOVER PARTITIONS` serializes all partition metadata into task payloads — too large for Pi nodes | Keep `manageFilesourcePartitions=false`; never run MSCK at startup; use HDFS FileSystem API for partition discovery |
| SSH known_hosts stale after container/node restart | `/home/appuser/.ssh` is mounted read-only from host; host keys regenerate on restart | Run `ssh-keyscan` at container startup writing to `/tmp/ssh/known_hosts`; pass via `CLUSTER_KNOWN_HOSTS` env var to rsync's `-o UserKnownHostsFile` |

## DAOHive — correct pattern for partition discovery and writing

With `manageFilesourcePartitions=false`, never use `SHOW PARTITIONS` or `spark.catalog.listPartitions`. Use the Hadoop FileSystem API:

```python
HDFS_BASE = "hdfs://spark-master:9000/datasets"

def _get_table_location(table):
    db, tbl = table.split(".")
    return f"{HDFS_BASE}/{db}/{tbl}"

def _hdfs_fs(spark):
    return spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

def _hdfs_path(spark, path_str):
    return spark._jvm.org.apache.hadoop.fs.Path(path_str)

def get_latest_partition_date(spark, table, index_value):
    location = _get_table_location(table)
    fs = _hdfs_fs(spark)
    index_path = _hdfs_path(spark, f"{location}/index={index_value}")
    if not fs.exists(index_path):
        return None
    dates = [
        s.getPath().getName().split("=", 1)[1]
        for s in fs.listStatus(index_path)
        if s.getPath().getName().startswith("cuote_date=")
    ]
    return max(dates) if dates else None
```

For writing, use `write.parquet(location)` instead of `saveAsTable`:
```python
df.write.partitionBy("index", "cuote_date").mode("append").parquet(location)
```

For reading the latest partition, use direct path read instead of `read.table().filter()`:
```python
path = f"{location}/index={index_value}/cuote_date={latest_date}"
spark.read.option("basePath", location).parquet(path)
```

## Delta JAR rule
Any container running a Spark driver (client mode) must have Delta JARs locally:
```dockerfile
RUN mkdir -p /opt/delta-jars && \
    wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar \
        -P /opt/delta-jars/ && \
    wget https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar \
        -P /opt/delta-jars/
```
And in session builder: `.config("spark.driver.extraClassPath", "/opt/delta-jars/*")`
Executors get JARs from `spark.yarn.jars` (spark-hadoop image).

## SSH / rsync pattern for read-only mounted .ssh
`/home/appuser/.ssh` is mounted `:ro` from the Pi host. To handle rotating host keys:
```bash
# entrypoint.sh — runs at every container startup
mkdir -p /tmp/ssh
ssh-keyscan -H spark-master spark-worker-1 spark-worker-2 spark-worker-3 \
    > /tmp/ssh/known_hosts 2>/dev/null
export CLUSTER_KNOWN_HOSTS=/tmp/ssh/known_hosts
```
```python
# LoadData.py — rsync call
known_hosts = os.environ.get("CLUSTER_KNOWN_HOSTS", "/home/appuser/.ssh/known_hosts")
ssh_cmd = f"ssh -i /home/appuser/.ssh/id_etl_rsync -o UserKnownHostsFile={known_hosts}"
rsync_cmd = ["rsync", "-avz", "-e", ssh_cmd] + src_files + [f"appuser@{node}:/glustervol1/work/"]
```

## Performance results (after all fixes)
- SparkJob: ~2 min per asset across all 6 timeframes
- ETL LoadData: ~1 min per asset
- Jobs run independently in parallel queues via RabbitMQ
