from dao.DAOCsv import read_binance_csv
from dao.DAOSparkInMemory import load_crypto_to_hdfs
from pyspark.sql import SparkSession
from datetime import datetime

import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="Spark job with parameters")

    parser.add_argument(
        "--symbol",
        required=True,
        help="Cryptocurrency symbol to process (e.g., BTCUSDT)",
    )


    parser.add_argument(
        "--exchange",
        required=True,
        help="Exchange to process (e.g., binance)",
    )

    return parser.parse_args()

print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Initializing Input Variables...")

args = parse_args()
print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"Symbol: {args.symbol}")
print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"Exchange: {args.exchange}") 

print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Initializing Spark Session...")

spark = SparkSession.builder \
    .appName("Job Loader for" + args.symbol + " on " + args.exchange) \
    .master("yarn") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.yarn.queue", "jupyter") \
    .config("spark.yarn.stagingDir", "hdfs:///tmp/spark-staging") \
    .config("spark.yarn.jars", "local:///usr/spark-4.0.1-bin-hadoop3/jars/*") \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.memory", "512m") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memoryOverhead", "256") \
    .config("spark.driver.memory", "512m") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://spark-master:9000") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.pool.enabled", "false") \
    .config("spark.kryo.referenceTracking", "false") \
    .config("spark.kryo.unsafe", "false") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.hadoop.yarn.resourcemanager.address", "spark-master:8032") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "spark-master") \
    .config("spark.driver.extraJavaOptions", 
        "--add-opens=java.base/java.nio=ALL-UNNAMED " 
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " 
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " 
                "--add-opens=java.base/java.util=ALL-UNNAMED " 
                "--add-opens=java.base/java.lang=ALL-UNNAMED ") \
    .config("spark.executor.extraJavaOptions",
        "--add-opens=java.base/java.nio=ALL-UNNAMED " 
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " 
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " 
                "--add-opens=java.base/java.util=ALL-UNNAMED " 
                "--add-opens=java.base/java.lang=ALL-UNNAMED ") \
    .getOrCreate()


# \
#    .config("spark.rpc.message.maxSize", "256") \
#    .config("spark.yarn.jars", "hdfs://spark-master:9000/shared-libs/*") \

#    .config("spark.sql.files.ignoreCorruptFiles", "true") \
#    .config("spark.sql.files.ignoreMissingFiles", "true") \
#    .config("spark.hadoop.parquet.enable.summary-metadata", "false") \

print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + " Spark Session initialized.")

print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"Spark versão: {spark.version}")
print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"Catálogo: {spark.conf.get('spark.sql.catalogImplementation')}")  # → in-memory
print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"fs.defaultFS: {spark.conf.get('spark.hadoop.fs.defaultFS')}")


print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Load databases and tables in-memory catalog...")

base =  "hdfs://spark-master:9000/datasets/"
for db_path in spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
                 .listStatus(spark._jvm.org.apache.hadoop.fs.Path(base)):
    
    db_name = db_path.getPath().getName()
    #if not db_name.endswith(".db"):
    #    continue
    db = db_name#[:-3]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    print(f"{datetime.now():%Y.%m.%d %H:%M:%S} → OK DATABASE: → {db}")

    for tbl_path in spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
                     .listStatus(db_path.getPath()):
        tbl = tbl_path.getPath().getName()
        if tbl.startswith("_") or tbl.startswith("."):
            continue
            
        full_path = tbl_path.getPath().toString()
        
        try:
            # AQUI É A BALA DE PRATA
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {db}.{tbl}
                USING PARQUET
                OPTIONS (
                    path '{full_path}',
                    ignoreCorruptFiles 'true'
                )
            """)
            
            #spark.sql(f"REFRESH TABLE {db}.{tbl}")
            spark.sql(f"ALTER TABLE {db}.{tbl} RECOVER PARTITIONS")
            print(f"{datetime.now():%Y.%m.%d %H:%M:%S} → OK TABLE: → {db}.{tbl}")
            
        except Exception as e:
            print(f"{datetime.now():%Y.%m.%d %H:%M:%S} → PULOU → {db}.{tbl} → {str(e)[:80]}")


print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Databases and Tables loaded in-memory catalog.")


possibleTimeFrames = ["15m","30m","1h","4h","1d","1w"]

#possibleTimeFrames = ["30m","1h","4h","1d"]

def carga_zero(ind_curr:str):
  qtde = len(possibleTimeFrames)
  for iCount in range(qtde):
    dfAux0 = read_binance_csv(ind_curr,possibleTimeFrames[iCount],spark)
    load_crypto_to_hdfs(ind_curr,possibleTimeFrames[iCount],spark,dfAux0,True)

def carga(ind_curr:str):
  qtde = len(possibleTimeFrames)
  for iCount in range(qtde):
    dfAux0 = read_binance_csv(ind_curr,possibleTimeFrames[iCount],spark)
    load_crypto_to_hdfs(ind_curr,possibleTimeFrames[iCount],spark,dfAux0,False)


idAsset = args.symbol
print(idAsset+ " - Carregando arquivo do ativo: "+ idAsset+ "...")
carga(idAsset)


