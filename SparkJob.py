from dao.DAOCsv import read_binance_csv
from dao.DAOSparkInMemory import load_crypto_to_hdfs
from pyspark.sql import SparkSession
from datetime import datetime

import argparse

import pika
import json
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

QUEUE_SPARK_JOB_NAME = "spark_job_assets"

def parse_args():
    parser = argparse.ArgumentParser(description="Spark job with parameters")

    parser.add_argument(
        "--symbol",
        required=False,
        help="Cryptocurrency symbol to process (e.g., BTCUSDT)",
    )


    parser.add_argument(
        "--exchange",
        required=False,
        help="Exchange to process (e.g., binance)",
    )

    return parser.parse_args()

logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Initializing Input Variables...")
logger.info(f"Name:{__name__}")
args = parse_args()
logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"Symbol: {args.symbol}")
logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"Exchange: {args.exchange}") 
logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Initializing Spark Session...")

#    .appName("Job Loader for " + args.symbol + " on " + args.exchange) \
spark = SparkSession.builder \
    .appName("Job Loader for All assets in RabbitMQ") \
    .master("yarn") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.yarn.queue", "jupyter") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
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
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "hdfs://spark-master:9000/spark-logs") \
    .config("spark.history.fs.logDirectory", "hdfs://spark-master:9000/spark-logs") \
    .config("spark.eventLog.compress", "true") \
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

logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + " Spark Session initialized.")

logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"Spark versão: {spark.version}")
logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"Catálogo: {spark.conf.get('spark.sql.catalogImplementation')}")  # → in-memory
logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"fs.defaultFS: {spark.conf.get('spark.hadoop.fs.defaultFS')}")


logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Load databases and tables in-memory catalog...")

base =  "hdfs://spark-master:9000/datasets/"
for db_path in spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
                 .listStatus(spark._jvm.org.apache.hadoop.fs.Path(base)):
    
    db_name = db_path.getPath().getName()
    #if not db_name.endswith(".db"):
    #    continue
    db = db_name#[:-3]

    logger.info(f"{datetime.now():%Y.%m.%d %H:%M:%S} → CREATING DATABASE: → {db}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    logger.info(f"{datetime.now():%Y.%m.%d %H:%M:%S} → OK DATABASE: → {db}")

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
            logger.info(f"{datetime.now():%Y.%m.%d %H:%M:%S} → OK TABLE: → {db}.{tbl}")
            
        except Exception as e:
            logger.warning(f"{datetime.now():%Y.%m.%d %H:%M:%S} → PULOU → {db}.{tbl} → {str(e)[:80]}")

logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Databases and Tables loaded in-memory catalog.")

possibleTimeFrames = ["15m","30m","1h","4h","1d","1w"]
def carga(symbol: str):
    for tf in possibleTimeFrames:
        logger.info(f"Processando {symbol} - timeframe {tf}")
        df = read_binance_csv(symbol, tf, spark)
        load_crypto_to_hdfs(symbol, tf, spark, df, False)  # ou True, conforme sua lógica

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        symbol = data["symbol"]
        exchange = data["exchange"]

        logger.info(f"🔥 Recebido job → {symbol} ({exchange})")

        carga(symbol)

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"✅ Job concluído: {symbol}")

    except Exception as e:
        logger.error(f"❌ Erro ao processar {body}: {e}", exc_info=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # ou True se quiser retry

def start_consumer():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
            channel = connection.channel()
            #channel.queue_declare(queue=QUEUE_SPARK_JOB_NAME, durable=True)
            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(queue=QUEUE_SPARK_JOB_NAME, on_message_callback=callback)
            logger.info("🚀 Consumidor Spark iniciado – SparkSession compartilhada ativa")
            logger.info("Aguardando mensagens...")

            channel.start_consuming()  # bloqueia aqui para sempre (até shutdown)

        except Exception as e:
            logger.error(f"Conexão perdida, reconectando em 5s... ({e})")
            time.sleep(5)
        finally:
            if 'connection' in locals() and connection.is_open:
                connection.close()

if __name__ == "__main__":
    try:
        start_consumer()
    except KeyboardInterrupt:
        logger.info("Shutdown solicitado – parando SparkSession")
        spark.stop()
        logger.info("SparkSession parada com sucesso")