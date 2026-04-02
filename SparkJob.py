from dao.DAOCsv import read_binance_csv
from dao.DAOHive import load_crypto_to_hive
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

possibleTimeFrames = ["15m","30m","1h","4h","1d","1w"]
spark = None  # Variável global para a SparkSession

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


def initialize_spark_session():
    global spark
    logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Initializing Spark Session...")

    #    .appName("Job Loader for " + args.symbol + " on " + args.exchange) \
    
    spark = SparkSession.builder \
        .appName("Job Leader with hive to All Assets in RabbitMQ") \
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
        .config("spark.executor.heartbeatInterval",             "10s") \
        .config("spark.network.timeout",                        "120s") \
        \
        .config("spark.serializer",                             "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.referenceTracking",                 "false") \
        .config("spark.kryo.unsafe",                            "false") \
        \
        .config("spark.sql.adaptive.enabled",                   "false") \
        .config("spark.sql.hive.filesourcePartitionFileCacheSize", "52428800") \
        .config("spark.sql.hive.manageFilesourcePartitions", "false") \
        .config("spark.ui.proxyBase", "") \
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

    logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + " Spark Session initialized.")

    logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"Spark versão: {spark.version}")
    logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"Catálogo: {spark.conf.get('spark.sql.catalogImplementation')}")
    logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"fs.defaultFS: {spark.conf.get('spark.hadoop.fs.defaultFS')}")

    try:
        databases = [row[0] for row in spark.sql("SHOW DATABASES").collect()]
        logger.info(f"Databases visíveis no Hive: {databases}")
        if "crypto" in databases:
            tables = [row[1] for row in spark.sql("SHOW TABLES IN crypto").collect()]
            logger.info(f"Tabelas em crypto: {tables}")
        else:
            logger.warning("Database 'crypto' NÃO encontrado no metastore!")
    except Exception as e:
        logger.error(f"Falha ao listar databases/tables do Hive: {e}")


def carga(symbol: str):
    global spark
    for tf in possibleTimeFrames:
        logger.info(f"Processando {symbol} - timeframe {tf}")
        if spark is None:
            logger.error("SparkSession não inicializada!")
            return
        df = read_binance_csv(symbol, tf, spark)
        load_crypto_to_hive(symbol, tf, spark, df, False)  # ou True, conforme sua lógica

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        symbol = data["symbol"]
        exchange = data["exchange"]
        priority = data["priority"]

        logger.info(f"🔥 Recebido job → {symbol} ({exchange})- priority: {priority}")

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
        logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Initializing Input Variables...")
        logger.info(f"Name:{__name__}")
        args = parse_args()
        logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"Symbol: {args.symbol}")
        logger.info(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + f"Exchange: {args.exchange}") 
        initialize_spark_session()
        start_consumer()
    except KeyboardInterrupt:
        logger.info("Shutdown solicitado – parando SparkSession")
        spark.stop()
        logger.info("SparkSession parada com sucesso")