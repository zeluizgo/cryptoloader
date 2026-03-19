
from datetime import datetime, timedelta
from calendar import monthrange
import requests
import zipfile
import os
from io import BytesIO
import pika
import json
import time

import subprocess
import glob
import os

import logging


            
import threading
import time
from collections import deque

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

possibleTimeFrames = ["15m","30m","1h","4h","1d","1w"]

url_prefix_Daily = "https://data.binance.vision/data/spot/daily/klines/"
url_prefix_Monthly = "https://data.binance.vision/data/spot/monthly/klines/"

QUEUE_SPARK_JOB_NAME = "spark_job_assets"
QUEUE_HIST_ASSETS_NAME = "hist_assets"

# Global or class-level
last_known_counts = {"hist_assets": -1, "spark_job_assets": -1}
check_lock = threading.Lock()
should_trigger_full_reload = threading.Event()


def get_last_timestamp(filename: str, bSemanal: bool) -> datetime:
    """
    Lê a última linha do arquivo CSV e retorna o timestamp (primeiro token) como datetime.
    O timestamp está em milissegundos.
    """
    try:
        filepath = "/glustervol1/work/" + filename
        if not os.path.isfile(filepath):
            logger.warning(f"Arquivo não encontrado: {filepath}")
            return None
        with open(filepath, 'r') as f:
            last_line = f.readlines()[-1]  # última linha
        
        # extrai o primeiro token (timestamp em ms)
        timestamp_ms = int(last_line.split(',')[0])
        timestamp_s = 0
        if bSemanal:
            timestamp_s = timestamp_ms / 1000000 # converte milisegundos para segundos
        else:
            # converte milissegundos para segundos
            timestamp_s = timestamp_ms / 1000000  # seus dados parecem estar em microsegundos!
            
        # converte para datetime
        dt = datetime.utcfromtimestamp(timestamp_s)
        
        return dt
    except (IndexError, ValueError) as e:
        logger.error(f"Erro ao ler timestamp: {e}")
        return None


def etl_download(symbol: str, exchange: str, dt_start: datetime = None, dt_end: datetime = None):

    if dt_start is None:
        dt_start = datetime.now()  
    if dt_end is None:
        dt_end = datetime.now()

    # ...restante da função...
    logger.info(f"dt_start = {dt_start}, dt_end = {dt_end}")

    dtHoje = datetime.now()
    dtIniMes = datetime(dtHoje.year, dtHoje.month, 1)

    qtdeLines = len(possibleTimeFrames)

    idAsset = symbol

    for iCount in range(qtdeLines):
        logger.info(idAsset)
        logger.info(possibleTimeFrames[iCount])

        filenameFinal = idAsset + "-" + possibleTimeFrames[iCount] + ".csv"
        lastdatetimeloaded = get_last_timestamp(filenameFinal, possibleTimeFrames[iCount] == "1w")
        logger.info(f"lastdatetimeloaded = {lastdatetimeloaded}")
        if lastdatetimeloaded is not None:
            actual_Daily = lastdatetimeloaded + timedelta(days=1)
            actual_Monthly = datetime(actual_Daily.year, actual_Daily.month, 1)


            only_Daily = dtHoje.month == actual_Daily.month and dtHoje.year == actual_Daily.year
            only_Monthly = datetime(dtHoje.year, dtHoje.month, 1) > dt_end
            if only_Daily and only_Monthly:
                logger.info(f"No data to download for {filenameFinal}")
                continue
        else:
            actual_Monthly = datetime(dt_start.year, dt_start.month, 1)
            actual_Daily = dtIniMes

            only_Daily = dtHoje.month == dt_start.month and dtHoje.year == dt_start.year
            only_Monthly = dtIniMes > dt_end
        logger.info(f"actual_Daily = {actual_Daily.strftime('%Y.%m.%d')} {actual_Daily.strftime('%H:%M:%S')}")
        logger.info(f"actual_Monthly = {actual_Monthly.strftime('%Y.%m.%d')} {actual_Monthly.strftime('%H:%M:%S')}")
        logger.info(f"only_Daily = {only_Daily}")
        logger.info(f"only_Monthly = {only_Monthly}")


        max_Monthly = datetime(dt_end.year, dt_end.month, 1)

        max_Daily = dt_end


        if max_Monthly == dtIniMes:

            max_Monthly = max_Monthly - timedelta(days=1)
            daysAux = monthrange(max_Monthly.year, max_Monthly.month)[1]
            date_add = timedelta(days=daysAux-1)
            max_Monthly = max_Monthly - date_add
            logger.info(f"Adjusting max monthly: {actual_Monthly.strftime('%Y.%m.%d')} {actual_Monthly.strftime('%H:%M:%S')}")


        if exchange == "binance":

            while actual_Monthly <= max_Monthly and not only_Daily:

                logger.info(f"Downloading month: {actual_Monthly.strftime('%Y.%m.%d')} {actual_Monthly.strftime('%H:%M:%S')} started")


                filename = idAsset + "-" + possibleTimeFrames[iCount] + "-" + actual_Monthly.strftime("%Y-%m") + ".zip"
                filenameOrig = idAsset + "-" + possibleTimeFrames[iCount] + "-" + actual_Monthly.strftime("%Y-%m") + ".csv"
                url = url_prefix_Monthly + idAsset + "/" + possibleTimeFrames[iCount]+ "/" + filename

                logger.info(url)

                # Downloading the file by sending the request to the URL
                req = requests.get(url)
            
                # Split URL to get the file name
                #filename = url.split('/')[-1]
                
                # Writing the file to the local file system
                #with open(filename,'wb') as output_file:
                #    output_file.write(req.content)
                logger.info(f"Downloading month: {actual_Monthly.strftime('%Y.%m.%d')} {actual_Monthly.strftime('%H:%M:%S')} completed")

#                logger.info(f"status_code=[{req.status_code}]")
#                logger.info(f"type=[{req.headers.get('Content-Type')}]")
#                logger.info(f"bytes=[{len(req.content)}]")
                # extracting the zip file contents
                if(req.status_code == 200):
                    logger.info(f" - Extraindo de zip: {filename}")
                    zf= zipfile.ZipFile(BytesIO(req.content))
                    zf.extractall('./downloads/') #/Users/joseluiz/Documents/trade_codes/crypto')

                    logger.info(f" - lendo arquivo csv +c append final: {filenameOrig}")
                    fOrig = open('./downloads/'+filenameOrig,'r')

                    f = open("/glustervol1/work/" +filenameFinal,'a')
                    #f.write(fOrig.read())
                    f.writelines("%s" % fOrig.read())
                    #f.writelines("%s\n" % t for t in cotacoes)
                    f.close()

                daysAux = monthrange(actual_Monthly.year, actual_Monthly.month)[1]
                date_add = timedelta(days=daysAux)
                actual_Monthly = actual_Monthly + date_add

                if(req.status_code == 200):
                    actual_Daily = actual_Monthly


            #actual_Daily = min_Daily
            while actual_Daily <= max_Daily and not only_Monthly and not possibleTimeFrames[iCount] == "1w":

                logger.info(datetime.now().strftime("%Y.%m.%d") + " " + datetime.now().strftime("%H:%M:%S") + "Downloading day: " + actual_Daily.strftime("%Y.%m.%d") + " " + actual_Daily.strftime("%H:%M:%S") + " started")


                filename = idAsset + "-" + possibleTimeFrames[iCount] + "-" + actual_Daily.strftime("%Y-%m-%d") + ".zip"
                filenameOrig = idAsset + "-" + possibleTimeFrames[iCount] + "-" + actual_Daily.strftime("%Y-%m-%d") + ".csv"
                url = url_prefix_Daily + idAsset + "/" + possibleTimeFrames[iCount]+ "/" + filename

                logger.info(url)

                # Downloading the file by sending the request to the URL
                req = requests.get(url)
                
                # Split URL to get the file name
                #filename = url.split('/')[-1]
                
                # Writing the file to the local file system
                #with open(filename,'wb') as output_file:
                #    output_file.write(req.content)
                logger.info(f"Downloading day: {actual_Daily.strftime('%Y.%m.%d')} {actual_Daily.strftime('%H:%M:%S')} completed")

#                logger.info(f"status_code=[{req.status_code}]")
#                logger.info(f"type=[{req.headers.get('Content-Type')}]")
#                logger.info(f"bytes=[{len(req.content)}]")

                # extracting the zip file contents
                if(req.status_code == 200):
                    logger.info(f" - Extraindo de zip: {filename}")
                    # extracting the zip file contents
                    zf= zipfile.ZipFile(BytesIO(req.content))
                    zf.extractall('./downloads/') #/Users/joseluiz/Documents/trade_codes/crypto')


                    logger.info(f" - lendo arquivo csv +c append final: {filenameOrig}")
                    fOrig = open('./downloads/'+filenameOrig,'r')

                    f = open("/glustervol1/work/" +filenameFinal,'a+')
                    #f.write(fOrig.read())
                    f.writelines("%s" % fOrig.read())
                    #f.writelines("%s\n" % t for t in cotacoes)
                    f.close()

                daysAux = 1
                date_add = timedelta(days=daysAux)
                actual_Daily = actual_Daily + date_add
            
            if (possibleTimeFrames[iCount] == "1w" and not os.path.isfile("/glustervol1/work/" + filenameFinal)):

                f = open("/glustervol1/work/" +filenameFinal,'a+')
                f.close()

            logger.info(f" - finalizado append no arquivo csv para pasta distribuida o arquivo: {filenameFinal}")
            #copyfile("/work/" + filenameFinal,"/glustervol1/work/" + filenameFinal)

        #TO-DO: implementar para outras exchanges
        if exchange == "coinex":
            logger.info("Exchange coinex not implemented yet.")

def sync_downloaded_files(symbol: str, exchange: str):
    # TO-DO: implement for other exchanges if needed
    if exchange != "binance":
        logger.warning(f"Sync not implemented for exchange: {exchange}")
        return
    # primeiro: rsync dos arquivos locais (expande o glob em Python)
    #nodes = ["ubuntu-pi-01.local", "ubuntu-pi-02.local", "ubuntu-pi-03.local", "ubuntu-pi-04.local"]
    nodes = ["spark-master", "spark-worker-1", "spark-worker-2", "spark-worker-3"]
    #nodes = ["192.168.15.119", "192.168.15.107", "192.168.15.101", "192.168.15.43"]
    #nodes = ["node-01", "node-02", "node-03", "node-04"]
    src_pattern = "/glustervol1/work/" + symbol + "*"
    src_files = glob.glob(src_pattern)
    if src_files:
        for node in nodes:
            rsync_cmd = ["rsync", "-avz", "-e", "ssh -i /home/appuser/.ssh/id_etl_rsync"] + src_files + [f"appuser@{node}:/glustervol1/work/"]
            #logger.info(f"Running for node({node}):", " ".join(rsync_cmd))
            rs = subprocess.run(rsync_cmd) #, capture_output=True, text=True)
            logger.info(f"rsync returncode for node({node})={rs.returncode}")
            if rs.returncode != 0:
                logger.error(f"rsync stderr for node({node}): {rs.stderr}")
    else:
        logger.warning(f"Nenhum arquivo encontrado para o padrão: {src_pattern}")

def submit_spark_job(symbol: str, exchange: str):
    #TO-DO: implementar exchange se necessário
    if exchange != "binance":
        logger.warning(f"Spark job not implemented for exchange: {exchange}")
        return

    
    spark_submit_cmd = ["python3", "/app/SparkJob.py", "--symbol", symbol, "--exchange", exchange]
    #spark_submit_cmd = ["spark-submit", "--py-files", "/app/dao.zip", "/app/SparkJob.py"]
    #logger.info(f"Running spark-submit: " .join(spark_submit_cmd))
    rs = subprocess.run(spark_submit_cmd) #, capture_output=True, text=True)
    logger.info(f"spark-submit returncode: {rs.returncode}")
    if rs.returncode != 0:
        logger.error(f"spark-submit stderr: {rs.stderr}")


def loop_download_all():
    url = "http://crypto-trader:8091/cryptoassets"

    response = requests.get(url)
    assets = response.json()
    logger.info(f"Total assets to process: {len(assets)}")

    for asset in assets:
        exchange = asset["id"].split("_")[0]
        symbol = asset["symbol"]            
        etl_download(symbol, exchange, datetime(2025, 1, 1), datetime.now())
        sync_downloaded_files(symbol, exchange)
        submit_spark_job(symbol, exchange)

def loop_download_all_decoupled():
    url = "http://crypto-trader:8091/cryptoassets"

    response = requests.get(url)
    assets = response.json()
    logger.info(f"Total assets to process: {len(assets)}")

    for asset in assets:
        exchange = asset["id"].split("_")[0]
        symbol = asset["symbol"]
        add_to_download_queue(symbol, exchange)
    logger.info("✅ All ETL + sync queued in RabbitMQ to donload and sync after!")

def reload_queue_when_all_empty():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel_spark = connection.channel()
    channel_spark.queue_declare(queue=QUEUE_SPARK_JOB_NAME, durable=True)

    if channel_spark.message_count() == 0:
        logger.info("⏳ No more messages in Spark job queue, triggering download for all assets in queue just in case...")
        loop_download_all_decoupled()


def add_to_spark_job_queue(symbol: str, exchange: str, priority: int=0):
    """Publica spark job no RabbitMQ para o consumer processar depois"""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()
        
        channel.queue_declare(queue=QUEUE_SPARK_JOB_NAME, durable=True)
        
        message = {"symbol": symbol, "exchange": exchange, "priority": priority}
        
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_SPARK_JOB_NAME,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2,priority=0)  # persistent
        )
        connection.close()
        logger.info(f"📨 Published Spark job → {symbol} ({exchange} {priority})")
        
    except Exception as e:
        logger.error(f"❌ Failed to publish {symbol}: {e}")


def add_to_download_queue(symbol: str, exchange: str, priority: int = 0):
    """Publica ETL Download no RabbitMQ para o consumer processar depois"""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()
        
        channel.queue_declare(queue=QUEUE_HIST_ASSETS_NAME, durable=True)
        
        message = {"symbol": symbol, "exchange": exchange, "priority": priority}
        
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_HIST_ASSETS_NAME,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2, priority=0)  # persistent
        )
        connection.close()
        logger.info(f"📨 Published ETL Download job → {symbol} ({exchange} {priority})")
        
    except Exception as e:
        logger.error(f"❌ Failed to publish {symbol}: {e}")


def callback_normal(ch, method, properties, body):
    try:
        data = json.loads(body)
        symbol = data["symbol"]
        exchange = data["exchange"]
        priority = data["priority"]

        logger.info(f"🔥 Received New (normal) Symbol to do ETL Download → {symbol} ({exchange} {priority})")

        etl_download(symbol, exchange, datetime(2025, 1, 1), datetime.now())
        sync_downloaded_files(symbol, exchange)

        add_to_spark_job_queue(symbol, exchange,priority)  # prioridade 10 para os novos assets, para serem processados antes dos demais que estão na fila com prioridade 0

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"✅ ETL do (normal -new symbol) completed: {symbol}\n")
        #logger.info(f"# Qtde messages in queue normal: {ch.message_count()}")
        # Example: react only when we know queues are empty
#        if should_trigger_full_reload.is_set():
#            logger.info("Detected both queues drained → running full reload / final sync")
#            loop_download_all_decoupled()   # or whatever final action
#            should_trigger_full_reload.clear()  # prevent repeating

#        if ch.() == 0:
#            logger.info("⏳ No more messages in queue, triggering download for all assets in queue just in case...")
#            loop_download_all_decoupled()

    except Exception as e:
        logger.error(f"❌ Error processing {body}: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)  # retry later

def start_consumer():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
            channel_normal = connection.channel()
            channel_normal.queue_declare(queue=QUEUE_HIST_ASSETS_NAME, durable=True)
            channel_normal.basic_qos(prefetch_count=1)   # process one at a time

            channel_normal.basic_consume(queue=QUEUE_HIST_ASSETS_NAME, on_message_callback=callback_normal)
            logger.info("✅ Spark Consumer connected to RabbitMQ and consuming queues!")   
            logger.info(f"⏳ Waiting for messages in queue {QUEUE_HIST_ASSETS_NAME} (normal - existing symbols)...")          
            logger.info("⏳ If no messages arrive in 5s, the consumer will automatically trigger a download for all assets in queue...")  
            #logger.info(f"# Qtde messages in queue normal: {result.method.message_count}")
            channel_normal.start_consuming()
            time.sleep(5)

        except Exception as e:
            logger.error(f"Connection lost, retrying in 5s... ({e})")
            time.sleep(5)


def queue_monitor_thread():
    while True:
        try:

            logger.info("Checking if Both queues are EMPTY...")
            conn = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
            ch = conn.channel()

            for qname in [QUEUE_HIST_ASSETS_NAME, QUEUE_SPARK_JOB_NAME]:
                try:
                    res = ch.queue_declare(queue=qname, passive=True)
                    count = res.method.message_count
                    with check_lock:
                        last_known_counts[qname] = count
                except pika.exceptions.ChannelClosedByBroker as e:
                    if e.reply_code == 404:
                        last_known_counts[qname] = 0
                    else:
                        raise

            conn.close()

            # Decide
            with check_lock:
                if last_known_counts[QUEUE_HIST_ASSETS_NAME] == 0 and \
                   last_known_counts[QUEUE_SPARK_JOB_NAME] == 0:
                    logger.info("Both queues EMPTY → triggering full logic / reload / sync-all")
 #                   should_trigger_full_reload.set()   # or call function directly

                    logger.info("Loading all assets in queue...")
                    loop_download_all_decoupled()
        except Exception as e:
            logger.error(f"Queue monitor failed: {e}")

        time.sleep(15)   # tune: 5–15 seconds is usually fine


# ────────────────────────────────────────────────
# In your main / start_consumer()
# ────────────────────────────────────────────────

if __name__ == "__main__":

    # Start background checker
    monitor_thread = threading.Thread(target=queue_monitor_thread, daemon=True)
    monitor_thread.start()

    # Optional: wait once for initial empty state if desired
    # time.sleep(10)

    start_consumer()




#loop_download_all()

#etl_download("BTCUSDT", "binance", datetime(2025, 1, 1), datetime.now())
#sync_downloaded_files("BTCUSDT", "binance")
#submit_spark_job("BTCUSDT", "binance")