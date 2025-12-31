
from datetime import datetime, timedelta
from calendar import monthrange
import requests
import zipfile
import os
from io import BytesIO

import subprocess
import glob
import os

possibleTimeFrames = ["15m","30m","1h","4h","1d","1w"]

url_prefix_Daily = "https://data.binance.vision/data/spot/daily/klines/"
url_prefix_Monthly = "https://data.binance.vision/data/spot/monthly/klines/"


def get_last_timestamp(filename: str, bSemanal: bool) -> datetime:
    """
    Lê a última linha do arquivo CSV e retorna o timestamp (primeiro token) como datetime.
    O timestamp está em milissegundos.
    """
    try:
        filepath = "/glustervol1/work/" + filename
        if not os.path.isfile(filepath):
            print(f"Arquivo não encontrado: {filepath}")
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
        print(f"Erro ao ler timestamp: {e}")
        return None


def etl_download(symbol: str, exchange: str, dt_start: datetime = None, dt_end: datetime = None):

    if dt_start is None:
        dt_start = datetime.now()  
    if dt_end is None:
        dt_end = datetime.now()

    # ...restante da função...
    print("dt_start =", dt_start, "dt_end =", dt_end)

    dtHoje = datetime.now()
    dtIniMes = datetime(dtHoje.year, dtHoje.month, 1)

    qtdeLines = len(possibleTimeFrames)

    idAsset = symbol

    for iCount in range(qtdeLines):
        print(idAsset)
        print(possibleTimeFrames[iCount])

        filenameFinal = idAsset + "-" + possibleTimeFrames[iCount] + ".csv"
        lastdatetimeloaded = get_last_timestamp(filenameFinal, possibleTimeFrames[iCount] == "1w")
        print("lastdatetimeloaded =", lastdatetimeloaded)
        if lastdatetimeloaded is not None:
            actual_Daily = lastdatetimeloaded + timedelta(days=1)
            actual_Monthly = datetime(actual_Daily.year, actual_Daily.month, 1)


            only_Daily = dtHoje.month == actual_Daily.month and dtHoje.year == actual_Daily.year
            only_Monthly = datetime(dtHoje.year, dtHoje.month, 1) > dt_end
            if only_Daily and only_Monthly:
                print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "No data to download for " + filenameFinal)
                continue
        else:
            actual_Monthly = datetime(dt_start.year, dt_start.month, 1)
            actual_Daily = dtIniMes

            only_Daily = dtHoje.month == dt_start.month and dtHoje.year == dt_start.year
            only_Monthly = dtIniMes > dt_end

        print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + actual_Daily.strftime(" actual_Daily = %Y.%m.%d\t%H:%M:%S"))
        print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + actual_Monthly.strftime(" actual_Monthly = %Y.%m.%d\t%H:%M:%S"))      
        print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "only_Daily = " + str(only_Daily))
        print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "only_Monthly = " + str(only_Monthly))


        max_Monthly = datetime(dt_end.year, dt_end.month, 1)

        max_Daily = dt_end


        if max_Monthly == dtIniMes:

            max_Monthly = max_Monthly - timedelta(days=1)
            daysAux = monthrange(max_Monthly.year, max_Monthly.month)[1]
            date_add = timedelta(days=daysAux-1)
            max_Monthly = max_Monthly - date_add
            print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Adjusting max monthly: " + max_Monthly.strftime("%Y.%m.%d\t%H:%M:%S") + " adjusted")


        if exchange == "binance":

            while actual_Monthly <= max_Monthly and not only_Daily:

                print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Downloading month: " + actual_Monthly.strftime("%Y.%m.%d\t%H:%M:%S") + " started")

                filename = idAsset + "-" + possibleTimeFrames[iCount] + "-" + actual_Monthly.strftime("%Y-%m") + ".zip"
                filenameOrig = idAsset + "-" + possibleTimeFrames[iCount] + "-" + actual_Monthly.strftime("%Y-%m") + ".csv"
                url = url_prefix_Monthly + idAsset + "/" + possibleTimeFrames[iCount]+ "/" + filename

                print(url)

                # Downloading the file by sending the request to the URL
                req = requests.get(url)
            
                # Split URL to get the file name
                #filename = url.split('/')[-1]
                
                # Writing the file to the local file system
                #with open(filename,'wb') as output_file:
                #    output_file.write(req.content)
                print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Downloading month: " + actual_Monthly.strftime("%Y.%m.%d\t%H:%M:%S") + " Completed")

#                print("status_code=["+str(req.status_code)+"]")
#                print("type=["+str(req.headers.get("Content-Type"))+"]")
#                print("bytes=["+str(len(req.content))+"]")
                # extracting the zip file contents
                if(req.status_code == 200):
                    print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + " - Extraindo de zip:" + filename)
                    zf= zipfile.ZipFile(BytesIO(req.content))
                    zf.extractall('./downloads/') #/Users/joseluiz/Documents/trade_codes/crypto')

                    print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + " - lendo arquivo csv +c append final:" + filenameOrig)
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

                print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Downloading day: " + actual_Daily.strftime("%Y.%m.%d\t%H:%M:%S") + " started")


                filename = idAsset + "-" + possibleTimeFrames[iCount] + "-" + actual_Daily.strftime("%Y-%m-%d") + ".zip"
                filenameOrig = idAsset + "-" + possibleTimeFrames[iCount] + "-" + actual_Daily.strftime("%Y-%m-%d") + ".csv"
                url = url_prefix_Daily + idAsset + "/" + possibleTimeFrames[iCount]+ "/" + filename

                print(url)

                # Downloading the file by sending the request to the URL
                req = requests.get(url)
                
                # Split URL to get the file name
                #filename = url.split('/')[-1]
                
                # Writing the file to the local file system
                #with open(filename,'wb') as output_file:
                #    output_file.write(req.content)
                print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + "Downloading day: " + actual_Daily.strftime("%Y.%m.%d\t%H:%M:%S") + " Completed")

#                print("status_code=["+str(req.status_code)+"]")
#                print("type=["+str(req.headers.get("Content-Type"))+"]")
#                print("bytes=["+str(len(req.content))+"]")

                # extracting the zip file contents
                if(req.status_code == 200):
                    print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + " - Extraindo de zip:" + filename)
                    # extracting the zip file contents
                    zf= zipfile.ZipFile(BytesIO(req.content))
                    zf.extractall('./downloads/') #/Users/joseluiz/Documents/trade_codes/crypto')


                    print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + " - lendo arquivo csv +c append final:" + filenameOrig)
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

            print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + " - finalizado append no arquivo csv para pasta distribuida o arquivo:" + filenameFinal)
            #copyfile("/work/" + filenameFinal,"/glustervol1/work/" + filenameFinal)

        #TO-DO: implementar para outras exchanges
        if exchange == "coinex":
            print("Exchange coinex not implemented yet.")

def sync_downloaded_files(symbol: str, exchange: str):
    # TO-DO: implement for other exchanges if needed
    if exchange != "binance":
        print("Sync not implemented for exchange:", exchange)
        return
    # primeiro: rsync dos arquivos locais (expande o glob em Python)
    #nodes = ["ubuntu-pi-01.local", "ubuntu-pi-02.local", "ubuntu-pi-03.local", "ubuntu-pi-04.local"]
    nodes = ["spark-master", "spark-worker-1", "spark-worker-2", "spark-worker-3"]
    src_pattern = "/glustervol1/work/" + symbol + "*"
    src_files = glob.glob(src_pattern)
    if src_files:
        for node in nodes:
            rsync_cmd = ["rsync", "-av", "-e", "ssh"] + src_files + [f"root@{node}:/glustervol1/work/"]
            print(f"Running for node({node}):", " ".join(rsync_cmd))
            rs = subprocess.run(rsync_cmd, capture_output=True, text=True)
            print(f"rsync returncode for node({node})=", rs.returncode)
            if rs.returncode != 0:
                print(f"rsync stderr for node({node}):", rs.stderr)
    else:
        print(f"Nenhum arquivo encontrado para o padrão: {src_pattern}")

def submit_spark_job(symbol: str, exchange: str):
    #TO-DO: implementar exchange se necessário
    if exchange != "binance":
        print("Spark job not implemented for exchange:", exchange)
        return


    spark_submit_cmd = ["python", "/app/SparkJob.py", symbol]
    #spark_submit_cmd = ["spark-submit", "--py-files", "/app/dao.zip", "/app/SparkJob.py"]
    print(f"Running spark-submit:", " ".join(spark_submit_cmd))
    rs = subprocess.run(spark_submit_cmd, capture_output=True, text=True)
    print(f"spark-submit returncode:", rs.returncode)
    if rs.returncode != 0:
        print(f"spark-submit stderr:", rs.stderr)


def loop_download_all():
    url = "http://crypto-trader:8091/cryptoassets"

    response = requests.get(url)
    assets = response.json()
    print(len(assets))

    for asset in assets:
        etl_download(asset["symbol"], asset["exchange"])
        sync_downloaded_files(asset["symbol"], asset["exchange"])
        submit_spark_job(asset["symbol"], asset["exchange"])

#loop_download_all()

etl_download("BTCUSDT", "binance", datetime(2025, 1, 1), datetime(2025, 12, 30))
sync_downloaded_files("BTCUSDT", "binance")
submit_spark_job("BTCUSDT", "binance")