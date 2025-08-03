from datetime import datetime, timedelta
from shutil import copyfile

bForceTIllStoppedEarlier = False
symbolStoppedEarlier = "VETBTC"
timeFrameStoppedEarlier = "30m"

possibleTimeFrames = ["30m","1h","4h","1d","1w"]
#id_moedas = [ "MATICUSDT","ETHBTC", "BTCUSDT"]

url_prefix_Daily = "https://data.binance.vision/data/spot/daily/klines/"

import urllib.request
import json


url = "http://crypto-trader:8091/cryptoassets"

response = urllib.request.urlopen(url).read()
assets = json.loads(response)
print(len(assets))


dtAgora = datetime.now()
date_add = timedelta(days=-1)
actual_Daily = dtAgora + date_add
# importing the requests module
import requests

qtdeLines = len(possibleTimeFrames)


for asset in assets:
    idAsset = asset["symbol"]
    for iCount in range(qtdeLines):


        print(bForceTIllStoppedEarlier)
        print(idAsset)
        print(possibleTimeFrames[iCount])


        if(idAsset==symbolStoppedEarlier and possibleTimeFrames[iCount]==timeFrameStoppedEarlier):
            bForceTIllStoppedEarlier = False

        print(bForceTIllStoppedEarlier)

        if not(bForceTIllStoppedEarlier) and asset["exchange"] == "binance":

            print('Downloading started')
            filename = idAsset + "-" + possibleTimeFrames[iCount] + "-" + actual_Daily.strftime("%Y-%m-%d") + ".zip"
            filenameOrig = idAsset + "-" + possibleTimeFrames[iCount] + "-" + actual_Daily.strftime("%Y-%m-%d") + ".csv"
            filenameFinal = idAsset + "-" + possibleTimeFrames[iCount] + ".csv"
            url = url_prefix_Daily + idAsset + "/" + possibleTimeFrames[iCount]+ "/" + filename

            #filename = idAsset + "-" + possibleTimeFrames[0] + "-" + max_Monthly.strftime("%Y-%m") + ".zip"
            #url = url_prefix_Monthly + idAsset + "/" + possibleTimeFrames[0]+ "/" + filename

            print(url)

            # Downloading the file by sending the request to the URL
            req = requests.get(url)

            if (req.status_code == 200):
                # Split URL to get the file name
                #filename = url.split('/')[-1]

                # Writing the file to the local file system
                #with open(filename,'wb') as output_file:
            #    output_file.write(req.content)
                print('Downloading Completed')

                # importing necessary modules
                import zipfile
                from io import BytesIO

                print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + " - Extraindo de zip:" + filename)
                # extracting the zip file contents
                zipfile= zipfile.ZipFile(BytesIO(req.content))
                zipfile.extractall('./downloads/') #/Users/joseluiz/Documents/trade_codes/crypto')


                print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + " - lendo arquivo csv +c append final:" + filenameOrig)
                fOrig = open('./downloads/'+filenameOrig,'r')

                f = open("/work/" +filenameFinal,'a')
                #f.write(fOrig.read())
                f.writelines("%s" % fOrig.read())
                #f.writelines("%s\n" % t for t in cotacoes)
                f.close()

                print(datetime.now().strftime("%Y.%m.%d\t%H:%M:%S") + " - copiando arquivo csv para pasta distribuida o arquivo:" + filenameFinal)
                copyfile("/work/" + filenameFinal,"/glustervol1/work/" + filenameFinal)

