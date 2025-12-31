from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, FloatType, NumericType, TimestampType
from pyspark.sql.functions import to_timestamp, concat, lit, avg, first, last, when, max
from pyspark.sql import Window, DataFrame
from datetime import datetime

#TO-DO ALL: comentar todos os métodos:

def de_para_crypto_database(timeframe) -> str:
  # De-Para dos timeframes bas bases da Binance para o nome das colections criadas no mongo:
  # M30...(30 minutos):
  if timeframe == "30m":
      return "crypto.m30"
  # H1...(60 minutos):
  elif timeframe == "1h":
      return "crypto.h1"
  # H4...(4 horas):
  elif timeframe == "4h":
      return "crypto.h4"
  # d1...(1 dia):
  elif timeframe == "1d":
      return "crypto.d1"
  # w1...(1 semana):
  elif timeframe == "1w":
      return "crypto.w1"
  return ""

  
def load_crypto_to_mongo(ind_curr, timeframe, spark, dfAux1, cargaZero):

  mongo_collection = de_para_crypto_database(timeframe)

  load_markets_to_mongo(ind_curr, mongo_collection, spark, dfAux1, cargaZero)


  
def load_markets_to_mongo(ind_curr, mongo_collection, spark, dfDadosOrigem, cargaZero):

  if not cargaZero:

    dfMongoAux = read_data_from_mongo ("markets", mongo_collection, spark)

    dfMongoAux0 = dfMongoAux.filter(dfMongoAux["index"] == lit(ind_curr))

    dfMongoAux1 = spark.sparkContext.parallelize(dfMongoAux0.orderBy(dfMongoAux0["cuote_timestamp"].desc()).collect(),3).collect()

    close_row = dfMongoAux1[0]['cuote_timestamp']

    #close_row = dfMongoAux0.groupBy().max("cuote_timestamp").collect()[0][0]
    print("close_row: " + str(close_row))

    dfAux2 = dfDadosOrigem.filter(dfDadosOrigem["cuote_timestamp"] > close_row)

    append_data_to_mongo("markets",mongo_collection,dfAux2)

    #dfAux3 = dfMongoAux0.drop("_id").union(dfAux2)

    #return dfAux3
  else:

    append_data_to_mongo("markets",mongo_collection,dfDadosOrigem)

    #return dfAux1


def update_data_to_mongo(mongo_database:str, mongo_collection:str, dfDadosParam:DataFrame):

    dfDadosParam.write.format("mongodb").mode("append").option("database",
    mongo_database).option("collection", mongo_collection).option("replaceDocument", "true").save()


def read_market_from_mongo(index:str, timeframe:str, spark:SparkSession) -> DataFrame:

    mongo_collection = de_para_b3_database(timeframe)

    dfMongoAux = read_data_from_mongo ("markets", mongo_collection, spark)

    dfMongoAux0 = dfMongoAux.filter(dfMongoAux["index"] == lit(index))

    return dfMongoAux0


def read_data_from_mongo (mongo_database:str, mongo_collection:str, spark:SparkSession) -> DataFrame:

    dfMongoAux = spark.read.format("mongodb").option("database",
    mongo_database).option("collection", mongo_collection).load()

    return dfMongoAux

def append_data_to_mongo (mongo_database:str, mongo_collection:str, dfDadosOrigem:DataFrame):

    dfDadosOrigem.write.format("mongodb").mode("append").option("database",
  mongo_database).option("collection", mongo_collection).save()



#def update_asset_to_mongo(index:str, description:str,	volume:float, 	tickVolume:float, status:str, sc:SparkContext):
#    data = [{"_id" : index, "description" : description, "volume" : volume, "tickVolume": tickVolume, "startTimestamp": datetime.now(), "status":status}]
#    dfAux0 = sc.parallelize(data).toDF()
#
#    mongo_collection = "markets.crypto.assets"
#    mongo_database = "models"
#
#    update_data_to_mongo(mongo_database, mongo_collection, dfAux0)

def read_assets_from_mongo(spark:SparkSession) -> list:

    dfMongoAux = read_data_from_mongo ("models", "markets.crypto.assets", spark)
    dfMongoAux0 = dfMongoAux.filter(dfMongoAux["status"] == lit("open"))

    dfAssets = spark.sparkContext.parallelize(dfMongoAux0.collect(),3).collect()

    return dfAssets
