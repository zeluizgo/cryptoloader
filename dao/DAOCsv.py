from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import to_timestamp, lit, from_unixtime, date_format, year, month, lpad, to_date
from pyspark.sql import  DataFrame

#TO-DO ALL: comentar todos os métodos:

def read_binance_csv(ind_curr:str, timeframe:str, spark:SparkSession) -> DataFrame:

  file_path = "file:///glustervol1/work/" + ind_curr + "-" +  timeframe + ".csv"

  dfDailySchema = StructType([\
      StructField('cuote_opentime',LongType(),True),\
      StructField('cuote_open',DoubleType(),True),\
      StructField('cuote_high',DoubleType(),True),\
      StructField('cuote_low',DoubleType(),True),\
      StructField('cuote_close',DoubleType(),True),\
      StructField('volume',DoubleType(),True),\
      StructField('cuote_closetime',LongType(),True),\
      StructField('asset_volume',DoubleType(),True),\
      StructField('qtde_trades',IntegerType(),True),\
      StructField('buy_volume',DoubleType(),True),\
      StructField('buy_asset_volume',DoubleType(),True),\
      StructField('ignore',StringType(),True) \
    ])

  dfSchema = dfDailySchema

  dfAux0 = spark.read.options(header='False', delimiter=',') \
    .schema(dfSchema) \
    .csv(file_path)
  #dfAux0.show()
  
  seconds_to_millis = 1000000
  if timeframe == "1w":
    seconds_to_millis = 1000

  #dfAux1 = dfAux0.withColumn('cuote_timestamp',  timestamp_millis(dfAux0['cuote_opentime']/1000)) \
  dfAux1 = dfAux0.withColumn('cuote_timestamp',  to_timestamp(from_unixtime(dfAux0['cuote_opentime']/seconds_to_millis),'yyyy-MM-dd HH:mm:ss')) \
                .withColumn('index', lit(ind_curr)) \
                .drop("cuote_opentime","cuote_closetime")
  #dfAux1.show()
  dfAux2 = dfAux1.withColumn('cuote_date', to_date(date_format(dfAux1['cuote_timestamp'], 'yyyy-MM-dd'))) \
                .withColumn('cuote_year', year(dfAux1['cuote_timestamp'])) \
                .withColumn('cuote_month', month(dfAux1['cuote_timestamp']))
  #dfAux2.show()
  return dfAux2
