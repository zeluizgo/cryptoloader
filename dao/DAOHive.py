from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from pyspark.sql import  DataFrame

#TO-DO ALL: comentar todos os métodos:

def de_para_crypto_database(timeframe) -> str:
  # De-Para dos timeframes bas bases da Binance para o nome das colections criadas no mongo:
  # M30...(30 minutos):
  if timeframe == "15m":
      return "binance_daily_hist_m15"
  # M30...(30 minutos):
  if timeframe == "30m":
      return "binance_daily_hist_m30"
  # H1...(60 minutos):
  elif timeframe == "1h":
      return "binance_daily_hist_h1"
  # H4...(4 horas):
  elif timeframe == "4h":
      return "binance_daily_hist_h4"
  # d1...(1 dia):
  elif timeframe == "1d":
      return "binance_monthly_hist_d1"
  # w1...(1 semana):
  elif timeframe == "1w":
      return "binance_monthly_hist_w1"
  return ""

#def read_market_from_hive(index:str, timeframe:str, spark:SparkSession) -> DataFrame:

#    hive_table = de_para_crypto_database(timeframe)

#    dfHiveAux = read_data_from_hive ("markets_crypto", hive_table, spark)

#    dfHiveAux0 = dfHiveAux.filter(dfHiveAux["index"] == lit(index))

#    return dfHiveAux0


def read_market_lastpartition_from_hive (hive_database:str, hive_table:str, ind_curr:str, spark:SparkSession) -> DataFrame:

  # Step 1: Get the list of partitions for the specific index
  partitions = spark.sql("""
      SHOW PARTITIONS {}.{}
      WHERE index = '{}'
  """.format(hive_database, hive_table, ind_curr)).collect()

  if hive_table == "binance_monthly_hist_w1" or hive_table == "binance_monthly_hist_d1": # Step 2: Parse partitions to find the most recent year and month
    # Partitions are in the format "index=BTCUSD/year=2025/month=03"
    #latest_partition = None
    latest_year_month = None
    if partitions:
        # Extract year and month from partitions and find the latest
        partition_values = [
            (row["partition"].split("/")[1].split("=")[1], row["partition"].split("/")[2].split("=")[1])
            for row in partitions
        ]
        # Sort by year and month (assuming month is zero-padded, e.g., '03')
        latest_year_month = max(partition_values, key=lambda x: (x[0], x[1]))
        latest_year, latest_month = latest_year_month

        return spark.read.table(hive_database + "."+ hive_table).filter(
            (col("index") == lit(ind_curr)) & (col("cuote_year") == lit(latest_year)) & (col("cuote_month") == lit(latest_month))
        )

  else:
    # Step 2: Parse partitions to find the most recent date
    # Partitions are in the format "index=BTCUSD/date=2025-08-08"
    latest_date = None
    if partitions:
        partition_dates = [row["partition"].split("/")[1].split("=")[1] for row in partitions]
        latest_date = max(partition_dates)  # Latest date in yyyy-MM-dd format
        
        return spark.read.table(hive_database + "."+ hive_table).filter(
            (col("index") == lit(ind_curr)) & (col("cuote_date") == lit(latest_date))
        )


  return spark.read.table(hive_database + "."+ hive_table).filter(
            (col("index") == lit(ind_curr))
        )


def load_crypto_to_hive(ind_curr, timeframe, spark, dfAux1, cargaZero):

  hive_table = de_para_crypto_database(timeframe)

  load_markets_to_hive(ind_curr, hive_table, spark, dfAux1, cargaZero)

  
def load_markets_to_hive(ind_curr, hive_table, spark, dfDadosOrigem, cargaZero):

  if not cargaZero:

    dfHiveAux = read_market_lastpartition_from_hive("markets_crypto", hive_table, ind_curr, spark)


    close_row = dfHiveAux.agg(max("cuote_timestamp").alias("max_timestamp")).collect()[0]["max_timestamp"]

    #close_row = dfHiveAux0.groupBy().max("cuote_timestamp").collect()[0][0]
    print("close_row: " + str(close_row))

    dfAux2 = dfDadosOrigem.filter(dfDadosOrigem["cuote_timestamp"] > close_row)

    append_data_to_hive("markets_crypto",hive_table,dfAux2)

    #dfAux3 = dfHiveAux0.drop("_id").union(dfAux2)

    #return dfAux3
  else:

    if(spark.catalog.tableExists("markets_crypto." + hive_table )):
      spark.sql("alter table markets_crypto." + hive_table + " drop IF EXISTS partition (index = \"" + ind_curr + "\")")

    append_data_to_hive("markets_crypto",hive_table,dfDadosOrigem)

    #return dfAux1


def append_data_to_hive (hive_database:str, hive_table:str, dfDadosOrigem:DataFrame):

  if hive_table == "binance_monthly_hist_w1" or hive_table == "binance_monthly_hist_d1": 
    dfAux0 = dfDadosOrigem.drop("cuote_date")
    dfAux0.write.partitionBy("index","cuote_year","cuote_month").format("parquet").mode("append").saveAsTable(hive_database + "."+ hive_table)
  else:
    dfAux0 = dfDadosOrigem.drop("cuote_year").drop("cuote_month")
    dfAux0.write.partitionBy("index","cuote_date").format("parquet").mode("append").saveAsTable(hive_database + "."+ hive_table)
