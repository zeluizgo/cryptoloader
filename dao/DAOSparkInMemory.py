from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, max
import pyspark.sql.functions as F
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


def get_latest_partition_date(spark, table, index_value):
    parts = spark.sql(f"SHOW PARTITIONS {table}")
    return (
        parts
        .filter(F.col("partition").startswith(f"index={index_value}/"))
        .select(
            F.regexp_extract("partition", r"cuote_date=([0-9\-]+)", 1)
            .alias("cuote_date")
        )
        .agg(F.max("cuote_date").alias("latest"))
        .collect()[0]["latest"]
    )


def get_latest_partition_year_month(spark, table, index_value):
    parts = spark.sql(f"SHOW PARTITIONS {table}")
    return (
         parts
        .filter(F.col("partition").startswith(f"index={index_value}/"))
        .select(
            (
                F.regexp_extract("partition", r"cuote_year=([0-9]{4})", 1).cast("int") * 100 +
                F.regexp_extract("partition", r"cuote_month=([0-9]{2})", 1).cast("int")
            ).alias("yyyymm")
        )
        .agg(F.max("yyyymm").alias("latest"))
        .collect()[0]["latest"]
    )



def read_market_lastpartition_from_hdfs (database:str, table:str, ind_curr:str, spark:SparkSession) -> DataFrame:

  if table == "binance_monthly_hist_w1" or table == "binance_monthly_hist_d1": # Step 2: Parse partitions to find the most recent year and month
    # Partitions are in the format "index=BTCUSD/year=2025/month=03"
    #latest_partition = None
    latest_year_month = get_latest_partition_year_month(
    spark,f"{database}.{table}",
    ind_curr
    )
    if latest_year_month:
        latest_year, latest_month = divmod(latest_year_month, 100)  # Latest year and month as integers

        return spark.read.table(database + "."+ table).filter(
            (col("index") == lit(ind_curr)) & (col("cuote_year") == lit(latest_year)) & (col("cuote_month") == lit(latest_month))
        )

  else:
    # Step 2: Parse partitions to find the most recent date
    # Partitions are in the format "index=BTCUSD/date=2025-08-08"
    latest_date = get_latest_partition_date(
    spark,f"{database}.{table}",
    ind_curr
    )
    if latest_date:
        return spark.read.table(database + "."+ table).filter(
            (col("index") == lit(ind_curr)) & (col("cuote_date") == lit(latest_date))
        )


  return None


def load_crypto_to_hdfs(ind_curr, timeframe, spark, dfAux1, cargaZero):

  table = de_para_crypto_database(timeframe)

  load_markets_to_hdfs(ind_curr, table, spark, dfAux1, cargaZero)

  
def load_markets_to_hdfs(ind_curr, table, spark, dfDadosOrigem, cargaZero):

  if not cargaZero:

    dfHiveAux = read_market_lastpartition_from_hdfs("crypto", table, ind_curr, spark)

    if dfHiveAux is not None and dfHiveAux.count() > 0:

      close_row = dfHiveAux.agg(max("cuote_timestamp").alias("max_timestamp")).collect()[0]["max_timestamp"]

      #close_row = dfHiveAux0.groupBy().max("cuote_timestamp").collect()[0][0]
      print("close_row: " + str(close_row))

      dfAux2 = dfDadosOrigem.filter(dfDadosOrigem["cuote_timestamp"] > close_row)

      append_data_to_hdfs("crypto",table,dfAux2)
    else:
      append_data_to_hdfs("crypto",table,dfDadosOrigem)

    #dfAux3 = dfHiveAux0.drop("_id").union(dfAux2)

    #return dfAux3
  else:

    if(spark.catalog.tableExists("crypto." + table )):
      spark.sql("alter table crypto." + table + " drop IF EXISTS partition (index = \"" + ind_curr + "\")")

    append_data_to_hdfs("crypto",table,dfDadosOrigem)

    #return dfAux1


def append_data_to_hdfs (database:str, table:str, dfDadosOrigem:DataFrame):

  if table == "binance_monthly_hist_w1" or table == "binance_monthly_hist_d1": 
    dfAux0 = dfDadosOrigem.drop("cuote_date")
    dfAux0.write.partitionBy("index","cuote_year","cuote_month").format("parquet").mode("append").saveAsTable(database + "."+ table)
  else:
    dfAux0 = dfDadosOrigem.drop("cuote_year").drop("cuote_month")
    dfAux0.write.partitionBy("index","cuote_date").format("parquet").mode("append").saveAsTable(database + "."+ table)