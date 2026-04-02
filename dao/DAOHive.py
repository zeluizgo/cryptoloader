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


def get_latest_partition_date(spark, table, index_value):
    result = (
        spark.read.table(table)
        .filter(F.col("index") == index_value)
        .agg(F.max("cuote_date").alias("latest"))
        .collect()
    )
    return result[0]["latest"] if result else None


def get_latest_partition_year_month(spark, table, index_value):
    result = (
        spark.read.table(table)
        .filter(F.col("index") == index_value)
        .select(
            (F.col("cuote_year").cast("int") * 100 + F.col("cuote_month").cast("int")).alias("yyyymm")
        )
        .agg(F.max("yyyymm").alias("latest"))
        .collect()
    )
    if result and result[0]["latest"] is not None:
        return result[0]["latest"]
    return None


def read_market_lastpartition_from_hive(database:str, table:str, ind_curr:str, spark:SparkSession) -> DataFrame:

  if not spark.catalog.tableExists(f"{database}.{table}"):
    return None

  if table == "binance_monthly_hist_w1" or table == "binance_monthly_hist_d1":
    latest_year_month = get_latest_partition_year_month(
        spark, f"{database}.{table}",
        ind_curr
    )
    if latest_year_month:
        latest_year, latest_month = divmod(latest_year_month, 100)

        return spark.read.table(database + "." + table).filter(
            (col("index") == lit(ind_curr)) & (col("cuote_year") == lit(latest_year)) & (col("cuote_month") == lit(latest_month))
        )

  else:
    latest_date = get_latest_partition_date(
        spark, f"{database}.{table}",
        ind_curr
    )
    if latest_date:
        return spark.read.table(database + "." + table).filter(
            (col("index") == lit(ind_curr)) & (col("cuote_date") == lit(latest_date))
        )

  return None


def load_crypto_to_hive(ind_curr, timeframe, spark, dfAux1, cargaZero):

  table = de_para_crypto_database(timeframe)

  load_markets_to_hive(ind_curr, table, spark, dfAux1, cargaZero)


def load_markets_to_hive(ind_curr, table, spark, dfDadosOrigem, cargaZero):

  if not cargaZero:

    dfHiveAux = read_market_lastpartition_from_hive("crypto", table, ind_curr, spark)

    if dfHiveAux is not None and dfHiveAux.count() > 0:

      close_row = dfHiveAux.agg(max("cuote_timestamp").alias("max_timestamp")).collect()[0]["max_timestamp"]

      print("close_row: " + str(close_row))

      dfAux2 = dfDadosOrigem.filter(dfDadosOrigem["cuote_timestamp"] > close_row)

      append_data_to_hive("crypto", table, dfAux2)
    else:
      append_data_to_hive("crypto", table, dfDadosOrigem)

  else:

    if(spark.catalog.tableExists("crypto." + table)):
      spark.sql("alter table crypto." + table + " drop IF EXISTS partition (index = \"" + ind_curr + "\")")

    append_data_to_hive("crypto", table, dfDadosOrigem)


def append_data_to_hive(database:str, table:str, dfDadosOrigem:DataFrame):

  if table == "binance_monthly_hist_w1" or table == "binance_monthly_hist_d1":
    dfAux0 = dfDadosOrigem.drop("cuote_date")
    dfAux0.write.partitionBy("index","cuote_year","cuote_month").format("parquet").mode("append").saveAsTable(database + "." + table)
  else:
    dfAux0 = dfDadosOrigem.drop("cuote_year").drop("cuote_month")
    dfAux0.write.partitionBy("index","cuote_date").format("parquet").mode("append").saveAsTable(database + "." + table)
