from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
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


def _get_table_location(spark, table):
    rows = (spark.sql(f"DESCRIBE FORMATTED {table}")
                 .filter(F.col("col_name") == "Location")
                 .collect())
    return rows[0]["data_type"].strip() if rows else None


def _hdfs_fs(spark):
    return spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())


def _hdfs_path(spark, path_str):
    return spark._jvm.org.apache.hadoop.fs.Path(path_str)


def get_latest_partition_date(spark, table, index_value):
    location = _get_table_location(spark, table)
    if not location:
        return None
    fs = _hdfs_fs(spark)
    index_path = _hdfs_path(spark, f"{location}/index={index_value}")
    if not fs.exists(index_path):
        return None
    dates = [
        s.getPath().getName().split("=", 1)[1]
        for s in fs.listStatus(index_path)
        if s.getPath().getName().startswith("cuote_date=")
    ]
    return max(dates) if dates else None


def get_latest_partition_year_month(spark, table, index_value):
    location = _get_table_location(spark, table)
    if not location:
        return None
    fs = _hdfs_fs(spark)
    index_path = _hdfs_path(spark, f"{location}/index={index_value}")
    if not fs.exists(index_path):
        return None
    yyyymm_values = []
    for year_status in fs.listStatus(index_path):
        year_name = year_status.getPath().getName()
        if not year_name.startswith("cuote_year="):
            continue
        year = int(year_name.split("=", 1)[1])
        for month_status in fs.listStatus(year_status.getPath()):
            month_name = month_status.getPath().getName()
            if month_name.startswith("cuote_month="):
                month = int(month_name.split("=", 1)[1])
                yyyymm_values.append(year * 100 + month)
    return max(yyyymm_values) if yyyymm_values else None


def read_market_lastpartition_from_hive(database:str, table:str, ind_curr:str, spark:SparkSession) -> DataFrame:

  if not spark.catalog.tableExists(f"{database}.{table}"):
    return None

  full_table = f"{database}.{table}"
  location = _get_table_location(spark, full_table)

  if table == "binance_monthly_hist_w1" or table == "binance_monthly_hist_d1":
    latest_year_month = get_latest_partition_year_month(spark, full_table, ind_curr)
    if latest_year_month and location:
        latest_year, latest_month = divmod(latest_year_month, 100)
        path = f"{location}/index={ind_curr}/cuote_year={latest_year}/cuote_month={latest_month}"
        return spark.read.option("basePath", location).parquet(path)

  else:
    latest_date = get_latest_partition_date(spark, full_table, ind_curr)
    if latest_date and location:
        path = f"{location}/index={ind_curr}/cuote_date={latest_date}"
        return spark.read.option("basePath", location).parquet(path)

  return None


def load_crypto_to_hive(ind_curr, timeframe, spark, dfAux1, cargaZero):

  table = de_para_crypto_database(timeframe)

  load_markets_to_hive(ind_curr, table, spark, dfAux1, cargaZero)


def load_markets_to_hive(ind_curr, table, spark, dfDadosOrigem, cargaZero):

  if not cargaZero:

    dfHiveAux = read_market_lastpartition_from_hive("crypto", table, ind_curr, spark)

    if dfHiveAux is not None and dfHiveAux.count() > 0:

      close_row = dfHiveAux.agg(F.max("cuote_timestamp").alias("max_timestamp")).collect()[0]["max_timestamp"]

      print("close_row: " + str(close_row))

      dfAux2 = dfDadosOrigem.filter(dfDadosOrigem["cuote_timestamp"] > close_row)

      append_data_to_hive("crypto", table, dfAux2, spark)
    else:
      append_data_to_hive("crypto", table, dfDadosOrigem, spark)

  else:

    if(spark.catalog.tableExists("crypto." + table)):
      spark.sql("alter table crypto." + table + " drop IF EXISTS partition (index = \"" + ind_curr + "\")")

    append_data_to_hive("crypto", table, dfDadosOrigem, spark)


def append_data_to_hive(database:str, table:str, dfDadosOrigem:DataFrame, spark:SparkSession):

  location = _get_table_location(spark, f"{database}.{table}")

  if table == "binance_monthly_hist_w1" or table == "binance_monthly_hist_d1":
    dfAux0 = dfDadosOrigem.drop("cuote_date")
    dfAux0.write.partitionBy("index","cuote_year","cuote_month").mode("append").parquet(location)
  else:
    dfAux0 = dfDadosOrigem.drop("cuote_year").drop("cuote_month")
    dfAux0.write.partitionBy("index","cuote_date").mode("append").parquet(location)
