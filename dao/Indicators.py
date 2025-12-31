from pyspark.sql.functions import  avg,  first, last, stddev, min, max, udf ,col
from pyspark.sql import Window
from pyspark.sql import DataFrame

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

def calc_Division(num, denum):
    if (denum > 0):
        return num/denum
    return 0.0

def calc_ma(field, window):
    wAux = Window.partitionBy("index").rowsBetween(-window,0)
    return avg(field).over(wAux)

def calc_gain(price_change):
    if (price_change > 0):
        return price_change
    return 0.0

def calc_loss(price_change):
    if (price_change < 0):
        return -price_change
    return 0.0

def calc_priceChange(field, window =1):
    wAux = Window.partitionBy("index").rowsBetween(-window,0)
    return last(field).over(wAux)-first(field).over(wAux)

def calc_RSI(rsIndex):
    if(rsIndex == -1):
        return 0.0
    return 100 - 100/(1+rsIndex)

def calc_Stochastic_K(close,hHighK,lLowK):
    return calc_Division((close-lLowK),(hHighK-lLowK))*100

def calc_BBandLower(bbmidle,devpad):
    if bbmidle is None or devpad is None:
        return None
    return bbmidle - 2.0*devpad

def calc_BBandUpper(bbmidle,devpad):
    if bbmidle is None or devpad is None:
        return None
    return bbmidle + 2.0*devpad

def calc_devpad(field, window):
    wAux = Window.partitionBy("index").rowsBetween(-window,0)
    return stddev(field).over(wAux)

def calc_highest(field, window):
    wAux = Window.partitionBy("index").rowsBetween(-window,0)
    return max(field).over(wAux)

def calc_lowest(field, window):
    wAux = Window.partitionBy("index").rowsBetween(-window,0)
    return min(field).over(wAux)

@pandas_udf(DoubleType())
def ema_udf(v: pd.Series, span: pd.Series) -> pd.Series:
    # span must be an integer column in the DataFrame
    return v.ewm(span=span.iloc[0], adjust=False).mean()


udfIndex_Division = udf(lambda num, denum: calc_Division(num, denum),DoubleType())
udfIndex_MA = udf(lambda field, window: calc_ma(field,window), DoubleType())
udfIndex_Gain = udf(lambda price_change: calc_gain(price_change),DoubleType())
udfIndex_Loss = udf(lambda price_change: calc_loss(price_change),DoubleType())
udfPriceChange = udf(lambda field, window: calc_priceChange(field, window), DoubleType())
udfIndex_RSI = udf(lambda rsInDex: calc_RSI(rsInDex),DoubleType())
udfIndex_Stochastic_K = udf(lambda close,hHighK,lLowK: calc_Stochastic_K(close,hHighK,lLowK),DoubleType())
udfIndex_BBandLower = udf(lambda bbmidle,devpad: calc_BBandLower(bbmidle,devpad),DoubleType())
udfIndex_BBandUpper = udf(lambda bbmidle,devpad: calc_BBandUpper(bbmidle,devpad),DoubleType())
udfIndex_DevPad = udf(lambda field, window: calc_devpad(field,window), DoubleType())
udfIndex_Highest = udf(lambda field, window: calc_highest(field,window), DoubleType())
udfIndex_Lowest = udf(lambda field, window: calc_lowest(field,window), DoubleType())

def addRSIIndex_toDF(df_cuotes, window) -> DataFrame:
    
    dfAux3 = df_cuotes.select('*', calc_priceChange(col('cuote_close')).alias('in_change')).sort('cuote_timestamp')
    
    dfAux4 = dfAux3.withColumn('in_gain', udfIndex_Gain('in_change')) \
                    .withColumn('in_loss', udfIndex_Loss('in_change'))
   
    dfAux5 = dfAux4.select('*', calc_ma(col('in_gain'),window).alias('in_mov_avg_gain'), calc_ma(col('in_loss'),window).alias('in_mov_avg_loss')).sort('cuote_timestamp')

    dfAux6 = dfAux5.withColumn('in_gain_over_loss', udfIndex_Division('in_mov_avg_gain','in_mov_avg_loss')) \
                .drop('in_change','in_gain','in_mov_avg_gain','in_loss','in_mov_avg_loss')

    return dfAux6.withColumn('in_RSI', udfIndex_RSI('in_gain_over_loss')) \
                .drop('in_gain_over_loss')

def addBollingerBandIndex_toDF(df_cuotes, window) -> DataFrame:
    dfAux0 = df_cuotes.select('*', calc_ma(col('cuote_close'),window).alias('in_BBandMiddle'), calc_devpad(col('cuote_close'),window).alias('in_dev_pad_boerlinger')).sort('cuote_timestamp')
    return dfAux0.withColumn('in_BBandLower', udfIndex_BBandLower('in_BBandMiddle','in_dev_pad_boerlinger')) \
                    .withColumn('in_BBandUpper', udfIndex_BBandUpper('in_BBandMiddle','in_dev_pad_boerlinger')) \
                    .drop('in_dev_pad_boerlinger')

def addStochasticIndex_toDF(df_cuotes, kWindow,dWindow) -> DataFrame:
    dfAux0 = df_cuotes.select('*', calc_highest(col('cuote_high'),kWindow).alias('in_StochHhighK'), calc_lowest(col('cuote_low'),kWindow).alias('in_StochLlowK')).sort('cuote_timestamp')
    dfAux1 = dfAux0.withColumn('in_StochLineK', udfIndex_Stochastic_K('cuote_close','in_StochHhighK','in_StochLlowK'))  
    dfAux2 = dfAux1.select('*', calc_ma(col('in_StochLineK'),dWindow).alias('in_StochLineD')).sort('cuote_timestamp')
    return dfAux2.drop('in_StochHhighK','in_StochLlowK')

