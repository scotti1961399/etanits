from lib2to3.pgen2.pgen import DFAState
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Window as W
from pyspark.sql.types import DoubleType,DateType,StringType,ArrayType, StructField, StructType, IntegerType,MapType
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, sum
import pyspark.sql.functions as F
from pyspark.sql import Row
from datetime import datetime
import json
import random
import os
import sys

#匯入系統參數
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#民國轉西元年，並轉換%Y%m%d to %Y-%m-%d
def to_yyyymmdd(x):
    print(x)
    x = str(x)
    aa = datetime.strptime(x.replace(x[0:len(x)-4], str(int(x[0:len(x)-4]) + 1911),1), '%Y%m%d').strftime("%Y-%m-%d")
    return str(aa)

# 開啟Spark
spark = SparkSession \
    .builder \
    .master("local[1]")\
    .appName("Read CSV File into DataFrame") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
# 匯入資料同時加入城市名稱
# 開始匯入
path = [r"D:\SCOTT\伊藤\real_estate1082/A_lvr_land_A.csv"]
df = spark.read.csv(path, sep=',',inferSchema=True, header=True)
df_A = df.withColumn("city", F.lit("台北市"))
path = [r"D:\SCOTT\伊藤\real_estate1082/B_lvr_land_A.csv"]
df2 = spark.read.csv(path, sep=',',inferSchema=True, header=True)
df_B=df2.withColumn("city",  F.lit("台中市"))
path = [r"D:\SCOTT\伊藤\real_estate1082/E_lvr_land_A.csv"]
df3 = spark.read.csv(path, sep=',',inferSchema=True, header=True)
df_E=df2.withColumn("city",  F.lit("高雄市"))
path = [r"D:\SCOTT\伊藤\real_estate1082/F_lvr_land_A.csv"]
df4 = spark.read.csv(path, sep=',',inferSchema=True, header=True)
df_F=df2.withColumn("city",  F.lit("新北市"))
path = [r"D:\SCOTT\伊藤\real_estate1082/H_lvr_land_A.csv"]
df5 = spark.read.csv(path, sep=',',inferSchema=True, header=True)
df_H=df2.withColumn("city",  F.lit("桃園市"))
# 結束匯入
# 整合資料
result = df_A.union(df_B).union(df_E).union(df_F).union(df_H)
# 重新命名columnsname
result = result.withColumnRenamed("鄉鎮市區","District").withColumnRenamed("建物型態","building_state")
# 移除特殊列
result=result.filter(result.District!='The villages and towns urban district')
# 執行日期轉換
func =  F.udf (lambda x: to_yyyymmdd(x), StringType())
df = result.withColumn('date', func(col('交易年月日')))
# 篩選資料 主要用途 = 住家用 and 建物型態 like %住宅大樓% and 移除小於十三層的數據
array = ["一層","二層","三層","四層","五層","六層","七層","八層","九層","十層","十一層","十二層"]
a = df.select('city','date','District','building_state').filter((df.主要用途  == "住家用") & (df.building_state.like("%住宅大樓%") )  & ~df.總樓層數.isin(array) ).distinct().orderBy(col("city").asc(),col("date").desc())

# 整合 鄉鎮市區 與 建物型態 成 json 並刪除多於資料
build_District_udf = udf(lambda District, building_state: {
    'District': District,
    'building_state': building_state
}, MapType(StringType(), StringType()))

nested_event_df = (
    a
    .withColumn('events', build_District_udf(df['District'], df['building_state']))
    .drop('District')
    .drop('building_state')
)
# 整合 城市與日期 並將events資料整併
j_1 = (nested_event_df.groupby(['city','date']).agg(F.collect_list('events').alias("events"))).orderBy(col("city").asc(),col("date").desc())# .toJSON()

# 整合 日期 與 events成 json 並刪除多於資料
build_event_udf = udf(lambda date, events: {
    'date': date,
    'events': events
}, MapType(StringType(), StringType()))

j_2= (
    j_1
    .withColumn('time_slots', build_event_udf(j_1['date'], j_1['events']))
    .drop('date')
    .drop('events')
)
# 隨機挑選 城市至json file

cityarray= ["台中市","高雄市","台北市","新北市","桃園市"]
selectarray=[]
for i in cityarray:
    aa = random.randint(1,2)
    if aa ==1:
        selectarray.append(i)
print(selectarray)
j_3 = (j_2.groupby(['city']).agg(F.collect_list('time_slots').alias("time_slots"))).filter(~j_2.city.isin(selectarray) ).orderBy(col("city").asc()).toJSON()
j_3_1 = (j_2.groupby(['city']).agg(F.collect_list('time_slots').alias("time_slots"))).filter(j_2.city.isin(selectarray) ).orderBy(col("city").asc()).toJSON()
# j_3.first()
# 輸出json file
j_3list = j_3.collect()
jsonStr = json.dumps(j_3list)
jsonFile = open(r".\result-part1.json", "w")
jsonFile.write(jsonStr)
jsonFile.close()
j_3_1list = j_3_1.collect()
jsonStr = json.dumps(j_3_1list)
jsonFile = open(r".\result-part2.json", "w")
jsonFile.write(jsonStr)
jsonFile.close()