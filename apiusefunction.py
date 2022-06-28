import pandas as pd
import numpy as np
from flask import Flask, jsonify, request, abort
import json
import re
import sys
import traceback
import os
import time
from datetime import datetime
from pyspark.sql import SparkSession
# from lib2to3.pgen2.pgen import DFAState
from pyspark import SparkConf,SparkContext
# from pyspark.sql import SparkSession, Window as W
# from pyspark.sql.types import DoubleType,DateType,StringType,ArrayType, StructField, StructType, IntegerType,MapType
# from pyspark.sql.functions import pandas_udf
# from pyspark.sql.functions import udf
# from pyspark.sql.functions import col, sum
# import pyspark.sql.functions as F
# from pyspark.sql import Row
import imp

num_dict= \
{"0":"零",
 "1":"一",
 "2":"二",
 "3":"三",
 "4":"四",
 "5":"五",
 "6":"六",
 "7":"七",
 "8":"八",
 "9":"九",
 "-":"負"}

concat_mid_list = ["", "十", "百", "千", "萬"]

class DataFrameToJSONArray():
  def __init__(self, dataframe, filepath='DataFrameToJSONArrayFile.json'):
    self.__DataFrame = dataframe
    self.__FilePath = filepath
  def funChangeDataFrameType(self):
    for i in range(len(self.__DataFrame.columns)):
      s = re.sub(r'\'>', '', re.sub(r'\d', '', str(type(self.__DataFrame.iloc[:, i][0])))).replace('\'', ' ').replace('.',
                                                        ' ').split(
        ' ')[-1]
      if s == 'Timestamp':
        self.__DataFrame.iloc[:, i] = self.__DataFrame.iloc[:, i].astype("big5")
      else:
        self.__DataFrame.iloc[:, i] = self.__DataFrame.iloc[:, i].astype(s)
    return self.__DataFrame
  def funSaveJSONArrayFile(self):
    list001 = []
    for i in range(len(self.__DataFrame.columns)):
      list001.append(list(self.__DataFrame.iloc[:, i]))
    list002 = []
    list003 = []
    for i in range(len(list001[0])):
      for j in range(len(self.__DataFrame.columns)):
        list003.append(list001[j][i])
      list002.append(list003)
      list003 = []
    Final_JSON = json.dumps(list002, sort_keys=True, indent=4, ensure_ascii=False)
    with open(self.__FilePath, 'w') as f:
      f.write(Final_JSON)
    return Final_JSON



def auth(num):
    if not isinstance(num, int):
        print("error input, should be integer")
        return False
    if abs(num) > 1e8:
        print("error input, abs value should be less than 1e8")
        return False
    return True
    
def num2chinese(num):
    if not auth(num):
        return ""
    temp_chinese = derect_translate(num)
    # print("temp_chinese is ", temp_chinese)
    updated_chinese = update(temp_chinese)
    if num >= 0:
        return updated_chinese
    return num_dict[str(num)[0]] + updated_chinese

def derect_translate(num): 
    return [num_dict[x] for x in str(abs(num))]

def update(temp_chinese):
    tmp_inf = []
    for ix, x in enumerate(temp_chinese[::-1]):
        if x == "零":
            # 當前位為0時 特殊處理重複零(上一個為零)問題
            if tmp_inf and (tmp_inf[-1] == "零" or tmp_inf[-1] == "零"):
                pass
            elif tmp_inf or len(temp_chinese) == 1:
                tmp_inf.append(x)
        else:
            tmp_inf.append(x + concat_mid_list[ix % 4])
        # 特殊處理 萬這個單位上的字元
        if ix == 3 and len(temp_chinese) > 4:
            tmp_inf.append("萬")
    # print("tmp_inf is ", tmp_inf)
    tmp_inf.reverse()
    return "".join(tmp_inf)

def ouput1(arearname,BuildingType,strFloors):
    #匯入系統參數
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # 開啟Spark
    conf = SparkConf()\
    .setAppName("PostGreSQL")\
    .setMaster("local[*]")\
    .set("spark.jars", "path/to/jar/postgresql-9.1-901-1.jdbc4.jar")\
    .set("spark.driver.extraClassPath", "path/to/jar/lib/postgresql-9.1-901-1.jdbc4.jar") 

    spark = SparkSession\
    .builder\
    .config(conf=conf)\
    .getOrCreate()
    # 匯入資料
    path = [r"D:\SCOTT\伊藤\real_estate1082/A_lvr_land_A.csv",
            r"D:\SCOTT\伊藤\real_estate1082/B_lvr_land_A.csv",
            r"D:\SCOTT\伊藤\real_estate1082/E_lvr_land_A.csv",
            r"D:\SCOTT\伊藤\real_estate1082/F_lvr_land_A.csv",
            r"D:\SCOTT\伊藤\real_estate1082/H_lvr_land_A.csv"]
    df = spark.read.csv(path, sep=',',
                           inferSchema=True, header=True)
    # 篩選資料 鄉鎮市區 = arearname and 建物型態 like %BuildingType% and 總樓層數 = strFloors
    df.filter((df.鄉鎮市區  == arearname) & (df.建物型態.like("%"+ str(BuildingType) +"%"))  & (df.總樓層數 == strFloors) ) \
        .show(truncate=False)    
    df.printSchema()
    df1 = df.toPandas()
    df1 = df1.to_json(orient='records',force_ascii=False)
    return df1
