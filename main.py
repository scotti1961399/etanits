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
from lib2to3.pgen2.pgen import DFAState
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession, Window as W
from pyspark.sql.types import DoubleType,DateType,StringType,ArrayType, StructField, StructType, IntegerType,MapType
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, sum
import pyspark.sql.functions as F
from pyspark.sql import Row
import imp
import apiusefunction as apif

imp.reload(sys)
app=Flask(__name__)
#  default 畫面
@app.route('/',methods=['GET'])
def index():
    print("aa")
    return "Hello Flask!"
#  GET 匯入查詢條件arearname(鄉鎮市區),BuildingType(建物型態),Floors(總樓層數)
@app.route('/areas/<string:arearname>/BuildingType/<string:BuildingType>/Floors/<int:Floors>',methods=['GET'])
def plvrdata(arearname,BuildingType,Floors):
  # request_data = request.json()
  # 專換國字數字
  strFloors = apif.num2chinese(Floors) + "樓"
  # 呼叫方法
  df1 = apif.ouput1(arearname,BuildingType,strFloors)
  # 回傳json
  return jsonify(df1)

# @app.route('/default',methods=['GET'])
# def default():
#     # request_data = request.json()
#     spark = SparkSession.builder\
#               .appName("Read CSV File into DataFrame")\
#               .getOrCreate()
#     path = [r"D:\SCOTT\伊藤\real_estate1082/A_lvr_land_A.csv",
#             r"D:\SCOTT\伊藤\real_estate1082/B_lvr_land_A.csv",
#             r"D:\SCOTT\伊藤\real_estate1082/E_lvr_land_A.csv",
#             r"D:\SCOTT\伊藤\real_estate1082/F_lvr_land_A.csv",
#             r"D:\SCOTT\伊藤\real_estate1082/H_lvr_land_A.csv"]
#     df = spark.read.csv(path, sep=',',
#                            inferSchema=True, header=True)
#     df.filter((df.鄉鎮市區  == arearname) & (df.建物型態  == BuildingType)  & (df.總樓層數 == strFloors) ) \
#         .show(truncate=False)    
#     df.printSchema()
#     df1 = df.toPandas()
#     df1 = df1.to_json(orient='records',force_ascii=False)
#     return jsonify(df1)

if __name__=='__main__':
    app.run(debug=True)
