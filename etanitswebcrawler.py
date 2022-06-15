import requests
import os
import zipfile
import time
import pandas as pd
import csv 
import io


dataTPE = pd.read_csv("https://plvr.land.moi.gov.tw//DownloadSeason?season=108S2&fileName=A_lvr_land_A.csv")
dataTXG = pd.read_csv("https://plvr.land.moi.gov.tw//DownloadSeason?season=108S2&fileName=B_lvr_land_A.csv")
dataKHH = pd.read_csv("https://plvr.land.moi.gov.tw//DownloadSeason?season=108S2&fileName=E_lvr_land_A.csv")
dataTPH = pd.read_csv("https://plvr.land.moi.gov.tw//DownloadSeason?season=108S2&fileName=F_lvr_land_A.csv")
dataTYC = pd.read_csv("https://plvr.land.moi.gov.tw//DownloadSeason?season=108S2&fileName=H_lvr_land_A.csv")

def real_estate_crawler(year, season):
  if year > 1000:
    year -= 1911

  # download real estate zip content
  res = requests.get("https://plvr.land.moi.gov.tw//DownloadSeason?season="+str(year)+"S"+str(season)+"&type=zip&fileName=lvr_landcsv.zip")

  # save content to file
  fname = 'D:\SCOTT\伊藤\\' + str(year)+str(season)+'.zip'
  open(fname, 'wb').write(res.content)

  # make additional folder for files to extract
  folder = 'D:\SCOTT\伊藤\\real_estate' + str(year) + str(season)
  if not os.path.isdir(folder):
    os.mkdir(folder)

  # extract files to the folder
  with zipfile.ZipFile(fname, 'r') as zip_ref:
      zip_ref.extractall(folder)

  time.sleep(10)
  
real_estate_crawler(108, 2)

dataTPE = pd.read_csv(r"D:\SCOTT\伊藤\real_estate1082\A_lvr_land_A.csv")
dataTXG = pd.read_csv(r"D:\SCOTT\伊藤\real_estate1082\B_lvr_land_A.csv")
dataKHH = pd.read_csv(r"D:\SCOTT\伊藤\real_estate1082\E_lvr_land_A.csv")
dataTPH = pd.read_csv(r"D:\SCOTT\伊藤\real_estate1082\F_lvr_land_A.csv")
dataTYC = pd.read_csv(r"D:\SCOTT\伊藤\real_estate1082\H_lvr_land_A.csv")
