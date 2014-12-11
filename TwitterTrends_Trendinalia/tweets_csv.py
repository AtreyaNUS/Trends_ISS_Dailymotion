# -*- coding: utf-8 -*-
"""
Created on Mon Dec  8 14:11:40 2014

@author: atreya
"""

import os,json
from pandas.io.json import json_normalize

def writeTweetsToCSV(country):
        root = os.path.dirname(os.getcwd())
        keysPath = os.path.normpath(root+"//TwitterTrends_Trendinalia//DataDumps//tweets_"+country+".json")
        #print keysPath    
        with open(keysPath, mode='r+') as jsonFile:
            jsonData = json.load(jsonFile)
        root = os.path.dirname(os.getcwd())
        keysPath = os.path.normpath(root+"//TwitterTrends_Trendinalia//DataDumps//tweets_"+country+"2.csv")
        tweets_df=json_normalize(jsonData['tweet_data'])
        tweets_df.to_csv(keysPath,encoding="utf-8")  
        

def writeTrendsToCSV(country):
        root = os.path.dirname(os.getcwd())
        keysPath = os.path.normpath(root+"//TwitterTrends_Trendinalia//DataDumps//trends_"+country+".json")
        #print keysPath    
        with open(keysPath, mode='r+') as jsonFile:
            jsonData = json.load(jsonFile)
        root = os.path.dirname(os.getcwd())
        keysPath = os.path.normpath(root+"//TwitterTrends_Trendinalia//DataDumps//trends_"+country+".csv")
        tweets_df=json_normalize(jsonData['trends_'+country])
        tweets_df.to_csv(keysPath,encoding="utf-8") 


writeTrendsToCSV('india')