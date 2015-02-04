# -*- coding: utf-8 -*-
"""
Created on Mon Dec  8 14:11:40 2014

@author: atreya
"""
from MongoDB.mongoDB import Mongo
import os,json
from pandas.io.json import json_normalize
from pandas import concat
from datetime import datetime
from Utils import utils

# def main():
#     extractionObject={
#                         "type":"json",
#                         "start":"01/10/2014",
#                         "end":"today",
#                         "last_n_months":4,
#                         "users":True,
#                         "minimum tweets":10
#                     }
#
#
# def getTweets(trend,country):
#     tweets= client.db['tweets_'+country].find({'trend':trend})
#     tweets=list(tweets)
#     print "Getting Tweets for"+trend
#     df_tweets = json_normalize(tweets)
#     return df_tweets
#
# def writeTweetsToCSV(unique_trends,country):
#     tweets=map(lambda trend:getTweets(trend,country),unique_trends)
#     print "Concatenating list of dataframes"
#     df_tweets=concat(tweets)
#     print len(df_tweets)
#     print "Writing Tweets to File..."
#     df_tweets.to_csv(utils.getDataDumpsPath(country)+"//tweets_"+str(end.date())+".csv",encoding="utf-8")
#     print "Writing Successful !"
#
# def writeTrendsToCSV():
#     trends = client.db['trends_'+country].find({"date":{"$gte":start,"$lte":end}},{'_id':0}).sort([("date",-1)])
#     unique_trends=trends.distinct("trending topic")
#     print len(unique_trends)
#     trends= list(trends)
#     print len(trends)
#     dataframe = json_normalize(trends)
#     print dataframe.head()
#     print "Writing Trends to file.."
#     dataframe.to_csv(utils.getDataDumpsPath(country)+"//trends_"+str(end.date())+".csv",encoding="utf-8")
#     print "Writing Successful !"
#     return unique_trends

country="france"
jsonFilePath=utils.getDataDumpsPath(country,"json")+"//tweets.json"
with open(jsonFilePath, mode='r+') as jsonFile:
            jsonData = json.load(jsonFile)

print jsonData[0]
#df=json_normalize(jsonData)








# client =Mongo()
# start=datetime(2014,9,1)
# end = datetime.today()

# unique_trends=writeTrendsToCSV()
# writeTweetsToCSV(unique_trends,country)


#trends=client.db['trends_india'].aggregate([{"$sort": {"date": -1} },{"$limit": 100}])
#trends = client.db['trends_india'].find().sort([("date",1)]).limit(5000)

#trends=trends.distinct("trending topic")
# trends =iter(trends)
# map(lambda trend:getTweets(trend),trends)