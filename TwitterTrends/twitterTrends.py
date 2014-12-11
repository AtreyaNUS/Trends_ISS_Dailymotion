# -*- coding: utf-8 -*-
"""
Created on Mon Dec 01 13:17:43 2014

@author: Atreya
"""
import os,json
import twitter
import sched, time
import pandas as pd
from pandas.io.json import json_normalize
WOE_IN = "23424848"
WOE_FR = "23424819"


def getAuthTokensTwitter():
    root = os.path.dirname(os.getcwd())
    keysPath = os.path.normpath(root+"//API_Keys_Auth//api_keys.json")
    with open(keysPath) as jsonFile:
        jsonData = json.load(jsonFile)["Twitter"]
        return jsonData

def performAuthentication(i=2):
    authArray = getAuthTokensTwitter()
    auths = authArray[i]
    auth = twitter.oauth.OAuth(auths['access_token'], auths['access_token_secret'],
                           auths['consumer_key'],auths['consumer_secret'])

    twitterAPI = twitter.Twitter(auth=auth)
    return twitterAPI,i,len(authArray)

def getTwitterObj():
    twitterAPI,i,count=performAuthentication()
    if(hasRateLimit(twitterAPI)==False):
        if count==i:
            i=0;
        else:
            i=i+1
        twitterAPI,i,count=performAuthentication(i)
    return twitterAPI   
        
   
def hasRateLimit(twitterAPI):
    apiLimits = twitterAPI.application.rate_limit_status()
    if apiLimits['resources'].has_key("trends")==False:
        print "Trends not found in the Rate Limiting Response !!"
        return
    if apiLimits['resources'].has_key("search")==False:
        print "Search not found in the Rate Limiting Response !!"
        return
    dictLimit_Trends = apiLimits['resources']['trends']['/trends/place']
    dictLimit_Search = apiLimits['resources']['search']['/search/tweets']
    print "Trends Remaining:"+str(dictLimit_Trends['remaining'])
    print "Search Remaining:"+str(dictLimit_Search['remaining'])
    if dictLimit_Search['remaining']==0 or dictLimit_Trends['remaining']==0:
        return False
    else:
        return True

def writeTrendsToFile(names,trends,filename ="trends"):
        root = os.path.dirname(os.getcwd())
        if filename=="trends":
            keysPath = os.path.normpath(root+"//DataDumps//trends.json")
        with open(keysPath, mode='r+') as jsonFile:
            jsonData = json.load(jsonFile)
        jsonData['trend_data']=jsonData['trend_data']+trends
        jsonData['trend_names']=jsonData['trend_names']+names
        with open(keysPath, mode='w+') as jsonFile:            
            jsonFile.write(json.dumps(jsonData, jsonFile,indent=1))
        

def writeTweetsToFile(data,filename="tweets"):
        #print json.dumps(data[0:2],indent=1)
        if len(data)==0:
            return
        ids=map(lambda d:getId(d),data)
        root = os.path.dirname(os.getcwd())
        if filename=="tweets":
            keysPath = os.path.normpath(root+"//DataDumps//tweets.json")
        with open(keysPath, mode='r+') as jsonFile:
            jsonData = json.load(jsonFile)
        jsonData['tweet_data']=jsonData['tweet_data']+data
        jsonData['tweet_id']=jsonData['tweet_id']+ids
        with open(keysPath, mode='w+') as jsonFile:            
            jsonFile.write(json.dumps(jsonData, jsonFile,indent=1))
        writeTweetsToCSV(jsonData)
            
def writeTweetsToCSV(jsonData):
        root = os.path.dirname(os.getcwd())
        keysPath = os.path.normpath(root+"//DataDumps//tweets.csv")
        tweets_df=json_normalize(jsonData)
        tweets_df.to_csv(keysPath,encoding="utf-8")
    
    
def getId(d):
    id_tweet= d['id']
    return id_tweet
    
  
def getExistTrendsObj():
   root = os.path.dirname(os.getcwd())
   keysPath = os.path.normpath(root+"//DataDumps//trends.json")
   with open(keysPath, mode='r+') as jsonFile:
            jsonData = json.load(jsonFile)
   return jsonData


def removeExistingTrends(data):
    trends=getExistTrendsObj()['trend_names']
    names=[]
    filterData=[]
    for d in data:
        if d['name'] not in trends:
            names.append(d['name'])
            filterData.append(d)
    #print filterData
    return names,filterData



    
def getTrendsFromAPI(twitterAPI,WOE_IDS):
    trendsAll=[]
    for WOE_ID in WOE_IDS:
        if hasRateLimit(twitterAPI)==False:
            twitterAPI=getTwitterObj()
        trendObj = twitterAPI.trends.place(_id=WOE_ID)
        trends = trendObj[0]['trends']
        for i in range(0,len(trends)):
            trends[i]["locations"]=trendObj[0]['locations'][0]['name']
            trends[i]["created_at"]=trendObj[0]['created_at']
            trends[i]["as_of"]=trendObj[0]['as_of']
            trends[i]["source"]="Twitter"
            trends[i]["tweet_count"]=0
        trendsAll=trendsAll+trends
    return trendsAll


def getExistingTweetIds():
   root = os.path.dirname(os.getcwd())
   keysPath = os.path.normpath(root+"//DataDumps//tweets.json")
   with open(keysPath, mode='r+') as jsonFile:
            jsonData = json.load(jsonFile)
   return jsonData["tweet_id"]


def checkTweetExists(tweet):
    return tweet['id'] in getExistingTweetIds()


def processTweet(status):
    if checkTweetExists(status)==False:
        processed_tweet={}
        processed_tweet['query']=status['query']
        processed_tweet['id']=status['id']
        processed_tweet['text']=status['text']
        processed_tweet['geo']=status['geo']
        processed_tweet['lang']=status['lang']
        processed_tweet['place']=status['place']
        processed_tweet['created_at']=status['created_at']
        processed_tweet['user_followers_count']=status['user']['followers_count']
        processed_tweet['user_statuses_count']=status['user']['statuses_count']
        processed_tweet['user_description']=status['user']['description']
        processed_tweet['user_friends_count']=status['user']['friends_count']
        processed_tweet['user_location']=status['user']['location']
        processed_tweet['user_name']=status['user']['name']
        processed_tweet['user_screen_name']=status['user']['screen_name']
        processed_tweet['user_time_zone']=status['user']['time_zone']
        return processed_tweet
    else:
        return None

def processTweets(statuses):
    processed_statuses = map(lambda status: processTweet(status),statuses)
    processed_statuses = filter(None,processed_statuses)
    return processed_statuses

def insertHashTagName(status,trend):
    status['query']=trend
    #print status
    return status

def getTweets(twitterAPI,trends):
    statuses=[]
    for trend in trends:        
        if hasRateLimit(twitterAPI)==False:
            twitterAPI=getTwitterObj()
        results = twitterAPI.search.tweets(q=trend,count=100)
        print "trend"
        print trend
        print "results"
        print results
        print results["statuses"]
        nstatuses = map(lambda status:insertHashTagName(status,trend) ,results["statuses"])
        print "nStatuses:"        
        print nstatuses         
        statuses=statuses+nstatuses
        #print statuses 
    print "Statuses:"        
    print statuses        
    cleanTweets=processTweets(statuses)
    return cleanTweets

def keepGettingTrends(twitterAPI):    
    trends=getTrendsFromAPI(twitterAPI,[WOE_FR,WOE_IN])
    names,newTrends=removeExistingTrends(trends)
    #writeTrendsToFile(names,newTrends)
    return newTrends
    

def keepGettingTweets(twitterAPI,trends):
    tweets= getTweets(twitterAPI,trends)
#    print "Trends in fn Tweets"
#    print trends
#    print "Tweets in fn Tweets"
#    print tweets
    writeTweetsToFile(tweets)

def main():
    print "Retrieving Twitter Object"
    twitterAPI=getTwitterObj()
    print "Getting Trends"
    trends=keepGettingTrends(twitterAPI)
    print trends
#    trends=getExistTrendsObj()['trend_names']
#    trends=trends[len(trends)-7:len(trends)]
    print "Getting Tweets"    
    keepGettingTweets(twitterAPI,trends)
    
def mainScheduler(sc):
    count=0
    trends=keepGettingTrends()
    while count<100:            
        keepGettingTweets(trends)
    sc.enter(3600, 1, main, (sc,))  

def runScheduler():
    s = sched.scheduler(time.time, time.sleep)
    s.enter(3600, 1, mainScheduler(), (s,))
    s.run()    
    
main()
    

        
        


