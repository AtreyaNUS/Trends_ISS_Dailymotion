# -*- coding: utf-8 -*-
"""
Created on Tue Dec  9 17:28:39 2014

@author: atreya
"""
import os,json
import time
from datetime import date, datetime
from MongoDB.mongoDB import Mongo
import twitter
from Utils import utils
os.environ['http_proxy']=''

def getAuthTokensTwitter():
    keysPath = utils.getAPIKeysPath()
    with open(keysPath) as jsonFile:
        jsonData = json.load(jsonFile)["Twitter"]
        return jsonData

def performAuthentication(i=1):
    authArray = getAuthTokensTwitter()
    auths = authArray[i]
    auth = twitter.oauth.OAuth(auths['access_token'], auths['access_token_secret'],
                           auths['consumer_key'],auths['consumer_secret'])

    twitterAPI = twitter.Twitter(auth=auth)
    return twitterAPI,i,len(authArray)


def hasRateLimit(twitterAPI):
    apiLimits = twitterAPI.application.rate_limit_status()
    if apiLimits['resources'].has_key("trends")==False:
        print "Trends not found in the Rate Limiting Response !!"
        return
    if apiLimits['resources'].has_key("search")==False:
        print "Search not found in the Rate Limiting Response !!"
        return
    #dictLimit_Trends = apiLimits['resources']['trends']['/trends/place']
    dictLimit_Search = apiLimits['resources']['search']['/search/tweets']
    #print "Trends Remaining:"+str(dictLimit_Trends['remaining'])
    print "Search Remaining:"+str(dictLimit_Search['remaining'])
    if dictLimit_Search['remaining']<=5:
        return False
    else:
        return True


def getTwitterObj():
    twitterAPI,i,count=performAuthentication()
    if(hasRateLimit(twitterAPI)==False):
        if count==i:
            i=0;
        else:
            i=i+1
        twitterAPI,i,count=performAuthentication(i)
    return twitterAPI   

def getTweetsLastUpdatedDate(country,db):
    if 'tweets_'+country not in db.collection_names():
        obj=db['last_updated_collections'].find_one()
        obj["tweets_"+country]=utils.getStartDate()
        db['last_updated_collections'].save(obj)        
    last_updated_date=db['last_updated_collections'].find_one()['tweets_'+country]  
    #last_updated_date=date(last_updated_date.year,last_updated_date.month,last_updated_date.day)
    return last_updated_date
    
def getUniqueNotUpdatedTrends(country,db):
    last_updated_date=getTweetsLastUpdatedDate(country,db)
    new_trends = db['trends_'+country].find({'date':{'$gte':last_updated_date}},{'trending topic':1,'_id':0})
    #print len(list(new_trends))
    print list(new_trends)
    new_trends=new_trends.distinct("trending topic")
    existing_trends=getExistingTrendsForTweets(country,db)
    required_trends=[x for x in new_trends if x not in existing_trends]
    return required_trends      
    
    return new_trends
    

def getExistingTrendsForTweets(country,db):
    existing_trends=db['tweets_'+country].find({},{'trend':1,'_id':0})
    existing_trends=existing_trends.distinct("trend")
    #print list(existing_trends)
    return existing_trends

def processTweet(status):
    processed_tweet={}
    processed_tweet['trend']=status['trend']
    processed_tweet['source']=status['source']
    processed_tweet['retweeted']=status['retweeted']
    processed_tweet['favorited']=status['favorited']
    processed_tweet['favorite_count']=status['favorite_count']
    processed_tweet['id']=status['id']
    processed_tweet['text']=status['text']
    processed_tweet['geo']=status['geo']
    processed_tweet['lang']=status['lang']
    processed_tweet['place']=status['place']
    processed_tweet['created_at']=status['created_at']
    processed_tweet['retweet_count']=status['retweet_count']
    processed_tweet['user']=status['user']
    return processed_tweet

def processTweets(statuses):
    processed_statuses = map(lambda status: processTweet(status),statuses)
    print "Processed Statuses"     
    print len(processed_statuses)   
    return processed_statuses

def removeTrend(trend,country,db):
    db['trends_'+country].remove({'trending topic':trend})

def insertHashTagName(status,trend):
    status['trend']=trend
    #print status
    return status
    
def getAndWriteTweets(country,db,write=False):
    twitterAPI=getTwitterObj()
    trends=getUniqueNotUpdatedTrends(country,db)
    print "Getting tweets for "+country+", no of trends:"+str(len(trends))
    for trend in trends: 
        trend=trend.encode('utf8')
        print trend
        if hasRateLimit(twitterAPI)==False:                      
            twitterAPI=getTwitterObj()        
        try:
            statuses=[]
            #time.sleep(20)
            results = twitterAPI.search.tweets(q=trend,count=100)
            results_statuses=results["statuses"]
            print "results_statuses"
            print len(results_statuses)
            if len(results_statuses)!=0:
                nstatuses = map(lambda status:insertHashTagName(status,trend) ,results_statuses)
                statuses=statuses+nstatuses
                if write:
                    db["tweets_"+country].insert(statuses)
                    obj=db['last_updated_collections'].find_one()
                    _id=obj['_id']
                    obj["tweets_"+country]=datetime.today()
                    db['last_updated_collections'].update({'_id':_id}, {"$set": obj}, upsert=False)
                else:
                    print "Finished !"
            else:
                removeTrend(trend,country,db)
        except ValueError:
            print "ValueError"+trend
            continue

def main():
    countries=utils.getCountries()
    client =Mongo()
    for country in countries:
        getAndWriteTweets(country,client.db,write=True)

main()