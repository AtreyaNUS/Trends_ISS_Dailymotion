import os,json
import twitter
import sched, time
import pandas as pd
from pandas.io.json import json_normalize
WOE_IN = "23424848"
WOE_FR = "23424819"
os.environ['http_proxy']=''

def getAuthTokensTwitter():
    root = os.path.dirname(os.getcwd())
    keysPath = os.path.normpath(root+"//API_Keys_Auth//api_keys.json")
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
    #dictLimit_Trends = apiLimits['resources']['trends']['/trends/place']
    dictLimit_Search = apiLimits['resources']['search']['/search/tweets']
    #print "Trends Remaining:"+str(dictLimit_Trends['remaining'])
    print "Search Remaining:"+str(dictLimit_Search['remaining'])
    if dictLimit_Search['remaining']==5:
        return False
    else:
        return True

def getNotUpdatedTrends(country):
    root = os.path.dirname(os.getcwd())
    keysPath = os.path.normpath(root+"//TwitterTrends_Trendinalia//DataDumps//trends_"+country+".json")
    with open(keysPath, mode='r+') as jsonFile:
            jsonData = json.load(jsonFile)
    trends_country=jsonData['unique_trends_'+country]
    keysPath2 = os.path.normpath(root+"//TwitterTrends_Trendinalia//DataDumps//tweets_"+country+".json")
    with open(keysPath2, mode='r+') as jsonFile2:
        jsonData = json.load(jsonFile2)
    trends_present=jsonData['trends']
    notUpdatedTrends=list(set(trends_country).difference(trends_present))
    print notUpdatedTrends     
    return trends_country

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
        
        
        
def insertHashTagName(status,trend):
    status['trend']=trend
    #print status
    return status



def writeTweetsToFile(trends,tweets,country,filename="tweets"):
    root = os.path.dirname(os.getcwd())
    keysPath = os.path.normpath(root+"//TwitterTrends_Trendinalia//DataDumps//tweets_"+country+".json")
    #print keysPath    
    with open(keysPath, mode='r+') as jsonFile:
        jsonData = json.load(jsonFile)
    jsonData['tweet_data']=jsonData['tweet_data']+tweets
    jsonData['trends']=jsonData['trends']+trends
    #jsonData['tweet_id'+country]=jsonData['tweet_id'+country]+ids
    with open(keysPath, mode='w+') as jsonFile:            
        jsonFile.write(json.dumps(jsonData, jsonFile,indent=1))
   
        
def writeTweetsToCSV():
        root = os.path.dirname(os.getcwd())
        keysPath = os.path.normpath(root+"//TwitterTrends_Trendinalia//DataDumps//tweets_"+country+".json")
        #print keysPath    
        with open(keysPath, mode='r+') as jsonFile:
            jsonData = json.load(jsonFile)
        root = os.path.dirname(os.getcwd())
        keysPath = os.path.normpath(root+"//TwitterTrends_Trendinalia//DataDumps//tweets_"+country+".csv")
        tweets_df=json_normalize(jsonData['tweet_data'])
        tweets_df.to_csv(keysPath,encoding="utf-8")        


def getTweets():
    twitterAPI=getTwitterObj()
    trends=getNotUpdatedTrends(country)
    print trends
    #trends=trends[1:2]
    print len(trends)
    statuses=[]
    trendsGot=[]
    for trend in trends: 
        #trend=urllib2.quote(trend)
        trend=trend.encode('utf8')
        print trend
        if hasRateLimit(twitterAPI)==False:                      
            twitterAPI=getTwitterObj()
        
        try:
            results = twitterAPI.search.tweets(q=trend,count=100)
            results_statuses=results["statuses"]
            print "results_statuses"
            print len(results_statuses)
            if len(results_statuses)!=0:
                nstatuses = map(lambda status:insertHashTagName(status,trend) ,results_statuses)
                statuses=statuses+nstatuses
                print "Total tweets till now"
                print len(statuses)
                trendsGot.append(trend)
        except ValueError:
            print "ValueError"+trend
            continue
      
    cleanTweets=processTweets(statuses)
    return cleanTweets,trendsGot





#country='france'
#trends=getNotUpdatedTrends(country)
#print trends
countries=['france']
for country in countries:
    tweets,trends=getTweets()
#    writeTweetsToFile(trends,tweets,country)
#writeTweetsToCSV()




