# -*- coding: utf-8 -*-
"""
Created on Tue Dec  9 01:27:26 2014

@author: atreya
"""

from bs4 import BeautifulSoup
import requests
from datetime import date, timedelta,datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from Utils import utils
from MongoDB.mongoDB import Mongo

def getPageIdentifier(d):
    return ((d.year%1000)*10000)+(d.month*100)+d.day

def getTrendsLastUpdatedDate(country,db):
    #print db.collection_names()
    if 'last_updated_collections' not in db.collection_names():
        obj={}
        for country in utils.getCountries():
            obj["_id"]=1
            obj["trend_"+country]=utils.getStartDate()
        db['last_updated_collections'].save(obj)
        
    last_updated_date=db['last_updated_collections'].find_one()['trend_'+country]  
    #last_updated_date=datetime.strptime(last_updated_date,"%d/%m/%Y")
    last_updated_date=date(last_updated_date.year,last_updated_date.month,last_updated_date.day)
    print "Last Updated Date"    
    print last_updated_date    
    return last_updated_date

def getGLobalTrends(country,db):
    global_trends = db['trends_'+country].find({},{'trending topic':1,'_id':0})
    global_trends=global_trends.distinct("trending topic")
    return global_trends

def webScrapTrendsandInsert(country,db,write=False):
    print country
    print "--------------------------" 
    todaysDate=date.today()
    endDate=todaysDate
    globalTrends=getGLobalTrends(country,db)
    last_updated_date=getTrendsLastUpdatedDate(country,db)
    if last_updated_date==todaysDate:
        print "Trends for "+country+" is up to date"
        return
    startDate=last_updated_date
    while startDate<endDate:
        print "Date"
        print startDate
        pageIdentifier=getPageIdentifier(startDate)
        url = "http://www.trendinalia.com/twitter-trending-topics/"+country+"/"+country+"-"+str(pageIdentifier)+".html"
        r  = requests.get(url)
        data = r.text
        soup = BeautifulSoup(data)
        
        try:
            table=soup.findAll('table')[0].findAll('tr')        
            for i in range(1,len(table)-1):            
                trend=table[i].findAll('a')[0].text
                print trend
                trendObj = {}           
                trendObj['date']=datetime.combine(startDate,datetime.min.time())         
                trendObj['trending topic']=trend
                if trend in globalTrends:
                    trendObj['global trend']=True
                else:
                    trendObj['global trend']=False
                trendObj['_id']=ObjectId()
                if write:
                    db['trends_'+country].insert(trendObj)
                else:
                    print trendObj
        except IndexError:
             startDate=startDate+timedelta(days=1)
             
        startDate=startDate+timedelta(days=1)
    if write:
        obj=db['last_updated_collections'].find_one()
        _id=obj['_id']
        obj["trend_"+country]=datetime.today()
        db['last_updated_collections'].update({'_id':_id}, {"$set": obj}, upsert=False)
    else:
        print "Finished !"

def main():
    countries=utils.getCountries()
    countries.insert(0,"globales")
    print countries
    client =Mongo()
    print client.db
    for country in countries:
        webScrapTrendsandInsert(country,client.db,write=True)

main()










