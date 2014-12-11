from bs4 import BeautifulSoup
import requests
import os,json
from datetime import date, timedelta,datetime

def writeTrendsToFile(trendsArray,trends,country,filename ="trends"):
        print "Writing to file"
        root = os.path.dirname(os.getcwd())
        if filename=="trends":
            keysPath = os.path.normpath(root+"//TwitterTrends_Trendinalia//DataDumps//trends_"+country+".json")
        with open(keysPath, mode='r+') as jsonFile:
            jsonData = json.load(jsonFile)
        jsonData["trends_"+country]=jsonData["trends_"+country]+trendsArray
        jsonData["unique_trends_"+country]=jsonData["unique_trends_"+country]+trends
        jsonData['count_'+country]=len(jsonData["unique_trends_"+country])
        jsonData['last_updated_date']=todaysDate.strftime('%d/%m/%Y')
        with open(keysPath, mode='w+') as jsonFile:            
            jsonFile.write(json.dumps(jsonData, jsonFile,indent=1))

def getPageIdentifier(d):
    return ((d.year%1000)*10000)+(d.month*100)+d.day

def getGlobalTrends():
    root = os.path.dirname(os.getcwd())    
    keysPath = os.path.normpath(root+"//TwitterTrends_Trendinalia//DataDumps//trends_globales.json")
    with open(keysPath, mode='r+') as jsonFile:
            jsonData = json.load(jsonFile)
    return jsonData['unique_trends_globales']

def getLastUpdatedDate():
    root = os.path.dirname(os.getcwd())
    keysPath = os.path.normpath(root+"//TwitterTrends_Trendinalia//DataDumps//trends_"+country+".json")
    with open(keysPath, mode='r+') as jsonFile:
            jsonData = json.load(jsonFile)
    last_updated_date=jsonData['last_updated_date']
    if last_updated_date==" ":
        last_updated_date=date(2014,10,1)
    else:   
        last_updated_date=datetime.strptime(last_updated_date,"%d/%m/%Y")
    return last_updated_date

country='india'
todaysDate=date.today()
endDate=todaysDate
last_updated_date=getLastUpdatedDate()
startDate=date(last_updated_date.year,last_updated_date.month,last_updated_date.day)-timedelta(days=1)

uniqueTrends=[]
trendsArray=[]

while startDate<endDate:  
    startIdentifier=getPageIdentifier(startDate)
    endIdentifier=getPageIdentifier(endDate)
    print startIdentifier,endIdentifier
    
    url = "http://www.trendinalia.com/twitter-trending-topics/"+country+"/"+country+"-"+str(startIdentifier)+".html"
    r  = requests.get(url)
    data = r.text
    soup = BeautifulSoup(data)
    try:
        table=soup.findAll('table')[0].findAll('tr')
        #print table
        
        for i in range(1,len(table)-1):
            
            trend=table[i].findAll('a')[0].text
            #print trend
            trendObj = {}
            trendObj['date']=startDate.strftime("%d/%m/%Y")
            trendObj['trending topic']=trend
            if country !='globales':
                globalTrends = getGlobalTrends()
                if trend in globalTrends:
                    trendObj['global trend']=True
                else:
                    trendObj['global trend']=False
            if trendObj not in trendsArray:
                trendsArray.append(trendObj)
            if trend not in uniqueTrends:
                uniqueTrends.append(trend)
            
            #print uniqueTrends
    except IndexError:
        startDate=startDate+timedelta(days=1)
    startDate=startDate+timedelta(days=1)
print "Number of Trends"
print len(uniqueTrends)
#print trendsArray
writeTrendsToFile(trendsArray,uniqueTrends,country)
 
    
