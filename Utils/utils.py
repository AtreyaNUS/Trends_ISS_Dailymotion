__author__ = 'atreya'

import os,json
import datetime

def getRootPath():
    return os.path.dirname(os.getcwd())

def getAPIKeysPath():
    root = getRootPath()
    apiKeysPath = os.path.normpath(root+"//API_Keys_Auth//api_keys.json")
    return apiKeysPath

def getConfigPath(config_key="data"):
    config_path = getRootPath()+"/Config//"+config_key+"Config.json"
    print config_path
    return config_path

def getStartDate():
    with open(getConfigPath(config_key="data")) as jsonFile:
        jsonData = json.load(jsonFile)["start_date"]
    jsonData=datetime.datetime.strptime(jsonData,"%d/%m/%Y")
    return jsonData

def getCountries():
    path =getConfigPath()
    with open(getConfigPath(config_key="data")) as jsonFile:
        jsonData = json.load(jsonFile)["locations"]
    return jsonData

def getDatabaseConfig():
    with open(getConfigPath(config_key="database")) as jsonFile:
        jsonData = json.load(jsonFile)
    return jsonData