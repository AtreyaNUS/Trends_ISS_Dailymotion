__author__ = 'atreya'

from pymongo import MongoClient
from Utils.utils import getDatabaseConfig

class Mongo:

    def __init__(self):
        dbConfig=getDatabaseConfig()
        self.client = MongoClient(dbConfig["hostname"], dbConfig["port"])
        self.db = self.client[dbConfig["database_name"]]






