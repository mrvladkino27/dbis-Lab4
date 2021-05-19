from pymongo import MongoClient

NamePath_files = '../../../../DATA/Odata{year}File.csv'
CONECTING_LINK = "mongodb://admin:123456789@localhost:27017/?authSource=admin"
class Connect(object):
    @staticmethod    
    def get_connection():
        return MongoClient(CONECTING_LINK) 