from pymongo import MongoClient

c = MongoClient('mongodb://localhost:27017/')
db = c.sentiment_analysis
collection = db.tweet
cursor = collection.find({})
for document in cursor:
    print(document)