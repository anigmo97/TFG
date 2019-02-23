# CREATE DATABASE
use DATABASE_NAME

# CHECK CURRENT DB
db

#show dbs list
show dbs

# show collections
show collections

# show count of a collection
db.tweets.count()

#find
db.tweets.find()

# gets document id of documents
db.tweets.find({},{_id:1})

# drop a collection
db.tweets.drop()

# do bigger th buffer with responses in shell
DBQuery.shellBatchSize = 300

# DISTINCT
db.tweets.distinct("created_at")

# get statistics files
db.test.find({ _id:'0000000000'})

#get tweets count of the statistics file
db.test.find({ _id:'0000000000'},{tweets_count:1})

# get some fields of a document in base of a condition
db.test.find({retweeted_status: { $exists: false }},{favorite_count:1,id_str:1})    

# delete document by id
db.test.remove( {"_id":'0000000000'})