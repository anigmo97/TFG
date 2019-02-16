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