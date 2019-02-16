from pymongo import MongoClient

MONGO_HOST= 'mongodb://localhost/tweet'
client = MongoClient(MONGO_HOST)
db = client.twitterdb

def get_tweet_ids_list_from_database():
    return db.tweets.find( {},{ "id_str": 1, "_id": 0 } )


def update_many_tweets_dicts_in_mongo(tweets_list):
    # mycollection.update_one({'_id':mongo_id}, {"$set": post}, upsert=False)
    # replace
    pass

# def update_tweets(tweets_ids_list):
#     API = tweepy.API(auth)
#     tweets = API.statuses_lookup(tweets_ids_list)
#     for tweet in tweets:
#         consumer.update_tweet_dict_in_mongo(tweet["id_str"],tweet)