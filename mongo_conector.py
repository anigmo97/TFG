from pymongo import MongoClient

MONGO_HOST= 'mongodb://localhost/tweet'
client = MongoClient(MONGO_HOST)
db = client.twitterdb

def get_tweet_ids_list_from_database():
    cursor_resultados = db.tweets.find({},{ "id_str": 1, "_id": 0 } )
    tweets_id_list = [x["id_str"] for x in cursor_resultados]
    return tweets_id_list


def update_many_tweets_dicts_in_mongo(tweets_list):
    # replaceOne
    # update_one
    # db.tweets.update_many(tweets_list) hace falta un filter y un update tal vez se pueda hacer
    for tweet in tweets_list:
        tweet_id = tweet["id_str"]
        db.tweets.replace_one({"_id" : tweet_id },tweet)


def insertar_multiples_tweets_en_mongo(mongo_tweets_list):
    db.tweets.insert_many(mongo_tweets_list)
