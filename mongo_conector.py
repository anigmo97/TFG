from pymongo import MongoClient,errors
import traceback
import json

MONGO_HOST= 'mongodb://localhost/tweet'
client = MongoClient(MONGO_HOST)
db = client.twitterdb

def get_tweet_ids_list_from_database():
    cursor_resultados = db.tweets.find({},{ "id_str": 1, "_id": 0 } )
    tweets_id_list = [x["id_str"] for x in cursor_resultados]
    return tweets_id_list

def get_tweets_cursor_from_mongo():
    return db.tweets.find({})


def update_many_tweets_dicts_in_mongo(tweets_list):
    # replaceOne
    # update_one
    # db.tweets.update_many(tweets_list) hace falta un filter y un update tal vez se pueda hacer
    for tweet in tweets_list:
        tweet_id = tweet["id_str"]
        db.tweets.replace_one({"_id" : tweet_id },tweet)


def insertar_multiples_tweets_en_mongo(mongo_tweets_list):
    #TODO COMPROBAR EN EJECUCIONES POR QUERY QUE NO ESTÁN YA
    try:
        db.tweets.insert_many(mongo_tweets_list)
    except errors.BulkWriteError as bwe:
        detalles = bwe.details
        for error in detalles["writeErrors"]:
            del error["op"]
        print("\n\n"+traceback.format_exc())
        print("[MONGO INSERT MANY ERROR] {}\n\n".format(bwe))
        print(json.dumps(detalles["writeErrors"],indent=4, sort_keys=True))
        exit(1)
    except Exception as e:
        print("\n\n"+traceback.format_exc())
        print("[MONGO INSERT MANY ERROR] {}\n\n".format(e))
        exit(1)
