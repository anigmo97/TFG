from pymongo import MongoClient,errors
from bson.objectid import ObjectId
from global_functions import change_dot_in_keys_for_bullet,change_bullet_in_keys_for_dot
import traceback
import json
from datetime import datetime

MONGO_HOST= 'mongodb://localhost/tweet'
client = MongoClient(MONGO_HOST)
current_collection = "tweets"
default_collection = "tweets"
statistics_file_id = "0000000000"
db = client.twitterdb


##########################################################################################
##################################### GET INFO ###########################################
##########################################################################################

def get_count_of_a_collection(collection):
    return db[collection].count()

def get_tweet_ids_list_from_database(collection="tweets"):
    cursor_resultados = db[(collection or "tweets")].find({},{ "id_str": 1, "_id": 0 } )
    tweets_id_list = [x["id_str"] for x in cursor_resultados if x["id_str"] != statistics_file_id ]
    return tweets_id_list

def get_tweets_cursor_from_mongo(collection="tweets"):
    print("[MONGO GET CURSOR INFO] Coleccion = {}".format(collection))
    return db[(collection or "tweets")].find({'_id': {'$nin': [statistics_file_id]}})

def get_tweets_ids_that_are_already_in_the_database(tweet_ids_list,collection):
    map(ObjectId,tweet_ids_list)
    cursor_resultados = db[collection].find({'_id': {'$in': tweet_ids_list}},{'_id':1})
    tweets_id_list = [x["_id"] for x in cursor_resultados]
    return tweets_id_list

def get_statistics_file_from_collection(collection):
    cursor_resultados = db[(collection or "tweets")].find({"_id": statistics_file_id } )
    file_list = [x for x in cursor_resultados]
    if len(file_list) >1:
        raise Exception('[MONGO STATISTICS ERROR] Hay mas de un fichero con _id igual al de estadísticas : _id ={} '.format(statistics_file_id))
    elif len(file_list) == 1:
        print("[MONGO STATISTICS INFO] Fichero de estadísticas correctamente recuperado para la colección {}".format(collection))
        statistics_dict = file_list[0]
        way_of_send_with_keys_with_dots =  change_bullet_in_keys_for_dot(statistics_dict["way_of_send_counter"])
        statistics_dict["way_of_send_counter"] = way_of_send_with_keys_with_dots
        return statistics_dict
    else:
        print("[MONGO STATISTICS INFO] No hay fichero de estadísticas para la colección {}".format(collection))
        return None




##########################################################################################
##################################### UPDATE   ###########################################
##########################################################################################

def update_many_tweets_dicts_in_mongo(tweets_list,collection="tweets"):
    # replaceOne
    # update_one
    # db.tweets.update_many(tweets_list) hace falta un filter y un update tal vez se pueda hacer
    for tweet in tweets_list:
        tweet_id = tweet["id_str"]
        db[(collection or "tweets")].replace_one({"_id" : tweet_id },tweet)



##########################################################################################
##################################### INSERT   ###########################################
##########################################################################################

def insertar_multiples_tweets_en_mongo(mongo_tweets_dict,mongo_tweets_ids_list,collection="tweets"):
    print("[MONGO INSERT MANY INFO] Inserting tweets in mongo Collection = '{}' ".format(collection))
    #TODO COMPROBAR EN EJECUCIONES POR QUERY QUE NO ESTÁN YA
    tweets_no_insertados = 0
    try:
        repited_tweet_ids = get_tweets_ids_that_are_already_in_the_database(mongo_tweets_ids_list,collection)
        for repeated_id in repited_tweet_ids:
            del mongo_tweets_dict[repeated_id]
            tweets_no_insertados +=1

        tweets_no_repetidos = mongo_tweets_dict.values()
        if len(tweets_no_repetidos) >0:
            db[(collection or "tweets")].insert_many(tweets_no_repetidos)
        if tweets_no_insertados > 0:
            print("[MONGO INSERT MANY WARN] {} messages weren't inserted because they were already in the collection {}".format(tweets_no_insertados,collection))
    except errors.BulkWriteError as bwe:
        detalles = bwe.details
        for error in detalles["writeErrors"]:
            del error["op"]
        print("\n\n"+traceback.format_exc())
        print("[MONGO INSERT MANY ERROR] {}\n\n".format(bwe))
        print("[MONGO INSERT MANY ERROR] \n {}".format(json.dumps(detalles["writeErrors"],indent=4, sort_keys=True)))
        exit(1)
    except Exception as e:
        print("\n\n"+traceback.format_exc())
        print("[MONGO INSERT MANY ERROR] {}\n\n".format(e))
        exit(1)
    print("[MONGO INSERT MANY INFO] Finish sucessfully ")
    return tweets_no_repetidos

def insert_statistics_file_in_collection(statistics_dict,collection):
    statistics_dict["_id"] = statistics_file_id
    statistics_dict["ultima_modificación"] = str(datetime.now())
    way_of_send_with_keys_without_dots =  change_dot_in_keys_for_bullet(statistics_dict["way_of_send_counter"])
    statistics_dict["way_of_send_counter"] = way_of_send_with_keys_without_dots

    if get_statistics_file_from_collection(collection) == None:
        print("[MONGO INSERT STATISTICS FILE INFO] Inserting new statistics file")
        db[collection].insert(statistics_dict)
        print("[MONGO INSERT STATISTICS FILE INFO] The statistics file has been save sucessfully")
    else:
        print("[MONGO INSERT STATISTICS FILE INFO] Replacing statistics file")
        db[collection].replace_one({"_id" : statistics_file_id },statistics_dict) 
        print("[MONGO INSERT STATISTICS FILE INFO] The statistics file has been replaced save sucessfully")


# get_statistics_file_from_collection("tweets")
# print(get_count_of_a_collection("tweets"))

print(get_statistics_file_from_collection("test"))