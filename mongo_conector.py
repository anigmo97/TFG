from pymongo import MongoClient,errors
from bson.objectid import ObjectId
import traceback
import json

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
    tweets_id_list = [x["id_str"] for x in cursor_resultados]
    return tweets_id_list

def get_tweets_cursor_from_mongo(collection="tweets"):
    return db[(collection or "tweets")].find({})

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
        print("[MONGO STATISTICS INFO] Fichero de estadísticas correctamente cargado para la colección {}".format(collection))
        return file_list[0]
    else:
        print("[MONGO STATISTICS INFO] No hay fichero de estadísticas para la colección {}".format(collection))
        return None
        # if get_count_of_a_collection(collection) > 0:
        #     print("[MONGO STATISTICS INFO] No hay fichero de estadísticas para la colección {} pero la coleccion tiene registros por lo que se generará uno analizando en primer lugar los tweets de la coleccion".format(collection))
        #     return (None, True)
        # else:
        #     print("[MONGO STATISTICS INFO] No hay fichero de estadísticas para la colección {} y la coleccion no tiene datos, se generará un fichero nuevo con los nuevos mensajes que se recopilen".format(collection))
        # return (None,False)



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
        print("[MONGO INSERT MANY INFO] {} messages weren't inserted because they were already in the collection {}".format(tweets_no_insertados,collection))
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


get_statistics_file_from_collection("tweets")
print(get_count_of_a_collection("tweets"))