from pymongo import MongoClient,errors
from bson.objectid import ObjectId
from global_functions import change_dot_in_keys_for_bullet,change_bullet_in_keys_for_dot
import traceback
import json
import ast # to load query string to dict
from datetime import datetime
import re
from bson.code import Code

MONGO_HOST= 'mongodb://localhost/tweet'
client = MongoClient(MONGO_HOST)
current_collection = "tweets"
default_collection = "tweets"
statistics_file_id = "0000000000"
query_file_id = "1111111111"
streamming_file_id = "2222222222"
users_file_id = "3333333333"
db = client.twitterdb


#additional_function_pattern = re.compile(".*\)\.(\w+)\(.*")
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
    return db[(collection or "tweets")].find({'_id': {'$nin': [statistics_file_id, query_file_id, streamming_file_id]}})

def get_tweets_ids_that_are_already_in_the_database(tweet_ids_list,collection):
    map(ObjectId,tweet_ids_list)
    cursor_resultados = db[collection].find({'_id': {'$in': tweet_ids_list}},{'_id':1})
    tweets_id_list = [x["_id"] for x in cursor_resultados]
    return tweets_id_list

def get_statistics_file_from_collection(collection):
    def delete_statistics_file():
        print("[MONGO STATISTICS WARN] Deleting statistics file")
        db[collection].remove({"_id":'0000000000'})
        print("[MONGO STATISTICS WARN] Statistics file has been deleted")

    cursor_resultados = db[(collection or "tweets")].find({"_id": statistics_file_id } )
    file_list = [x for x in cursor_resultados]
    if len(file_list) >1:
        raise Exception('[MONGO STATISTICS ERROR] Hay mas de un fichero con _id igual al de estadísticas : _id ={} '.format(statistics_file_id))
    elif len(file_list) == 1:
        print("[MONGO STATISTICS INFO] Fichero de estadísticas correctamente recuperado para la colección {}".format(collection))
        statistics_dict = file_list[0]
        if statistics_dict["messages_count"]==0:
            print("[MONGO STATISTICS WARN] El fichero está corrupto messages_count=0 se recalcularán las estadísticas...")
            delete_statistics_file()
            return None
        elif get_count_of_a_collection(collection) not in range(statistics_dict["messages_count"],statistics_dict["messages_count"]+4):
            print("[MONGO STATISTICS WARN] El fichero está corrupto messages_count={} database_count={}".format(statistics_dict["messages_count"],get_count_of_a_collection(collection)))
            delete_statistics_file()
            return None

        way_of_send_with_keys_with_dots =  change_bullet_in_keys_for_dot(statistics_dict["way_of_send_counter"])
        statistics_dict["way_of_send_counter"] = way_of_send_with_keys_with_dots
        return statistics_dict
    else:
        print("[MONGO STATISTICS INFO] No hay fichero de estadísticas para la colección {}".format(collection))
        return None


def get_query_file(collection):
    cursor_resultados = db[(collection or "tweets")].find({"_id": query_file_id})
    file_list = [ x for x in cursor_resultados]
    if len(file_list) >1:
        raise Exception('[MONGO QUERY_FILE ERROR] Hay mas de un fichero con _id igual al de querys: _id = {}'.format(query_file_id))
    elif len(file_list) == 1:
        print("[MONGO QUERY_FILE INFO] Fichero de querys correctamente recuperado para la colección {}".format(collection))
        return file_list[0]
    else:
        print("[MONGO QUERY_FILE INFO] No hay fichero de querys para la colección {}".format(collection))
        return None

def get_streamming_file(collection):
    cursor_resultados = db[(collection or "tweets")].find({"_id": streamming_file_id})
    file_list = [ x for x in cursor_resultados]
    if len(file_list) >1:
        raise Exception('[MONGO STREAMMING_FILE ERROR] Hay mas de un fichero con _id igual al de streamming: _id = {}'.format(streamming_file_id))
    elif len(file_list) == 1:
        print("[MONGO STREAMMING_FILE INFO] Fichero de querys correctamente recuperado para la colección {}".format(collection))
        return file_list[0]
    else:
        print("[MONGO STREAMMING_FILE INFO] No hay fichero de querys para la colección {}".format(collection))
        return None

def get_users_file(collection):
    cursor_resultados = db[(collection or "tweets")].find({"_id": users_file_id})
    file_list = [ x for x in cursor_resultados]
    if len(file_list) >1:
        raise Exception('[MONGO USERS_FILE ERROR] Hay mas de un fichero con _id igual al de usuarios: _id = {}'.format(streamming_file_id))
    elif len(file_list) == 1:
        print("[MONGO USERS_FILE INFO] Fichero de usuarios correctamente recuperado para la colección {}".format(collection))
        return file_list[0]
    else:
        print("[MONGO USERS_FILE INFO] No hay fichero de usuarios para la colección {}".format(collection))
    return None

def get_users_from_users_file(users_dict):
    if users_dict !=None:
        aux = users_dict
        del aux["_id"]
        return aux.keys()
    else:
        return []    

def get_querys_from_query_file(query_dict):
    if query_dict !=None:
        return [ query_dict[str(i)]["query"] for i in range(len(query_dict)-1) ]
    else:
        return []

def get_list_of_words_comprobation_list_from_streamming_file(streamming_dict):
    if streamming_dict !=None:
        return [ streamming_dict[str(i)]["words_comprobation"] for i in range(len(streamming_dict)-1) ]
    else:
        return []


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


def insert_or_update_query_file(collection, query,captured_tweets, min_tweet_id, max_tweet_id, min_creation_date, max_creation_date ):
    query_dict = get_query_file(collection)
    if query_dict != None:
        nuevo_fichero = False
        query_list = get_querys_from_query_file(query_dict)
    else:
        nuevo_fichero =True
        query_list = []
        query_dict = {"_id" : query_file_id}
    #TODO improve comprobation
    if query not in query_list:
        aux = {}
        aux["query"] = query
        aux["last_execution"] = str(datetime.now())
        aux["max_tweet_id"] = max_tweet_id
        aux["min_tweet_id"] = min_tweet_id
        aux["min_creation_date"] = min_creation_date
        aux["max_creation_date"] = max_creation_date
        aux["search_type"] = "tweets captured by query"
        aux["captured_tweets"] = captured_tweets
        query_dict[str(len(query_dict)-1)]= aux
    else:
        l = []
        for i in range(len(query_dict)-1):
            value = query_dict[str(i)]
            if value["query"] == query:
                l.append(str(i))
        index=l[0]
        aux = query_dict[index]
        aux["last_execution"] = str(datetime.now())
        aux["max_tweet_id"] = max(max_tweet_id,aux["max_tweet_id"])
        aux["min_tweet_id"] = min(min_tweet_id,aux["min_tweet_id"])
        aux["min_creation_date"] = min(str(min_creation_date),aux["min_creation_date"])
        aux["max_creation_date"] = max(str(max_creation_date),aux["max_creation_date"])
        aux["captured_tweets"] = aux["captured_tweets"]+captured_tweets
        query_dict[index] = aux
         
    
    if nuevo_fichero:
        print("[MONGO INSERT QUERY FILE INFO] Inserting new query file")
        db[collection].insert(query_dict)
        print("[MONGO INSERT QUERY FILE INFO] The query file has been save sucessfully")
    else:
        print("[MONGO INSERT QUERY FILE INFO] Replacing query file")
        db[collection].replace_one({"_id" : query_file_id },query_dict)
        print("[MONGO INSERT QUERY FILE INFO] The query file has been replaced save sucessfully")



def insert_or_update_query_file_streamming(collection, words_list ,captured_tweets, min_tweet_id, max_tweet_id, min_creation_date, max_creation_date ):
    streamming_dict = get_streamming_file(collection)
    if streamming_dict != None:
        print("[MONGO INSERT STREAMMING FILE INFO] There is streamming file")
        nuevo_fichero = False
        comprobation_words_list = get_list_of_words_comprobation_list_from_streamming_file(streamming_dict)
    else:
        print("[MONGO INSERT STREAMMING FILE INFO] THERE IS NO streamming file")
        nuevo_fichero =True
        comprobation_words_list = []
        streamming_dict = {"_id" : streamming_file_id}

    #TODO improve comprobation
    words_comprobation =",".join(sorted([i.lower() for i in words_list]))
    if words_comprobation not in comprobation_words_list:
        print("[MONGO INSERT STREAMMING FILE INFO] Words are not in file")
        aux = {}
        aux["words"] = words_list
        aux["words_comprobation"] = words_comprobation
        aux["last_execution"] = str(datetime.now())
        aux["max_tweet_id"] = max_tweet_id
        aux["min_tweet_id"] = min_tweet_id
        aux["min_creation_date"] = min_creation_date
        aux["max_creation_date"] = max_creation_date
        aux["search_type"] = "tweets captured by streamming"
        aux["captured_tweets"] = captured_tweets
        streamming_dict[str(len(streamming_dict)-1)]= aux
    else:
        print("[MONGO INSERT STREAMMING FILE INFO] Words are in file")
        l = []
        for i in range(len(streamming_dict)-1):
            value = streamming_dict[str(i)]
            if value["words_comprobation"] == words_comprobation:
                l.append(str(i))
        index=l[0]
        aux = streamming_dict[index]
        aux["last_execution"] = str(datetime.now())
        aux["max_tweet_id"] = max(max_tweet_id,aux["max_tweet_id"])
        aux["min_tweet_id"] = min(min_tweet_id,aux["min_tweet_id"])
        aux["min_creation_date"] = min(str(min_creation_date),aux["min_creation_date"])
        aux["max_creation_date"] = max(str(max_creation_date),aux["max_creation_date"])
        aux["captured_tweets"] = aux["captured_tweets"]+captured_tweets
        streamming_dict[index] = aux
         
    
    if nuevo_fichero:
        print("[MONGO INSERT STREAMMING FILE INFO] Inserting new streamming file")
        db[collection].insert(streamming_dict)
        print("[MONGO INSERT STREAMMING FILE INFO] The query streamming has been save sucessfully")
    else:
        print("[MONGO INSERT STREAMMING FILE INFO] Replacing streamming file")
        db[collection].replace_one({"_id" : streamming_file_id },streamming_dict)
        print("[MONGO INSERT STREAMMING FILE INFO] The streamming file has been replaced save sucessfully")
    


def insert_or_update_user_file(collection, user,captured_tweets, min_tweet_id, max_tweet_id, min_creation_date, max_creation_date):
    user= user.lower()
    users_dict = get_users_file(collection)
    if users_dict != None:
        nuevo_fichero = False
        users_list = get_users_from_users_file(users_dict)
    else:
        nuevo_fichero =True
        users_list = []
        users_dict = {"_id" : users_file_id}
    #TODO improve comprobation
    if user not in users_list:
        aux = {}
        aux["user"] = user
        aux["last_execution"] = str(datetime.now())
        aux["max_tweet_id"] = max_tweet_id
        aux["min_tweet_id"] = min_tweet_id
        aux["min_creation_date"] = min_creation_date
        aux["max_creation_date"] = max_creation_date
        aux["search_type"] = "tweets captured by user"
        aux["captured_tweets"] = captured_tweets
        users_dict[user]= aux
    else:
        aux = users_dict[user]
        aux["last_execution"] = str(datetime.now())
        aux["max_tweet_id"] = max(max_tweet_id,aux["max_tweet_id"])
        aux["min_tweet_id"] = min(min_tweet_id,aux["min_tweet_id"])
        aux["min_creation_date"] = min(str(min_creation_date),aux["min_creation_date"])
        aux["max_creation_date"] = max(str(max_creation_date),aux["max_creation_date"])
        aux["captured_tweets"] = aux["captured_tweets"]+captured_tweets
        users_dict[user] = aux

    if nuevo_fichero:
        print("[MONGO INSERT USER FILE INFO] Inserting new user file")
        db[collection].insert(users_dict)
        print("[MONGO INSERT USER FILE INFO] The user file has been save sucessfully")
    else:
        print("[MONGO INSERT USER FILE INFO] Replacing user file")
        db[collection].replace_one({"_id" : users_file_id },users_dict)
        print("[MONGO INSERT USER FILE INFO] The user file has been replaced save sucessfully")



