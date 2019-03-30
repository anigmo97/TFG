from pymongo import MongoClient,errors
from bson.objectid import ObjectId
# from global_functions import change_dot_in_keys_for_bullet,change_bullet_in_keys_for_dot
import traceback
import json
import ast # to load query string to dict
from datetime import datetime
import re
from bson.code import Code

MONGO_HOST= 'mongodb://localhost/tweet'
client = MongoClient(MONGO_HOST)
db = client.twitterdb

current_collection = "tweets"
default_collection = "tweets"

###################### SPECIAL DOCS #######################################################
statistics_file_id = "statistics_file_id"
query_file_id = "query_file_id"
streamming_file_id = "streamming_file_id"
searched_users_file_id = "searched_users_file_id"
likes_list_file_id = "likes_list_file_id"
users_file_id = "users_file_id"

special_doc_ids = [statistics_file_id,query_file_id,streamming_file_id,searched_users_file_id,likes_list_file_id,users_file_id]


##########################################################################################
##################################### AUXILIAR ###########################################
##########################################################################################
def replace_bullet_with_dot(word):
    return word.replace('•','.')

def replace_dot_with_bullet(word):
    return word.replace('.','•') 

def change_dot_in_keys_for_bullet(dicctionary):
    new_dict = {}
    for k,v in dicctionary.items():
        if "." in k:
            print("[CHANGE DOT FOR BULLET INFO] Changing '.' in key {} for '•'".format(k))
            new_key = replace_dot_with_bullet(k)
            new_dict[new_key] = v
        else:
            new_dict[k] = v
    return new_dict

def change_bullet_in_keys_for_dot(dicctionary):
    new_dict = {}
    for k,v in dicctionary.items():
        if "•" in k:
            print("[CHANGE BULLET FOR DOT INFO] Changing '•' in key {} for '.'".format(k))
            new_key = replace_bullet_with_dot(k)
            new_dict[new_key] = v
        else:
            new_dict[k] = v
    return new_dict




#additional_function_pattern = re.compile(".*\)\.(\w+)\(.*")
##########################################################################################
##################################### GET INFO ###########################################
##########################################################################################

def get_count_of_a_collection(collection):
    return db[collection].count()

def get_tweet_ids_list_from_database(collection="tweets"):
    cursor_resultados = db[(collection or "tweets")].find({},{ "id_str": 1, "_id": 1 } )
    tweets_id_list = [x["id_str"] for x in cursor_resultados if x["_id"] not in special_doc_ids ]
    return tweets_id_list

def get_tweet_ids_list_of_a_user_from_collection(user_id,collection="tweets"):
    cursor_resultados = db[(collection or "tweets")].find({"user.id_str" : user_id},{ "id_str": 1, "_id": 1 } )
    tweets_id_list = [x["id_str"] for x in cursor_resultados if x["_id"] not in special_doc_ids ]
    return tweets_id_list

def get_user_id_wih_screenname(user_screen_name):
    users_file = get_searched_users_file(current_collection)
    user = users_file.get(user_screen_name,None)
    print("[GET USER_ID WITH SCREEN NAME INFO] user = {}".format(json.dumps(user,indent=4)))
    return user.get("user_id",None)

def get_users_of_a_political_party(political_party,collection):
    ["PP","pp","PSOE","psoe","Psoe","Podemos","podemos","PODEMOS","ciudadanos","cs","CIUDADANOS","CS","Ciudadanos"]
    political_party = political_party.upper()
    if political_party=="CS":
        political_party="CIUDADANOS"
    searched_users_file = get_searched_users_file(collection)
    politics = []
    if searched_users_file != None:
        for k,v in searched_users_file.items():
            if k != "_id" and k!="total_captured_tweets":
                if v["partido"] == political_party:
                    politics.append(k)

    return politics

    

def get_tweets_cursor_from_mongo(collection="tweets"):
    print("[MONGO GET CURSOR INFO] Coleccion = {}".format(collection))
    return db[(collection or "tweets")].find({'_id': {'$nin': special_doc_ids }})

def get_tweets_ids_that_are_already_in_the_database(tweet_ids_list,collection):
    map(ObjectId,tweet_ids_list)
    cursor_resultados = db[collection].find({'_id': {'$in': tweet_ids_list}},{'_id':1})
    tweets_id_list = [x["_id"] for x in cursor_resultados]
    return tweets_id_list

def get_keys_of_special_file_except_doc_id(special_doc):
    if special_doc !=None:
        aux = special_doc
        del aux["_id"]
        return aux.keys()
    else:
        return []

def get_collection_names():
    return db.collection_names()


def get_user_screen_name_of_tweet_id(tweet_id,collection):
    cursor_resultados = db[collection].find({"_id": tweet_id})
    return cursor_resultados[0]["user"]["screen_name"]

def get_users_screen_name_dict_of_tweet_ids(tweet_id_list,collection):
    cursor_resultados = db[collection].find({'_id': {'$in': tweet_id_list}},{'_id':1,'user.screen_name':1})
    dict_tweet_user = {}
    for e in cursor_resultados:
        print(e)
        dict_tweet_user[e["_id"]] = e["user"]["screen_name"]
    print(dict_tweet_user)
    return dict_tweet_user

def get_tweets_to_analyze_or_update_stats(collection,limit=0):
    lista_tweets = list(db[collection].find({"analyzed" :{"$ne": True}, '_id': {'$nin': special_doc_ids }}).limit(limit))
    print("[TWEETS FOR ANALYZE] {} tweets retrieved".format(len(lista_tweets)))
    return lista_tweets


def get_tweet_owner_dict_data_of_tweet_ids(tweet_id_list,collection):
    cursor_resultados = db[collection].find({'_id': {'$in': tweet_id_list}},
    {'_id':1,'user.screen_name':1,
    'retweeted_status.user.id_str':1,'retweeted_status.user.screen_name':1,'retweeted_status.id_str':1,
    'quoted_status.user.id_str':1,'quoted_status.user.screen_name':1,'quoted_status.id_str':1,'last_update':1})
    dict_tweet_user = {}
    for e in cursor_resultados:
        print(e)
        aux = {}
        aux["user_screen_name"] = e["user"]["screen_name"]
        aux["last_update"] = e["last_update"]
        aux["is_retweet"] = bool(e.get("retweeted_status",False)) or False
        if aux["is_retweet"]:
            aux["retweeted_user_screen_name"] = e["retweeted_status"]["user"]["screen_name"]
            aux["retweeted_tweet_id"] = e["retweeted_status"]["id_str"]
        aux["is_quote"] = bool(e.get("quoted_status",False)) or False
        if aux["is_quote"]:
            aux["quoted_user_screen_name"] = e["quoted_status"]["user"]["screen_name"]
            aux["quoted_tweet_id"] = e["quoted_status"]["id_str"]
        dict_tweet_user[e["_id"]] = aux

    print(dict_tweet_user)
    return dict_tweet_user

def get_users_screen_name_dict_of_tweet_ids_for_tops_in_statistics_file(statistics_file,collection):
    top_10_name_lists = ["global_most_favs_tweets","global_most_rt_tweets","local_most_replied_tweets","local_most_quoted_tweets"]
    tweet_id_list = []
    for top_list in top_10_name_lists:
        for e in statistics_file[top_list]:
            tweet_id_list.append(e[0])

    return  get_users_screen_name_dict_of_tweet_ids(tweet_id_list,collection)



##########################################################################################
##################################### UPDATE   ###########################################
##########################################################################################

def update_many_tweets_dicts_in_mongo(tweets_list,collection="tweets"):
    # replaceOne
    # update_one
    # db.tweets.update_many(tweets_list) hace falta un filter y un update tal vez se pueda hacer
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for tweet in tweets_list:
        tweet_id = tweet["id_str"]
        tweet["last_update"] = now
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
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for e in tweets_no_repetidos:
                e["first_capture"] = now
                e["last_update"] = now
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


##########################################################################################
############################### SPECIAL DOCS MANAGEMENT ##################################
##########################################################################################

def do_additional_actions_for_statistics_file(statistics_dict,collection):
    # def delete_statistics_file():
    #     print("[MONGO STATISTICS WARN] Deleting statistics file")
    #     db[collection].remove({"_id":statistics_file_id})
    #     print("[MONGO STATISTICS WARN] Statistics file has been deleted")

    # if statistics_dict["messages_count"]==0:
    #     print("[MONGO STATISTICS WARN] El fichero está corrupto messages_count=0 se recalcularán las estadísticas...")
    #     delete_statistics_file()
    #     return None
    # elif get_count_of_a_collection(collection) not in range(statistics_dict["messages_count"],statistics_dict["messages_count"]+len(special_doc_ids)+1):
    #     print("[MONGO STATISTICS WARN] El fichero está corrupto messages_count={} database_count={}".format(statistics_dict["messages_count"],get_count_of_a_collection(collection)))
    #     delete_statistics_file()
    #     return None
    print("[MONGO STATISTICS INFO] Changing bullets for dots")
    way_of_send_with_keys_with_dots =  change_bullet_in_keys_for_dot(statistics_dict["way_of_send_counter"])
    statistics_dict["way_of_send_counter"] = way_of_send_with_keys_with_dots
    return statistics_dict

def get_log_dict_for_special_file_id(file_id):
    aux = {
        statistics_file_id : { "upper_name" : "STATISTICS_FILE", "file_aux" :"Fichero de estadisticas" , "file_id" : statistics_file_id },
        query_file_id : { "upper_name" : "QUERY_FILE", "file_aux" :"Fichero de querys" , "file_id" : query_file_id },
        streamming_file_id : { "upper_name" : "STREAMMING_FILE", "file_aux" :"Fichero de busquedas por streamming" , "file_id" : streamming_file_id },
        searched_users_file_id : { "upper_name" : "SEARCHED_USERS_FILE", "file_aux" :"Fichero de usuarios buscados" , "file_id" : searched_users_file_id },
        users_file_id : { "upper_name" : "USERS_FILE", "file_aux" :"Fichero de usuarios" , "file_id" : users_file_id },
        likes_list_file_id : { "upper_name" : "LIKES_FILE", "file_aux" :"Fichero de likes" , "file_id" : likes_list_file_id }
        
    }
    return aux.get(file_id,None)

def _get_special_file(collection,file_id):    
    e = get_log_dict_for_special_file_id(file_id)
    cursor_resultados = db[(collection or "tweets")].find({"_id": e["file_id"]})
    file_list = [ x for x in cursor_resultados]
    if len(file_list) >1:
        raise Exception('[MONGO {} ERROR] Hay mas de un fichero con _id igual al {}: _id = {}'.format(e["upper_name"],e["file_aux"],e["file_id"]))
    elif len(file_list) == 1:
        print("[MONGO {} INFO] {} correctamente recuperado para la colección {}".format(e["upper_name"],e["file_aux"],collection))
        retrieved_file = file_list[0]
        if e["file_id"] != statistics_file_id:
            return retrieved_file
        else:
            return do_additional_actions_for_statistics_file(retrieved_file,collection)
    else:
        print("[MONGO {} INFO] No hay {} para la colección {}".format(e["upper_name"],e["file_aux"],collection))
        return None

def get_statistics_file_from_collection(collection):
    return _get_special_file(collection,statistics_file_id)

def get_query_file(collection):
    return _get_special_file(collection,query_file_id)

def get_streamming_file(collection):
    return _get_special_file(collection,streamming_file_id)

def get_searched_users_file(collection):
    return _get_special_file(collection,searched_users_file_id)

def get_likes_list_file(collection):
    return _get_special_file(collection,likes_list_file_id)

def get_users_file(collection):
    return _get_special_file(collection,users_file_id)


########################################################## INSERTS #########################################################

def insert_statistics_file_in_collection(statistics_dict,collection):
    statistics_dict["_id"] = statistics_file_id
    statistics_dict["ultima_modificación"] = str(datetime.now())
    way_of_send_with_keys_without_dots =  change_dot_in_keys_for_bullet(statistics_dict["way_of_send_counter"])
    statistics_dict["way_of_send_counter"] = way_of_send_with_keys_without_dots

    print("[MONGO INSERT STATISTICS FILE INFO] Inserting new statistics file in collection {}".format(collection))
    if get_statistics_file_from_collection(collection) == None:
        print("[MONGO INSERT STATISTICS FILE INFO] Inserting new statistics file")
        db[collection].insert(statistics_dict)
        print("[MONGO INSERT STATISTICS FILE INFO] The statistics file has been save sucessfully")
    else:
        print("[MONGO INSERT STATISTICS FILE INFO] Replacing statistics file")
        db[collection].replace_one({"_id" : statistics_file_id },statistics_dict) 
        print("[MONGO INSERT STATISTICS FILE INFO] The statistics file has been replaced save sucessfully")


def create_new_common_management_special_doc_dict(captured_tweets, min_tweet_id, max_tweet_id, min_creation_date, max_creation_date,capture_type):
    aux = {}
    aux["last_execution"] = str(datetime.now())
    aux["max_tweet_id"] = max_tweet_id
    aux["min_tweet_id"] = min_tweet_id
    aux["min_creation_date"] = min_creation_date
    aux["max_creation_date"] = max_creation_date
    aux["search_type"] = capture_type
    aux["captured_tweets"] = captured_tweets
    return aux

def update_common_management_special_doc_dict(dict_for_update,max_tweet_id,min_tweet_id,min_creation_date,max_creation_date,captured_tweets):
    aux = dict_for_update
    aux["last_execution"] = str(datetime.now())
    aux["max_tweet_id"] = max(max_tweet_id,aux["max_tweet_id"])
    aux["min_tweet_id"] = min(min_tweet_id,aux["min_tweet_id"])
    aux["min_creation_date"] = min(str(min_creation_date),aux["min_creation_date"])
    aux["max_creation_date"] = max(str(max_creation_date),aux["max_creation_date"])
    aux["captured_tweets"] = aux["captured_tweets"]+captured_tweets
    return aux

def _insert_or_update_special_file(collection,captured_tweets, min_tweet_id, max_tweet_id, min_creation_date, max_creation_date,file_id,
            query=None,words=None,words_comprobation=None,user=None,user_id=None,partido=None):
    
    print("[INSERT OR UPDATE SPECIAL FILE INFO]")
    if file_id not in special_doc_ids:
        raise Exception("El id {} no está entre los ids especiales".format(file_id))
        # tal vez solo use 3 ids

    logs = get_log_dict_for_special_file_id(file_id)

    special_doc_dict = _get_special_file(collection,file_id)

    if special_doc_dict != None:
        print("[INSERT OR UPDATE {0} INFO] There is {0} in collection {1}".format(logs["upper_name"],collection))
        nuevo_fichero = False
        special_doc_dict["total_captured_tweets"] = special_doc_dict["total_captured_tweets"] + captured_tweets
    else:
        print("[INSERT OR UPDATE {0} INFO] There is NO {0} in collection {1}".format(logs["upper_name"],collection))
        nuevo_fichero =True
        special_doc_dict = {"_id" : file_id}
        special_doc_dict["total_captured_tweets"] = captured_tweets

    if file_id == query_file_id:
        if query not in special_doc_dict:
            print("[INSERT OR UPDATE {0} INFO] Query is not in {0} (collection {1}), adding new entry ...".format(logs["upper_name"],collection))
            aux = create_new_common_management_special_doc_dict(captured_tweets, min_tweet_id, max_tweet_id, min_creation_date, max_creation_date,"tweets captured by query")
            aux["query"] = query
            special_doc_dict[query]= aux
        else:
            print("[INSERT OR UPDATE {0} INFO] Query is in {0} already (collection {1}), updating entry ...".format(logs["upper_name"],collection))
            aux = update_common_management_special_doc_dict(special_doc_dict[query],max_tweet_id,min_tweet_id,min_creation_date,max_creation_date,captured_tweets)
            special_doc_dict[query] = aux
    elif file_id == streamming_file_id:
        words_comprobation =",".join(sorted([i.lower() for i in words]))
        if words_comprobation not in special_doc_dict:
            print("[INSERT OR UPDATE {0} INFO] WordsComprobation are not in {0} (collection {1}), adding new entry ...".format(logs["upper_name"],collection))
            aux = create_new_common_management_special_doc_dict(captured_tweets, min_tweet_id, max_tweet_id, min_creation_date, max_creation_date,"tweets captured by streamming")
            aux["words"] = words
            aux["words_comprobation"] = words_comprobation
            special_doc_dict[words_comprobation]= aux
        else:
            print("[INSERT OR UPDATE {0} INFO] Words comprobation are in {0} already (collection {1}), updating entry ...".format(logs["upper_name"],collection))
            aux = update_common_management_special_doc_dict(special_doc_dict[words_comprobation],max_tweet_id,min_tweet_id,min_creation_date,max_creation_date,captured_tweets)
            special_doc_dict[words_comprobation] = aux
    elif file_id == searched_users_file_id:
        if user not in special_doc_dict:
            print("[INSERT OR UPDATE {0} INFO] User not in {0} (collection {1}), adding new entry ...".format(logs["upper_name"],collection))
            aux = create_new_common_management_special_doc_dict(captured_tweets, min_tweet_id, max_tweet_id, min_creation_date, max_creation_date,"tweets captured by user")
            aux["user"] = user
            aux["user_id"] = user_id
            aux["partido"] = partido
            special_doc_dict[user]= aux
        else:
            print("[INSERT OR UPDATE {0} INFO] User is in {0} already (collection {1}), updating entry ...".format(logs["upper_name"],collection))
            aux = update_common_management_special_doc_dict(special_doc_dict[user],max_tweet_id,min_tweet_id,min_creation_date,max_creation_date,captured_tweets)
            special_doc_dict[user] = aux
    else:
        raise Exception("El id {} no está entre los ids especiales".format(file_id))


    if nuevo_fichero:
        print("[MONGO INSERT {0} INFO] Inserting new {0}".format(logs["upper_name"]))
        db[collection].insert(special_doc_dict)
        print("[MONGO INSERT {0} INFO] The {0} has been save sucessfully".format(logs["upper_name"]))
    else:
        print("[MONGO INSERT {0} INFO] Replacing {0}".format(logs["upper_name"]))
        db[collection].replace_one({"_id" : file_id },special_doc_dict)
        print("[MONGO INSERT {0} INFO] The {0} has been replaced and save sucessfully".format(logs["upper_name"]))
    


def insert_or_update_query_file(collection, query,captured_tweets, min_tweet_id, max_tweet_id, min_creation_date, max_creation_date ):
    _insert_or_update_special_file(collection=collection,captured_tweets=captured_tweets, min_tweet_id=min_tweet_id, max_tweet_id=max_tweet_id,min_creation_date = min_creation_date, max_creation_date=max_creation_date,
     file_id=query_file_id,query=query)
         

def insert_or_update_query_file_streamming(collection, words_list ,captured_tweets, min_tweet_id, max_tweet_id, min_creation_date, max_creation_date ):
    _insert_or_update_special_file(collection=collection,captured_tweets=captured_tweets, min_tweet_id=min_tweet_id, max_tweet_id=max_tweet_id,min_creation_date = min_creation_date, max_creation_date=max_creation_date,
     file_id=streamming_file_id,words=words_list)
    


def insert_or_update_searched_users_file(collection, user,user_id,captured_tweets, min_tweet_id, max_tweet_id, min_creation_date, max_creation_date,partido):
    user= user.lower()
    _insert_or_update_special_file(collection=collection,captured_tweets=captured_tweets, min_tweet_id=min_tweet_id, max_tweet_id=max_tweet_id,min_creation_date = min_creation_date, max_creation_date=max_creation_date,
     file_id=searched_users_file_id,user=user,user_id=user_id,partido=partido)


def insert_or_update_users_file(collection,user_id, user_screen_name,likes_to_PP,likes_to_PSOE,likes_to_PODEMOS,likes_to_CIUDADANOS,likes_to_VOX,likes_to_COMPROMIS):

    logs = get_log_dict_for_special_file_id(users_file_id)

    special_doc_dict = _get_special_file(collection,users_file_id)

    if special_doc_dict != None:
        print("[INSERT OR UPDATE {0} INFO] There is {0} in collection {1}".format(logs["upper_name"],collection))
        nuevo_fichero = False
    else:
        print("[INSERT OR UPDATE {0} INFO] There is NO {0} in collection {1}".format(logs["upper_name"],collection))
        nuevo_fichero =True
        special_doc_dict = {"_id" : users_file_id}

    if user_id not in special_doc_dict:
        print("[INSERT OR UPDATE {0} INFO] Query is not in {0} (collection {1}), adding new entry ...".format(logs["upper_name"],collection))
        aux = {}
        aux["user_id"] = user_id
        aux["user_screen_name"] = user_screen_name
        aux["likes_to_PP"] = (likes_to_PP or 0)
        aux["likes_to_PSOE"] = (likes_to_PSOE or 0)
        aux["likes_to_PODEMOS"] = (likes_to_PODEMOS or 0)
        aux["likes_to_CIUDADANOS"] = (likes_to_CIUDADANOS or 0)
        aux["likes_to_VOX"] = (likes_to_VOX or 0)
        aux["likes_to_COMPROMIS"] = (likes_to_COMPROMIS or 0)
        aux["last_like_registered"] = str(datetime.now())
        special_doc_dict[user_id]= aux
    else:
        print("[INSERT OR UPDATE {0} INFO] Query is in {0} already (collection {1}), updating entry ...".format(logs["upper_name"],collection))
        aux = special_doc_dict[user_id]
        aux["likes_to_PP"] = aux["likes_to_PP"] + (likes_to_PP or 0)
        aux["likes_to_PSOE"] = aux["likes_to_PSOE"] + (likes_to_PSOE or 0)
        aux["likes_to_PODEMOS"] = aux["likes_to_PODEMOS"] + (likes_to_PODEMOS or 0)
        aux["likes_to_CIUDADANOS"] = aux["likes_to_CIUDADANOS"] + (likes_to_CIUDADANOS or 0)
        aux["likes_to_VOX"] = aux["likes_to_VOX"] + (likes_to_VOX or 0)
        aux["likes_to_COMPROMIS"] = aux["likes_to_COMPROMIS"] + (likes_to_COMPROMIS or 0)
        aux["last_like_registered"] = str(datetime.now())
        special_doc_dict[user_id] = aux


    if nuevo_fichero:
        print("[MONGO INSERT {0} INFO] Inserting new {0}".format(logs["upper_name"]))
        db[collection].insert(special_doc_dict)
        print("[MONGO INSERT {0} INFO] The {0} has been save sucessfully".format(logs["upper_name"]))
    else:
        print("[MONGO INSERT {0} INFO] Replacing {0}".format(logs["upper_name"]))
        db[collection].replace_one({"_id" : users_file_id },special_doc_dict)
        print("[MONGO INSERT {0} INFO] The {0} has been replaced and save sucessfully".format(logs["upper_name"]))
    



def insert_or_update_likes_list_file(collection,tweet_id,num_likes,users_who_liked_list,author_id,author_screen_name):

    logs = get_log_dict_for_special_file_id(likes_list_file_id)

    special_doc_dict = _get_special_file(collection,likes_list_file_id)

    if special_doc_dict != None:
        print("[INSERT OR UPDATE {0} INFO] There is {0} in collection {1}".format(logs["upper_name"],collection))
        nuevo_fichero = False
    else:
        print("[INSERT OR UPDATE {0} INFO] There is NO {0} in collection {1}".format(logs["upper_name"],collection))
        nuevo_fichero =True
        special_doc_dict = {"_id" : likes_list_file_id}

    if tweet_id not in special_doc_dict:
        print("[INSERT OR UPDATE {0} INFO] Query is not in {0} (collection {1}), adding new entry ...".format(logs["upper_name"],collection))
        aux = {}
        aux["tweet_id"] = tweet_id
        aux["user_id"] = author_id
        aux["user_screen_name"] = author_screen_name
        aux["users_who_liked"] = users_who_liked_list
        aux["num_likes"] = num_likes
        aux["last_like_resgistered"] = str(datetime.now())
        special_doc_dict[tweet_id]= aux
    else:
        print("[INSERT OR UPDATE {0} INFO] Query is in {0} already (collection {1}), updating entry ...".format(logs["upper_name"],collection))
        aux = special_doc_dict[tweet_id]
        last_25_likes = aux["users_who_liked"][-25:]
        last_25_ids = [x[0] for x in last_25_likes]
        new_likes = [ x for x in users_who_liked_list if x[0] not in last_25_ids]
        aux["users_who_liked"] = users_who_liked_list + new_likes
        aux["num_likes"] = num_likes
        aux["last_like_resgistered"] = str(datetime.now())
        special_doc_dict[tweet_id] = aux


    if nuevo_fichero:
        print("[MONGO INSERT {0} INFO] Inserting new {0}".format(logs["upper_name"]))
        db[collection].insert(special_doc_dict)
        print("[MONGO INSERT {0} INFO] The {0} has been save sucessfully".format(logs["upper_name"]))
    else:
        print("[MONGO INSERT {0} INFO] Replacing {0}".format(logs["upper_name"]))
        db[collection].replace_one({"_id" : likes_list_file_id },special_doc_dict)
        print("[MONGO INSERT {0} INFO] The {0} has been replaced and save sucessfully".format(logs["upper_name"]))

def mark_docs_as_analyzed(docs_ids,collection):
    print("[mark_docs_as_analyzed] marking as analyzed {} tweets".format(len(docs_ids)))
    db[collection].update({'_id':{'$in': docs_ids}}, {'$set': {"analyzed":True}}, multi=True)

