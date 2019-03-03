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
db = client.twitterdb

not_controlled_functions = ["aggregate","bulkWrite","copyTo","estimatedDocumentCount","createIndex",
    "createIndexes","dataSize","dropIndex","dropIndexes","ensureIndex","explain"
    ,"findAndModify","findOneAndDelete","findOneAndReplace","findOneAndUpdate","getIndexes","getShardDistribution",
    "getShardVersion","insertMany","isCapped","latencyStats","mapReduce","reIndex",
    "renameCollection","save","storageSize","totalIndexSize","totalSize","watch","validate"]
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
    return db[(collection or "tweets")].find({'_id': {'$nin': [statistics_file_id]}})

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
        elif get_count_of_a_collection(collection) != statistics_dict["messages_count"]+1:
            print("[MONGO STATISTICS WARN] El fichero está corrupto messages_count={} database_count={}".format(statistics_dict["messages_count"],get_count_of_a_collection(collection)))
            delete_statistics_file()
            return None

        way_of_send_with_keys_with_dots =  change_bullet_in_keys_for_dot(statistics_dict["way_of_send_counter"])
        statistics_dict["way_of_send_counter"] = way_of_send_with_keys_with_dots
        return statistics_dict
    else:
        print("[MONGO STATISTICS INFO] No hay fichero de estadísticas para la colección {}".format(collection))
        return None






def execute_string_query(string_query):
    collection_name = string_query.split(".")[1]
    method_name = string_query.split(".")[2].split("(")[0]
    
    first_index = string_query.index("{")
    last_index = len(string_query) - string_query[::-1].index("}")
    query_body = string_query[first_index:last_index]

    query_dict = parse_string_query(query_body)
    
    # print(collection_name)
    # print(method_name)
    # print(string_query[first_index:last_index])

    if method_name in not_controlled_functions:
        print("[MONGO EXECUTE STRING QUERY ERROR ] FUNCION {} ENCONTRADA PERO NO CONTROLADA/IMPLEMENTADA".format(method_name))
        return None
    elif method_name == "insert":
        res = db[collection_name].insert(query_body)
    elif method_name == "findOne":
        res = db[collection_name].find_one(query_body)
    elif method_name == "find":
        res = db[collection_name].find(query_body)
    elif method_name == "update":
        res = db[collection_name].update(query_body)
    elif method_name == "updateOne":
        res = db[collection_name].update_one(query_body)
    elif method_name == "updateMany":
        res = db[collection_name].update_many(query_body)
    elif method_name == "count":
        res = db[collection_name].count(query_body)  
    elif method_name == "countDocuments":
        res = db[collection_name].count_documents(query_body)
    elif method_name == "deleteOne":
        res = db[collection_name].delete_one(query_body)
    elif method_name == "deleteMany":
        res = db[collection_name].delete_many(query_body)
    elif method_name == "distinct":
        res = db[collection_name].distinct(query_body)
    elif method_name == "drop":
        res = db[collection_name].drop(query_body)
    elif method_name == "group":
        res = db[collection_name].group(query_dict["key"],query_dict.get("cond",{}),query_dict["initial"],query_dict["reduce"]) #CHECKED
    elif method_name == "insertOne":
        res = db[collection_name].insert_one(query_body)
    elif method_name == "replaceOne":
        res = db[collection_name].replace_one(query_body)
    elif method_name == "stats":
        res = db[collection_name].stats(query_body)
    elif method_name == "remove":
        res = db[collection_name].remove(query_body)
    else:
        print("[MONGO EXECUTE STRING QUERY ERROR ] FUNCION {} NO ENCONTRADA".format(method_name))
        return None
    return res

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
def remove_lateral_spaces_and_quotes(string):
    aux = string.strip()
    if aux[0]== '"':
        aux = aux[1:]
    if aux[-1]== '"':
        aux = aux[:-1]
    if aux[-1]== ',':
        aux = aux[:-1]
    return aux

def build_query_dict_for_unique_dict(string_query,show=False):
    # parte de la query puede ser codigo que no esta entre comillas y da problemas por lo que con estas regex,
    # uno cada key : value del dict en una linea para poder luego controlarlos
    query2 = re.sub("\s\s\s\s\s\s+", "", string_query) #eliminamos la identacion de los elementos internos
    query3 = re.sub("\s\s\s\s\s+}", "}", query2) #eliminamos la identacion de los diccionarios valores
    query4 = query3.split("\n")[1:-1] # Separamos por líneas dejando en cada linea la key y su valor y eliminamos los {} mas externos
    query5 = [ x.split(":",1) for x in query4] # separamos el contenido de cada línea en key value
    query6 = [(remove_lateral_spaces_and_quotes(x[0]),remove_lateral_spaces_and_quotes(x[1])) for x in query5] # quitamos los espacios laterales y las comillas externas
    

    query_dict = {}
    for key,value in query6:
        if value.startswith("{"):
            query_dict[key] = json.loads(value)
        elif value.startswith("function"):
            query_dict[key] = Code(value)
        else:
            print("\n\n\n[MONGO build_query_dict_for_unique_dict ] Value doesn't controlled {}".format(value))
            exit(1)

    return query_dict

def parse_string_query(string_query,show=False):
    print(string_query)
    input()

    functions_called = re.findall("\)\.(\w+)\(",string_query)
    if len(functions_called)>0:
        end_index = string_query.index(functions_called[0])-2
        string_query = string_query[:end_index]
        print(" DELETED END FUNCTIONS\n {}".format(string_query))

    result_is_one_dict = False
    should_be_builded = False
    result_dict = {}
    if string_query.split("\n")[0]=="{":
        result_is_one_dict = True
    if "function(" in string_query:
        should_be_builded = True

    if result_is_one_dict and should_be_builded:
        result_dict = build_query_dict_for_unique_dict(string_query,show)
    elif result_is_one_dict and not should_be_builded:
        result_dict = json.loads(string_query)
    elif not result_is_one_dict and should_be_builded:
        pass
    else:
        #CONTINUAR: VER COMO SEPARAR LOS DICTS
        lines = string_query.split("\n")
        strings_list = []
        aux = []
        for line in lines:
            for letter_index in range(len(line)):
                aux.append(line[letter_index])
                if line[letter_index] =='}' and letter_index <= 5:
                    strings_list.append("".join(aux).strip())
                    aux=[]

        for e in strings_list:
            if e.startswith(","):
                e = e[1:]
            print(e)
        pass
    
    for k,v in result_dict.items():
        print("{}    {}".format(k,v))
    
    return result_dict


query='''db.tweets.find({}, {
    "user.id": 1
}).sort({
    "user.id": 1
});'''

query2 = '''db.tweets.group({
    "key": {
        "user.id": true
    },
    "initial": {
        "countstar": 0
    },
    "reduce": function(obj, prev) {
        if (true != null) if (true instanceof Array) prev.countstar += true.length;
        else prev.countstar++;
    }
});'''

query3='''db.tweets.group({
    "key": {
        "user.id": true
    },
    "initial": {}
});'''

resultado = execute_string_query(query)
print(resultado)