import argparse
import json
from re import findall
import sys
import os 
import timeit
# USER MODULES IMPORTS
import global_variables
from global_functions import update_top_10_list,throw_error,notNone,checkParameter,isJsonFile,increment_dict_counter
from global_functions import get_utc_time_particioned,insert_tweet_in_date_dict,create_dir_if_not_exits,get_string_datetime_with_n_min_more_than_now
from global_functions import get_string_datetime_now
from logger import show_info,show_parameters
import twitter_api_consumer as consumer
import twitter_web_consumer
import mongo_conector
from threading import Thread


patron_way_of_send = u"rel(.*)>([\s\S]*?)<(.*)"

def read_json_file(file_path):
    try:
        with open(file_path) as handle:
            tweet_dict = json.loads(handle.read())
    except:
        raise Exception("El fichero {} no existe o es erroneo".format(file_path))
    return tweet_dict


def check_if_is_verified(user_id,verified,retweeted):
    if verified:
        global_variables.verified_account_messages +=1
        increment_dict_counter(global_variables.verified_account_dict_tweets,user_id)
        if retweeted:
            global_variables.verified_account_retweets += 1
        else:
            global_variables.verified_account_tweets += 1
    else:
        global_variables.not_verified_account_messages +=1
        increment_dict_counter(global_variables.not_verified_account_dict_tweets,user_id)
        if retweeted:
            global_variables.not_verified_account_retweets += 1
        else:
            global_variables.not_verified_account_tweets += 1

def check_polarity(polarity):
    pass

def check_way_of_send(way_of_send):
    way_of_send = findall(patron_way_of_send,way_of_send)[0][1]
    if way_of_send not in global_variables.way_of_send_counter:
        global_variables.way_of_send_counter[way_of_send] = 1
    else:
        old_value = global_variables.way_of_send_counter[way_of_send]
        global_variables.way_of_send_counter[way_of_send] = old_value + 1


def retrieveTweetsFromFileSystem(file,directory,directory_of_directories):
    tweets_files_list = []
    if notNone(file):
        if not os.path.isfile(file):
            throw_error("INPUT ERROR","The file {} doesn't exists".format(file))
        tweets_files_list = [file]
    elif notNone(directory):
        if not os.path.isdir(directory):
            throw_error("INPUT ERROR","The directory {} doesn't exist".format(directory))
        else:
            for root, dirs, files in os.walk(directory):  
                for filename in files:
                    if isJsonFile(filename):
                        tweets_files_list.append("{}/{}".format(directory,filename))
    elif notNone(directory_of_directories):
        if not os.path.isdir(directory_of_directories):
            throw_error("INPUT ERROR","The directory {} doesn't exist".format(directory))
        for root, dirs, files in os.walk(directory_of_directories):  
            for dir in dirs:
                father_dir_name = "{}/{}".format(directory_of_directories,dir)
                for root, dirs, files in os.walk(father_dir_name):  
                    for filename in files:
                        if isJsonFile(filename):
                            tweets_files_list.append("{}/{}".format(father_dir_name,filename))
    else:
        throw_error(sys.modules[__name__],"Ni hay fichero, ni directorio ni directorio de directorios")
    return tweets_files_list

def add_to_user_dict(user_id,name,nickname):
    if user_id not in global_variables.users_dict:
        global_variables.users_dict[user_id] = {"names":[name],"screen-names":[nickname]}
    else:
        names = global_variables.users_dict[user_id]["names"]
        screen_names = global_variables.users_dict[user_id]["screen-names"]
        if name not in names:
            global_variables.users_dict[user_id]["names"] = names + [name]
        if nickname not in screen_names:
            global_variables.users_dict[user_id]["screen-names"] = screen_names + [nickname]

def check_if_is_reply_or_has_quotes(tweet_id,is_retweet,user_id,has_quote,replied_tweet_id,replied_user_id,replied_user_screen_name):
    if is_retweet:
        if replied_tweet_id not in [False,None]  : # es una respuesta
            global_variables.retweets_with_replies_count +=1

            num_replies_tweet = increment_dict_counter(global_variables.local_replied_tweets_couter,replied_tweet_id)
            update_top_10_list(global_variables.local_most_replied_tweets,(replied_tweet_id,num_replies_tweet))

            num_replies_user = increment_dict_counter(global_variables.local_replied_users_counter,replied_user_id)
            update_top_10_list(global_variables.local_most_replied_users,(replied_user_id,num_replies_user))

            add_to_user_dict(replied_user_id,None,replied_user_screen_name)
            if has_quote:
                global_variables.retweets_with_quotes_count +=1
                global_variables.retweets_with_replies_and_quotes_count+=1
            else:
                global_variables.retweets_without_quotes_count +=1
        else:
            global_variables.retweets_without_replies_count +=1
            if has_quote:
                global_variables.retweets_with_quotes_count +=1
            else:
                global_variables.retweets_without_quotes_count +=1

                
    else: # es un tweet
        if replied_tweet_id not in [False,None]: # es una respuesta
            global_variables.tweets_with_replies_count +=1
            num_replies_tweet = increment_dict_counter(global_variables.local_replied_tweets_couter,replied_tweet_id)
            update_top_10_list(global_variables.local_most_replied_tweets,(replied_tweet_id,num_replies_tweet))

            num_replies_user = increment_dict_counter(global_variables.local_replied_users_counter,replied_user_id)
            update_top_10_list(global_variables.local_most_replied_users,(replied_user_id,num_replies_user))

            add_to_user_dict(replied_user_id,None,replied_user_screen_name)
            if has_quote:
                global_variables.tweets_with_replies_and_quotes_count+=1
                global_variables.tweets_with_quotes_count +=1
            else:
                global_variables.tweets_without_quotes_count +=1
        else:
            global_variables.tweets_without_replies_count +=1
            if has_quote:
                global_variables.tweets_with_quotes_count +=1
            else:
                global_variables.tweets_without_quotes_count +=1


def is_quoted_tweet(quote_id):
    if quote_id== False or quote_id == None:
        return False
    else:
        return True



def check_if_is_retweet(tweet_id,retweeted,user_id):
    if retweeted:
        num_retweets = increment_dict_counter(global_variables.local_user_retweets_counter,user_id)
        update_top_10_list(global_variables.local_most_retweets_users,(user_id,num_retweets))
        global_variables.retweets_count += 1
    else:
        num_tweets = increment_dict_counter(global_variables.local_user_tweets_counter,user_id)
        update_top_10_list(global_variables.local_most_tweets_users,(user_id,num_tweets),True)
        global_variables.tweets_count += 1

    num_messages = increment_dict_counter(global_variables.local_user_messages_counter,user_id)
    update_top_10_list(global_variables.local_most_messages_users,(user_id,num_messages))

def recalculate_statistics_for_collection_if_is_necessary(recalculate_statistics,statistics_file,collection):
    if recalculate_statistics:
        print("[ RECALCULATE STATISTICS INFO] Starting collection messages analysis")
        tweets_list = mongo_conector.get_tweets_cursor_from_mongo(collection)
        analyze_tweets(tweets_list)
        print("[ RECALCULATE STATISTICS INFO] Messages analyzed sucessfully")
        mongo_conector.insert_statistics_file_in_collection(global_variables.get_statistics_dict(),collection)


def analyze_new_versions_of_tweets(current_tweet_dict_list):
    print("[ANALYZE NEW VERSIONS TWEETS INFO] Starting analysis...")
    num = 0
    for current_tweet_dict in current_tweet_dict_list:
        num+=1
        # tweet info
        tweet_id = current_tweet_dict["id_str"]

        # user info
        user_id = current_tweet_dict["user"]["id_str"]
        user_name = current_tweet_dict["user"]["name"]
        user_nickname = current_tweet_dict["user"]["screen_name"] 
                
        # We add user info to our user_dict 
        # key = user_id
        # value = dictionary with two keys 'names' and 'screen-names' that have a list of names as value
        add_to_user_dict(user_id,user_name,user_nickname)
            
        # We add the current tweet to our tweet dictionary in order to have inmediate access
        # key = tweet_id
        # value = json_dict
        global_variables.tweets_dict[tweet_id] = current_tweet_dict

        #check_polarity(tweet_dict[])
            
        # we update our lists every time to keep the ten best scores
        update_top_10_list(global_variables.global_most_favs_tweets,(tweet_id,current_tweet_dict.get("favorite_count",0)))
        update_top_10_list(global_variables.global_most_rt_tweets,(tweet_id,current_tweet_dict["retweet_count"]))

        update_top_10_list(global_variables.global_most_favs_users,(user_id,current_tweet_dict["user"]["favourites_count"]))
        update_top_10_list(global_variables.global_most_tweets_users,(user_id,current_tweet_dict["user"]["statuses_count"]))
        update_top_10_list(global_variables.global_most_followers_users,(user_id,current_tweet_dict["user"]["followers_count"]))
            
    print("[ANALYZE NEW VERSIONS TWEETS INFO] Analysis finished sucessfully {} messages has been updated".format(num))    

def get_tweets_ids_of_tops():
    top_10_name_lists = [global_variables.global_most_favs_tweets,global_variables.global_most_rt_tweets,
    global_variables.local_most_replied_tweets,global_variables.local_most_quoted_tweets]
    tweet_id_list = []
    for top_list in top_10_name_lists:
        for e in top_list:
            tweet_id_list.append(e[0])
    return tweet_id_list

def get_users_screen_name_dict_of_tweet_ids_for_tops_in_variables(collection):
    tweet_id_list = get_tweets_ids_of_tops()
    return  mongo_conector.get_users_screen_name_dict_of_tweet_ids(tweet_id_list,collection)

def get_owner_dict_data_of_tweet_ids_for_tops_in_variables(collection):
    tweet_id_list = get_tweets_ids_of_tops()
    return  mongo_conector.get_tweet_owner_dict_data_of_tweet_ids(tweet_id_list,collection)
    

def update_tweets_owner_dict():
    new_dict = get_owner_dict_data_of_tweet_ids_for_tops_in_variables(mongo_conector.current_collection)
    for k,v in new_dict.items():
        global_variables.tweets_owner_dict[k] = v

def build_embed_top_tweets_dict():
    tweet_id_list = get_tweets_ids_of_tops()
    driver = twitter_web_consumer.open_twitter_and_login()
    for tweet_id in tweet_id_list:
        if tweet_id != 0 and not global_variables.tweets_embed_html_dict.get(tweet_id,False):
            print("ENTRA")
            registry_dict = global_variables.tweets_owner_dict.get(tweet_id,None)
            

            if registry_dict == None: # guardar los tweets respondidos y citados en owner_dict
                print("[REVISAR] tweet_id = {} (no tenemos su dueño) ".format(tweet_id))
            else:
                user_screen_name = registry_dict["user_screen_name"]
                embed_with_media,embed_without_media = twitter_web_consumer.get_embed_html_of_a_tweet(user_screen_name,tweet_id,driver)
                if (embed_without_media != None):
                    aux = { "embed_with_media" : embed_with_media , "embed_without_media" : embed_without_media}
                    global_variables.tweets_embed_html_dict[tweet_id] = aux
                else:
                    print("[build_embed_top_tweets_dict] entry added with error indicator {}".format(tweet_id))
                    aux = { "embed_with_media" : '<h3> Tweet possibly removed from twitter</h3>' ,
                     "embed_without_media" : '<h3> Tweet possibly removed from twitter</h3>'}
                    global_variables.tweets_embed_html_dict[tweet_id] = aux
    driver.close()

def initialize_likes_queue(users,collection,initial_messages,likes_ratio):
    likes_queue_dict = {}
    for user in users:
        if user != "_id" and user != "total_captured_tweets":
            user_registry = { }
            tweet_queue = []
            partido = searched_users_file[user]["partido"]
            likes_to_PP,likes_to_PSOE,likes_to_PODEMOS,likes_to_CIUDADANOS,likes_to_VOX,likes_to_COMPROMIS = get_likes_values(partido)
            user_id = mongo_conector.get_user_id_wih_screenname(user)
            if user_id != None:
                ids = mongo_conector.get_last_n_tweets_of_a_user_in_a_collection(user_id,mongo_conector.current_collection,args.initial_messages or 20)
                for tweet_id in ids:
                    tweet_queue.append(tweet_id)
                    # We don't count the first likes retrieve
                    user_registry[tweet_id] = { "likes_count":0 , "timeout": get_string_datetime_with_n_min_more_than_now(30)}
                    num_likes,users_who_liked = twitter_web_consumer.get_last_users_who_liked_a_tweet(user,tweet_id,driver)
                    mongo_conector.insert_or_update_likes_list_file(mongo_conector.current_collection,tweet_id,num_likes,users_who_liked,user_id,user)
                    for u_id,u,u_screen in users_who_liked:
                        mongo_conector.insert_or_update_users_file(mongo_conector.current_collection,u_id,u_screen,likes_to_PP,likes_to_PSOE,likes_to_PODEMOS,likes_to_CIUDADANOS,likes_to_VOX,likes_to_COMPROMIS)

            user_registry["tweet_queue"] = tweet_queue
            likes_queue_dict[user] = user_registry
    
    # print(likes_queue_dict)
    # input()
    return likes_queue_dict


def analyze_tweets_from_filesystem(json_files_paths):
    for json_file in json_files_paths:
        current_tweet_dict_list = read_json_file(json_file)
        analyze_tweets(current_tweet_dict_list)

def analyze_tweets(current_tweet_dict_list):
    print("[ANALYZE TWEETS INFO] Starting analysis...")
    tweets_ids_set = set()
    insertions_in_set = 0


    start = timeit.default_timer()
    for current_tweet_dict in current_tweet_dict_list:

        # tweet info
        tweet_id = current_tweet_dict["id_str"]

        #check duplicates
        tweets_ids_set.add(tweet_id)
        insertions_in_set +=1

        # user info
        user_id = current_tweet_dict["user"]["id_str"]
        user_name = current_tweet_dict["user"]["name"]
        user_nickname = current_tweet_dict["user"]["screen_name"]
        # Check if is retweet or not
        is_retweet = current_tweet_dict.get("retweeted_status",False)
        # Quotes info
        #has_quote = current_tweet_dict.get("is_quote_status",False)
        has_quote = is_quoted_tweet(current_tweet_dict.get("quoted_status",False))
        if has_quote:
            quoted_user_id = current_tweet_dict["quoted_status"]["user"]["id_str"]
            quoted_user_name = current_tweet_dict["quoted_status"]["user"]["name"]
            quoted_user_nickname = current_tweet_dict["quoted_status"]["user"]["screen_name"]
            quoted_tweet_id = current_tweet_dict["quoted_status"]["id_str"]
            #print(quoted_user_id,quoted_user_name,quoted_user_nickname)
            #input()
            global_variables.quotes_dict[quoted_tweet_id] = current_tweet_dict["quoted_status"]
            add_to_user_dict(quoted_user_id,quoted_user_name,quoted_user_nickname)
            num_quotes_tweet = increment_dict_counter(global_variables.local_quoted_tweets_counter,quoted_tweet_id)
            update_top_10_list(global_variables.local_most_quoted_tweets,(quoted_tweet_id,num_quotes_tweet))

            num_quotes_user = increment_dict_counter(global_variables.local_quoted_users_counter,quoted_user_id)
            update_top_10_list(global_variables.local_most_quoted_users,(quoted_user_id,num_quotes_user)) 
                
                
        quoted_tweet_id = current_tweet_dict.get("quoted_status_id_str",False)
        # Replies info
        replied_tweet_id = current_tweet_dict.get("in_reply_to_status_id_str",False) # it's the way to known if is a reply
        replied_user_id = current_tweet_dict.get("in_reply_to_user_id_str",False)
        replied_user_nickname = current_tweet_dict.get("in_reply_to_screen_name",False)

        # We add user info to our user_dict 
        # key = user_id
        # value = dictionary with two keys 'names' and 'screen-names' that have a list of names as value
        add_to_user_dict(user_id,user_name,user_nickname)
            
        # We add the current tweet to our tweet dictionary in order to have inmediate access
        # key = tweet_id
        # value = json_dict
        global_variables.tweets_dict[tweet_id] = current_tweet_dict

        # check if this tweet is send by a verified user and compute its stadistics
        check_if_is_verified(user_id,current_tweet_dict['user']["verified"],is_retweet)

        #check_polarity(tweet_dict[])
        check_way_of_send(current_tweet_dict["source"])

            
        # we update our lists every time to keep the ten best scores
        update_top_10_list(global_variables.global_most_favs_tweets,(tweet_id,current_tweet_dict.get("favorite_count",0)))
        update_top_10_list(global_variables.global_most_rt_tweets,(tweet_id,current_tweet_dict["retweet_count"]))

        update_top_10_list(global_variables.global_most_favs_users,(user_id,current_tweet_dict["user"]["favourites_count"]))
        update_top_10_list(global_variables.global_most_tweets_users,(user_id,current_tweet_dict["user"]["statuses_count"]))
        update_top_10_list(global_variables.global_most_followers_users,(user_id,current_tweet_dict["user"]["followers_count"]))
            

        global_variables.messages_count +=1
            
        check_if_is_retweet(tweet_id,is_retweet,user_id)
        check_if_is_reply_or_has_quotes(tweet_id,is_retweet,user_id,has_quote,replied_tweet_id,replied_user_id,replied_user_nickname)

        fecha,hora,minuto = get_utc_time_particioned(current_tweet_dict["created_at"])
        insert_tweet_in_date_dict(tweet_id,fecha,hora,minuto)



        if len(tweets_ids_set) < insertions_in_set:
            print("[ANALYZE_TWEETS WARN] There are duplicates in the messages analyzed")
            # input() 
    

    #show_info() #TODO decidir si llamarlo solo una vez cuno se le pase directorios

    print('\n\nMensajes analizados: {} Time: {}\n\n'.format(global_variables.messages_count,timeit.default_timer() - start))

def analyze_tweets_and_mark_in_mongo(cursor_tweets):
    lista_tweets_nuevos = []
    lista_tweets_actualizados =[]
    for tweet in cursor_tweets:
        # print(json.dumps(tweet,indent=4,sort_keys=True))
        # print(tweet["_id"])
        if tweet["analyzed"] == False:
            lista_tweets_nuevos.append(tweet)
        else:
            lista_tweets_actualizados.append(tweet) # tweets analyzed but outdated

    if len(lista_tweets_nuevos) > 0:
        analyze_tweets(lista_tweets_nuevos)
        mongo_conector.insert_statistics_file_in_collection(global_variables.get_statistics_dict(),mongo_conector.current_collection)
        mongo_conector.mark_docs_as_analyzed([x["_id"] for x in lista_tweets_nuevos],mongo_conector.current_collection)
        lista_tweets_nuevos = []
    if len(lista_tweets_actualizados) > 0:
        analyze_new_versions_of_tweets(lista_tweets_actualizados)
        mongo_conector.insert_statistics_file_in_collection(global_variables.get_statistics_dict(),mongo_conector.current_collection)
        mongo_conector.mark_docs_as_analyzed([x["_id"] for x in lista_tweets_actualizados],mongo_conector.current_collection)
        lista_tweets_actualizados =[]

def put_additional_doc_in_mongo_with_tweets_ids_of_searched_users_not_captured_yet(searched_users_file,collection):
    aux = {"_id" : mongo_conector.tweet_of_searched_users_not_captured_yet_file_id}
    for x in searched_users_file.keys():
        if x not in ("_id","total_captured_tweets"):
            driver_aux = twitter_web_consumer.open_twitter_and_login()
            lista_tweets_ids,lista_retweets_ids = twitter_web_consumer.get_tweets_of_a_user_until(x,driver_aux,tweet_id_limit=searched_users_file[x]["max_tweet_id"])
            aux[x] = lista_retweets_ids + lista_tweets_ids
    mongo_conector.insert_tweet_of_searched_users_not_captured_yet_file(aux,collection)
    driver_aux.close()

def put_hashtag_in_query(query):
    if not query.startswith("#"):
        query = "#" + query
    return query

def get_likes_values(partido):
    try:
        if partido=="PP":
            val = (1,0,0,0,0,0)
        elif partido == "PSOE":
            val =  (0,1,0,0,0,0)
        elif partido == "PODEMOS":
            val= (0,0,1,0,0,0)
        elif partido == "CIUDADANOS":
            val = (0,0,0,1,0,0)
        elif partido == "VOX":
            val = (0,0,0,0,1,0)
        elif partido == "COMPROMIS":
            val = (0,0,0,0,0,1)
        else:
            val = (0,0,0,0,0,0)
    except:
        return (0,0,0,0,0,0)
    return val

#############################################################################################################################
######################       MAIN PROGRAM       #############################################################################
#############################################################################################################################
if __name__ == "__main__":
    start = timeit.default_timer()

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "-F","--file", help="input json file",type=str)
    parser.add_argument("-d","-D","--directory", help="directory of input json files", type=str)
    parser.add_argument("-dd","-DD","--directory_of_directories", help="father directory of json directories", type=str)
    #TODO añadir mdb option

    parser.add_argument("-o","-O","--output_file",help="choose name for file with results", type=str)

    parser.add_argument("-a","-A","--analyze",help="analyze tweets and calculate stadistics",action='store_true')
    parser.add_argument("-up","-UP","--update",help="update your tweets in mongoDb",action='store_true')
    parser.add_argument("-s","-S","--streamming",help="get tweets from twitter API by streamming save it in mongoDb",action='store_true')
    parser.add_argument("-q","-Q","--query",help="get tweets from twitter API by query save it in mongoDb", type=str)
    parser.add_argument("-qu","-QU","-uq","--query_user",nargs='+',help="get tweets from twitter API by user save it in mongoDb", type=str)
    parser.add_argument("-qf","-QF","--query_file",help="get tweets from twitter API by query save it in a file", type=str)
    parser.add_argument("--likes",help="get likes from tweets of searched users", action="store_true")

    parser.add_argument("-w","-W","--words",nargs='+',help="specify words that should be used in the collected tweets.This option has to be used in streamming",type=str)
    parser.add_argument("-mm","-MM","--max_messages",help="specify maximum num of messages to collect",type=int)
    parser.add_argument("-mt","-MT","--max_time",help="specify maximum time of collecting in minutes.This option has to be used in streamming",type=int)
    parser.add_argument("-p","-P","--partido",help="specify political party (only can be used with -qu option)",
    choices=["PP","pp","PSOE","psoe","Psoe","Podemos","podemos","PODEMOS","ciudadanos","cs","CIUDADANOS","CS","Ciudadanos",
    "vox","VOX","Vox","compromis","Compromis","COMPROMIS"])
    parser.add_argument("-l","-L","--loop",help="execute the action in loop",action="store_true")
    parser.add_argument("-t","-T","--analysis_trunk",help="trunk to use in analysis to not overload data",type=int)
    parser.add_argument("-lr","--likes_ratio",help="Sets a number of likes to get for a tweets in 30 min to keep capturing likes",type=int)
    parser.add_argument("-im","--initial_messages",help="Sets in how many tweets capture likes ( last n tweets)",type=int)


    parser.add_argument("-c","-C","--collection",help="MongoDB collection to use",type=str)
    parser.add_argument("-cq", "-CQ","--collection_query",help="Execute querys registered in the query file of a collection",type=str)
    parser.add_argument("-cu", "-CU","--collection_users",help="Retrieve tweets from users registered in the searched_users file of a collection",type=str)

    parser.add_argument("-e","-E","--examples",action='store_true')
    args = parser.parse_args()
    show_parameters(args)
    fileSystemMode = False
    exist_thread = False
    recalculate_statistics = False
    mongo_conector.current_collection = ((args.collection or args.collection_query or args.collection_users) or "tweets")



###################################################################################################################################################
###################################################################### CHECK ERRORS ###############################################################
###################################################################################################################################################

    # We control filesystem options
    if checkParameter(args.file) + checkParameter(args.directory) + checkParameter(args.directory_of_directories) > 1:
        throw_error(sys.modules[__name__],"No se pueden usar las opciones '-f' '-d' o -dd de forma simultanea ")
    elif checkParameter(args.file) + checkParameter(args.directory) + checkParameter(args.directory_of_directories) == 1:
        if checkParameter(args.update) + checkParameter(args.streamming) + checkParameter(args.query) + checkParameter(args.query_file) \
        + checkParameter(args.words) + checkParameter(args.max_messages) + checkParameter(args.max_time) + checkParameter(args.collection) \
        + checkParameter(args.collection_query) + checkParameter(args.query_user) + checkParameter(args.collection_users) \
        + checkParameter(args.loop) + checkParameter(args.partido) + checkParameter(args.examples)>0 + checkParameter(args.analyze)\
        + checkParameter(args.analysis_trunk) + checkParameter(args.likes) + checkParameter(args.likes_ratio) \
        + checkParameter(args.initial_messages)>0:
            throw_error(sys.modules[__name__],"Con las opciones '-f' '-d' o -dd solo se puede usar la opcion -o ")
    # There is no filesystem options so we are going to check -s -q -qf -cq options
    elif checkParameter(args.streamming) + checkParameter(args.query) + checkParameter(args.query_file) + checkParameter(args.query_user) \
        + checkParameter(args.collection_query) + checkParameter(args.collection_users) + checkParameter(args.analyze)\
            + checkParameter(args.likes)> 1:
        throw_error(sys.modules[__name__],"No se pueden usar las opciones '-s' '-q' -a --likes -qf -qu -cu o -cq de forma simultanea ")
    elif checkParameter(args.streamming) + checkParameter(args.query) + checkParameter(args.query_file) \
        + checkParameter(args.collection_query) + checkParameter(args.query_user) + checkParameter(args.collection_users) \
            + checkParameter(args.analyze) + checkParameter(args.likes) == 1:
        if checkParameter(args.query): # -q option
            if checkParameter(args.words) + checkParameter(args.max_time) + checkParameter(args.update) + checkParameter(args.partido) \
                + checkParameter(args.loop) + checkParameter(args.output_file) +checkParameter(args.analysis_trunk) \
                    + checkParameter(args.likes_ratio) + checkParameter(args.initial_messages)> 0:
                throw_error(sys.modules[__name__],"Con la opción -q no se pueden usar las opciones -w, -lr, -im, -mt, -p, -t -l ,-up o -o")
        elif checkParameter(args.query_file): # -qf option
            if checkParameter(args.words) + checkParameter(args.max_time) +checkParameter(args.update) + checkParameter(args.partido) \
                + checkParameter(args.loop) + checkParameter(args.examples) + checkParameter(args.collection) +checkParameter(args.analysis_trunk)\
                + checkParameter(args.likes_ratio) + checkParameter(args.initial_messages) > 0:
                throw_error(sys.modules[__name__],"Con la opción -qf solo se pueden usar la opción -mm y -o")
        elif checkParameter(args.query_user): # -qu option
            if checkParameter(args.words) + checkParameter(args.max_time) + checkParameter(args.update) + checkParameter(args.loop) \
                + checkParameter(args.output_file) +checkParameter(args.analysis_trunk) + checkParameter(args.likes_ratio) +\
                     checkParameter(args.initial_messages)> 0:
                throw_error(sys.modules[__name__],"Con la opción -qu no se pueden usar las opciones -w ,-mt, -im, -lr, -t, -l, -up o -o")
        elif checkParameter(args.analyze):
            if checkParameter(args.words) + checkParameter(args.max_time) + checkParameter(args.update) + checkParameter(args.output_file) +\
                + checkParameter(args.partido) +checkParameter(args.max_messages) + checkParameter(args.likes_ratio) + checkParameter(args.initial_messages)> 0:
                throw_error(sys.modules[__name__],"Con la opción -a no se pueden usar las opciones -w -p -lr -im -mm -mt -up -mm o -o")
        elif checkParameter(args.likes): # --likes option
            if checkParameter(args.words) + checkParameter(args.max_time) + checkParameter(args.update) + checkParameter(args.output_file) +\
                + checkParameter(args.partido) +checkParameter(args.max_messages)> 0:
                throw_error(sys.modules[__name__],"Con la opción --likes no se pueden usar las opciones -w -p -mm -mt -up -mm o -o")
        elif checkParameter(args.collection_query): # -cq option
            if checkParameter(args.words) + checkParameter(args.max_time) + checkParameter(args.update) + checkParameter(args.partido) +\
                checkParameter(args.collection) +checkParameter(args.analysis_trunk) + checkParameter(args.likes_ratio) + checkParameter(args.initial_messages)> 0:
                throw_error(sys.modules[__name__],"Con la opción -cq no se pueden usar las opciones -w, -im, -lr -mt, -p, -t, -up o -c")
        elif checkParameter(args.collection_users): # -cu option
            if checkParameter(args.words) + checkParameter(args.max_time) + checkParameter(args.update) + checkParameter(args.partido) \
                + checkParameter(args.collection) + checkParameter(args.analysis_trunk) + checkParameter(args.likes_ratio) + checkParameter(args.initial_messages)> 0:
                throw_error(sys.modules[__name__],"Con la opción -cu no se pueden usar las opciones -w, -lr , -im, -mt, -t -p, -up o -c")
        else: # -s option TODO poner filtros
            if checkParameter(args.update) == 1:
                throw_error(sys.modules[__name__],"La opcion update solo esta disponible en el modo por defecto")
    else:
        if checkParameter(args.words) + checkParameter(args.max_messages) +checkParameter(args.max_time) + \
        checkParameter(args.partido) + checkParameter(args.loop) + checkParameter(args.likes_ratio) + checkParameter(args.initial_messages)>1:
            throw_error(sys.modules[__name__],"En el modo por defecto ( no se usan las optiones (-f, -d, -dd, -q, -qf, -s) no se pueden usar las opciones -w -mm -mt -im -lr -l o -p")

###################################################################################################################################################
###################################################################### FIN CHECK ERRORS ###########################################################
###################################################################################################################################################
    
    print("\n\n[ MAIN INFO ] There is no errors in the command options ")
    # We control filesystem options
    if checkParameter(args.file) + checkParameter(args.directory) + checkParameter(args.directory_of_directories) == 1:
        json_files_path_list = retrieveTweetsFromFileSystem(args.file,args.directory,args.directory_of_directories)
        fileSystemMode = True
    else:
        if checkParameter(args.query): # -q option
            tweets_files_list = consumer.collect_tweets_by_query_and_save_in_mongo(args.max_messages or 3000,args.query or "#python")

        elif checkParameter(args.query_file): # -qf option
            create_dir_if_not_exits("tweets")
            tweets_files_list = consumer.collect_tweets_by_query_and_save_in_file(args.max_messages or 3000,args.query_file or "#python")

        elif checkParameter(args.query_user): # -qu option
            if checkParameter(args.partido) > 0:
                args.partido = args.partido.upper()
                if args.partido =="CS":
                    args.partido = "CIUDADANOS"
            for screen_name in args.query_user:
                tweets_files_list = consumer.collect_tweets_by_user_and_save_in_mongo(user_screen_name=screen_name,max_tweets =(args.max_messages or 3000),partido=args.partido)

        elif checkParameter(args.streamming): # -s option
            argumentos_funcion = (args.words or ["futbol","#music"], args.max_messages or 10000, args.max_time or 10)
            consumer.collect_tweets_by_streamming_and_save_in_mongo(args.words or ["futbol","#music"], args.max_messages or 10000, args.max_time or 10)

        elif checkParameter(args.analyze): # -a option
            statistics_file = mongo_conector.get_statistics_file_from_collection(mongo_conector.current_collection)
            if statistics_file != None:
                global_variables.set_statistics_from_statistics_dict(statistics_file)
            trunk = args.analysis_trunk or 500
            
            cond = True
            while cond:
                lista_tweets = mongo_conector.get_tweets_to_analyze_or_update_stats(mongo_conector.current_collection,trunk)
                while len(lista_tweets) >0:
                    analyze_tweets_and_mark_in_mongo(lista_tweets)
                    lista_tweets = mongo_conector.get_tweets_to_analyze_or_update_stats(mongo_conector.current_collection,trunk)
                cond = args.loop
            

        elif checkParameter(args.collection_query): # -cq option
            cond = True
            while cond:
                query_file = mongo_conector.get_query_file(mongo_conector.current_collection)
                tweets_files_list = []
                for query in mongo_conector.get_keys_of_special_file_except_doc_id(query_file):
                    element = query_file[query]
                    max_tweet_id = element["max_tweet_id"]
                    if checkParameter(args.max_messages) > 0:
                        tweets_files_list = consumer.collect_tweets_by_query_and_save_in_mongo(max_tweets=args.max_messages,query=query,until_tweet_id=max_tweet_id)
                    else:
                        tweets_files_list = consumer.collect_tweets_by_query_and_save_in_mongo(query=query,until_tweet_id=max_tweet_id)
                cond = args.loop
            update_tweets_owner_dict()
            build_embed_top_tweets_dict()

        elif checkParameter(args.collection_users): # -cu option
            cond = True
            while cond:
                searched_users_file = mongo_conector.get_searched_users_file(mongo_conector.current_collection)
                users = searched_users_file.keys()
                tweets_files_list = []
                for user in users:
                    if user != "_id" and user != "total_captured_tweets":
                        print("user = {}".format(user))
                        max_tweet_id = searched_users_file[user]["max_tweet_id"]
                        if checkParameter(args.max_messages) > 0:
                            tweets_files_list = consumer.collect_tweets_by_user_and_save_in_mongo(max_tweets=args.max_messages,user_screen_name=user,until_tweet_id=max_tweet_id)
                        else:
                            tweets_files_list = consumer.collect_tweets_by_user_and_save_in_mongo(user_screen_name=user,until_tweet_id=max_tweet_id)
                cond = args.loop

        elif checkParameter(args.likes): # --likes option
            cond = True
            mongo_conector.delete_tweet_of_searched_users_not_captured_yet_file(mongo_conector.current_collection) 
            driver = twitter_web_consumer.open_twitter_and_login()
            searched_users_file = mongo_conector.get_searched_users_file(mongo_conector.current_collection)
            users = searched_users_file.keys()
            likes_queue = initialize_likes_queue(users,mongo_conector.current_collection,args.initial_messages,args.likes_ratio)
            while cond:
                for user in users:
                    if user != "_id" and user != "total_captured_tweets":
                        partido = searched_users_file[user]["partido"]
                        likes_to_PP,likes_to_PSOE,likes_to_PODEMOS,likes_to_CIUDADANOS,likes_to_VOX,likes_to_COMPROMIS = get_likes_values(partido)
                        user_id = mongo_conector.get_user_id_wih_screenname(user)
                        if user_id != None:
                            ids = mongo_conector.get_last_n_tweets_of_a_user_in_a_collection(user_id,mongo_conector.current_collection,args.initial_messages or 20)
                            ## add new tweets to likes queue
                            likes_queue_of_user = likes_queue[user]["tweet_queue"]
                            for tweet_id in ids:
                                if tweet_id > likes_queue_of_user[-1] and tweet_id not in likes_queue[user]:
                                    likes_queue_of_user.append(tweet_id)
                                    likes_queue[user][tweet_id] = { "likes_count":0 , "timeout": get_string_datetime_with_n_min_more_than_now(30)}
                            additional_ids_file = mongo_conector.get_tweet_of_searched_users_not_captured_yet_file(mongo_conector.current_collection)
                            if additional_ids_file != None:
                                for tweet_id in additional_ids_file:
                                    if tweet_id > likes_queue_of_user[-1] and tweet_id not in likes_queue[user]:
                                        likes_queue_of_user.append(tweet_id)
                                        likes_queue[user][tweet_id] = { "likes_count":0 , "timeout": get_string_datetime_with_n_min_more_than_now(30)}
                            likes_queue[user]["tweet_queue"] = likes_queue_of_user

                            tweet_queue_aux= []

                            for tweet_id in likes_queue_of_user:
                                num_likes,users_who_liked = twitter_web_consumer.get_last_users_who_liked_a_tweet(user,tweet_id,driver)
                                new_likes = mongo_conector.insert_or_update_likes_list_file(mongo_conector.current_collection,tweet_id,num_likes,users_who_liked,user_id,user)
                                likes_queue[user][tweet_id]["likes_count"] = likes_queue[user][tweet_id]["likes_count"] + len(new_likes)
                                for u_id,u,u_screen in users_who_liked:
                                    mongo_conector.insert_or_update_users_file(mongo_conector.current_collection,u_id,u_screen,likes_to_PP,likes_to_PSOE,likes_to_PODEMOS,likes_to_CIUDADANOS,likes_to_VOX,likes_to_COMPROMIS)

                                # delete messages with few likes of the queue
                                if get_string_datetime_now()>likes_queue[user][tweet_id]["timeout"] and likes_queue[user][tweet_id]["likes_count"]< (args.likes_ratio or 30):
                                    print("[LIKES] TWEET {} DELETED FROM LIKES QUEUE ({} likes in 30 minutes)".format(tweet_id,likes_queue[user][tweet_id]["likes_count"]))
                                else:
                                    tweet_queue_aux.append(tweet_id)
                            likes_queue[user]["tweet_queue"] = tweet_queue_aux
                                

                searched_users_file = mongo_conector.get_searched_users_file(mongo_conector.current_collection)
                users = searched_users_file.keys()
                cond = args.loop
                if cond:
                    thread = Thread(target=put_additional_doc_in_mongo_with_tweets_ids_of_searched_users_not_captured_yet, args=(searched_users_file,mongo_conector.current_collection,))
                    thread.start()
                    #thread.run()
        # There is no options in [ -f, -d, -dd, -q, -qf,-cq, -s]
        else:
            if checkParameter(args.update):
                tweets_ids = mongo_conector.get_tweet_ids_list_from_database(mongo_conector.current_collection)
                consumer.get_specifics_tweets_from_api_and_update_mongo(tweets_ids)
                # marcar los ficheros actualizados con analized: to update 
                # si no hay fichero no hago nada pero si hay tengo k ir actualizando las estadisticas de los tweets analizados
            tweets_files_list = mongo_conector.get_tweets_cursor_from_mongo(mongo_conector.current_collection)



    if fileSystemMode:
        analyze_tweets_from_filesystem(json_files_path_list)

    


    # borrar
    #print(json.dumps(mongo_conector.get_query_file("test2"),indent=4,sort_keys=True))
