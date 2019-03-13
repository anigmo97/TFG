import tweepy
import datetime
import json
import time
from pymongo import MongoClient
import global_variables
from global_functions import get_utc_time
import mongo_conector
from threading import Timer,Thread
from sript import analyze_tweets

now = datetime.datetime.now()



consumer_key = ""
consumer_secret = ""
access_token = ""
access_secret = ""
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

class StreamListener(tweepy.StreamListener):    
    #This is a class provided by tweepy to access the Twitter Streaming API. 
    
    def __init__(self,api=None,max_tweets=10000,max_mins=10,words_list=None):
        self.streamming_tweets = 0
        self.max_mins = max_mins
        self.max_tweets = max_tweets
        self.words_list = words_list
        self.start_time = time.time()
        self.message_timer = Timer(5,self.show_messages_info)
        self.message_timer.start()
        self.time_timer = Timer(max_mins*60,self.finalize_by_time)
        self.time_timer.start()
        self.mongo_tweets_dict = {}
        self.mongo_tweets_ids_list = []
        self.tweets_no_repetidos = []
        self.trunk= min(500,max_tweets)
        self.first_tweet_id = None
        self.last_tweet_id = None
        self.max_created_at = None
        self.min_created_at = None
        # self.client = MongoClient(mongo_conector.MONGO_HOST)
        # # Use twitterdb database. If it doesn't exist, it will be created.
        # self.db = self.client.twitterdb

    def show_messages_info(self):
        print("[SHOW MESSAGES INFO] {} tweets collected in {:.0f} seconds".format(self.streamming_tweets,(time.time() - self.start_time )))
        self.message_timer.run()

    
    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("[ON CONNECT] You are now connected to the streaming API.")
    
    def finalize_by_time(self):
        self.on_disconnect("Se ha agotado el tiempo maximo especificado por el usuario")

    def on_disconnect(self, notice):
        self.time_timer.cancel()
        self.message_timer.cancel()
        print("[ON DISCONNECT INFO] {}".format(notice))
        print("[ON DISCONNECT INFO] You are now disconnected to the streaming API.\n\n")
        if len (self.mongo_tweets_ids_list)>0:
            #coger lo que retorna el insert many
            tweets_no_reps = mongo_conector.insertar_multiples_tweets_en_mongo(self.mongo_tweets_dict,self.mongo_tweets_ids_list,mongo_conector.current_collection) # cambiar por insert many
            analyze_tweets(tweets_no_reps)
            mongo_conector.insert_statistics_file_in_collection(global_variables.get_statistics_dict(),mongo_conector.current_collection)
            mongo_conector.insert_or_update_query_file_streamming(mongo_conector.current_collection,self.words_list,self.streamming_tweets,self.first_tweet_id,self.last_tweet_id,self.min_created_at, self.max_created_at)
            print("[ON DISCONNECT INFO] FINISH")
        

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('\n\n[STREAM ERROR]  An Error has occured: ' + repr(status_code))
        codigo = 0
        try:
            codigo= int(status_code)
            if codigo == 420:
                print("[ON ERROR] You have reached your rate limit\n")
        except Exception as e:
            pass

    def on_limit(self, track):
        print(track + "\n")
        return   

    def on_status(self, status):
        """Called when a new status arrives"""
        pass

    def on_data(self, data):
        try:
            # if "start" in data:
            #     print(data)
            # Decode the JSON from Twitter
            datajson = get_mongo_document(data) # controlamos los errores antes de insertarlo
            if datajson is not None:
                #analizar tweet
                current_tweet_id = datajson["id_str"]
                current_created_at = datajson["created_at"]
                self.last_tweet_id = current_tweet_id
                self.max_created_at = current_created_at
                if self.first_tweet_id == None:
                    self.first_tweet_id = current_tweet_id
                    self.min_created_at = current_created_at
                self.mongo_tweets_dict[datajson["_id"]] = datajson
                self.mongo_tweets_ids_list.append(datajson["_id"])
                if len(self.mongo_tweets_ids_list) > self.trunk:
                    print("[ON DATA INFO] {} messages are going to be inserted in mongo".format(len(self.mongo_tweets_ids_list)))
                    self.tweets_no_repetidos = mongo_conector.insertar_multiples_tweets_en_mongo(self.mongo_tweets_dict,self.mongo_tweets_ids_list,mongo_conector.current_collection) # cambiar por insert many
                    print("[ON DATA INFO] {} messages are going to be analyzed".format(len(self.tweets_no_repetidos)))
                    analyze_tweets(self.tweets_no_repetidos)
                    mongo_conector.insert_statistics_file_in_collection(global_variables.get_statistics_dict(),mongo_conector.current_collection)
                    mongo_conector.insert_or_update_query_file_streamming(mongo_conector.current_collection,self.words_list,self.streamming_tweets,self.first_tweet_id,current_tweet_id,self.min_created_at, current_created_at)
                    self.mongo_tweets_dict = {}
                    self.mongo_tweets_ids_list = []
                if(self.streamming_tweets >= self.max_tweets):
                    print("[ON_DATA] \n\n{} messages has been collected".format(self.streamming_tweets))
                    self.on_disconnect("User disconnected after get the required amount of tweets")
                    return False # paramos el streamming
                self.streamming_tweets += 1
        except Exception as e:
            print("[ON DATA ERROR] {} {}".format(e,e.__cause__))
        



 

def collect_tweets_by_query_and_save_in_file(max_tweets=3000,query="#science",filename="tweets"): 
        print("[ QUERY TO FILE INFO ] Collectings tweets by query = '{}'".format(query))
        API = tweepy.API(auth)
        cursor_respuestas = tweepy.Cursor(API.search, q=query,count=1000,lang="es", since="2017-04-03").items(max_tweets)
        tweets_list = [status._json for status in cursor_respuestas]
        
        with open('tweets/{}{}.json'.format(filename,now.strftime("%Y-%m-%d__%H-%M")), 'w', encoding='utf8') as file:
            file.writelines(json.dumps(tweets_list,indent=4, sort_keys=True))
        
        print("{} tweets capturados".format(len(tweets_list)))

        return tweets_list
        #dumps -> dump string


def collect_tweets_by_query_and_save_in_mongo(max_tweets=3000,query="#science",filename="tweets"): 
        print("[ QUERY TO MONGO INFO ] Collectings tweets by query = '{}'".format(query))
        #wait_on_rate_limit = True, wait_on_rate_limit_notify = True
        API = tweepy.API(auth)
        mongo_tweets_dict = {}
        mongo_tweets_id_list = [] 
        for status in tweepy.Cursor(API.search,q=query,count=100,lang="es", since="2017-04-03").items(max_tweets):
            tweet = status._json
            if(tweet.get("id_str",False) != False):
                tweet_id = tweet["id_str"]
                tweet["_id"]= tweet_id
                mongo_tweets_id_list.append(tweet_id)
                mongo_tweets_dict[tweet_id] = tweet
        
        max_tweet_id = mongo_tweets_id_list[0]
        min_tweet_id = mongo_tweets_id_list[-1]
        min_creation_date = mongo_tweets_dict[min_tweet_id]["created_at"]
        max_creation_date = mongo_tweets_dict[max_tweet_id]["created_at"]

        print("{} tweets capturados".format(len(mongo_tweets_id_list)))
        tweets_sin_repetir = mongo_conector.insertar_multiples_tweets_en_mongo(mongo_tweets_dict,mongo_tweets_id_list,mongo_conector.current_collection)
        
        mongo_conector.insert_or_update_query_file(mongo_conector.current_collection,query,len(tweets_sin_repetir),min_tweet_id,max_tweet_id,min_creation_date,max_creation_date)

        return  tweets_sin_repetir


def collect_tweets_by_streamming_and_save_in_mongo(WORDS=["#python"],max_tweets=10000,max_mins=2):
    print("[ STREAMMING TO MONGO INFO ] Starting API")
    print("words {}\t max_tweets {} \t max_mins {}".format(WORDS,max_tweets,max_mins))
    try:
        API = tweepy.API(wait_on_rate_limit=True)
        listener = StreamListener(api=API,max_tweets=max_tweets,max_mins=max_mins,words_list=WORDS) 
        streamer = tweepy.Stream(auth=auth, listener=listener)
        print("Tracking: " + str(WORDS))
        if len(WORDS) > 0:
            #threading.Thread(target=self._thread_function, args=(arg1,),kwargs={'arg2':arg2}, name='thread_function').start()
            #streamming_thread = Thread(target=streamer.filter,kwargs=dict(languages=["en","es"],track=WORDS,is_async=True))
            streamer.filter(languages=["en","es"],track=WORDS,is_async=True)
        else:
            #streamming_thread = Thread(target=streamer.filter,kwargs=dict(languages=["en","es"],is_async=True))
            streamer.filter(languages=["en","es"],is_async=True)
        #streamming_thread.start()
        #streamming_thread.join()
        #while streamming_thread.is_alive: #echo para sincronizar
            #pass
    except Exception as e:
        print("[STREAMMING ERROR] {}".format(e))
        #REVISAR
        #streamer.filter(languages=["en","es"],async=True)
    

def get_specifics_tweets_from_api_and_update_mongo(tweets_ids_list):
    start_time = time.time()
    maximum_tweet_ratio = 100
    API = tweepy.API(auth)

    deleted_tweets_counter = 0 
    not_collected_tweets_counter = 0
    tweets_for_update = len(tweets_ids_list)
    updated_tweets_count = 0

    i=0
    while i < tweets_for_update:
        print("[LOOKUP-TWEETS-IDS INFO]{}/{} ({:.3f}%) tweets updated (with {} faileds)  TIME:{}"
        .format(updated_tweets_count,tweets_for_update,(updated_tweets_count/tweets_for_update)*100,not_collected_tweets_counter,
        time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))
        try:
            tweets = API.statuses_lookup(tweets_ids_list[i:i+maximum_tweet_ratio])
        except Exception as e:
            print("[LOOKUP-TWEETS-IDS ERROR] {}".format(e))
            exit(1)

        x=0
        new_version_of_tweets = []
        while x < len(tweets):
            
            tweet_dict = tweets[x]._json
            if tweet_dict.get('errors',False) != False: 
                not_collected_tweets_counter += 1
                if tweet_dict['errors'].get('code',0) == 144:
                    deleted_tweets_counter +=1
                print("actualizado erroneo")
            elif tweet_dict.get('limit',False) != False:
                not_collected_tweets_counter += 1
                print("limite alcanzado")
            else:
                tweet_dict["_id"] = tweet_dict["id_str"]
                new_version_of_tweets.append(tweet_dict)
                updated_tweets_count += 1
            x+=1

        mongo_conector.update_many_tweets_dicts_in_mongo(new_version_of_tweets,mongo_conector.current_collection)
        i+=maximum_tweet_ratio
    

    print("\n\n[LOOKUP-TWEETS-IDS INFO] Tweets para actualizar = {}".format(tweets_for_update))
    print("[LOOKUP-TWEETS-IDS INFO] Tweets actualizados = {}".format(tweets_for_update-not_collected_tweets_counter))
    print("[LOOKUP-TWEETS-IDS INFO] Tweets no actualizados = {}".format(not_collected_tweets_counter))
    print("[LOOKUP-TWEETS-IDS INFO] Tweets que ya no están en la base de datos de Twitter = {}".format(deleted_tweets_counter))




def get_mongo_document(json_tweet):
    try:
        tweet_dict = json.loads(json_tweet)
        tweet_id = tweet_dict["id_str"]
        tweet_dict["_id"]= tweet_id
    except Exception:
        print("[GET MONGO DOCUMENT ERROR] {} ".format(json.dumps(tweet_dict,indent=4, sort_keys=True)))
        return None
    return tweet_dict


  
if __name__ == '__main__': 
    # while True:
    #     get_tweets()
    #     time.sleep(1200) # sleep 20 min

    #collect_tweets_by_streamming_and_save_in_mongo(["red"])

    pass
