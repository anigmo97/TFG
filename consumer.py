import tweepy
import datetime
import json
import time
from pymongo import MongoClient
import global_variables
from global_functions import get_utc_time
import mongo_conector
from threading import Timer,Thread

now = datetime.datetime.now()



consumer_key = ""
consumer_secret = ""
access_token = ""
access_secret = ""
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

class StreamListener(tweepy.StreamListener):    
    #This is a class provided by tweepy to access the Twitter Streaming API. 
    
    def __init__(self,api=None,max_tweets=10000,max_mins=10):
        self.streamming_tweets = 0
        self.max_mins = max_mins
        self.max_tweets = max_tweets
        self.start_time = time.time()
        self.message_timer = Timer(5,self.show_messages_info)
        self.message_timer.start()
        self.time_timer = Timer(max_mins*60,self.finalize_by_time)
        self.time_timer.start()
        # self.client = MongoClient(mongo_conector.MONGO_HOST)
        # # Use twitterdb database. If it doesn't exist, it will be created.
        # self.db = self.client.twitterdb

    def show_messages_info(self):
        print("{} tweets collected in {:.0f} seconds".format(self.streamming_tweets,(time.time() - self.start_time )))
        self.message_timer.run()

    
    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")
    
    def finalize_by_time(self):
        self.on_disconnect("Se ha agotado el tiempo maximo especificado por el usuario")

    def on_disconnect(self, notice):
        self.time_timer.cancel()
        self.message_timer.cancel()
        print(notice)
        print("You are now disconnected to the streaming API.\n\n")
        

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('\n\n[STREAM ERROR]  An Error has occured: ' + repr(status_code))
        codigo = 0
        try:
            codigo= int(status_code)
            if codigo == 420:
                print("You have reached your rate limit\n")
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
            # Decode the JSON from Twitter
            datajson = get_mongo_document(data) # controlamos los errores antes de insertarlo
            #insert the data into the mongoDB into a collection called tweets
            #if tweets doesn't exist, it will be created.
            if datajson is not None:
                mongo_conector.db[mongo_conector.current_collection].insert(datajson) # cambiar por insert many
                if(self.streamming_tweets >= self.max_tweets):
                    print("\n\n{} messages has been collected".format(self.streamming_tweets))
                    self.on_disconnect("User disconnected after get the required amount of tweets")
                    return False # paramos el streamming
                self.streamming_tweets += 1
        except Exception as e:
           print("[ON DATA] {} {}".format(e,e.__cause__))
        



 

def collect_tweets_by_query_and_save_in_file(max_tweets=3000,query="#science",filename="tweets"): 
        API = tweepy.API(auth)
        cursor_respuestas = tweepy.Cursor(API.search, q=query,count=1000,lang="es", since="2017-04-03").items(max_tweets)
        tweets_list = [status._json for status in cursor_respuestas]
        
        with open('tweets/{}{}.json'.format(filename,now.strftime("%Y-%m-%d__%H-%M")), 'w', encoding='utf8') as file:
            file.writelines(json.dumps(tweets_list,indent=4, sort_keys=True))
        
        print("{} tweets capturados".format(len(tweets_list)))

        return tweets_list
        #dumps -> dump string


def collect_tweets_by_query_and_save_in_mongo(max_tweets=3000,query="#science",filename="tweets"): 
        #wait_on_rate_limit = True, wait_on_rate_limit_notify = True
        API = tweepy.API(auth)
        mongo_tweets_dict = {}
        mongo_tweets_id_list = [] 
        for status in tweepy.Cursor(API.search, q=query,count=100,lang="es", since="2017-04-03").items(max_tweets):
            tweet = status._json
            if(tweet.get("id_str",False) != False):
                tweet_id = tweet["id_str"]
                tweet["_id"]= tweet_id
                mongo_tweets_id_list.append(tweet_id)
                mongo_tweets_dict[tweet_id] = tweet
        mongo_conector.insertar_multiples_tweets_en_mongo(mongo_tweets_dict,mongo_tweets_id_list,mongo_conector.current_collection)
        print("{} tweets capturados".format(len(mongo_tweets_id_list)))
        return  mongo_tweets_dict.values()


def collect_tweets_by_streamming_and_save_in_mongo(WORDS=["#python"],max_tweets=10000,max_mins=2):
    print("words {}\t max_tweets {} \t max_mins {}".format(WORDS,max_tweets,max_mins))
    try:
        API = tweepy.API(wait_on_rate_limit=True)
        listener = StreamListener(api=API,max_tweets=max_tweets,max_mins=max_mins) 
        streamer = tweepy.Stream(auth=auth, listener=listener)
        print("Tracking: " + str(WORDS))
        if len(WORDS) > 0:
            #threading.Thread(target=self._thread_function, args=(arg1,),kwargs={'arg2':arg2}, name='thread_function').start()
            streamming_thread = Thread(target=streamer.filter,kwargs=dict(languages=["en","es"],track=WORDS,is_async=True))
        else:
            streamming_thread = Thread(target=streamer.filter,kwargs=dict(languages=["en","es"],is_async=True))
        streamming_thread.start()
        streamming_thread.join()
        while streamming_thread.is_alive: #echo para sincronizar
            pass
    except Exception as e:
        print(e)
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
        print("{}/{} ({:.3f}%) tweets updated (with {} faileds)  TIME:{}"
        .format(updated_tweets_count,tweets_for_update,(updated_tweets_count/tweets_for_update)*100,not_collected_tweets_counter,
        time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time))))
        try:
            tweets = API.statuses_lookup(tweets_ids_list[i:i+maximum_tweet_ratio])
        except Exception as e:
            print("[LOOKUP-TWEETS-IDS] {}".format(e))
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
    

    print("\n\nTweets para actualizar = {}".format(tweets_for_update))
    print("Tweets actualizados = {}".format(tweets_for_update-not_collected_tweets_counter))
    print("Tweets no actualizados = {}".format(not_collected_tweets_counter))
    print("Tweets que ya no están en la base de datos de Twitter = {}".format(deleted_tweets_counter))




def get_mongo_document(json_tweet):
    try:
        tweet_dict = json.loads(json_tweet)
        tweet_id = tweet_dict["id_str"]
        tweet_dict["_id"]= tweet_id
    except Exception:
        print(json.dumps(tweet_dict,indent=4, sort_keys=True))
        return None
    return tweet_dict


  
if __name__ == '__main__': 
    # while True:
    #     get_tweets()
    #     time.sleep(1200) # sleep 20 min

    #collect_tweets_by_streamming_and_save_in_mongo(["red"])

    get_specifics_tweets_from_api_and_update_mongo(mongo_conector.get_tweet_ids_list_from_database(default_collection))
