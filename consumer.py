import tweepy
import datetime
import json
import time
from pymongo import MongoClient
import global_variables
from global_functions import get_utc_time
import mongo_conector
from threading import Timer

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
        self.message_timer = Timer(2,self.show_messages_info)
        self.message_timer.start()
        # self.client = MongoClient(mongo_conector.MONGO_HOST)
        # # Use twitterdb database. If it doesn't exist, it will be created.
        # self.db = self.client.twitterdb

    def show_messages_info(self):
        print("{} tweets collected in {:.0f} seconds".format(self.streamming_tweets,(time.time() - self.start_time )))
        self.message_timer.run()

    
    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")
    
    def on_disconnect(self, notice):
        self.message_timer.cancel()
        print(notice)
        print("You are now disconnected to the streaming API.\n\n")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False
 
    def on_status(self, status):
        """Called when a new status arrives"""
        pass

    def on_data(self, data):
        try:
            # Decode the JSON from Twitter
            datajson = get_mongo_document(data)
            #insert the data into the mongoDB into a collection called tweets
            #if tweets doesn't exist, it will be created.
            if datajson is not None:
                mongo_conector.db.tweets.insert(datajson) # cambiar por insert many
                if(self.streamming_tweets > self.max_tweets):
                    print("\n\n{} messages has been collected".format(self.streamming_tweets))
                    self.on_disconnect("User disconnected after get the required amount of tweets")
                    return False # paramos el streamming
                self.streamming_tweets += 1
        except Exception as e:
           print("[ON DATA] {} {}".format(e,e.__cause__))
        



 

def get_tweets(max_tweets=3000,query="#science",filename="tweets"): 
        API = tweepy.API(auth)
        tweets_list = [status._json for status in tweepy.Cursor(API.search, q=query,count=100,lang="es", since="2017-04-03").items(max_tweets)]
        
        with open('tweets/{}{}.json'.format(filename,now.strftime("%Y-%m-%d__%H-%M")), 'w', encoding='utf8') as file:
            file.writelines(json.dumps(tweets_list,indent=4, sort_keys=True))
        
        print("{} tweets capturados".format(len(tweets_list)))
        #dumps -> dump string

def get_tweets_by_streamming(WORDS=[],max_tweets=100,max_mins=2):
    API = tweepy.API(wait_on_rate_limit=True)
    listener = StreamListener(api=API,max_tweets=max_tweets,max_mins=max_mins) 
    streamer = tweepy.Stream(auth=auth, listener=listener)
    print("Tracking: " + str(WORDS))
    if len(WORDS) > 0:
        streamer.filter(languages=["en","es"],track=WORDS,is_async=True)
    else:
        streamer.filter(languages=["en","es"],is_async=True)
        #REVISAR
        #streamer.filter(languages=["en","es"],async=True)

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
    get_tweets_by_streamming(["red"])
    