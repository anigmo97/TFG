import tweepy
import datetime
import json
import time
from pymongo import MongoClient
import global_variables
from global_functions import get_utc_time

now = datetime.datetime.now()
MONGO_HOST= 'mongodb://localhost/tweet'


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
        self.client = MongoClient(MONGO_HOST)
    
    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")
    
    def on_disconnect(self, notice):
        print(notice)
        print("You are now disconnected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False
 
    def on_status(self, status):
        """Called when a new status arrives"""
        pass

    def on_data(self, data):
         
        try:
            
            # Use twitterdb database. If it doesn't exist, it will be created.
            db = self.client.twitterdb
    
            # Decode the JSON from Twitter
            datajson = json.loads(data)
 
            #print out a message to the screen that we have collected a tweet
            print("Tweet collected at " + str(get_utc_time(datajson['created_at'])))
            
            #insert the data into the mongoDB into a collection called tweets
            #if twitter_search doesn't exist, it will be created.
            db.tweets.insert(datajson)
            self.streamming_tweets += 1
            if(self.streamming_tweets > self.max_tweets):
                self.on_disconnect("User disconnected after get the required amount of tweets")
                return False # paramos el streamming
        except Exception as e:
           print(e)


 

def get_tweets(max_tweets=3000,query="#science",filename="tweets"): 
        API = tweepy.API(auth)
        tweets_list = [status._json for status in tweepy.Cursor(API.search, q=query,count=100,lang="es", since="2017-04-03").items(max_tweets)]
        
        with open('tweets/{}{}.json'.format(filename,now.strftime("%Y-%m-%d__%H-%M")), 'w', encoding='utf8') as file:
            file.writelines(json.dumps(tweets_list,indent=4, sort_keys=True))
        
        print("{} tweets capturados".format(len(tweets_list)))
        #dumps -> dump string

def get_tweets_by_streamming(WORDS=[],max_tweets=300,max_mins=2):
    API = tweepy.API(wait_on_rate_limit=True)
    listener = StreamListener(api=API,max_tweets=max_tweets,max_mins=max_mins) 
    streamer = tweepy.Stream(auth=auth, listener=listener)
    print("Tracking: " + str(WORDS))
    if len(WORDS) > 0:
        streamer.filter(languages=["en","es"],track=WORDS)
  
if __name__ == '__main__': 
    # while True:
    #     get_tweets()
    #     time.sleep(1200) # sleep 20 min
    get_tweets_by_streamming("red")
    