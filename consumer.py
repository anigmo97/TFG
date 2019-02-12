import tweepy
import datetime
import json
import time

now = datetime.datetime.now()
def toJSON(item):
    return item._json

#Twitter API credentials
def load_api():
    ''' Function that loads the twitter API after authorizing the user. '''

    consumer_key = ""
    consumer_secret = ""
    access_token = ""
    access_secret = ""
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    # load the twitter API via tweepy
    return tweepy.API(auth)

def get_tweets(): 
          
        api = load_api()
        max_tweets = 3000
        tweets_list = [status._json for status in tweepy.Cursor(api.search,  q="#BarçaValencia",count=100,lang="es",since="2017-04-03").items(max_tweets)]
        #tweets_list = tweepy.Cursor(api.search,q="#vox",count=100,lang="es",since="2017-04-03").items()
        #print(tweets_list)
        with open('tweets/barcVal{}.json'.format(now.strftime("%Y-%m-%d__%H-%M")), 'w', encoding='utf8') as file:
            file.writelines(json.dumps(tweets_list,indent=4, sort_keys=True))
        
        print("{} tweets capturados".format(len(tweets_list)))
        #dumps -> dump string
  
  
# Driver code 
if __name__ == '__main__': 
  
    # Here goes the twitter handle for the user 
    # whose tweets are to be extracted.
    while True:
        get_tweets()
        time.sleep(1200) 
    

# if __name__ == '__main__':
    
#     stream.filter(track=["#maratonpotter1"])
#     #get_all_tweets("user name goes here")  #Example:@realDona