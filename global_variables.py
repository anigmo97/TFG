import global_functions
# GLOBAL VARIABLES
ID = 0
AMOUNT = 1
count=0


messages_count = 0
tweets_count = 0
retweets_count = 0

tweets_with_replies_count = 0
tweets_without_replies_count = 0
tweets_with_quotes_count = 0
tweets_without_quotes_count = 0

retweets_with_replies_count = 0
retweets_without_replies_count = 0
retweets_with_quotes_count = 0
retweets_without_quotes_count = 0

#quote tweet
#entities	(hashtags,media,urls,user_mentions,symbols)

verified_account_messages = 0
verified_account_tweets = 0
verified_account_retweets = 0

not_verified_account_messages = 0
not_verified_account_tweets = 0
not_verified_account_retweets = 0

tweets_by_polarity = [0,0,0,0,0,0] #TODO

tweets_dict = {}
users_dict = {}
verified_account_dict_tweets = {}
not_verified_account_dict_tweets = {}

tweets_by_date_dict = {} # [yyyy-mm-dd] -> dict[hh] -> dic[min] -> list[tweet_id1...]

# RANKINGS DE TWEETS
way_of_send_counter = {}
global_most_favs_tweets = global_functions.create_top_ten_list()
global_most_rt_tweets = global_functions.create_top_ten_list()
# global_most_reply_tweets = global_functions.create_top_ten_list() DESCARTADO: Solo en Premium and Enterprise

# RANKINGS DE USUARIOS
#IMPORTANTE SON LOS LIKES QUE HA DADO ESTE USUARIO NO LOS RECIBIDOS
global_most_favs_users = global_functions.create_top_ten_list()
global_most_tweets_users = global_functions.create_top_ten_list()
global_most_followers_users = global_functions.create_top_ten_list()

# RANKINGS DE USUARIOS
local_user_messages_counter = {} # dict [user] -> num_mensajes_analizados
local_user_tweets_counter = {} # dict [user] -> num_tweets_analizados
local_user_retweets_counter = {} # dict [user] -> num_retweets_analizados
local_followers_counter = {} #INVESTIGATE HOW TO GET FOLLOWERS LIST
local_replied_users_counter = {} # dict [user] -> num_tweets que tenemos que le responden
local_replied_tweets_couter = {} # dict [tweet_id] -> num_respuestas que nosotros tenemos 


local_most_messages_users = global_functions.create_top_ten_list()
local_most_tweets_users = global_functions.create_top_ten_list()
local_most_retweets_users = global_functions.create_top_ten_list()

local_most_favs_users = global_functions.create_top_ten_list() #INVESTIGATE HOW TO GET WHO'S LIKE A TWEET
local_most_followers_users = global_functions.create_top_ten_list() #INVESTIGATE HOW TO GET FOLLOWERS LIST
local_most_replied_users = global_functions.create_top_ten_list()
local_most_replied_tweets = global_functions.create_top_ten_list()

