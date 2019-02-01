import global_functions
# GLOBAL VARIABLES
ID = 0
AMOUNT = 1

tweet_messages_count = 0
tweets_count = 0
retweets_count = 0
verified_account_messages = 0
verified_account_tweets = 0
verified_account_retweets = 0

not_verified_account_messages = 0
not_verified_account_tweets = 0
not_verified_account_retweets = 0

tweets_by_polarity = [0,0,0,0,0,0] #TODO
tweets_dict = {}
verified_account_dict = {}

# RANKINGS DE TWEETS
way_of_send_counter = {}
global_most_favs_tweets = global_functions.create_top_ten_list()
global_most_rt_tweets = global_functions.create_top_ten_list()
# global_most_reply_tweets = global_functions.create_top_ten_list() DESCARTADO: Solo en Premium and Enterprise

# RANKINGS DE USUARIOS
global_most_favs_users = global_functions.create_top_ten_list()
global_most_tweets_users = global_functions.create_top_ten_list()
global_most_followers_users = global_functions.create_top_ten_list()

# RANKINGS DE USUARIOS
local_tweets_counter = {}
local_followers_counter = {} #INVESTIGATE HOW TO GET FOLLOWERS LIST
local_most_replied_users_counter = {}

local_most_favs_users = global_functions.create_top_ten_list()
local_most_tweets_users = global_functions.create_top_ten_list()
local_most_followers_users = global_functions.create_top_ten_list()
local_most_replied_users = global_functions.create_top_ten_list()


