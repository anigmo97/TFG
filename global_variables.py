import global_functions
# GLOBAL VARIABLES
ID = 0
AMOUNT = 1

tweets_count = 0
verified_account_tweets = 0
not_verified_account_tweets = 0
tweets_by_polarity = [0,0,0,0,0,0]
tweets_dict = {}

# RANKINGS DE TWEETS
global_way_of_send_counter = {}

global_most_favs_tweets = global_functions.create_list_with_size_ten()
global_most_rt_tweets = global_functions.create_list_with_size_ten()
global_most_replay_tweets = global_functions.create_list_with_size_ten()

# RANKINGS DE TWEETS EN NUESTRO CONJUNTO
local_way_of_send_counter = {}
local_like_counter_dict={}
local_rt_counter_dict={}
local_replay_counter_dict={}

local_most_favs_tweets = global_functions.create_list_with_size_ten()
local_most_rt_tweets = global_functions.create_list_with_size_ten()
local_most_replay_tweets = global_functions.create_list_with_size_ten()

# RANKINGS DE USUARIOS
global_most_favs_users = global_functions.create_list_with_size_ten()
global_most_tweets_users = global_functions.create_list_with_size_ten()
global_most_followers_users = global_functions.create_list_with_size_ten()

# RANKINGS DE USUARIOS
local_tweets_counter = {}
local_followers_counter = {}

local_most_favs_users = global_functions.create_list_with_size_ten()
local_most_tweets_users = global_functions.create_list_with_size_ten()
local_most_followers_users = global_functions.create_list_with_size_ten()