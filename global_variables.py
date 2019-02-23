def create_top_ten_list():
    return [(0,0)] * 10
	
# GLOBAL VARIABLES
ID = 0
AMOUNT = 1
#count=0


messages_count = 0
tweets_count = 0
retweets_count = 0

tweets_with_replies_count = 0
tweets_without_replies_count = 0
tweets_with_quotes_count = 0
tweets_without_quotes_count = 0
tweets_with_replies_and_quotes_count = 0

retweets_with_replies_count = 0
retweets_without_replies_count = 0
retweets_with_quotes_count = 0
retweets_without_quotes_count = 0
retweets_with_replies_and_quotes_count = 0

#quote tweet
#entities	(hashtags,media,urls,user_mentions,symbols)

verified_account_messages = 0
verified_account_tweets = 0
verified_account_retweets = 0

not_verified_account_messages = 0
not_verified_account_tweets = 0
not_verified_account_retweets = 0

tweets_by_polarity = [0,0,0,0,0,0] #TODO

tweets_dict = {} #contains the captured messages
retweets_dict = {} #contains tweets that was retweeted in our captured messages
quotes_dict = {} #contains tweets that was quoted in our captured messages
users_dict = {}
verified_account_dict_tweets = {}
not_verified_account_dict_tweets = {}

tweets_by_date_dict = {} # [yyyy-mm-dd] -> dict[hh] -> dic[min] -> list[tweet_id1...]

# RANKINGS DE TWEETS
way_of_send_counter = {}
global_most_favs_tweets = create_top_ten_list()
global_most_rt_tweets = create_top_ten_list()
# global_most_reply_tweets = create_top_ten_list() DESCARTADO: Solo en Premium and Enterprise

# RANKINGS DE USUARIOS
#IMPORTANTE SON LOS LIKES QUE HA DADO ESTE USUARIO NO LOS RECIBIDOS
global_most_favs_users = create_top_ten_list()
global_most_tweets_users = create_top_ten_list()
global_most_followers_users = create_top_ten_list()

# RANKINGS DE USUARIOS
local_user_messages_counter = {} # dict [user] -> num_mensajes_analizados
local_user_tweets_counter = {} # dict [user] -> num_tweets_analizados
local_user_retweets_counter = {} # dict [user] -> num_retweets_analizados
#local_followers_counter = {} #INVESTIGATE HOW TO GET FOLLOWERS LIST
local_replied_users_counter = {} # dict [user] -> num_tweets que tenemos que le responden
local_replied_tweets_couter = {} # dict [tweet_id] -> num_respuestas que nosotros tenemos

local_quoted_tweets_counter = {}  # dict [tweet_id] -> num_tweets que tenemos que le citan
local_quoted_users_counter = {}  # dict [user] -> num_tweets que tenemos que le responden


local_most_messages_users = create_top_ten_list()
local_most_tweets_users = create_top_ten_list()
local_most_retweets_users = create_top_ten_list()

#local_most_favs_users = create_top_ten_list() #INVESTIGATE HOW TO GET WHO'S LIKE A TWEET
#local_most_followers_users = create_top_ten_list() #INVESTIGATE HOW TO GET FOLLOWERS LIST

local_most_replied_users = create_top_ten_list()
local_most_replied_tweets = create_top_ten_list()

local_most_quoted_users = create_top_ten_list()
local_most_quoted_tweets = create_top_ten_list()

def get_user_variables_names():
	def check_condition(k,v):
		# si no s una variable protegida no es tmp (tmp es local) y no es especial
		key_conditions = not k.startswith('_') and k!='tmp' and k!='In' and k!='Out' and k!="ID" and k!="AMOUNT"
		# si no es una funcion
		value_condition = not hasattr(v, '__call__')
		return  key_conditions and value_condition
	tmp = globals().copy()
	return [k for k,v in tmp.items() if check_condition(k,v)]

def get_user_variables_names_and_values():
	def check_condition(k,v):
		# si no s una variable protegida no es tmp (tmp es local) y no es especial
		key_conditions = not k.startswith('_') and k!='tmp' and k!='In' and k!='Out' and k!="ID" and k!="AMOUNT"
		# si no es una funcion
		value_condition = not hasattr(v, '__call__')
		return  key_conditions and value_condition
	tmp = globals().copy()
	return [(k,v) for k,v in tmp.items() if check_condition(k,v)]

def set_variable_value(variable_string_name,value):
	try:
		globals()[variable_string_name] = value
	except Exception as e:
		print(e)
		print("[SET VARIABLE VALUE ERROR ] Error setting {} variable with the value : {} ".format(variable_string_name,value))

