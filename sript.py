import argparse
import json
from re import findall
import sys
import os 
import timeit
# USER MODULES IMPORTS
import global_variables
from global_functions import update_top_10_list,throw_error,notNone,isJsonFile,increment_dict_counter
from global_functions import get_utc_time_particioned,insert_tweet_in_date_dict
from logger import show_info


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



def analyze_tweets(json_files):
    for json_file in json_files:
        current_tweet_dict_list = read_json_file(json_file)
        for current_tweet_dict in current_tweet_dict_list:
            # tweet info
            tweet_id = current_tweet_dict["id_str"]
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

    show_info()

    print('\n\nMensajes analizados: {} Time: {}'.format(global_variables.messages_count,timeit.default_timer() - start))


######################################################################
######################       MAIN PROGRAM       ######################
######################################################################
if __name__ == "__main__":
    start = timeit.default_timer()

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="input json file",type=str)
    parser.add_argument("-o","--output_file",help="choose name for file with results", type=str)
    parser.add_argument("-D","-d","--directory", help="directory of input json files", type=str)
    parser.add_argument("-DD","-dd","--directory_of_directories", help="father directory of json directories", type=str)
    parser.add_argument("-up","-UP","--update",action='store_true')
    parser.add_argument("-l","-L","--live",action='store_true',help="get tweets from twitter and then analyze them")
    parser.add_argument("-u","-U","--usage",action='store_true')
    parser.add_argument("-e","-E","--examples",action='store_true')
    args = parser.parse_args()
    file_mode = True
    if notNone(args.file) + notNone(args.directory) + notNone(args.directory_of_directories) > 1:
        throw_error(sys.modules[__name__],"No se pueden usar las opciones '-f' '-d' o -dd de forma simultanea ")
    elif notNone(args.file) + notNone(args.directory) + notNone(args.directory_of_directories) == 1:
        tweets_files_list = retrieveTweetsFromFileSystem(args.file,args.directory,args.directory_of_directories)
    else: 
        pass # check live option
    analyze_tweets(tweets_files_list)
