import json
import global_variables
from global_functions import update_top_10_list
import re

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
        global_variables.verified_account_dict[user_id] = True
        global_variables.verified_account_messages +=1
        if retweeted:
            global_variables.verified_account_retweets += 1
        else:
            global_variables.verified_account_tweets += 1
    else:
        global_variables.not_verified_account_messages +=1
        if retweeted:
            global_variables.not_verified_account_retweets += 1
        else:
            global_variables.not_verified_account_tweets += 1

def check_polarity(polarity):
    pass

def check_way_of_send(way_of_send):
    way_of_send = re.findall(patron_way_of_send,way_of_send)[0][1]
    if way_of_send not in global_variables.way_of_send_counter:
        global_variables.way_of_send_counter[way_of_send] = 1
    else:
        old_value = global_variables.way_of_send_counter[way_of_send]
        global_variables.way_of_send_counter[way_of_send] = old_value + 1

def check_if_is_retweet(retweeted):
    if retweeted:
        global_variables.retweets_count += 1
    else:
        global_variables.tweets_count += 1


def show_info():
    print("Numero de mensajes {}".format(global_variables.tweet_messages_count))
    print("Numero de tweets {}".format(global_variables.tweets_count))
    print("Numero de retweets {}\n".format(global_variables.retweets_count))

    print("Numero de mensajes de cuentas verificadas {}".format(global_variables.verified_account_messages))
    print("Numero de tweets de cuentas verificadas {}".format(global_variables.verified_account_tweets))
    print("Numero de retweets de cuentas verificadas {}\n".format(global_variables.verified_account_retweets))

    print("Numero de mensajes de cuentas no verificadas {}".format(global_variables.not_verified_account_messages))
    print("Numero de tweets de cuentas no verificadas {}".format(global_variables.not_verified_account_tweets))
    print("Numero de retweets de cuentas no verificadas {}\n".format(global_variables.not_verified_account_retweets))

    print("Numero de distintas vias de envio {}".format(len(global_variables.way_of_send_counter)))
    for k,v in global_variables.way_of_send_counter.items():
        print("Numero de mensajes enviados via '{}' = {}".format(k,v))
    
    print("\nEl tweet mas likes tiene {} likes".format(global_variables.global_most_favs_tweets[0][1]))
    print("El tweet que mas retweets tiene, tiene {} retweets".format(global_variables.global_most_rt_tweets[0][1]))


def analyze_tweets(json_files):
    for json_file in json_files:
        current_tweet_dict_list = read_json_file(json_file)
        for current_tweet_dict in current_tweet_dict_list:
            tweet_id = current_tweet_dict["id_str"]
                
            global_variables.tweets_dict[tweet_id] = current_tweet_dict

            check_if_is_verified(current_tweet_dict["user"]["id_str"],current_tweet_dict['user']["verified"],current_tweet_dict.get("retweeted_status",False))
            #check_polarity(tweet_dict[])
            check_way_of_send(current_tweet_dict["source"])

            update_top_10_list(global_variables.global_most_favs_tweets,(tweet_id,current_tweet_dict["favorite_count"]))
            update_top_10_list(global_variables.global_most_rt_tweets,(tweet_id,current_tweet_dict["retweet_count"]))
            
            global_variables.tweet_messages_count +=1
            check_if_is_retweet(current_tweet_dict.get("retweeted_status",False))
    show_info()


######################################################################
######################       MAIN PROGRAM       ######################
######################################################################
if __name__ == "__main__":
    analyze_tweets(["/home/angel/Escritorio/TFG/cunsumir/tweets2019-02-01 20-54.json"])
