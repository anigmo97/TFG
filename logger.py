# encoding: utf-8
from global_functions import get_top_user,show_date_dicctionary,show_date_dicctionary_simple,print_num_tweets_per_date,print_all_top_ten_lists
import global_variables
#from global_variables import *     (imports variables)
import timeit

def show_info():

    print("Numero de cuentas de las que se han extraido tweets {}".format(len(global_variables.verified_account_dict_tweets)+len(global_variables.not_verified_account_dict_tweets)))
    print("Numero de mensajes {}".format(global_variables.messages_count))
    print("Numero de tweets {}".format(global_variables.tweets_count))
    print("Numero de retweets {}\n".format(global_variables.retweets_count))

    print("Numero de retweets con respuesta {}  (no existen)".format(global_variables.retweets_with_replies_count))
    print("Numero de retweets sin respuesta {}".format(global_variables.retweets_without_replies_count))
    print("Numero de retweets con cita {}  ".format(global_variables.retweets_with_quotes_count))
    print("Numero de retweets sin cita {}".format(global_variables.retweets_without_quotes_count))
    print("Numero de retweets con respuesta y cita {} (no hay reteets con respuesta)\n".format(global_variables.retweets_with_replies_and_quotes_count))

    print("Numero de tweets con respuesta {}".format(global_variables.tweets_with_replies_count))
    print("Numero de tweets sin respuesta {}".format(global_variables.tweets_without_replies_count))
    print("Numero de tweets con cita {}".format(global_variables.tweets_with_quotes_count))
    print("Numero de tweets sin cita {}".format(global_variables.tweets_without_quotes_count))
    print("Numero de tweets con respuesta y cita {}\n".format(global_variables.tweets_with_replies_and_quotes_count))

    print("Numero de cuentas verificadas distintas de las que se han extraido tweets {}".format(len(global_variables.verified_account_dict_tweets)))
    print("Numero de mensajes de cuentas verificadas {}".format(global_variables.verified_account_messages))
    print("Numero de tweets de cuentas verificadas {}".format(global_variables.verified_account_tweets))
    print("Numero de retweets de cuentas verificadas {}\n".format(global_variables.verified_account_retweets))

    print("Numero de cuentas no verificadas distintas de las que se han extraido tweets {}".format(len(global_variables.not_verified_account_dict_tweets)))
    print("Numero de mensajes de cuentas no verificadas {}".format(global_variables.not_verified_account_messages))
    print("Numero de tweets de cuentas no verificadas {}".format(global_variables.not_verified_account_tweets))
    print("Numero de retweets de cuentas no verificadas {}\n".format(global_variables.not_verified_account_retweets))

    print("Numero de distintas vias de envio {}".format(len(global_variables.way_of_send_counter)))
    for k,v in global_variables.way_of_send_counter.items():
        print("Numero de mensajes enviados via '{}' = {}".format(k,v))
    
    print("\nEl tweet mas likes tiene {} likes".format(global_variables.global_most_favs_tweets[0][1]))
    print("El tweet que mas retweets tiene, tiene {} retweets\n".format(global_variables.global_most_rt_tweets[0][1]))

    print("El usuario que mas likes ha dado es '@{}', ha dado {} likes".format(get_top_user(global_variables.global_most_favs_users)[1],get_top_user(global_variables.global_most_favs_users)[2]))
    print("El usuario que mas tweets tiene es '@{}', tiene {} mensajes (entre tweets y retweets)".format(get_top_user(global_variables.global_most_tweets_users)[1],get_top_user(global_variables.global_most_tweets_users)[2]))
    print("El usuario que mas followers tiene es '@{}', tiene {} followers\n".format(get_top_user(global_variables.global_most_followers_users)[1],get_top_user(global_variables.global_most_followers_users)[2]))

    print("El usuario del cual tenemos mas mensajes es '@{}' con {} mensajes".format(get_top_user(global_variables.local_most_messages_users)[1],get_top_user(global_variables.local_most_messages_users)[2]))
    print("El usuario del cual tenemos mas tweets es '@{}' con {} tweets".format(get_top_user(global_variables.local_most_tweets_users)[1],get_top_user(global_variables.local_most_tweets_users)[2]))
    print("El usuario del cual tenemos mas retweets es '@{}' con {} retweets\n".format(get_top_user(global_variables.local_most_retweets_users)[1],get_top_user(global_variables.local_most_retweets_users)[2]))

    print("El usuario que más veces ha sido respondido por los mensajes que tenemos es '@{}' con {} respuestas".format(get_top_user(global_variables.local_most_replied_users)[1],get_top_user(global_variables.local_most_replied_users)[2]))
    print("El tweet que más veces ha sido respondido por los mensajes que tenemos tiene {} respuestas\n".format(global_variables.local_most_replied_tweets[0][1]))

    print("El usuario que más veces ha sido citado en por los mensajes que tenemos es '@{}' con {} citas".format(get_top_user(global_variables.local_most_quoted_users)[1],get_top_user(global_variables.local_most_quoted_users)[2]))
    print("El tweet que más veces ha sido citado por los mensajes que tenemos tiene {} citas\n".format(global_variables.local_most_quoted_tweets[0][1]))
    #show_date_dicctionary_simple() # muestra un ejemplo de registros del diccionario de tweets indexados por fecha
    #print_num_tweets_per_date() # suma el numero de tweets indexados guardados en el deiccionario de fechas
    print_all_top_ten_lists() # printa los distintos top e una forma mas humana



def show_parameters(args):
    print("args.file = {}".format(args.file))
    print("args.directory = {}".format(args.directory))
    print("args.directory_of_directories = {}".format(args.directory_of_directories))
    print("args.output_file = {}".format(args.output_file))
    print("args.update = {}".format(args.update))
    print("args.streamming = {}".format(args.streamming))
    print("args.query = {}".format(args.query))
    print("args.query_file = {}".format(args.query_file))
    print("args.words = {}".format(args.words))
    print("args.max_messages = {}".format(args.max_messages))
    print("args.max_time = {}".format(args.max_time))