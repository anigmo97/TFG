import global_variables
import os
from datetime import datetime,timedelta



# GLOBAL FUNCTIONS
def create_list_with_size_ten():
    return [None,None,None,None,None,None,None,None,None,None]


def create_dir_if_not_exits(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def toJSON(item):
    return item._json

def update_top_10_list(lista,tuple_id_amount,show=False):
    def get_id_pos(local_id):
        try:
            pos = [x[0] for x in lista[0:10]].index(local_id)
        except ValueError:
            pos = 10
        return pos

    id,amount =tuple_id_amount
    borrar = True
    i = get_id_pos(id)
    if i<10:
        amount = max(lista[i][1],amount)
        del lista[i]
        borrar = False
    while i >= 1 and lista[i-1][global_variables.AMOUNT] < amount:
        i-=1
    lista.insert(i,(id,amount))
    if borrar:
        lista.pop()
    #global_variables.count +=1 #revisar

    

def notNone(value):
    return value != None
    
def checkParameter(parameter):
    if parameter == None or parameter == False:
        return 0
    else:
        return 1

def throw_error(module_name, error_message):
    print("\n\n\n [{}] {}".format(module_name,error_message))
    exit(1)

def isJsonFile(filename):
    return filename[-5:]==".json"

def get_one_screen_name(user_id):
    nickname = "unknown"
    if user_id in global_variables.users_dict:
        if "screen-names" in global_variables.users_dict[user_id]:
            nickname = global_variables.users_dict[user_id]["screen-names"][0]

    return nickname

def get_top_user(top_10_list):
    user_id,amount = top_10_list[0]
    screen_name = get_one_screen_name(user_id)
    return user_id,screen_name,amount


def increment_dict_counter(dictionary,id):
    val = dictionary.get(id,0)+1
    dictionary[id] = val
    return val

#twitter usa horario pdt lo que son 9 horas mas en españa
def get_utc_time_particioned(date,utc_offset=9):
    datetime_object = datetime.strptime(date, '%a %b %d %H:%M:%S +0000 %Y') + timedelta(hours=utc_offset)
    return datetime_object.strftime("%Y-%m-%d"),datetime_object.hour,datetime_object.minute
    # [yyyy-mm-dd] -> dict[hh] -> dic[min] -> list[tweet_id1...]

#twitter usa horario pdt lo que son 9 horas mas en españa
def get_utc_time(date,utc_offset=9):
    return datetime.strptime(date, '%a %b %d %H:%M:%S +0000 %Y') + timedelta(hours=utc_offset)

def insert_tweet_in_date_dict(tweet_id,fecha,hora,minuto):
    min_dict = { minuto:[tweet_id] }
    hour_dict = {hora: min_dict}
    if fecha not in global_variables.tweets_by_date_dict: 
        global_variables.tweets_by_date_dict[fecha]=hour_dict
    else:
        if hora not in global_variables.tweets_by_date_dict[fecha]:
            global_variables.tweets_by_date_dict[fecha].update(hour_dict)
        else:
            if minuto not in global_variables.tweets_by_date_dict[fecha][hora]:
                global_variables.tweets_by_date_dict[fecha][hora].update(min_dict)
            else:
                # posible mejora1: mirar los segundos y son 30 o mas insertar por el final
                # y si son de 0 a 29 insertar por el principio
                # posible mejora 2 insertar tupla (tweet_id,segs)
                # posible mejora 3 lista con 12 listas (intervalos de 5 segs)
                global_variables.tweets_by_date_dict[fecha][hora][minuto] += [tweet_id] 

def is_user(id):
    if id in global_variables.users_dict:
        return True
    elif id in global_variables.tweets_dict:
        return False
    else:
        return None

def is_tweet(id):
    if id in global_variables.users_dict:
        return False
    elif id in global_variables.tweets_dict:
        return True
    else:
        return None    

def load_statistics_file_in_global_variables(statistics_file):
    if not (type(statistics_file) is dict):
        raise Exception('[LOAD STATISTICS ERROR] Statistics_file debe ser un diccionario')
    set(globals(global_variables)).issubset(statistics_file.keys())

###################################################################################################################################
###################################################################################################################################
################################################# DEBUG METHODS ###################################################################
################################################# It doesn't add functionality ####################################################
###################################################################################################################################


def show_date_dicctionary():
    for fecha,dict_horas in global_variables.tweets_by_date_dict.items():
        for hora,dict_mins in dict_horas.items():
            for minuto,tweets_ids in dict_mins.items():
                print("fecha {}-  hora {}:{}  {}".format(fecha,hora,minuto,tweets_ids[0:8]))

def show_date_dicctionary_simple():
    count_local = 0
    try:
        print("\n\nNum     Diccionario1  Diccionario2    Diccionario3     Value Diccionario3")
        print("         key=fecha      key=hora       key=minuto")
        for fecha,dict_horas in global_variables.tweets_by_date_dict.items():
            for hora,dict_mins in dict_horas.items():
                for minuto,tweets_ids in dict_mins.items():
                    count_local+=1
                    print("{0:<8} {1:<14}->  {2:<10} -> {3:<8}     [tweets_ids_list]".format(count_local,fecha,hora,minuto))
                    if count_local >= 20:
                        raise Exception  
    except:
        pass  

def print_num_tweets_per_date():
    count_local = 0
    for fecha,dict_horas in global_variables.tweets_by_date_dict.items():
        for hora,dict_mins in dict_horas.items():
            for minuto,tweets_ids in dict_mins.items():
                count_local+=len(tweets_ids)
    print(count_local)
        

def print_top_10_list(lista,titulo):
    print(titulo)
    print("{0:>30} {1:>20}{2:>15}    {3:<20}      {4:<15}".format("ID","AMOUNT","IN USERS DICT", "IN TWEETS DICT","USER"))
    for index,(id,amount) in enumerate(lista,1):
        res1 = global_variables.users_dict.get(id,False)
        res2 = global_variables.tweets_dict.get(id,False)
        res3 = global_variables.quotes_dict.get(id,False)
        res4 = global_variables.retweets_dict.get(id,False)
        if res1!= False:
            userId = id
        elif res2 != False:
            userId = global_variables.tweets_dict[id]["user"]["id_str"]
        elif res3 != False: 
            userId = global_variables.quotes_dict[id]["user"]["id_str"]
        elif res4 != False:
            userId = global_variables.retweets_dict[id]["user"]["id_str"]
        else:
            userId = "unknown"
        user = get_one_screen_name(userId)

        print("{0:<3}   {1:>34}  {2:<15} {3:<15} {4:>4} {5:>25}".format(index,id,amount,str(res1!=False),str(res2!=False),user))
    print("\n\n\n")

def print_all_top_ten_lists():
    print_top_10_list(global_variables.global_most_favs_tweets,"tweets con mas likes")
    print_top_10_list(global_variables.global_most_favs_users,"usuarios que mas likes dan")
    print_top_10_list(global_variables.global_most_followers_users,"usuarios con mas followers")
    print_top_10_list(global_variables.global_most_rt_tweets,"tweets con mas retweets")
    print_top_10_list(global_variables.global_most_tweets_users,"usuarios con mas tweets publicados")

    # top 10 referente a nuestro conjunto de datos y sus estadisticas internas
    print_top_10_list(global_variables.local_most_messages_users,"usuarios de los que tenemos mas mensajes ( entre tweets y retweets")
    print_top_10_list(global_variables.local_most_retweets_users,"usuarios de los cuales tenemos mas retweets")
    print_top_10_list(global_variables.local_most_tweets_users,"usuarios de los cuales tenemos mas tweets")

    print_top_10_list(global_variables.local_most_replied_tweets,"tweets para los cuales tenemos mas respuestas")
    print_top_10_list(global_variables.local_most_replied_users,"usuarios para los cuale tenemos mas respuestas")
    
    print_top_10_list(global_variables.local_most_quoted_tweets,"tweets para los cuales tenemos mas citas")
    print_top_10_list(global_variables.local_most_quoted_users,"usuarios para los cuales tenemos mas citas")

    #print_top_10_list(global_variables.local_most_favs_users,"")
    #print_top_10_list(global_variables.local_most_followers_users,"usuarios de los cuales tenemos mas followers")

x= global_variables.get_statistics_dict()
print(x)