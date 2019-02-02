import global_variables
from datetime import datetime,timedelta



# GLOBAL FUNCTIONS
def create_list_with_size_ten():
    return [None,None,None,None,None,None,None,None,None,None]

def create_top_ten_list():
    return [(0,0)] * 10

# def update_top_10_list(lista,tuple_id_amount):
#     id,amount =tuple_id_amount
#     if lista[9][global_variables.AMOUNT] < amount :
#         i = 9
#         while lista[i][global_variables.AMOUNT] < amount and i >= 0:
#             lista[i+1] = lista[i]
#             lista[i] = tuple_id_amount
#             i-=1



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
    global_variables.count +=1

    

def notNone(value):
    return value != None

def throw_error(module_name, error_message):
    print("\n\n\n [{}] {}".format(module_name,error_message))
    exit(1)

def isJsonFile(filename):
    return filename[-5:]==".json"

def get_one_screen_name(user_id):
    return global_variables.users_dict[user_id]["screen-names"][0]

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
                #posible mejora: mirar los segundos y son 30 o mas insertar por el final
                # y si son de 0 a 29 insertar por el principio
                # posible ejora 2 insertar tupla (tweet_id,segs)
                global_variables.tweets_by_date_dict[fecha][hora][minuto] += [tweet_id] 
    

def show_date_dicctionary():
    for fecha,dict_horas in global_variables.tweets_by_date_dict.items():
        for hora,dict_mins in dict_horas.items():
            for minuto,tweets_ids in dict_mins.items():
                print("fecha {}-  hora {}:{}  {}".format(fecha,hora,minuto,tweets_ids[0:8]))

def show_date_dicctionary_simple():
    count_local = 0
    try:
        for fecha,dict_horas in global_variables.tweets_by_date_dict.items():
            for hora,dict_mins in dict_horas.items():
                for minuto,tweets_ids in dict_mins.items():
                    count_local+=1
                    print("[{}]  fecha {} - hora {}:{}  [tweets_ids_list]".format(count_local,fecha,hora,minuto))
                    if count_local > 20:
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
        
    