import json
import global_variables
import global_functions

def read_json_file(file_path):
    try:
        with open(file_path) as handle:
            tweet_dict = json.loads(handle.read())
    except:
        raise Exception("El fichero {} no existe o es erroneo".format(file_path))
    return tweet_dict

def update_top_10_dictionary(dictionary,tuple_id_amount):
    id,amount =tuple_id_amount
    if dictionary[10][AMOUNT] < amount:
        i = 10
        while dictionary[i][AMOUNT] < amount:
            dictionary[i+1] = dictionary[i]
            dictionary[i] = tuple_id_amount
            i-=1
    

for json_file in json_files:
    tweet_dict = read_json_file(json_file)
    global_variables.tweets_dict[tweet_dict["id_str"]] = tweet_dict
    

print(dictdump["coordinates"])