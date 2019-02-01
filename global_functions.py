import global_variables

# GLOBAL FUNCTIONS
def create_list_with_size_ten():
    return [None,None,None,None,None,None,None,None,None,None]

def create_top_ten_list():
    return [(0,0)] * 11

def update_top_10_list(lista,tuple_id_amount):
    id,amount =tuple_id_amount
    if lista[9][global_variables.AMOUNT] < amount :
        i = 9
        while lista[i][global_variables.AMOUNT] < amount and i >= 0:
            lista[i+1] = lista[i]
            lista[i] = tuple_id_amount
            i-=1