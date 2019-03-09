from pymongo import MongoClient,errors
from bson.objectid import ObjectId
from global_functions import change_dot_in_keys_for_bullet,change_bullet_in_keys_for_dot
import traceback
import json
import ast # to load query string to dict
from datetime import datetime
import re
from bson.code import Code
import prueba_selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import sqlparse
import time
from threading import Thread


MONGO_HOST= 'mongodb://localhost/tweet'
client = MongoClient(MONGO_HOST)
db = client.twitterdb

not_controlled_functions = ["aggregate","bulkWrite","copyTo","estimatedDocumentCount","createIndex",
    "createIndexes","dataSize","dropIndex","dropIndexes","ensureIndex","explain"
    ,"findAndModify","findOneAndDelete","findOneAndReplace","findOneAndUpdate","getIndexes","getShardDistribution",
    "getShardVersion","insertMany","isCapped","latencyStats","mapReduce","reIndex",
    "renameCollection","save","storageSize","totalIndexSize","totalSize","watch","validate"]

def check_sql_sintax_in_web(sql_query,show =False):

	url = "https://www.eversql.com/sql-syntax-check-validator/"
	


	try:
		#chrome_options = Options()  
		#chrome_options.add_argument("--headless")
		driver = webdriver.Chrome(ChromeDriverManager().install())
		driver.get(url)
		
	except Exception as e:
		print("[PARSE SQL QUERY ERROR] Error conecting")
		print(e)
		exit(1)

	
	driver.maximize_window()

	editor = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="sql-editor"]/div/div[6]/div[1]/div/div/div/div[5]')))

	editor.click()
	if show:
		print("editor_clicado")
		print(editor.text)
		print(editor.get_attribute('innerHTML'))
		print("\n\n\n")

	# CREAMOS UN ACCIONADOR PARA BORRAR EL TEXTO Y ESCRIBIR EL NUESTRO
	accionador = webdriver.ActionChains(driver)
	accionador.move_to_element(editor).click().perform()
	accionador.send_keys(Keys.BACK_SPACE * 100).perform()
	#Thread(target=lambda: accionador.send_keys(Keys.CONTROL + 'a').perform()).start()
	time.sleep(1)
	thread1 = Thread(target=lambda: accionador.send_keys(sql_query).perform())
	thread1.start()
	thread1.join()
	time.sleep(1)
	
	# USAMOS UN NUEVO ACCIONADOR PORQUE SINO SE PUEDEN SOLAPAR
	boton = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, 'wizard-next-button'))) 
	webdriver.ActionChains(driver).move_to_element(boton).click().perform()
	
	error = False
	try:
		error_div = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, 'query-error-content'))) 
		error=True
	except:
		pass

	if(error):
		code = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="sql-editor"]/div/div[6]'))).text.split("\n") 
		for i in range(len(code)//2):
			print("{:4} {}".format(code[i*2],code[i*2+1]))
		print("\n\nQuery Error: {}".format(error_div.text))
		driver.close()
		return False
	driver.close()
	return True

def escape_true_and_false(sql_query):
	sql_query = sql_query.replace(" TRUE"," \\TRUE")
	sql_query = sql_query.replace(" FALSE"," \\FALSE")
	#sql_query = sql_query.replace(" NULL"," \\NULL")
	return sql_query

def convert_sql_query_to_mongo_query_in_web(sql_query,show=False):
	url = "http://www.querymongo.com/"
	driver = webdriver.Chrome(ChromeDriverManager().install())
	driver.get(url)
	driver.maximize_window()

	div_interna = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[1]/form/div[1]/div[3]')))

	div_interna.click()
	if show:
		print("div_interna_clicado")
		print(div_interna.text)
		print(div_interna.get_attribute('innerHTML'))
		print("\n\n\n")

	# CREAMOS UN ACCIONADOR PARA BORRAR EL TEXTO Y ESCRIBIR EL NUESTRO
	accionador = webdriver.ActionChains(driver)
	thread1 = Thread(target=lambda: accionador.move_to_element(div_interna).click().perform())
	thread1.start()
	thread1.join()
	time.sleep(1)

	accionador.send_keys(Keys.BACK_SPACE * 100).perform()

	thread2 = Thread(target=lambda: accionador.send_keys(Keys.PAGE_UP).perform())
	thread2.start()
	thread2.join()
	time.sleep(1)

	thread4 = Thread(target=lambda: accionador.send_keys(sql_query).perform())
	thread4.start()
	thread4.join()
	time.sleep(1)

	# USAMOS UN NUEVO ACCIONADOR PORQUE SINO SE PUEDEN SOLAPAR
	boton = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div/form/div[2]/button'))) 
	webdriver.ActionChains(driver).move_to_element(boton).click().perform()

	# PUEDE SER QUE LA WEB NOS DE UNA QUERY RESULTADO O UN MENSAJE DE ERROR

	try:
		div_consulta_resultado = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[2]/div/div[3]/div/div/div[2]/div')))
		consulta_resultado = div_consulta_resultado.text
		if show: print(consulta_resultado)
		driver.close()
		return consulta_resultado
	except:
		mensaje_error = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[2]/div'))).text
		print("\n\n\n{}".format(mensaje_error))
		driver.close()
	
	




# get_statistics_file_from_collection("tweets")
# print(get_count_of_a_collection("tweets"))
def remove_lateral_spaces_and_quotes(string):
    aux = string.strip()
    if aux[0]== '"':
        aux = aux[1:]
    if aux[-1]== '"':
        aux = aux[:-1]
    if aux[-1]== ',':
        aux = aux[:-1]
    return aux






def divide_string_query(string_query,show=False):
	first_index = string_query.index("{")
	last_index = len(string_query) - string_query[::-1].index("}")

	start = string_query[:first_index]
	body = string_query[first_index:last_index]
	end = string_query[last_index:]
	if show:
		print("start = {}\n\n\n".format(start))
		print("body = {}\n\n\n".format(body))
		print("end = {}\n\n\n".format(end))
	return start,body,end

def divide_dicts(string_lined):
    lines = string_lined.split("\n")
    strings_list = []
    #nombres = { 0: '"condition" :', 1: '"key" :'}
    aux = []
    for line in lines:
        for letter_index in range(len(line)):
            aux.append(line[letter_index])
            if line[letter_index] =='}' and letter_index <= 5:
                linea_unida ="".join(aux).strip()
                linea_unida = re.sub(r"(,?\s*{)\s*(.*})",r"\1 \2",linea_unida)
                if linea_unida.startswith(","):
                    linea_unida = linea_unida[1:]
                strings_list.append(linea_unida)
                aux=[]
    return strings_list

def capitalize_reserved_words(query):
	entrada = query.lower()   
	for palabra in ["select","from","where","group","by","having","count","distinct","table","order","sum","avg","true","in","false","is","not","null"]:
		r1 = "\("+palabra
		r2 = "("+palabra.upper()
		entrada = re.sub(",{}".format(palabra),", {}".format(palabra.upper()),entrada)
		entrada = re.sub("={}".format(palabra),"= {}".format(palabra.upper()),entrada)
		entrada = re.sub(r1,r2,entrada)
		entrada = re.sub(" {}".format(palabra)," {}".format(palabra.upper()),entrada)
		entrada = re.sub("{}".format(palabra),"{}".format(palabra.upper()),entrada)
		
	return entrada

def change_operators_and_add_semicolon(sql_query):
	sql_query = sql_query.replace("<>","!=")
	if not sql_query.strip().endswith(";"):
		sql_query+=';'
	return sql_query

def format_mongo_string_query_body(mongo_string_query,show=False):
	# ponemos en una linea cada elemento principal
	body_lined = re.sub("\s\s\s\s\s+}", "}", re.sub("\s\s\s\s\s\s+", "", mongo_string_query)) 
	# ponemos las funciones entre comillas para poder cargar el json (no ponemos el constructor Code(porque fallaría))
	body_lined_with_string_funtions = re.sub(r" (function\(.*\)\s*{.*})", r' "\1"', body_lined)
	if show:
		print("body_lined = {}\n\n\n".format(body_lined_with_string_funtions))
	return body_lined_with_string_funtions
	
def check_multiple_dicts_in_formatted_mongo_query(body_formatted):
	return len(re.findall("\n}",body_formatted))>1

def change_body_to_pymongo_compatible(body_lined_parsed_list,new_dict_list,multiple_dicts):
	for i in range(len(body_lined_parsed_list)):   
		for k,v in body_lined_parsed_list[i].items():
			if k =="distinct":
				new_dict_list[i]["command"] = k
				new_dict_list[i]["collection"] = v
			if type(v) == str and v.startswith("function"):
				new_dict_list[i][k] = Code(v)
			elif type(v) == bool and v:
				new_dict_list[i][k] = 1
			elif type(v) == bool and not v:
				new_dict_list[i][k] = 0
			else:
				new_dict_list[i][k] = v
			if not multiple_dicts:
				if "cond" in new_dict_list[i]:
					new_dict_list[i]["condition"] = new_dict_list[i].get("cond",{})
					del new_dict_list[i]["cond"]
    #AÑADIR IS NOT NULL
	return new_dict_list

def get_python_code_to_execute(start,body,end,multiple_dicts):
	if not multiple_dicts:
		# ponemos ** para que tome las keys del dict como nombre de argumento
		query_linked = "{}**{}{}".format(start,str(body[0]),end) 
	else:
		query_linked = "{}{}{}".format(start,",".join([str(e) for e in body]),end)
	return query_linked

def is_runCommand(code_to_exec_without_assign):
	return "runCommand" in code_to_exec_without_assign

def change_runCommand_to_command(code_to_exec_without_assign):
	code_to_exec_without_assign = code_to_exec_without_assign.replace("runCommand","command")
	code_to_exec_without_assign = re.sub(r"(.*).values.length",r'len(\1["values"])',code_to_exec_without_assign)
	return code_to_exec_without_assign

def replace_reserved_words_with_backslash(code_to_exec_without_assign):
	code_to_exec_without_assign = code_to_exec_without_assign.replace(r"this.\\FALSE","false")
	code_to_exec_without_assign = code_to_exec_without_assign.replace(r"this.\\TRUE","true")
	return code_to_exec_without_assign

def get_result(variable_to_check,is_command):
	if is_command:
		if type(variable_to_check) ==int:
			return [variable_to_check]
		else:
			return variable_to_check['values']
	else:
		print(type(variable_to_check))
		return [e for e in variable_to_check]



def get_final_command_to_execute(entrada):
	entrada = capitalize_reserved_words(entrada)
	entrada = change_operators_and_add_semicolon(entrada)
	print(entrada)

    
    # #not_ins = re.findall("(\w+)\s+IS+\s+NOT\s+NULL")
	sql_sintax_is_correct = check_sql_sintax_in_web(entrada)
	if not sql_sintax_is_correct:
		exit(1)
	
	entrada = escape_true_and_false(entrada)

	mongo_string_query = convert_sql_query_to_mongo_query_in_web(entrada)
	start,body,end = divide_string_query(mongo_string_query)

	mongo_string_query_formatted = format_mongo_string_query_body(body,show=True)
	
	body_has_multiple_dicts = check_multiple_dicts_in_formatted_mongo_query(mongo_string_query_formatted)

	if body_has_multiple_dicts:
		string_dict_list = divide_dicts(mongo_string_query_formatted)
		body_lined_parsed_list = [json.loads(s) for s in string_dict_list]
		new_dict_list = [{} for i in range(len(body_lined_parsed_list))]        
	else:
		body_lined_parsed_list = [json.loads(mongo_string_query_formatted)] 
		new_dict_list = [ {"key":{},"condition":{},"initial":{},"reduce":{}} ]
    
	new_dict_list = change_body_to_pymongo_compatible(body_lined_parsed_list,new_dict_list,body_has_multiple_dicts)


	code_to_exec_without_assign = get_python_code_to_execute(start,new_dict_list,end,body_has_multiple_dicts)

	is_command = is_runCommand(code_to_exec_without_assign)
	if is_command:
		code_to_exec_without_assign = change_runCommand_to_command(code_to_exec_without_assign)
	code_to_exec_without_assign = replace_reserved_words_with_backslash(code_to_exec_without_assign)

	python_command = code_to_exec_without_assign

	return python_command,is_command

# resultado = execute_string_query(query)
# print(resultado)
#   select user.id,count(*) from tweets group by user.id
#   select user.id from tweets where user.id > 10000
#   SELECT user.id,count(*) from tweets where user.verified =false group by user.id 
#   SELECT DISTINCT user.id from tweets
#   SELECT COUNT(DISTINCT user.id) from tweets
# ERROR	SELECT user.id,count(user.id) from tweets where user.verified is not null and user.verified =true group by user.id
# ERROR   SELECT user.id,count(user.id) from tweets where user.verified =true group by user.id
# ERROR   SELECT user.id,count(user.id) from tweets where user.verified =true group by user.id ORDER BY user.id
# ERROR  select id_str, sum(favorite_count.value) from tweets where favorite_count <> 'null' group by id_str having count(*) > 1 
# select id from tweets where id >10000
body_lined = '''{
    "key": {"user.id": true},
    "initial": {"countstar": 0},
    "reduce": function(obj, prev) {if (true != null) if (true instanceof Array) prev.countstar += true.length;else prev.countstar++;}
}'''


query='''db.tweets.find({}, {
    "user.id": 1
}).sort({
    "user.id": 1
});'''

query2 = '''db.tweets.group({
    "key": {
        "user.id": true
    },
    "initial": {
        "countstar": 0
    },
    "reduce": function(obj, prev) {
        if (true != null) if (true instanceof Array) prev.countstar += true.length;
        else prev.countstar++;
    }
});'''

query3='''db.tweets.group({
    "key": {
        "user.id": true
    },
    "initial": {}
});'''

lista_querys= [
	"select user.id,count(*) from tweets group by user.id",
	"select user.id from tweets where user.id > 10000",
	"SELECT user.id,count(*) from tweets where user.verified =false group by user.id",
	"SELECT DISTINCT user.id from tweets",
	"SELECT COUNT(DISTINCT user.id) from tweets",
	"SELECT user.id,count(user.id) from tweets where user.verified is not null and user.verified =true group by user.id",
	"SELECT user.id,count(user.id) from tweets where user.verified =true group by user.id",
	"SELECT user.id,count(user.id) from tweets where user.verified =true group by user.id ORDER BY user.id",
	"select id_str, sum(favorite_count.value) from tweets where favorite_count <> 'null' group by id_str having count(*) > 1",
	"select id from tweets where id >10000"
]


for query in lista_querys:
	try:
		command,runCommand = get_final_command_to_execute(query)
		python_command = "res = "+command
		print(query)
		print(python_command)
		exec(python_command)
		result = get_result(res,runCommand)
		[print(e) for e in result[:5]]
	except Exception as e:
		print("[ERROR] Query = {}".format(query))
		print(e)
	input()