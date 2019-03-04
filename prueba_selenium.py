from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import sqlparse
import time
from threading import Thread


def parse_sql_query(sql_query):

	url = "https://www.eversql.com/sql-syntax-check-validator/"

	try:
		driver = webdriver.Chrome(ChromeDriverManager().install())
		driver.get(url)
		
	except Exception as e:
		print("[PARSE SQL QUERY ERROR] Error conecting")
		print(e)
		exit(1)

	
	driver.maximize_window()

	editor = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="sql-editor"]/div/div[6]/div[1]/div/div/div/div[5]')))

	editor.click()
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
		
	

def convert_sql_query_to_mongo_query(sql_query):

	if not sql_query.strip().endswith(";"):
		sql_query+=';'

	if not parse_sql_query(sql_query):
		exit(1)


	url = "http://www.querymongo.com/"
	driver = webdriver.Chrome(ChromeDriverManager().install())
	driver.get(url)
	driver.maximize_window()


	#div_interna = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div/form/div[1]/div[3]/div/div/div[2]/div')))
	#div_interna = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[1]/form/div[1]/div[3]/div')))
	div_interna = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[1]/form/div[1]/div[3]')))

	div_interna.click()
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
	# thread3 = Thread(target=lambda: accionador.move_to_element(div_interna).click().perform())
	# thread3.start()
	# thread3.join()
	# time.sleep(1)

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
		return consulta_resultado
	except:
		mensaje_error = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[2]/div[2]/div'))).text
		print("\n\n\n{}".format(mensaje_error))
	
	driver.close()

#resultado = convert_sql_query_to_mongo_query("SELECT user.id from tweets Group by user.id")

#print(sqlparse.format("SELECT id from demo WHERE group by x docdsgs "))


# QUERYS COMPROBADAS
# "SELECT user.id,COUNT(*) from tweets  group by user.id"
