from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
import  urllib.request 
from threading import Thread
from lxml.html import parse
import time
try: 
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup
import datetime

driver = None

def open_twitter_and_login():
	driver = webdriver.Chrome(ChromeDriverManager().install())
	driver.get("https://twitter.com/login")
	accionador = webdriver.ActionChains(driver)

	username_text_field = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[class='js-username-field email-input js-initial-focus']")))
	password_text_field = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[class='js-password-field']")))
	
	username_text_field.send_keys("")
	driver.implicitly_wait(1)
	password_text_field.send_keys("")
	driver.implicitly_wait(1)
	login_button  = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[class='submit EdgeButton EdgeButton--primary EdgeButtom--medium']")))
	accionador.move_to_element(login_button).click().perform()
	return driver	


def get_last_users_who_liked_a_tweet(screen_name, tweet_id,driver):
	url = 'https://twitter.com/' + screen_name + '/status/' + tweet_id
	try:
		#chrome_options = Options()  
		#chrome_options.add_argument("--headless")

		driver.get(url)	
	except Exception as e:
		print("[get_twitter_user_rts_and_favs ERROR] Error conecting")
		print(e)
		exit(1)
	
	try:
		boton_likes = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "li[class='js-stat-count js-stat-favorites stat-count']")))
		num_likes = boton_likes.text
		num_likes = num_likes.split()[0]
		boton_likes.click()
		users_who_liked_section = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "ol[class='activity-popup-users dropdown-threshold']")))
		divs_ultimos_likes = users_who_liked_section.find_elements_by_css_selector("div[class='account  js-actionable-user js-profile-popup-actionable ']")
		
		result_list = []
		for div in divs_ultimos_likes:
			user_id_str = div.get_attribute('data-user-id')
			user_name = div.get_attribute('data-name')
			user_nickname = div.get_attribute('data-screen-name')
			result_list.append((user_id_str,user_name,user_nickname))
			# aqui podría capturar la bio del usuario

		for e in range(len(result_list)):
			print("{} {}".format(e+1,result_list[e]))
	except Exception as e:
		print("[get_twitter_user_rts_and_favs ERROR] There was an error")
		print(e.__cause__)
	
	return num_likes,result_list


def get_tweets_of_a_user_until(screen_name,driver,date_limit=False,tweet_id_limit=False,num_messages_limit=False,show=False):

	url = 'https://twitter.com/' + screen_name
	
	if date_limit != False:
		if type(date_limit) == str:
			try:
				local_date_limit = datetime.datetime.strptime(date_limit, "%d/%m/%Y")
				date_limit_epoch = int(local_date_limit.strftime('%s'))
			except:
				raise Exception("[GET TWEETS OF A USR UNTIL ERROR] Error setting date ( date format= 'dd/mm/YYYY)")
		elif isinstance(date_limit, datetime.datetime):
			date_limit_epoch = int(date_limit.strftime('%s'))
		elif type(date_limit) == int:
			date_limit_epoch = date_limit
		else:
			raise Exception("date_limit has to be int, datetime.datetime or str ( date format= 'dd/mm/YYYY)")

		date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(date_limit_epoch))
		print("[DATE LIMIT STABLISHED] {}".format(date))

	if tweet_id_limit != False:
		tipo = type(tweet_id_limit)
		if tipo == int:
			tweet_id_limit = str(tweet_id_limit)
		check_tweet_id = 'data-tweet-id="{}"'.format(tweet_id_limit)
		print("[TWEET_ID LIMIT STABLISHED] {}".format(check_tweet_id))
	if num_messages_limit != False:
		if type(num_messages_limit) != int:
			raise Exception("[GET TWEETS OF A USR UNTIL ERROR] num_messages_limit should be an int")
	

	driver.get(url)
	fin = False
	while not fin:
		height = driver.execute_script("return document.documentElement.scrollHeight")
		driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
		time.sleep(0.7)
		# recuperamos por los que tengan ambas clases ya que hay elementos que no nos interesan solo con una de ellas
		elements = driver.find_elements(By.CSS_SELECTOR,"div.original-tweet.tweet")

		if date_limit !=False:
			time_of_last_tweet = elements[-1].find_elements_by_css_selector("span[class*='_timestamp js-short-timestamp ']")[0]
			datatime = int(time_of_last_tweet.get_attribute("data-time"))
			date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(datatime))
			if datatime <= date_limit_epoch:
				fin = True
		if num_messages_limit != False:
			fin = len(elements)>=num_messages_limit
		if tweet_id_limit != False:
			html_with_messages = driver.find_element(By.CLASS_NAME,"stream").get_attribute('innerHTML')
			if check_tweet_id in html_with_messages:
				fin = True
		if height == driver.execute_script("return document.documentElement.scrollHeight"):
			print("Bottom REACHED")
			#TODO wait two reaches to exit
			fin=True
			
	
	# obtenemos los tweets y retweets
	lista_tweets = []
	lista_retweets = []
	for i in range(len(elements)):
		e = elements[i]

		#print(e.get_attribute("innerHTML"))
		try:
			tweet_id = e.get_attribute("data-tweet-id")
			creator_name = e.get_attribute("data-name")
			creator_screen_name = e.get_attribute("data-screen-name")
			#retweet info
			retweeted_tweet_id = e.get_attribute("data-retweet-id")
			retwitter_screen_name = e.get_attribute("data-retweeter")
			#creation info
			time_section = e.find_elements_by_css_selector("span[class*='_timestamp js-short-timestamp ']")[0]
			datatime = time_section.get_attribute("data-time")
			date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(datatime)))
		except Exception as e:
			print("[ERROR GETTING INFO OF A TWEET OF USER]")
		if date_limit != False:
			if int(datatime) < date_limit_epoch:
				print("break\n\n")
				break
		if tweet_id_limit !=False:
			if tweet_id <= tweet_id_limit:
				print("break\n\n")
				break
		if num_messages_limit != False and i >= num_messages_limit:
			print("break\n\n")
			break 


		if retweeted_tweet_id!= None:
			lista_retweets.append(tweet_id)
		else:
			lista_tweets.append(tweet_id)

		if show:
			print("\n"*3)
			print("tweet_id = {}".format(tweet_id))
			print("nombre_creador = {}".format(creator_name))
			print("nickname_creador = {}".format(creator_screen_name))
			print("fecha creacion = {}".format(date))
			if retweeted_tweet_id!= None:
				print("tipo = retweet")
				print("retweeted_tweet_id = {}".format(retweeted_tweet_id))
				print("usuario que retuiteó = {}".format(retwitter_screen_name))
			else:
				print("tipo = tweet")
		# if date_limit != False:
		# 	print("fecha creacion = {}".format(date))
		#input()

	#input()
	return (lista_tweets,lista_retweets)



#returns list(retweet users),list(favorite users) for a given screen_name and status_id
def get_twitter_user_rts_and_favs_v1(screen_name, status_id,driver):


	url = urllib.request.urlopen('https://twitter.com/' + screen_name + '/status/' + status_id)
	root = parse(url).getroot()

	num_rts = 0
	num_favs = 0
	rt_users = []
	fav_users = []

	for ul in root.find_class('stats'):
		for li in ul.cssselect('li'):

			cls_name = li.attrib['class']

			if cls_name.find('retweet') >= 0:
				num_rts = int(li.cssselect('a')[0].attrib['data-tweet-stat-count'])

			elif cls_name.find('favorit') >= 0:
				num_favs = int(li.cssselect('a')[0].attrib['data-tweet-stat-count'])

			elif cls_name.find('avatar') >= 0 or cls_name.find('face-pile') >= 0:#else face-plant

				for users in li.cssselect('a'):
                    #apparently, favs are listed before retweets, but the retweet summary's listed before the fav summary
                    #if in doubt you can take the difference of returned uids here with retweet uids from the official api
					[print(e) for e in users]
					input()
					if num_favs > 0:#num_rt > 0:
                        #num_rts -= 1
						num_favs -= 1
						#rt_users.append(users.attrib['data-user-id'])
						# 
						user_id = users.attrib.get('data-user-id',None)
						fav_users.append(user_id)
					else:                        
                        #fav_users.append(users.attrib['data-user-id'])
						rt_users.append(users.attrib['data-user-id'])

	return rt_users, fav_users




if __name__ == '__main__':
	driver = open_twitter_and_login()
	#print(get_last_users_who_liked_a_tweet('Albert_Rivera', '1100705346291683328'))
	
	#PRUEBA COGER TODOS LOS MENSAJES DE UN PERFIL
	#get_tweets_of_a_user_until("Albert_Rivera")
	#PRUEBA COGER TODOS LOS MENSAJES DE UN PERFIL HASTA UN TWEET ID
	
	limited_list = get_tweets_of_a_user_until("Albert_Rivera",tweet_id_limit=1100127150420754438,driver=driver)
	print("Limited list len = {}\n".format(len(limited_list[0])+len(limited_list[1])))

	limited_list = get_tweets_of_a_user_until("Albert_Rivera",tweet_id_limit="1100127150420754438",driver=driver)
	print("Limited list len = {}\n".format(len(limited_list[0])+len(limited_list[1])))

	limited_list = get_tweets_of_a_user_until("Albert_Rivera",date_limit=1551125726,driver=driver)
	print("Limited list len = {}".format(len(limited_list[0])+len(limited_list[1])))

	limited_list = get_tweets_of_a_user_until("Albert_Rivera",date_limit="25/02/2019",driver=driver)
	print("Limited list len = {}".format(len(limited_list[0])+len(limited_list[1])))

	limited_list = get_tweets_of_a_user_until("Albert_Rivera",date_limit=datetime.datetime(2019,2,25,0,0,0),driver=driver)
	print("Limited list len = {}".format(len(limited_list[0])+len(limited_list[1])))

	limited_list = get_tweets_of_a_user_until("Albert_Rivera",num_messages_limit=10,driver=driver)
	print("Limited list len = {}".format(len(limited_list[0])+len(limited_list[1])))

	print(limited_list)

	driver.close()
