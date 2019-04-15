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
import pyperclip #pip3 install pyperclip   if error -> sudo apt install xsel
try: 
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup
import datetime

driver = None


#####################################################################################################
############################## TWITTER DRIVER INIZIALIZATION ########################################
#####################################################################################################
def open_twitter_and_login():
	""" Open a chrome window and sign in on twitter"""
	driver = webdriver.Chrome(ChromeDriverManager().install())
	
	i=0
	error=True
	while i< 5 and error:
		try:
			driver.get("https://twitter.com/login")
			accionador = webdriver.ActionChains(driver)

			username_text_field = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[class='js-username-field email-input js-initial-focus']")))
			password_text_field = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[class='js-password-field']")))
			error=False
			i+=1
		except:
			error = True

	username_text_field.send_keys("")
	driver.implicitly_wait(1)
	password_text_field.send_keys("")
	driver.implicitly_wait(1)
	login_button  = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[class='submit EdgeButton EdgeButton--primary EdgeButtom--medium']")))
	accionador.move_to_element(login_button).click().perform()
	#driver.minimize_window()
	return driver	

#####################################################################################################
################ AUXILIAR METHODS (NEEDS A DRIVER IN THE CORRECT URL) ###############################
#####################################################################################################
def look_into_likes_list(driver):
	"""Auxiliar method used for others\n
		gets a driver that has to be in tweet url already\n
		and click in likes secction to get last users who liked list
	"""
	error=False
	result_dict = {}
	num_likes = 0
	time.sleep(0.2)
	try:
		boton_likes = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "li[class='js-stat-count js-stat-favorites stat-count']")))
		num_likes = boton_likes.text
		num_likes = num_likes.split()[0]
		boton_likes.click()
		users_who_liked_section = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "ol[class='activity-popup-users dropdown-threshold']")))
		divs_ultimos_likes = users_who_liked_section.find_elements_by_css_selector("div[class='account  js-actionable-user js-profile-popup-actionable ']")
		
	except Exception as e:
		error = True
		print("[get_twitter_user_rts_and_favs ERROR] There was an error scraping the webpage: {}".format(driver.current_url))
		print(e.__cause__)

	if not error:
		try:
			for div in divs_ultimos_likes:
				user_id_str = div.get_attribute('data-user-id')
				user_name = div.get_attribute('data-name')
				user_nickname = div.get_attribute('data-screen-name')
				result_dict[user_id_str]=(user_id_str,user_name,user_nickname)
				# aqui podría capturar la bio del usuario

			# for e,v in range(enumerate(len(result_dict.values()))):
			# 	print("{} {}".format(e+1,v))
		except Exception as e:
			print("[get_twitter_user_rts_and_favs ERROR] There was an error looking div_likes")
			print(e.__cause__)
		return num_likes,result_dict
	else:
		return 0,{}
	

def print_cursor_position(driver):
	y = driver.execute_script('return window.pageYOffset;')
	x = driver.execute_script('return window.pageXOffset;')
	print("( {} , {} )".format(x,y))


def close_favs_section(driver):
	accionador = webdriver.ActionChains(driver)
	time.sleep(0.2)
	boton_cerrar = driver.find_element(By.CSS_SELECTOR,".modal-btn.modal-close.js-close")
	accionador.click(boton_cerrar).perform()
	# print("likes_closed")
	# input()

def close_tweet(driver):
	accionador = webdriver.ActionChains(driver)
	time.sleep(0.2)
	boton_cerrar = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR,"#permalink-overlay > div.PermalinkProfile-dismiss.modal-close-fixed > span")))
	accionador.click(boton_cerrar).perform()
	# print("tweet_closed")
	# input()


#####################################################################################################
################ MAIN METHODS (NEEDS A DRIVER WITH TWITTER INITIALIZED)  ############################
#####################################################################################################

def get_last_users_who_liked_a_tweet(screen_name, tweet_id,driver):
	""" 
		Return actual num of likes of a tweet and a dict with the last 25 users who liked a tweet
		The key is the user screen_name
		The value has the form (id,name,nickname)
	"""

	url = 'https://twitter.com/' + screen_name + '/status/' + tweet_id
	num_likes = 0
	error = False
	try:
		#chrome_options = Options()  
		#chrome_options.add_argument("--headless")

		driver.get(url)	
	except Exception as e:
		print("[get_twitter_user_rts_and_favs ERROR] Error conecting")
		print(e)
		exit(1)
	

	return look_into_likes_list(driver)



def get_tweets_of_a_user_until(screen_name,driver,date_limit=False,tweet_id_limit=False,num_messages_limit=False,show=False):
	""" Returns a tuple with tweets and retweets of a user until an specified limit """

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
				print("[get_tweets_of_a_user_until] date limit break\n\n")
				break
		if tweet_id_limit !=False:
			if tweet_id <= tweet_id_limit:
				print("[get_tweets_of_a_user_until] tweet_id_limit break\n\n")
				break
		if num_messages_limit != False and i >= num_messages_limit:
			print("[get_tweets_of_a_user_until] break\n\n")
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



def get_embed_html_of_a_tweet(screen_name, tweet_id,driver):
	""" Given an tweet_url return a tuple with html code whith embed tweet with and without media"""
	url = 'https://twitter.com/' + screen_name + '/status/' + tweet_id
	print("[URL] {}".format(url))
	error = False
	try:
		driver.get(url)	
	except Exception as e:
		print("[get_twitter_user_rts_and_favs ERROR] Error conecting")
		print(e)
		exit(1)
	
	try:
		div_more = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div[class='IconContainer js-tooltip']")))
		div_more.click()
		print("[GET EMBED HTML] MORE dropdown button clicked")
		embed_tweet_li = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "li[class='embed-link js-actionEmbedTweet']")))
		embed_tweet_li.click()
		print("[GET EMBED HTML] MORE embed button clicked")

	except Exception as e:
		print("[get_twitter_user_rts_and_favs ERROR] There was an error ")
		print(e.__cause__)
		print("[get_twitter_user_rts_and_favs ERROR] {} embed won't be get (possibly was deleted fro Twitter)".format(tweet_id))
		error = True

	if (not error):
		try:
			media_check_box = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div[class='embed-include-card']")))
			has_media = True
		except Exception as e:
			has_media = False
			print("[GET EMBED HTML] Tweet_url = {} does not contain media".format(url))
		
		try:
			embed_html_text_element = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div[class='embed-destination-wrapper']")))
			#The element with the html for embed a tweet is not accesible so it's trickery time
			accionador = webdriver.ActionChains(driver)
			accionador.double_click(embed_html_text_element).perform()
			time.sleep(1)
			accionador.send_keys(Keys.CONTROL , Keys.INSERT).perform() # do the same of control c


			if has_media:
				embed_with_media = pyperclip.paste()
				embed_without_media =None
				media_check_box.click()
				time.sleep(1)
				while(embed_without_media==None or embed_with_media==embed_without_media):
					accionador.double_click(embed_html_text_element).perform()
					accionador.send_keys(Keys.CONTROL , Keys.INSERT).perform()				
					embed_without_media = pyperclip.paste()
				
			else:
				embed_without_media = pyperclip.paste()
				embed_with_media = None


			# print("embed_with_media {}".format(embed_with_media))

			# print("embed_without_media {}".format(embed_without_media))
		except Exception as e:
			print("[get_twitter_user_rts_and_favs ERROR] There was an error copying to the clipboard")
			print(e.__cause__)

		if embed_with_media != None:
			embed_with_media = embed_with_media.replace("\"","'")
		if embed_without_media != None:
			embed_without_media = embed_without_media.replace("\"","'")

		return embed_with_media,embed_without_media
	return None,None



#returns list(retweet users),list(favorite users) for a given screen_name and status_id
def get_twitter_user_rts_and_favs_v1(screen_name, status_id,driver):
	"""Returns a tuple with last users who retweeted and liked"""

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
					# input()
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



def get_last_users_who_like_last_n_tweets_of_user(user_screen_name,num_tweets,driver):
	"""
	Given a user_screename and a num_tweets.\n 
	Returns a dict with the last n tweets_ids of this user as keys\n
	The values of this dict will be a tuple formed by :
		- Current total likes num of this tweet
		- Dict[user_screen_name] -> (id,name,nickname)
	"""

	url = 'https://twitter.com/' + user_screen_name
	#driver.execute_script("document.body.style.zoom='20%'")
	driver.get(url)
	accionador = webdriver.ActionChains(driver)
	fin = False
	while not fin:
		height = driver.execute_script("return document.documentElement.scrollHeight")
		driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
		
		stream_items_ol = driver.find_element_by_id("stream-items-id")
		stream_items = stream_items_ol.find_elements_by_css_selector(".js-stream-item.stream-item.stream-item")

		#stop scrolling and looping
		if len(stream_items)>num_tweets:
			fin = True
		elif height == driver.execute_script("return document.documentElement.scrollHeight"):
			print("Bottom REACHED")
			#TODO wait two reaches to exit
			fin=True
	time.sleep(1)

	stream_items_ol = driver.find_element_by_id("stream-items-id")
	stream_items = stream_items_ol.find_elements_by_css_selector(".js-stream-item.stream-item.stream-item")
	WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR,"#stream-items-id:first-child")))
	final_dict={}
	for e in stream_items[0:num_tweets]:
		tweet_id = e.get_attribute("data-item-id")
		container_id = e.get_attribute("id")
		try:
			WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR,"#{} > div > div.content > div.js-tweet-text-container > p".format(container_id))))
			texto = e.find_element(By.CSS_SELECTOR,"#{} > div > div.content > div.js-tweet-text-container".format(container_id))
			print(tweet_id)
			print(texto.text)
			texto.click()
		except:
			print("ERROR clicking on tweet {}".format(tweet_id))

		try:
			boton_likes = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "li[class='js-stat-count js-stat-favorites stat-count']")))
			num_likes,resut_dict = look_into_likes_list(driver)
			if num_likes!= 0 :
				final_dict[tweet_id] = (num_likes,resut_dict)
			close_favs_section(driver)
			close_tweet(driver)
		except:
			if driver.current_url != url:
				driver.get(url)
			
	return final_dict
		
	




if __name__ == '__main__':
	driver = open_twitter_and_login()
	#get_embed_html_of_a_tweet(screen_name="Albert_Rivera",tweet_id="1100127150420754438",driver=driver)
	time_init = datetime.datetime.now()
	get_last_users_who_like_last_n_tweets_of_user("Albert_Rivera",10,driver)
	print(datetime.datetime.now()-time_init)
	driver.close()
