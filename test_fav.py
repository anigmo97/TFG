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


def get_last_users_who_liked_a_tweet(screen_name, tweet_id):
	url = 'https://twitter.com/' + screen_name + '/status/' + tweet_id
	try:
		#chrome_options = Options()  
		#chrome_options.add_argument("--headless")

		driver.get(url)	
	except Exception as e:
		print("[get_twitter_user_rts_and_favs ERROR] Error conecting")
		print(e)
		exit(1)
	
	boton_likes = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "li[class='js-stat-count js-stat-favorites stat-count']")))
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
	
	return result_list


def get_tweets_of_a_user_until(screen_name):
	date = "2/2/2019"
	url = 'https://twitter.com/' + screen_name
	date_limit = datetime.datetime.strptime(date, "%d/%m/%Y")
	date_limit_epoch = date_limit.strftime('%s')
	print(date_limit)
	print(date_limit_epoch)
	driver.get(url)
	# los retweets entan en divs y los tweets en li
	#reteets <span class="_timestamp js-short-timestamp"
	#<span class="_timestamp js-short-timestamp " data-aria-label-part="last" data-time="1551546578" data-time-ms="1551546578000" data-long-form="true">Mar 2</span>
	fin = False
	height = driver.execute_script("return document.documentElement.scrollHeight")
	while not fin:
		driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
		time.sleep(0.5)
		#elements = driver.find_elements(By.CLASS_NAME,"tweet")
		elements = driver.find_elements(By.CSS_SELECTOR,"div.original-tweet.tweet") #recuperamos por los que tengan ambas clases para eliminar un elemento al final molesto
		# print(len(elements))
		# for e in elements:
		# 	print(e.get_attribute("class"))
		# 	input()
		# input()
		#times = elements[-1].find_elements_by_css_selector("small[class='time']")
		# for e in times:
		# 	print(e.get_attribute("innerHTML"))
		# 	input()
		time_of_last_tweet = elements[-1].find_elements_by_css_selector("span[class*='_timestamp js-short-timestamp ']")[0]
		datatime = time_of_last_tweet.get_attribute("data-time")
		date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(datatime)))
		print(date)
		if datatime <= date_limit_epoch:
			fin=True
		if height == driver.execute_script("return document.documentElement.scrollHeight"):
			fin=True
	
	# obtenemos los tweets y retweets
	lista_tweets = []
	lista_retweets = []
	for e in elements:

		print(e.get_attribute("innerHTML"))
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

		print("\n"*3)
		print("tweet_id = {}".format(tweet_id))
		print("nombre_creador = {}".format(creator_name))
		print("nickname_creador = {}".format(creator_screen_name))

		print("fecha creacion = {}".format(date))
		if retweeted_tweet_id!= None:
			print("tipo = retweet")
			print("retweeted_tweet_id = {}".format(retweeted_tweet_id))
			print("usuario que retuiteó = {}".format(retwitter_screen_name))
			lista_retweets.append(tweet_id)
		else:
			print("tipo = tweet")
			lista_tweets.append(tweet_id)
		# hacer comprobación para que no se pasen el límite
		input()

	input()



#returns list(retweet users),list(favorite users) for a given screen_name and status_id
def get_twitter_user_rts_and_favs_v1(screen_name, status_id):
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


#example
if __name__ == '__main__':
	driver = open_twitter_and_login()
	#print(get_last_users_who_liked_a_tweet('Albert_Rivera', '1100705346291683328'))
	get_tweets_of_a_user_until("Albert_Rivera")
	driver.close()
