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
						fav_users.append((user_id,user_name,user_nickname))
					else:                        
                        #fav_users.append(users.attrib['data-user-id'])
						rt_users.append(users.attrib['data-user-id'])

	return rt_users, fav_users


#example
if __name__ == '__main__':
	driver = open_twitter_and_login()
	print(get_last_users_who_liked_a_tweet('Albert_Rivera', '1100705346291683328'))
	input()
	driver.close()
