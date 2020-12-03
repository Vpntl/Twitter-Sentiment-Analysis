import urllib3
#from twisted.internet.protocol import Factory, Protocol
import mysql.connector
from mysql.connector import Error
import tweepy
import json
from dateutil import parser
import time
import os
from http.client import IncompleteRead






//Your twitter application credentials
consumer_key = "***************************"
consumer_secret = "************************************************"
access_token = "*************************************************"
access_token_secret = "************************************************"

password = ""


def connect(username, created_at, tweet):
	
	"""
	connect to MySQL database and insert twitter data
	"""
	
	try:
		con = mysql.connector.connect(host = 'localhost',
		database='twitterdb', user='root', password = 'vaibhav09', charset = 'utf8')
		
		if con.is_connected():
			"""
			Insert twitter data
			"""
			cursor = con.cursor()
			
			
			query = "INSERT INTO tweets (username, created_at, tweet) VALUES (%s, %s, %s)"
			cursor.execute(query, (username, created_at, tweet))
			con.commit()
			
			
	except Error as e:
		print(e)
	
	
	
		

	cursor.close()
	con.close()

	return


# Tweepy class to access Twitter API
class Streamlistener(tweepy.StreamListener):
	

	def on_connect(self):
		print("You are connected to the Twitter API")


	def on_error(self):
		if status_code != 200:  #200 status code means everything worked 
			print("error found")
		
			return False  #disconnect from stream

	"""
	This method reads in tweet data as Json
	and extracts the data we want.
	"""
	def on_data(self,data):
	
	
		
        
		try:
			raw_data = json.loads(data)
			

			if ('text' in raw_data or 'full_text' in raw_data) :
				 
				
				username = raw_data['user']['screen_name']
				created_at = parser.parse(raw_data['created_at'])
				
				try:
					tweet= raw_data.extended_tweet['full_text']
				except AttributeError as e:
					tweet= raw_data['text']
				
				
				
				
				#tweet = raw_data['text']
				

				#insert into MySQL database
				connect(username, created_at, tweet)
				print("Tweet colleted at: {} ".format(str(created_at)))
		except Error as e:
			print(e)
			
		
			


if __name__== '__main__':
	


	# authentication so we can access twitter
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api =tweepy.API(auth, wait_on_rate_limit=True)

	#instance of Streamlistener
	listener = Streamlistener(api = api)
	stream = tweepy.Stream(auth, listener = listener,tweet_mode='extended')
	
	track = ['Narendra Modi', 'BJP', 'Amit Shah', 'National Democratic Alliance','JP Nadda', 'PMO India', 'Yogi Adityanath', 'Nirmala Sitharaman', 'Arun Jaithley', 'Centre Government of India', 'Sambit Patra', 'Piyush Goyal']   #For party in power at the centre 
	#track=['Rahul Gandhi', 'Sonia Gandhi', 'Congress', 'INC', 'United Progressive Alliance', 'Shashi Tharoor', 'Randeep Singh Sujrewala', 'Adhir Ranjan Chowdhury', 'P Chidambaram', 'Sachin Pilot', 'Alka Lamba', 'Pawan Kehra']  #For opposition  
	
	while True:
		try:
			stream.filter(track = track,languages=['en'])
		except IncompleteRead:
			continue
			
		except urllib3.exceptions.ProtocolError:
			continue
			
	

	
