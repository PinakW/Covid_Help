#https://docs.tweepy.org/en/v3.4.0/streaming_how_to.html
#https://developer.twitter.com/en/docs/twitter-api/v1/tweets/filter-realtime/guides/basic-stream-parameters
#https://www.dataquest.io/blog/streaming-data-python/
#https://www.bmc.com/blogs/track-tweets-location/
#https://geojson.io/#map=10/18.5532/73.6070	
from tweepy import Stream
from tweepy.auth import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
import sys
import csv
import os
import signal
from urllib3.exceptions import ProtocolError
#import pandas as pd

tweet_sequence=["Pune COVID Corona verified","Pune COVID verified", "Pune verified Oxygen", "Pune verified bed", "Pune verified ICU", "Pune verified Plasma", "Pune verified blood","Pune verified Remdesivir"]
ckey=''#Consumer Key
csecret=''#Consumer Secret
atoken=''#Access token
asecret=''#Access Secret

tweet_count = 0
child_pid = 0

class listener(StreamListener):
    def on_status(self, status):
        global tweet_count
        global child_pid
        child_pid = os.getpid()
        tweet_count =tweet_count + 1
        description = status.user.description
        loc = status.user.location
        #text = status.full_text
        coords = status.coordinates
        #geo = status.geo
        name = status.user.screen_name
        #id_str = status.id_str
        created = status.created_at
        #retweets = status.retweet_count

        if hasattr(status, "retweeted_status"):  # Check if Retweet
            print("Retweet received")
            try:
                text = status.retweeted_status.extended_tweet["full_text"]
                with open("FilteredOutput_retweets.csv", 'a', newline='') as csvfile:
                    writerrtwt = csv.writer(csvfile) 
                    writerrtwt.writerow([str(text),str(name),str(created),str(coords),str(loc)])
            except AttributeError:
                text = status.retweeted_status.text
                with open("FilteredOutput_retweets.csv", 'a', newline='') as csvfile:
                    writerrtwt = csv.writer(csvfile)
                    writerrtwt.writerow([str(text),str(name),str(created),str(coords),str(loc)])
        else:
            print("Tweet received")
            try:
                text = (status.extended_tweet["full_text"])
                with open("FilteredOutput.csv", 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([str(text),str(name),str(created),str(coords),str(loc)])
            except AttributeError:
                text = (status.text)
                with open("FilteredOutput.csv", 'a', newline='') as csvfile:
                    writer= csv.writer(csvfile)
                    writer.writerow([str(text),str(name),str(created),str(coords),str(loc)])

        
    def on_error(self, status_code):
        if status_code == 420:
            print("Seems like Connection was idle for too long. \tDisconnecting....\nPlease re-run this code using\npython Tweepy_Verified.py")
            return False

with open("FilteredOutput_retweets.csv", 'w', newline='') as csvfile:
    writerrtwt = csv.writer(csvfile)
    writerrtwt.writerow(["Tweet Details","Tweeted by","Time of tweet","Tweet coordinates","Tweet Location"])
 
with open("FilteredOutput.csv", 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["Tweet Details","Tweeted by","Time of tweet","Tweet coordinates","Tweet Location"])
 
auth = OAuthHandler(ckey,csecret)
auth.set_access_token(atoken,asecret)
twitterStream = Stream(auth,listener())
#twitterStream.filter(track=tweet_sequence, is_async=True)
flag = 0
while True:
    try:
        if flag==0:
            twitterStream.filter(track=tweet_sequence, is_async=True)
            flag=1
        if((tweet_count == 200) and (flag == 1)):
            print("Tweet Count is " + str(tweet_count) + "\nChild Process id: " + str(child_pid))
            flag=flag+1
            os.system("mutt -s \"Tweet Extractor script has capped at 200 tweets and halted!!\" -a FilteredOutput.csv -a FilteredOutput_retweets.csv -- example@something.com < template.txt")
            os.kill(child_pid, signal.SIGTERM)#signal.SIGKILL)
            sys.exit("You may want to consider saving and clearing csv files and then restarting the code. Exitting...\n")
    except (ValueError, ProtocolError):
        print("Value Error occured, continuing")
        flag=0
        continue
