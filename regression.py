from tweepy import Stream
from tweepy.auth import OAuthHandler
from tweepy.streaming import StreamListener
import csv
import signal
from time import sleep
from threading import Thread
import os
import onedrivesdk

Place = ["Delhi","Noida","Gurgaon","Gaziabad"]
Resource = ["bed"]#["bed","oxygen","medicine","remdecivir","ventilator","concentrator","icu","plasma","blood","food"]


child_pid = 0
class listener(StreamListener):
    def on_status(self, status):
#        global tweet_count
        global child_pid
        child_pid = os.getpid()
#        tweet_count =tweet_count + 1
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
                with open("Retweets_Delhi.csv", 'a', newline='') as csvfile:
                    writerrtwt = csv.writer(csvfile)
                    writerrtwt.writerow([str(text),str(name),str(created),str(coords),str(loc)])
            except AttributeError:
                text = status.retweeted_status.text
                with open("Retweets_Delhi.csv", 'a', newline='') as csvfile:
                    writerrtwt = csv.writer(csvfile)
                    writerrtwt.writerow([str(text),str(name),str(created),str(coords),str(loc)])
        else:
            print("Tweet received")
            try:
                text = (status.extended_tweet["full_text"])
                with open("Tweets_Delhi.csv", 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([str(text),str(name),str(created),str(coords),str(loc)])
            except AttributeError:
                text = (status.text)
                with open("Tweets_Delhi.csv", 'a', newline='') as csvfile:
                    writer= csv.writer(csvfile)
                    writer.writerow([str(text),str(name),str(created),str(coords),str(loc)])


    def on_error(self, status_code):
        if status_code == 420:
            print("Seems like Connection was idle for too long. \tDisconnecting....\nPlease re-run this code using\npython Tweepy_Verified.py")
            return False


def func(place, resource):
    csv = "/fac/proj_apps/pinakw/Help_for_Delhi/Tweets_Delhi.csv"
    command = "python3 query.py -l " + resource + " -c " + csv + " -p " + place
    os.system(command)
    csv = "/fac/proj_apps/pinakw/Help_for_Delhi/Retweets_Delhi.csv"
    command = "python3 query.py -l " + resource + " -c " + csv + " -p " + place
    os.system(command)


class StreamTweepy:
    def __init__(self):
        self._running = True
        self.ckey=''#Consumer Key
        self.csecret=''#Consumer Secret
        self.atoken=''#Access token
        self.asecret=''#Access Secret
        self.tweet_sequence=["Delhi COVID Corona verified","Delhi COVID verified", "Delhi verified Oxygen", "Delhi verified bed", "Delhi verified ICU", "Delhi verified Plasma", "Delhi verified blood","Delhi verified Remdesivir","Noida COVID Corona verified","Noida COVID verified", "Noida verified Oxygen", "Noida verified bed", "Noida verified ICU", "Noida verified Plasma", "Noida verified blood","Noida verified Remdesivir","Gurgaon COVID Corona verified","Gurgaon COVID verified", "Gurgaon verified Oxygen", "Gurgaon verified bed", "Gurgaon verified ICU", "Gurgaon verified Plasma", "Gurgaon verified blood","Gurgaon verified Remdesivir","Gaziabad COVID Corona verified","Gaziabad COVID verified", "Gaziabad verified Oxygen", "Gaziabad verified bed", "Gaziabad verified ICU", "Gaziabad verified Plasma", "Gaziabad verified blood","Gaziabad verified Remdesivir"]
    def terminate(self):
        self._running = False
        os.system("mutt -s \"Tweet Extractor script has executed for 10 minutes and halted!!\" -a Tweets_Delhi.csv -a Retweets_Delhi.csv -- pinakw@cadence.com,pshirur@cadence.com,joshih@cadence.com < template.txt")        
#    def initialize(self):
#        with open("Retweets_Delhi.csv", 'w', newline='') as csvfile:
#            writerrtwt = csv.writer(csvfile)
#            writerrtwt.writerow(["Tweet Details","Tweeted by","Time","Tweet coordinates","Tweet Location"])
#        with open("Tweets_Delhi.csv", 'w', newline='') as csvfile:
#            writer = csv.writer(csvfile)
#            writer.writerow(["Tweet Details","Tweeted by","Time","Tweet coordinates","Tweet Location"])
#        auth = OAuthHandler(ckey,csecret)
#        auth.set_access_token(atoken,asecret)
#        twitterStream = Stream(auth,listener())
    def StartStream(self):
        global child_pid
        with open("Retweets_Delhi.csv", 'w', newline='') as csvfile:
            writerrtwt = csv.writer(csvfile)
            writerrtwt.writerow(["Tweet Details","Tweeted by","Time","Tweet coordinates","Tweet Location"])
        with open("Tweets_Delhi.csv", 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Tweet Details","Tweeted by","Time","Tweet coordinates","Tweet Location"])
        auth = OAuthHandler(self.ckey,self.csecret)
        auth.set_access_token(self.atoken,self.asecret)
        twitterStream = Stream(auth,listener())

        flag = 0
        while(self._running==True):
            try:
                if(flag==0):
                    twitterStream.filter(track=self.tweet_sequence, is_async=True)
                    flag=1
            except:
                print("Received an exception, Continuing...")
                flag=0
                continue
        twitterStream.disconnect()
#        os.kill(child_pid, signal.SIGTERM)#signal.SIGKILL)
#        sys.exit("You may want to consider saving and clearing csv files and then restarting the code. Exitting...\n")

interval=10
strm = StreamTweepy()
while True:

    try:
        t=Thread(target = strm.StartStream)
        t.start()
        sleep(interval)
        strm.terminate()
        t.join()
        print("Streaming Converged without errors\n")
    except:
        print("Exception encountered in main issue\n\n")
    


    for p in Place:
        for r in Resource:
            func(p,r)
            print("Executed for " + p + " and " + r)
        print("")
    print("\n")

    print("copying necessary files....")
    command = "scp -r pinakw@tenappai-exe01:/fac/proj_apps/pinakw/Help_for_Delhi/Delhi pinakw@noi02nb01:/home/pinakw/Delhi"
    print(command)
