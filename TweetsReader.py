#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import tweepy
import json
import socket
import csv


# In[ ]:


from tweepy import OAuthHandler, Stream
from tweepy.streaming import StreamListener


# In[ ]:


consumer_key = 'FqPJl9fUzVCsonHx8ujMvPYG3'
consumer_secret = 'fb6qB3JjF53qUfFvqgd8L5W5VgdUTcV9G3d9XlAIxHkEhIStEg'
access_token = '887703454667350016-yo1bWgRxndMvGulEK8UauBLx9BLo1AP'
access_secret = 'es0IgkB2fnEo90ec5s6rbc20eAPZt0rs06AmI1WXMrPdv'


# In[ ]:


class TweetListener(StreamListener):
    def __init__(self,csocket):
        self.client_socket = csocket
    
    def on_data(self,data):
        try:
            msg = json.loads(data)
            tweet_text = str(msg['text'].encode('utf-8'))
            clean_text = msg['text']
            print("TWEET TEXT : " + clean_text)
            print ("------------------------------------------")
            tweet_screen_name = "SN:"+msg['user']['screen_name']
            print("USER NAME IS : " + tweet_screen_name)
            print ("------------------------------------------")
            self.client_socket.send((str(msg['text']) + "\n").encode('utf-8'))
            with open('tweets.csv','a') as x:
                writer = csv.writer(x)
                writer.writerow([tweet_text])
            with open('tweets.json','a') as y:
                y.write(data)
            with open('tweets.txt','a') as t:
                t.write(clean_text + "\n")
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
    
    def on_error(self,status):
        print(status)
        return True


# In[ ]:


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    twitter_stream = Stream(auth, TweetListener(c_socket))
    twitter_stream.filter(track=['music'])


# In[ ]:


if __name__ == '__main__':
    s = socket.socket()
    host = '127.0.0.1'
    port = 5555
    s.bind((host,port))
    
    print('Listening on port 5555')
    
    s.listen(10)
    c,addr = s.accept()
    
    sendData(c)


# In[ ]:




