#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init('/home/santosh311/spark-3.0.1-bin-hadoop2.7')


# In[2]:


import pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import desc
from pyspark.sql import Window


# In[3]:


sc = SparkContext().getOrCreate()
spark = SparkSession(sc)


# In[4]:


ssc = StreamingContext(sc,10)
sqlContext = SQLContext(sc)


# In[5]:


socket_stream = ssc.socketTextStream("127.0.0.1", 5555)


# In[6]:


line = socket_stream.window(10)
print(line)


# In[7]:


from collections import namedtuple
fields = ("tag", "count")
Tweet = namedtuple('Tweet', fields)
print(Tweet)


# In[8]:


(line.flatMap(lambda text: text.split(" ")) 
.filter(lambda word: word.lower().startswith("#")) 
.map(lambda word: (word.lower(), 1))
.reduceByKey(lambda a,b: a+b)
.map(lambda rec: Tweet(rec[0], rec[1]))
.foreachRDD(lambda rdd: rdd.toDF().sort(desc("count"))
.limit(5).registerTempTable("tweets") ) )
print(line)


# In[9]:


ssc.start()


# In[10]:


ssc.stop()


# In[11]:


import json
import re
import operator 
from textblob import TextBlob
from collections import Counter
import os, sys, codecs
from nltk import bigrams
import pandas as pd


# In[12]:


sc.stop()


# In[13]:


sc1 = SparkContext().getOrCreate()
spark1 = SparkSession(sc1)


# In[14]:


lines = sc1.textFile('/home/santosh311/tweets.txt').flatMap( lambda text: text.split( " " ) ) .filter( lambda word: word.lower().startswith("#") )
hashCounts = lines.map(lambda word: (word,1)).reduceByKey(lambda a,b:a+b)


# In[16]:


rdd_df = hashCounts.toDF(["Hashtag","Count"])
hash_df = rdd_df.toPandas()


# In[17]:


hash_df.sort_values(by=['Count'], ascending=False, inplace=True)
hash_df.reset_index(drop=True, inplace=True)
hash_df


# In[18]:


import matplotlib.pyplot as plt
import random
get_ipython().run_line_magic('matplotlib', 'inline')
hash_df.head(10)


# In[19]:


fig = plt.figure(figsize=(3,5))
ax = fig.add_axes([0,0,1,1])
hash_label = hash_df['Hashtag'][:10]
hashcount_label = hash_df['Count'][:10]
plt.title("Hashtag-Count Graph", fontsize=25)
plt.xlabel("Count", fontsize=20)
plt.ylabel("Hashtag", fontsize=20)
ax.barh(hash_label, hashcount_label, height=0.7, color=['r','b','g','orange','yellow','teal','indigo','maroon','gray','navy'])
plt.show()


# In[20]:


fname = '/home/santosh311/tweets.json'


# In[21]:


with open(fname, 'r') as f:
    lis=[]
    n=0
    net=0
    p=0
    count_all = Counter()
    count=0
    for line in f:
        tweet = json.loads(line)
        blob = TextBlob(tweet["text"])
        count+=1
        lis.append(blob.sentiment.polarity)
        if (blob.sentiment.polarity < 0):
            n+=1
        elif (blob.sentiment.polarity == 0) and (blob.sentiment.subjectivity == 0):
            net+=1
        elif (blob.sentiment.polarity > 0):
            p+=1


# In[22]:


print("Total tweets",len(lis))
print("Positive tweets : ",p)
print("Negative tweets : ",n)
print("Neutral tweets : ",net)


# In[23]:


labels = ['Positive','Negative','Neutral']
json_data = [p,n,net]


# In[24]:


fig,ax = plt.subplots()
ax.pie(json_data,labels=labels, autopct='%1.1f%%', colors=['#33ff88','#ffaa00','#0075ff'], textprops={'fontsize':14})
ax.axis('equal')
plt.show()


# In[25]:


import csv


# In[26]:


csvfile = '/home/santosh311/tweets.csv'


# In[27]:


pos_count = 0
neg_count = 0
neut_count = 0
tweet_count = 0


# In[28]:


with open(csvfile, 'r') as myfile:
    rows = csv.reader(myfile)
    for row in rows:
        sentence = row[0]
        blob = TextBlob(sentence)
        if(blob.sentiment.polarity>0.0):
            pos_count+=1
        elif(blob.sentiment.polarity<0.0):
            neg_count+=1
        elif(blob.sentiment.polarity==0.0) and (blob.sentiment.subjectivity == 0):
            neut_count+=1
        tweet_count+=1


# In[29]:


print("Total no of tweets : ",tweet_count)
print("Positive tweets : ",pos_count)
print("Negative tweets : ",neg_count)
print("Neutral tweets : ",neut_count)


# In[30]:


labels = ['Positive','Negative','Neutral']
csv_data = [pos_count, neg_count, neut_count]


# In[31]:


fig,ax = plt.subplots()
ax.pie(csv_data,labels=labels, autopct='%1.1f%%', colors=['#33ff88','#ffaa00','#0075ff'], textprops={'fontsize':14})
ax.axis('equal')
plt.show()


# In[ ]:





# In[ ]:




