import tweepy
import json
from pprint import pprint
import sys
from textblob import TextBlob
import pandas as pd
import uuid

def load_json_config(path, printOutput=False):
    with open(path) as f:
        json_config = json.load(f)
        if printOutput == True:
            pprint(json_config)
        return json_config

config = load_json_config('config.json')

auth = tweepy.OAuthHandler( config['consumer_key'], config['consumer_secret'])
auth.set_access_token( config['access_token_key'], config['access_token_secret'])


api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
# pprint( api.verify_credentials())
#public_tweets = api.home_timeline()
search_query = '@realdonaldtrump'
retweet_filter = '-filter:retweets'
query_string = search_query+retweet_filter

# twitter call loop parameters
max_id = -1
# how many you want to limit collection too
maxTweets = 10000
sinceId = None
tweetCount = 0
tweets_per_query = 100
print("downloaded max {0} tweets".format(maxTweets))

df = pd.DataFrame(columns=['id', 'created_at', 'tweet message', 'polarity', 'subjectivity'])

while tweetCount < maxTweets:
    try:
        if (max_id <= 0):
            if (not sinceId):
                new_tweets = api.search(q=query_string, count=tweets_per_query)
            else:
                new_tweets = api.search(q=query_string, count=tweets_per_query,
                                        since_id=sinceId)
        else:
            if (not sinceId):
                new_tweets = api.search(q=query_string, count=tweets_per_query,
                                        max_id=str(max_id - 1))
            else:
                new_tweets = api.search(q=query_string, count=tweets_per_query,
                                        max_id=str(max_id - 1),
                                        since_id=sinceId)
        if not new_tweets:
            print("No more tweets found")
            break

        # loop through each tweet add to export
        for tweet in new_tweets:
            tweet_message = tweet._json['text']
            tweet_id = tweet._json['id']
            created_at = tweet._json['created_at']
            tweet_NLP = TextBlob(tweet_message)

            # id', 'created_at', 'tweet message', 'polarity', 'subjectivity'])
            df = df.append({'id': tweet_id,
                            'created_at': created_at,
                            'tweet message': tweet_message,
                            'polarity': tweet_NLP.sentiment.polarity,
                            'subjectivity': tweet_NLP.sentiment.subjectivity
                            }, ignore_index=True)

        tweetCount += len(new_tweets)
        print("Downloaded {0} tweets".format(tweetCount))
        max_id = new_tweets[-1].id
    except tweepy.TweepError as e:
        # Just exit if any error
        print("some error : " + str(e))
        break


path = '.\export\output_' + str(uuid.uuid4()) + '.csv'
print('exported to ' , path )
df.to_csv(path, encoding='utf-8')