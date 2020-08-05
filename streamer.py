#!/usr/bin/env python3

import atexit
import tweepy
import dataset
from datafreeze import freeze
import sys
import os

class StreamListener(tweepy.StreamListener):
    def __init__(self, api):
        self.lines = 0
        self.api = api
        
    def on_status(self, status):

        if hasattr(status, "retweeted_status"):  # Check if Retweet
            try:
                text = status.retweeted_status.extended_tweet["full_text"]
            except AttributeError:
                text = status.retweeted_status.text
        else:
            try:
                text = status.extended_tweet["full_text"]
            except AttributeError:
                text = status.text

        description = status.user.description
        loc = status.user.location
        name = status.user.screen_name
        user_created = status.user.created_at
        followers = status.user.followers_count
        id_str = status.id_str
        created = status.created_at
        retweets = status.retweet_count
        
        table = db["tweets"]
        table.insert(dict(
            user_description=description,
            user_location=loc,
            text=text,
            user_name=name,
            user_created=user_created,
            user_followers=followers,
            id_str=id_str,
            created=created,
            retweet_count=retweets,
            ))
        
        print("{}\nTweeted by: @{}".format(text,name))
        print('-'*20)
        self.lines += 1
        if self.lines % 100 == 0:
            print('{} tweets collected'.format(self.lines))
        
    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False



if __name__ == '__main__':
    
    args = [term for term in sys.argv]
    
    db_file = args[1]
    to_track = args[2:]
    print('DB name: {}'.format(db_file))
    print('Tracking terms: {}'.format(to_track))
    with open('output/{}_tracked_terms.txt'.format(db_file), 'w') as f:
        for item in to_track:
            f.write('{}\n'.format(item))

    
    db = dataset.connect("sqlite:///output/{}.db".format(db_file))

    def cleanup_db():
        print('EXITING.')
        print('FREEZING DB...')
        result = db['tweets'].all()
        freeze(result, format='csv', filename= 'output/'+db_file+'.csv')
        for root, dirs, files in os.walk(os.getcwd()):
            if 'output/'+db_file+'.csv' in files:
                print('DB FROZEN. {}.csv SAVED'.format())

    
    try: 
        access_token = 
        access_token_secret = 
        consumer_key = 
        consumer_secret = 

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth)

        listener = StreamListener(api)
        stream = tweepy.Stream(auth = api.auth, listener = listener)
        stream.filter(track = to_track)
    except KeyboardInterrupt:
        print('FREEZING DB...')
        result = db['tweets'].all()
        freeze(result, format='csv', filename= 'output/'+db_file+'.csv')
        sys.exit()

    atexit.register(cleanup)
