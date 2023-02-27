#!/usr/bin/env python
# coding: utf-8

"""
Keep in memory a list of all events listed by events_stats.py .

Keep in memory a list of all original tweet ids belonging to these events.

Go through the complete tweet export files (all retweets), and write a file
that contains both original ids and retweets, associated with their event id.
"""

import os
import csv
import sys
import glob
import gzip
import casanova
import pandas as pd
from tqdm import tqdm


mp = pd.read_csv('https://raw.githubusercontent.com/regardscitoyens/twitter-parlementaires/master/data/deputes.csv')
mp_ids = set(mp.twitter_id.unique())

def get_file_path(words, event_id, output_folder, no_RT=True):

    event_id = str(event_id)
    prefix = event_id[:3]
    for i in range(2,4):
        if len(prefix) < i:
            prefix += "0"
    subfolder = os.path.join(output_folder, prefix)
    os.makedirs(subfolder, exist_ok=True)

    file_path = os.path.join(subfolder, event_id + "_" + words.replace(" ", "_"))
    
    if no_RT:
        return file_path + "_no_RT.csv"
    return file_path + ".csv"

def write_tweet_in_event_specific_file(words, event_id, output_folder, row, headers, positions, no_RT=True, retweeted_id=""):
    
    file_path = get_file_path(words, event_id, output_folder, no_RT=no_RT)
    write_list = [row[pos] for pos in positions]
    write_list.append("1" if int(row[positions[-1]]) in mp_ids else "")

    if not no_RT:
        write_list.append(retweeted_id)
        
    if not os.path.exists(file_path):
        with open(file_path, "w") as of:
            writer = csv.writer(of)
            writer.writerow(headers)
            writer.writerow(write_list)
    else:
        with open(file_path, "a") as of:
            writer = csv.writer(of)
            writer.writerow(write_list)

def write_tweets(event_file, stats_file, tweets_files_path, outputfile, total):
    events = {}
    with open(stats_file, "r") as f:
        reader = casanova.reader(f)
        thread_id_pos = reader.headers.thread_id
        words_pos = reader.headers.top_5_words

        for row in reader:
            events[int(row[thread_id_pos])] = row[words_pos].split("|")[0]
    # events = {1: "presidential election", 2: "hurricane ida", 3: "happy halloween"}

    tweets = {}
    with open(event_file, 'r') as f:

        reader = casanova.reader(f)
        id_pos = reader.headers.id
        thread_id_pos = reader.headers.thread_id
        # page_title_pos = reader.headers.page_title
        # page_description_pos = reader.headers.page_description
        # tweet_text_pos = reader.headers.tweet_text
        # time_pos = reader.headers.formatted_date
        # text_pos = reader.headers.text
        # user_pos = reader.headers.user_screen_name
        user_id_pos = reader.headers.user_id

        # headers = [
        #         "id", 
        #         "text", 
        #         "page_title", 
        #         "page_description", 
        #         "tweet_text", 
        #         "local_time", 
        #         "user_screen_name", 
        #         "user_id", 
        #         "is_MP"
        #         ]
        # positions = [id_pos, text_pos, page_title_pos, page_description_pos, tweet_text_pos, time_pos, user_pos, user_id_pos]

        for row in tqdm(reader):
            event_id = int(row[thread_id_pos])
            words = events.get(event_id)
            if words:
                tweets[int(row[id_pos])] = event_id
                #write_tweet_in_event_specific_file(words, event_id, output_folder, row, headers, positions)

    pbar = tqdm(total=int(total))

    for file in glob.glob(tweets_files_path):
        # with gzip.open(file, mode="rt") as f:
        print(file)
        with open(file, "r") as f, open(outputfile, "a") as of:
            enricher = casanova.enricher(f, of, ignore_null_bytes=True, 
                                         keep=["id", "text", "local_time", "user_screen_name", "user_id", "retweeted_id"],
                                         add=["thread_id", "is_MP"]
                                         )
            id_pos = enricher.headers.id
            retweeted_id_pos = enricher.headers.retweeted_id
            user_id_pos = enricher.headers.user_id


            for row in enricher:
                event_id = tweets.get(int(row[id_pos]))

                if event_id:
                    enricher.writerow(row, [str(event_id), "1" if int(row[user_id_pos]) in mp_ids else ""])
                    #write_tweet_in_event_specific_file(words, event_id, output_folder, row, new_headers, positions, no_RT=False)
                else:

                    retweeted_id = row[retweeted_id_pos]
                    if retweeted_id:
                        retweeted_id = int(retweeted_id)
                        event_id = tweets.get(retweeted_id)
                        if event_id:
                            enricher.writerow(row, [str(event_id), "1" if int(row[user_id_pos]) in mp_ids else ""])
                            #write_tweet_in_event_specific_file(words, event_id, output_folder, row, new_headers, positions, no_RT=False, retweeted_id=retweeted_id)

                pbar.update(1)
    pbar.close()

                    

        

if __name__ == '__main__':
    event_file = sys.argv[1]
    assert os.path.exists(event_file)

    stats_file = sys.argv[2]
    assert os.path.exists(stats_file)

    tweets_files = sys.argv[3]

    output_file = sys.argv[4]
    dir = os.path.dirname(output_file)
    assert os.path.isdir(dir)

    total = sys.argv[5]

    write_tweets(event_file, stats_file, tweets_files, output_file, total)
