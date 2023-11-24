#!/usr/bin/env python
# coding: utf-8

"""
Compute some aggregated stats about events:
- nb of tweets
- nb of words
- start date
- end date
- day with max nb of tweets
- top 5 words (according to chi2 metric)
# - top 5 users (idem)
# - top 5 media (idem)
# - first 5 users (in time)
- media shared in the event as urls
- media tweeting in the event
- French MPs tweeting in the event
- the text of the most retweeted tweet for the event 
- the user name of the person who wrote this tweet 
- the id of this tweet
- the text of the most retweeted tweet considering only the first 10% of the tweets of this event
- the user name of the person who wrote this tweet 
- the id of this tweet

"""

import os
import csv
import sys
import pytz
import casanova
from datetime import datetime
from tqdm import tqdm
from ural import get_domain_name
from ural.lru import LRUTrie
from collections import defaultdict, Counter
import re
import math

csv.field_size_limit(sys.maxsize)
TOTAL_TWEETS = 6896842
tz = pytz.timezone("Europe/Paris")

categories_sorted = ['Mainstream Media', 'Opinion Journalism', 'Counter-Informational Space', 'Periphery', None]

media_domains = LRUTrie()
media_index = {}
inverted_media_index = {}

media_twitter_accounts = set()

with casanova.reader('https://raw.githubusercontent.com/medialab/corpora/master/polarisation/medias.csv') as reader:
    for i, row in enumerate(reader):
        if row[reader.headers.wheel_category] in categories_sorted:
            name = row[reader.headers.name]
            media_index[name] = i
            inverted_media_index[i] = name
            for prefix in row[reader.headers.prefixes].split('|'):
                domain = get_domain_name(prefix)
                if domain == "twitter.com":
                    twitter_account = prefix.split("/")[-1].lower()
                    media_twitter_accounts.add(twitter_account)
                elif domain not in [None, 'facebook.com', 'youtube.com', 'dailymotion.com']:
                    media_domains.set(prefix, name)

with casanova.reader('https://raw.githubusercontent.com/regardscitoyens/twitter-parlementaires/master/data/deputes.csv') as reader:
    mp_ids = dict()
    for i, row in enumerate(reader):
        mp_ids[int(row[reader.headers.twitter_id])] = row[reader.headers.nom]

user_index = {}

def word_ngrams(tokens, stop_words=None, vocab=None, ngram_range=(1, 1)):
    """Turn tokens into a sequence of n-grams after stop words filtering.
    Source: https://github.com/scikit-learn/scikit-learn/blob/7db5b6a98ac6ad0976a3364966e214926ca8098a/sklearn/feature_extraction/text.py#L245
    """
    # handle stop words
    if stop_words is not None:
        tokens = [w for w in tokens if w not in stop_words]
    # handle vocab
    elif vocab is not None:
        tokens = [w for w in tokens if w in vocab]

    # handle token n-grams
    min_n, max_n = ngram_range
    if max_n != 1:
        original_tokens = tokens
        if min_n == 1:
            # no need to do any slicing for unigrams
            # just iterate through the original tokens
            tokens = list(original_tokens)
            min_n += 1
        else:
            tokens = []

        n_original_tokens = len(original_tokens)

        # bind method outside of loop to reduce overhead
        tokens_append = tokens.append
        space_join = " ".join

        range_ngrams = range(min_n, min(max_n + 1, n_original_tokens + 1))

        if vocab is not None:
            for n in range_ngrams:
                for i in range(n_original_tokens - n + 1):
                    w = space_join(original_tokens[i : i + n])
                    if w in vocab:
                        tokens_append(w)
        else:
            for n in range_ngrams:
                for i in range(n_original_tokens - n + 1):
                    tokens_append(space_join(original_tokens[i : i + n]))
    return tokens

def get_top_k_chi_squares(event_count, event_frequency, frequency, n, k):
    scores = []
    words = []

    for w, w_count in event_frequency.items():
        indep = event_count*frequency[w]/n
        chi_square = (w_count-indep)**2/indep

        if len(scores) < k:
            scores.append(chi_square)
            words.append(w)
        else:
            min_chi_square = min(scores)
            if chi_square > min_chi_square :
                index = scores.index(min_chi_square)
                scores[index] = chi_square
                words[index] = w
    return [word for score, word in sorted(zip(scores, words), reverse=True)]


def event_stats(source_file, vocab_file, outfile, format_thread_id, min_nb_docs=10):
    with casanova.reader(vocab_file) as reader:

        vocab = set(t for t in reader.cells("token"))
    
    events_stats = defaultdict(
            lambda: {
            "nb_words": 0,
            "nb_docs": 0,
            "nb_hashtags": 0,
            "max_tweets_per_day": 1,
            "nb_tweets_current_day": 0,
            "tf": defaultdict(int),
            "media": dict(),
            "tweets_by_media": set(),
            "mps": set(),
            "hashtags": Counter(),
            "id_most_retweeted":"",
            "tweet_text_most_retweeted": "",
            "user_most_retweeted":"",
            "retweet_count_most_retweeted":0,
            "id_trigger":"",
            "tweet_text_trigger":"",
            "user_trigger":""
    })
    with open(source_file, 'r') as f:

        reader = casanova.reader(f)

        text_pos = reader.headers.text
        event_pos = reader.headers.thread_id
        url_pos = reader.headers.selected_url
        date_pos = reader.headers.timestamp_utc
        user_id_pos = reader.headers.user_id
        user_name_pos = reader.headers.user_screen_name
        retweet_count=reader.headers.retweet_count
        tweet_text=reader.headers.tweet_text
        tweet_id=reader.headers.id

        term_frequency = Counter()
        hashtag_frequency = Counter()
        
        n = 0
        n_hashtags = 0
        token_pattern = re.compile(r'[a-z]+')
        hashtag_pattern = re.compile(r'#(\w+)')

        for row in tqdm(reader, total=TOTAL_TWEETS):
            event_id = format_thread_id(row[event_pos])

            current_date = int(row[date_pos])
            current_day = datetime.fromtimestamp(current_date, tz=tz).date()

            if event_id not in events_stats:
                stats = events_stats[event_id]
                stats["start_date"] = current_date
                stats["max_day"] = current_day
                previous_day = current_day
            else:
                stats = events_stats[event_id]
                previous_day =  datetime.fromtimestamp(stats["end_date"], tz=tz).date()

            stats["end_date"] = current_date

            stats["nb_docs"] += 1

            user_id = int(row[user_id_pos])
            if user_id in mp_ids:
                stats["mps"].add(user_id)

            user_screen_name = row[user_name_pos].lower()
            if user_screen_name in media_twitter_accounts:
                stats["tweets_by_media"].add(user_screen_name)

            if current_day != previous_day:
                previous_day = current_day
                stats["nb_tweets_current_day"] = 1
            else:
                stats["nb_tweets_current_day"] += 1

            if stats["nb_tweets_current_day"] > stats["max_tweets_per_day"]:
                stats["max_tweets_per_day"] = stats["nb_tweets_current_day"]
                stats["max_day"] = current_day

            media = media_domains.match(row[url_pos])
            if media:
                media = media_index[media]
                stats["media"][media] = None

            for token in word_ngrams(token_pattern.findall(row[text_pos].lower()), vocab=vocab, ngram_range=(1, 2)):
                stats["tf"][token] += 1
                term_frequency[token] += 1
                stats["nb_words"] += 1
                n += 1

            for hashtag in hashtag_pattern.findall(row[text_pos].lower()):
                stats["hashtags"][hashtag] += 1
                hashtag_frequency[hashtag] += 1
                stats["nb_hashtags"] += 1
                n_hashtags += 1

            if stats["retweet_count_most_retweeted"] < int(row[retweet_count]):
                stats["retweet_count_most_retweeted"]=int(row[retweet_count])
                stats["tweet_text_most_retweeted"]=row[tweet_text]
                stats["user_most_retweeted"]=row[user_name_pos]
                stats["id_most_retweeted"]=row[tweet_id]

    with open(source_file, 'r') as f:
        event_stats_ten_percent = defaultdict(
        lambda: {
        "ten_percent": 0,
        "count": 0,
        "tweet_text_most_retweeted": "",
        "user_most_retweeted":"",
        "retweet_count_most_retweeted":0,
        "id_most_retweeted":""
        })

        reader = casanova.reader(f)

        for row in reader :
            event_pos = reader.headers.thread_id
            user_name_pos = reader.headers.user_screen_name
            retweet_count=reader.headers.retweet_count
            tweet_text=reader.headers.tweet_text
            event_id = format_thread_id(row[event_pos])

            if event_id not in event_stats_ten_percent:
                stats = event_stats_ten_percent[event_id]
                stats["ten_percent"]=math.ceil(0.1*events_stats[event_id]["nb_docs"])
            else:
                stats = event_stats_ten_percent[event_id]
            
            if stats["count"] < stats["ten_percent"] :
                stats["count"]+=1
                if stats["retweet_count_most_retweeted"] < int(row[retweet_count]):
                    stats["retweet_count_most_retweeted"]=int(row[retweet_count])
                    stats["tweet_text_most_retweeted"]=row[tweet_text]
                    stats["user_most_retweeted"]=row[user_name_pos]
                    stats["id_most_retweeted"]=row[tweet_id]
            elif stats["count"] <= stats["ten_percent"] :
                events_stats[event_id]["tweet_text_trigger"]=stats["tweet_text_most_retweeted"]
                events_stats[event_id]["user_trigger"]=stats["user_most_retweeted"]
                events_stats[event_id]["id_trigger"]=stats["id_most_retweeted"]




        with open(outfile, "w") as of:
            writer = csv.writer(of)
            writer.writerow(["thread_id", "nb_docs", "nb_words", "top_chi_square_words", "top_chi_square_hashtags", \
                             "top_hashtags", "media_urls", "tweets_by_media",\
                             "start_date", "end_date", "max_docs_date", "MPs", "text_tweet_most_retweeted","user_most_retweeted",\
                                "id_most_retweeted","tweet_text_trigger","user_trigger",'id_trigger'])
            total = len(events_stats)
            for event, stats in tqdm(events_stats.items(), total=total):
                nb_docs = stats["nb_docs"]
                all_media = [inverted_media_index[index] for index in stats["media"]]
                if nb_docs >= min_nb_docs:
                    nb_words = stats["nb_words"]
                    nb_hashtags = stats["nb_hashtags"]
                    top_5_chi = get_top_k_chi_squares(nb_words, stats["tf"], term_frequency, n, 5)
                    top_5_chi_hashtags = get_top_k_chi_squares(nb_hashtags, stats["hashtags"], hashtag_frequency, n_hashtags, 5)
                    top_5_hashtags = [h[0] for h in stats["hashtags"].most_common(5)]
                    writer.writerow(
                        [
                            event,
                            nb_docs,
                            nb_words,
                            "|".join(top_5_chi),
                            "|".join(top_5_chi_hashtags),
                            "|".join(top_5_hashtags),
                            "|".join(all_media),
                            "|".join(stats["tweets_by_media"]),
                            datetime.fromtimestamp(stats["start_date"], tz=tz).date(),
                            datetime.fromtimestamp(stats["end_date"], tz=tz).date(),
                            stats["max_day"],
                            "|".join([mp_ids[user_id] for user_id in stats["mps"]]),
                            stats["tweet_text_most_retweeted"],
                            stats["user_most_retweeted"],
                            stats["id_most_retweeted"],
                            stats["tweet_text_trigger"],
                            stats["user_trigger"],
                            stats["id_trigger"],

                        ]
                    )

if __name__ == '__main__':
    in_file = sys.argv[1]
    assert os.path.exists(in_file)

    vocab_file = sys.argv[2]
    assert os.path.exists(vocab_file)

    formated_file = sys.argv[3]
    assert formated_file.endswith('.csv')

    format_thread_id = int if len(sys.argv) == 5 else str

    event_stats(in_file, vocab_file, formated_file, format_thread_id)
