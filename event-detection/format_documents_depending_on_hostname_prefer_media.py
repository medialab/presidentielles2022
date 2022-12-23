#!/usr/bin/env python
# coding: utf-8

"""
Write a csv file containing id,timestamp,text for each tweet.
The text is composed of the title and the description of the 
page referenced by the url in the tweet.

If the url points to a "live" ('en-direct', 
'les-infos-a-retenir', etc.), the text is composed of the 
text of the tweet instead.

If the tweet appears several times in the source_file (i.e. 
the tweet contains several urls), only one urls is selected
(The order of preference is listed in `categories_sorted`)
and the tweet is written only once.
"""

import os
import csv
import sys
import json
import casanova
import pandas as pd
from tqdm import tqdm
from ural import get_domain_name


csv.field_size_limit(sys.maxsize)
TOTAL_TWEETS = 1187326

medias = pd.read_csv('https://raw.githubusercontent.com/medialab/corpora/master/polarisation/medias.csv')
categories_sorted = ['Mainstream Media', 'Opinion Journalism', 'Counter-Informational Space', 'Periphery', None]


media_domains = dict()
for i, row in medias.iterrows():
    if row['wheel_category'] in categories_sorted:
        for prefix in row['prefixes'].split('|'):
            media_domains[get_domain_name(prefix)] = row['wheel_category']

for unwanted_domain in [None, 'facebook.com','twitter.com', 'youtube.com', 'dailymotion.com']:
    media_domains.pop(unwanted_domain)


def is_live_stream(url, domain):
    
    return domain == 'bfmtv.com' and 'en-direct' in url \
    or domain == 'lemonde.fr' and '/live/' in url \
    or domain == 'ouest-france.fr' and 'direct-presidentielle' in url \
    or domain == 'tf1info.fr' and 'direct' in url \
    or domain == 'dna.fr' and '/elections/' in url \
    or domain == 'leparisien.fr' and 'les-infos-a-retenir' in url


def one_url_per_tweet_preferably_mainstream_media(source_file, output_file):
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            tweets = json.load(f)
            return tweets

    tweets = {}
    with open(source_file, 'r') as f:
        reader = casanova.reader(f)
        
        id_pos = reader.headers.id
        url_pos = reader.headers.url
        
        for row in tqdm(reader, total=TOTAL_TWEETS):
            url = row[url_pos]
            if row[id_pos] not in tweets:
                tweets[row[id_pos]] = url
            else:
                # If there are several urls for one tweet id, keep one that is a media url with the highest rank in category order
                domain = get_domain_name(tweets[row[id_pos]])
                challenger = get_domain_name(url)
                if challenger in media_domains:
                    if categories_sorted.index(media_domains[challenger]) < categories_sorted.index(media_domains.get(domain, None)):
                        tweets[row[id_pos]] = url
    
    with open(output_file, 'w') as f:
        json.dump(tweets, f)
    
    return tweets


def write_formated_dataset(source_file, urls_file, output_file):
    tweets = one_url_per_tweet_preferably_mainstream_media(source_file, urls_file)

    with open(source_file, 'r') as f,\
    open(output_file, 'w') as of:
        enricher = casanova.enricher(f, of, keep=['id', 'timestamp', 'text'])
        
        id_pos = enricher.headers.id
        url_pos = enricher.headers.url
        title_pos = enricher.headers.title
        description_pos = enricher.headers.description
        raw_pos = enricher.headers.raw_content
        tweet_text_pos = enricher.headers.text
        for row in tqdm(enricher, total=TOTAL_TWEETS):
            url = row[url_pos]
            if tweets[row[id_pos]] == url:
                domain = get_domain_name(url)
                    
                if not is_live_stream(url, domain):
                    row[tweet_text_pos] = row[title_pos] + ' ' + row[description_pos]
                    
                enricher.writerow(row)


if __name__ == '__main__':
    tweets_file = sys.argv[1]
    assert os.path.exists(tweets_file)

    urls_file = sys.argv[2]
    assert urls_file.endswith('.json')

    formated_file = sys.argv[3]
    assert formated_file.endswith('.csv')

    write_formated_dataset(tweets_file, urls_file, formated_file)
