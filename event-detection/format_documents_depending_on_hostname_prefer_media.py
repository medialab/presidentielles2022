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
TOTAL_TWEETS = 7015695

medias = pd.read_csv('https://raw.githubusercontent.com/medialab/corpora/master/polarisation/medias.csv')
categories_sorted = ['Mainstream Media', 'Opinion Journalism', 'Counter-Informational Space', 'Periphery', None]


media_domains = dict()
for i, row in medias.iterrows():
    if row['wheel_category'] in categories_sorted:
        for prefix in row['prefixes'].split('|'):
            media_domains[get_domain_name(prefix)] = row['wheel_category']

for unwanted_domain in [None, 'facebook.com','twitter.com', 'youtube.com', 'dailymotion.com']:
    media_domains.pop(unwanted_domain)


def choose_tweet_text(url, domain, title, description):
    
    return domain == 'bfmtv.com' and 'en-direct' in url \
    or domain == "francebleu.fr" and "en-direct" in url \
    or domain == 'lemonde.fr' and '/live/' in url \
    or domain == 'ouest-france.fr' and 'direct-presidentielle' in url \
    or domain == 'tf1info.fr' and 'direct' in url \
    or domain == 'dna.fr' and '/elections/' in url \
    or domain == 'leparisien.fr' and 'les-infos-a-retenir' in url \
    or domain == 'spotify.com' \
    or url == 'https://www.tf1info.fr/' \
    or url == 'https://www.mesopinions.com/fr' \
    or url == 'https://odysee.com' \
    or url == 'https://www.challenges.fr/' \
    or domain == 'amzn.to' \
    or domain == 'amazon.fr' \
    or domain == 'lanouvellerepublique.fr' \
    or domain == '7sur7.be' \
    or description[:51] == "Profitez des vidéos et de la musique que vous aimez" \
    or title == "Bloomberg"


def one_url_per_tweet_preferably_mainstream_media(source_file, output_file):
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            tweets = json.load(f)
            return tweets

    tweets = {}
    with open(source_file, 'r') as f:
        reader = casanova.reader(csv.reader(l.replace('\0', '') for l in f))
        
        id_pos = reader.headers.id
        links_pos = reader.headers.links
        url_pos = reader.headers.canonical_url
        
        for row in tqdm(reader, total=TOTAL_TWEETS):
            if row[url_pos]:
                url = row[url_pos]
            else:
                url = row[links_pos]
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
    added_fields = ["tweet_text", "page_title", "page_description", "page_date", "selected_url", "selected_domain"]
    with open(source_file, 'r') as f,\
    open(output_file, 'w') as of:
        enricher = casanova.enricher(csv.reader(l.replace('\0', '') for l in f), of, keep=[
            "id", 
            "timestamp_utc", 
            "user_id",
            "links",
            "domains",
            "text"
        ], add=added_fields)
        
        id_pos = enricher.headers.id
        links_pos = enricher.headers.links
        url_pos = enricher.headers.canonical_url
            
        title_pos = enricher.headers.title
        description_pos = enricher.headers.description
        date_pos = enricher.headers.date
        
        text_pos = enricher.headers.text
        
        written = set()

        for row in tqdm(enricher, total=TOTAL_TWEETS):
            if row[url_pos]:
                url = row[url_pos]
            else:
                url = row[links_pos]
                        
            if url:
                if tweets[row[id_pos]] == url:
                    if row[id_pos] in written:
                        continue
                    domain = get_domain_name(url)
                    
                    add_list = [
                        # tweet_text
                        row[text_pos],
                        # page_title
                        row[title_pos],
                        # page_description
                        row[description_pos],
                        # page_date
                        row[date_pos],
                        # selected_url
                        url,
                        # selected_domain
                        domain
                    ]
                    
                    if (row[title_pos] or row[description_pos]) and not choose_tweet_text(url, domain, row[text_pos], row[description_pos]):
                        row[text_pos] = row[title_pos] + ". " + row[description_pos]

                    enricher.writerow(row, add=add_list)
                    written.add(row[id_pos])

            else:
                add_list = [
                    # tweet_text
                    row[text_pos]
                ] + ["" for i in range(len(added_fields) - 1)]
                enricher.writerow(row, add=add_list)

if __name__ == '__main__':
    tweets_file = sys.argv[1]
    assert os.path.exists(tweets_file)

    urls_file = sys.argv[2]
    assert urls_file.endswith('.json')

    formated_file = sys.argv[3]
    assert formated_file.endswith('.csv')

    write_formated_dataset(tweets_file, urls_file, formated_file)
