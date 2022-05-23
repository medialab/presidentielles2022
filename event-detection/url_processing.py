import gzip
import csv
import casanova
from tqdm import tqdm
from collections import Counter
import re

URLS = {}

with open('presidentielle_compiled.csv', 'rt') as f, open('urls_processed.csv', 'w') as fi:
    reader = casanova.reader(csv.reader(l.replace('\0', '') for l in f))
    writer = csv.writer(fi)

    links_pos = reader.headers.links
    candidates_pos = reader.headers.candidates
    timestamp_pos = reader.headers.timestamp
    text_pos = reader.headers.text
    retweet_count_pos = reader.headers.retweet_count
    retweeted_id_pos = reader.headers.retweeted_id
    retweeted_user_pos = reader.headers.retweeted_user
    share_count = Counter()

    for row in tqdm(reader, unit=' rows'):
        urls = row[links_pos].split("|")
        candidates = row[candidates_pos]
        timestamp = row[timestamp_pos]
        text = row[text_pos]
        retweet_count = int(row[retweet_count_pos])
        retweeted_id = row[retweeted_id_pos]
        retweeted_user = row[retweeted_user_pos]

        for url in urls:
            share_count[url] +=1

            URLS[url] = (
                [
                    url,
                    timestamp,
                    text,
                    retweet_count,
                    retweeted_id,
                    retweeted_user
                ],
                set(candidates.split('|'))
            )

    writer.writerow(['url', 'timestamp', 'text', 'retweet_count', 'retweeted_id',
    'retweeted_user', 'share_count', 'candidates'])

    for row, candidates in URLS.values():
        row.append(share_count[row[0]])
        row.append(candidates)
        writer.writerow(row)
