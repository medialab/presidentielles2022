import gzip
import csv
import casanova
from tqdm import tqdm
import glob
import re

'''WARNING: Memory issue when adding tweets' following metadata
- text
- retweet_count
- retweeted_id
- retweeted_user'''

TWEETS = {}

files = []
for file in sorted(glob.glob('presidentielle_*.csv.gz')):
    files.append(file)

for file in files:
    with gzip.open(file, 'rt') as f:
        reader = casanova.reader(csv.reader(l.replace('\0', '') for l in f))

        id_pos = reader.headers.id
        links_pos = reader.headers.links
        user_id_pos = reader.headers.user_id
        screen_name_pos = reader.headers.user_screen_name
        timestamp_pos = reader.headers.timestamp_utc
        text_pos = reader.headers.text
        retweet_count_pos = reader.headers.retweet_count
        retweeted_id_pos = reader.headers.retweeted_id
        retweeted_user_pos = reader.headers.retweeted_user

        start_pattern = r"^presidentielle_"
        candidate = re.sub(start_pattern, "", file)
        candidate = candidate.split(".")[0]

        for row in tqdm(reader, unit=' rows'):
            links = row[links_pos].strip()

            if not links:
                continue

            tweet_id = int(row[id_pos])
            links = row[links_pos]
            user_id = int(row[user_id_pos])
            user_screen_name = row[screen_name_pos].strip()
            timestamp = row[timestamp_pos]
            text = row[text_pos]
            retweet_count = int(row[retweet_count_pos])
            retweeted_id = row[retweeted_id_pos]
            retweeted_user = row[retweeted_user_pos]

            if tweet_id in TWEETS:
                TWEETS[tweet_id][1].append(candidate)
            else:
                TWEETS[tweet_id] = ([
                    tweet_id,
                    links,
                    user_id,
                    user_screen_name,
                    timestamp,
                    text,
                    retweet_count,
                    retweeted_id,
                    retweeted_user
                ], [candidate])

with open('presidentielle_compiled.csv', 'w') as f:
    writer = csv.writer(f)
    #add retweeted_id in exported file
    writer.writerow(['id', 'links', 'user_id', 'user_screen_name', 'timestamp', 'text',
    'retweet_count', 'retweeted_id', 'retweeted_user', 'candidates'])

    for row, candidates in TWEETS.values():
        row.append('|'.join(candidates))
        writer.writerow(row)
