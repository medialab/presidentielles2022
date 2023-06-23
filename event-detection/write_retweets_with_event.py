#!/usr/bin/env python
# coding: utf-8

"""
Keep in memory a list of all events listed by events_stats.py .

Keep in memory a list of all original tweet ids belonging to these events.

Go through the complete tweet export files (all retweets), and write a file
that contains both original ids and retweets, associated with their event id.
"""

import os
import sys
import glob
import casanova
from tqdm import tqdm

from extract_entities import EntityExtractor

extractor = EntityExtractor()
is_mp = extractor.is_mp


def write_tweets(stats_file, event_file, tweets_files_path, outputfile, total):
    events = set()
    with open(stats_file, "r") as f:
        reader = casanova.reader(f)
        thread_id_pos = reader.headers.thread_id
        words_pos = reader.headers.top_5_words

        for row in reader:
            words = row[words_pos]
            if words:
                events.add(int(row[thread_id_pos]))

    tweets = {}
    with open(event_file, 'r') as f:

        reader = casanova.reader(f)
        id_pos = reader.headers.id
        thread_id_pos = reader.headers.thread_id

        for row in tqdm(reader):
            event_id = int(row[thread_id_pos])
            if event_id in events:
                tweets[int(row[id_pos])] = event_id

    pbar = tqdm(total=int(total))

    for file in glob.glob(tweets_files_path):
        print(file)
        with open(outputfile, "a") as of:
            with casanova.enricher(file, of, strip_null_bytes_on_read=True,
                                         select=["id", "text", "local_time", "user_screen_name", "user_id", "retweeted_id"],
                                         add=["thread_id", "is_MP"]
                                         ) as enricher:
                id_pos = enricher.headers.id
                retweeted_id_pos = enricher.headers.retweeted_id
                user_id_pos = enricher.headers.user_id


                for row in enricher:
                    event_id = tweets.get(int(row[id_pos]))

                    if event_id:
                        enricher.writerow(row, [str(event_id), "1" if is_mp(row[user_id_pos]) else ""])
                    else:
                        retweeted_id = row[retweeted_id_pos]
                        if retweeted_id:
                            retweeted_id = int(retweeted_id)
                            event_id = tweets.get(retweeted_id)
                            if event_id:
                                enricher.writerow(row, [str(event_id), "1" if is_mp(row[user_id_pos]) else ""])

                    pbar.update(1)
    pbar.close()


if __name__ == '__main__':
    stats_file = sys.argv[1]
    assert os.path.exists(stats_file)

    event_file = sys.argv[2]
    assert os.path.exists(event_file)

    tweets_files = sys.argv[3]

    output_file = sys.argv[4]

    total = sys.argv[5]

    write_tweets(stats_file, event_file, tweets_files, output_file, total)
