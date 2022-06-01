import csv
import casanova
from tqdm import tqdm
import sys

URLS = {}

csv.field_size_limit(sys.maxsize)

with open('presidentielle_compiled.csv', 'rt') as f1, \
    open('EXTRACTED-P1.CSV', 'rt') as f2, \
        open('multithreaded_tweets.csv', 'w') as f3:
    reader_extracted = casanova.reader(f2, ignore_null_bytes=True)
    reader_compiled = casanova.reader(f1, ignore_null_bytes=True)
    fieldnames= [
        'url',
        'title',
        'share_count',
        'candidate',
        'id',
        'user_id',
        'user_screen_name',
        'timestamp',
        'text',
        'retweet_count',
        'retweeted_id',
        'retweeted_user'
        ]
    writer = csv.DictWriter(f3, fieldnames=fieldnames)
    writer.writeheader()

    url_pos_extracted = reader_extracted.headers.url
    share_count_pos = reader_extracted.headers.share_count
    title_pos = reader_extracted.headers.title

    for row in tqdm(reader_extracted, unit=' rows'):
        url = row[url_pos_extracted]
        title = row[title_pos]

        if not title:
            continue
        URLS[url] = {'title': title, 'share_count': row[share_count_pos]}

    url_pos_compiled = reader_compiled.headers.url
    id_pos = reader_compiled.headers.id
    user_id_pos = reader_compiled.headers.user_id
    user_screen_name_pos = reader_compiled.headers.user_screen_name
    candidates_pos = reader_compiled.headers.candidates
    timestamp_pos = reader_compiled.headers.timestamp
    text_pos = reader_compiled.headers.text
    retweet_count_pos = reader_compiled.headers.retweet_count
    retweeted_id_pos = reader_compiled.headers.retweeted_id
    retweeted_user_pos = reader_compiled.headers.retweeted_user

    for row in tqdm(reader_compiled, unit='rows'):
        urls = row[url_pos_compiled].split("|")
        candidates = row[candidates_pos].split("|")

        for url in urls:
            if not url:
                continue
            url_info = URLS.get(url)
            if url_info is None:
                continue
            for candidate in candidates:
                url_title = url_info['title']
                url_share_count = url_info['share_count']
                output_dict={
                    'url': url,
                    'title': url_title,
                    'share_count': url_share_count,
                    'candidate': candidate,
                    'id': row[id_pos],
                    'user_id': row[user_id_pos],
                    'user_screen_name': row[user_screen_name_pos],
                    'timestamp': row[timestamp_pos],
                    'text': row[text_pos],
                    'retweet_count': row[retweet_count_pos],
                    'retweeted_id': row[retweeted_id_pos],
                    'retweeted_user': row[retweeted_user_pos]}

                writer.writerow(output_dict)