import csv
import casanova
from tqdm import tqdm
import sys
from ural import is_homepage
import argparse
import os

URLS = {}

csv.field_size_limit(sys.maxsize)

parser = argparse.ArgumentParser(description='Path to files')
parser.add_argument('-p', '--path', type=str, help='path to the files containing the corpus of tweets', default='')
args = parser.parse_args()

with open(os.path.join(args.path, 'presidentielle_compiled.csv'), 'rt') as f1, \
    open(os.path.join(args.path, 'EXTRACTED-P1.CSV'), 'rt') as f2:
    reader_extracted = casanova.reader(f2, ignore_null_bytes=True)
    reader_compiled = casanova.reader(f1, ignore_null_bytes=True)
    fieldnames= [
        'url',
        'title',
        'description',
        'raw_content',
        'share_count',
        'candidates',
        'id',
        'user_id',
        'user_screen_name',
        'user_followers',
        'timestamp',
        'text',
        'retweet_count',
        'retweeted_id',
        'retweeted_user'
        ]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()

    url_pos_extracted = reader_extracted.headers.url
    share_count_pos = reader_extracted.headers.share_count
    title_pos = reader_extracted.headers.title
    description_pos = reader_extracted.headers.description
    raw_content_pos = reader_extracted.headers.raw_content

    for row in tqdm(reader_extracted, unit=' rows'):
        url = row[url_pos_extracted]
        title = row[title_pos]
        description = row[description_pos]
        raw_content = row[raw_content_pos]

        if not title:
            continue
        URLS[url] = {'title': title, 'description': row[description_pos], 'raw_content': row[raw_content_pos], 'share_count': row[share_count_pos]}

    url_pos_compiled = reader_compiled.headers.url
    id_pos = reader_compiled.headers.id
    user_id_pos = reader_compiled.headers.user_id
    user_screen_name_pos = reader_compiled.headers.user_screen_name
    user_followers_pos = reader_compiled.headers.user_followers
    candidates_pos = reader_compiled.headers.candidates
    timestamp_pos = reader_compiled.headers.timestamp
    text_pos = reader_compiled.headers.text
    retweet_count_pos = reader_compiled.headers.retweet_count
    retweeted_id_pos = reader_compiled.headers.retweeted_id
    retweeted_user_pos = reader_compiled.headers.retweeted_user

    for row in tqdm(reader_compiled, unit='rows'):
        urls = row[url_pos_compiled].split("|")
        candidates = row[candidates_pos]

        for url in urls:
            if not url or is_homepage(url)==True:
                continue
            url_info = URLS.get(url)
            if url_info is None:
                continue
            url_share_count = url_info['share_count']
            url_title = url_info['title']
            url_description = url_info['description']
            url_raw_content = url_info['raw_content']
            if url_title == 'JavaScript is not available.':
                continue
            output_dict={
                'url': url,
                'title': url_title,
                'description': url_description,
                'raw_content': url_raw_content,
                'share_count': url_share_count,
                'candidates': candidates,
                'id': row[id_pos],
                'user_id': row[user_id_pos],
                'user_screen_name': row[user_screen_name_pos],
                'user_followers': row[user_followers_pos],
                'timestamp': row[timestamp_pos],
                'text': row[text_pos],
                'retweet_count': row[retweet_count_pos],
                'retweeted_id': row[retweeted_id_pos],
                'retweeted_user': row[retweeted_user_pos]}

            writer.writerow(output_dict)
