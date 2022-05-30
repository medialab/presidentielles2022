import csv
#from imaplib import _CommandResults
#from multiprocessing import AuthenticationError
#from turtle import title
#from xml.etree.ElementTree import Comment
import casanova
from tqdm import tqdm

URLS = {}

with open('presidentielle_compiled.csv', 'rt') as f1, \
    open('unique_and_counted_urls.csv', 'rt') as f2, \
    open('EXTRACTED-P1.CSV', 'rt') as f3, \
        open('multithreaded_tweets.csv', 'w') as f4:
    reader_1 = casanova.reader(f1, ignore_null_bytes=True)
    reader_2 = casanova.reader(f2, ignore_null_bytes=True)
    reader_3 = casanova.reader(f3, ignore_null_bytes=True)
    writer = csv.writer(f4)

    url_pos = reader_1.headers.url
    id_pos = reader_1.headers.id
    user_id_pos = reader_1.headers.user_id
    user_screen_name_pos = reader_1.headers.user_screen_name
    candidates_pos = reader_1.headers.candidates
    timestamp_pos = reader_1.headers.timestamp
    text_pos = reader_1.headers.text
    retweet_count_pos = reader_1.headers.retweet_count
    retweeted_id_pos = reader_1.headers.retweeted_id
    retweeted_user_pos = reader_1.headers.retweeted_user
    share_count_pos = reader_2.headers.share_count
    title_pos = reader_3.headers.title
    description_pos = reader_3.headers.description
    raw_content_pos = reader_3.headers.raw_content
    comments_pos = reader_3.headers.comments
    author_pos = reader_3.headers.author
    sitename_pos = reader_3.headers.sitename

    for row in tqdm(reader_1, unit=' rows'):
        url = row[url_pos].split("|")

        if not url:
            continue

        URLS[row[url_pos]] = {'id': row[id_pos], 'user_id': row[user_id_pos], 'user_screen_name': row[user_screen_name_pos], 'timestamp': row[timestamp_pos], 'text': row[text_pos], 'retweet_count': row[retweet_count_pos], 'retweeted_id': row[retweeted_id_pos], 'retweeted_user': row[retweeted_user_pos], 'candidates': row[candidates_pos]}

    print(URLS)

    for row in tqdm(reader_3, unit=' rows'):
        URLS[row[url_pos]]['title'] = row[title_pos]
        URLS[row[url_pos]]['description'] = row[description_pos]
        URLS[row[url_pos]]['raw_content'] = row[raw_content_pos]
        URLS[row[url_pos]]['comments'] = row[comments_pos]
        URLS[row[url_pos]]['author'] = row[author_pos]
        URLS[row[url_pos]]['sitename'] = row[sitename_pos]

    print(URLS)

    for row in tqdm(reader_1, unit='rows'):
        for candidate in row[candidates_pos]:
            for url in row[url_pos]:
                url_info = URLS[row[url_pos]]
                writer.writerow(
                    [
                        'url',
                        'id',
                        'user_id',
                        'user_screen_name',
                        'timestamp',
                        'text',
                        'retweet_count',
                        'retweeted_id',
                        'retweeted_user',
                        'candidate',
                        'title',
                        'description',
                        'raw_content',
                        'comments',
                        'author',
                        'sitename'
                        ])






    # for row in tqdm(reader, unit=' rows'):
    #     url = row[url_pos]
    #     share_count = row[share_count]
    #     id = row[id_pos]
    #     user_id = row[user_id_pos]
    #     user_screen_name = row[user_screen_name]
    #     candidates = row[candidates_pos]
    #     timestamp = row[timestamp_pos]
    #     text = row[text_pos]
    #     retweet_count = int(row[retweet_count_pos])
    #     retweeted_id = row[retweeted_id_pos]
    #     retweeted_user = row[retweeted_user_pos]
    #     titles = row[title_pos]
    #     description = row[description_pos]
    #     raw_content = row[raw_content_pos]
    #     comments = row[comments_pos]
    #     author = row[author_pos]
    #     sitename = row[sitename_pos]






#for row in f1:
#    URLS[row.url] = {'share_count': row.share_count}

#for row in f2:
#    URLS[row.urls]['title'] = row.title

# urls are properly indexed here
# URLS = url => {'share_count': int, 'title': str}



        # for candidate in candidates:
        #     URLS[url] = [
        #             id,
        #             url,
        #             user_id,
        #             user_screen_name,
        #             timestamp,
        #             text,
        #             retweet_count,
        #             retweeted_id,
        #             retweeted_user,
        #             candidate
        #         ]


#    writer.writerow(['id', 'url', 'user_id', 'user_screen_name', 'timestamp', 'text',
#    'retweet_count', 'retweeted_id', 'retweeted_user', 'candidate'])

#    for row in URLS.values():
#        writer.writerow(row)
