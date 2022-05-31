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

        if not urls:
            continue
        for url in urls:
            for candidate in candidates:
                url_infos = URLS.get(url)
                for tinydict in url_infos:
                    if tinydict is not None:
                        print(tinydict)
                        url_title = tinydict.values()[0]
                        url_share_count = tinydict.values()[1]
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


    # description_pos = reader_3.headers.description
    # raw_content_pos = reader_3.headers.raw_content
    # comments_pos = reader_3.headers.comments
    # author_pos = reader_3.headers.author
    # sitename_pos = reader_3.headers.sitename

    # for row in tqdm(reader_3, unit=' rows'):
    #     URLS[row[url_pos]]['title'] = row[title_pos]
    #     URLS[row[url_pos]]['description'] = row[description_pos]
    #     URLS[row[url_pos]]['raw_content'] = row[raw_content_pos]
    #     URLS[row[url_pos]]['comments'] = row[comments_pos]
    #     URLS[row[url_pos]]['author'] = row[author_pos]
    #     URLS[row[url_pos]]['sitename'] = row[sitename_pos]
