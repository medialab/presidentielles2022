"""
Info about urls in the corpus

# domain_name
# probably_homepage
# europresse_id
# first_shared (date of the tweet sharing the url for the first time)
# thread_ids
# nb_tweets (nb of tweets sharing the url)
# nb_SE (nb of events sharing the url)
"""

from ural.lru import NormalizedLRUTrie
from ural import is_url, get_domain_name, is_homepage, normalize_url
from collections import defaultdict
from datetime import datetime
from tqdm import tqdm

import casanova
import string

import csv
import sys

columns = ["url", "domain_name", "probably_homepage", "europresse_id", "matched_on", "first_shared", "thread_ids", "nb_tweets", "nb_threads", "page_title", "europresse_title"]
more_punctuation = "«»’"

infile_tweets = sys.argv[1]
infile_europresse = sys.argv[2]
infile_media_websites = sys.argv[3]


url_dict = {}
europresse_url_trie = NormalizedLRUTrie()
europresse_title_dict = defaultdict(NormalizedLRUTrie)
media_homepages = {}

def clean(s):
    table = str.maketrans(dict.fromkeys(string.punctuation + more_punctuation))
    s = s.translate(table)
    return s.lower().strip()

with casanova.reader(infile_media_websites) as reader:
    if reader.headers:
        prefix_pos = reader.headers.prefixes
        media_pos = reader.headers.media

    for row in reader:
        if row[prefix_pos]:
            for prefix in row[prefix_pos].split(" "):
                media_homepages[row[media_pos]] = prefix
                break

with casanova.reader(infile_europresse) as reader:
    if reader.headers:
        europresse_headers = reader.headers
        url_pos = europresse_headers.url
        resolved_url_pos = europresse_headers.resolved_url
        id_pos = europresse_headers.id
        title_pos = europresse_headers.title
        media_pos = europresse_headers.media

    for row in reader :
        if row[url_pos] :
            if is_url(row[url_pos]):
                europresse_url_trie.set(row[url_pos], row[id_pos])
            else:
                europresse_url_trie.set("https://www.lemonde.fr" + row[url_pos], row[id_pos])

        elif row[media_pos] in media_homepages:
            media_homepage = media_homepages[row[media_pos]]
            europresse_title_dict[clean(row[title_pos])].set(media_homepage, row[id_pos])

titles = list(europresse_title_dict.keys())

count = casanova.count(infile_tweets)

with casanova.reader(infile_tweets) as reader:
    if reader.headers:
        url_pos = reader.headers.selected_url
        thread_pos = reader.headers.thread_id
        date_pos = reader.headers.timestamp_utc
        title_pos = reader.headers.page_title
        lead_pos = reader.headers.page_description

    for row in tqdm(reader, total=count, desc="Read tweets"):
        if row[url_pos]:
            url = normalize_url(row[url_pos])
            url_stats = url_dict.get(url)
            if url_stats:
                url_stats["nb_tweets"] += 1
                url_stats["thread_ids"].add(row[thread_pos])
            else:
                europresse_title = None
                match = europresse_url_trie.match(url)
                matched_on = "europresse_url"

                if not match:
                    matched_on = None
                    clean_title = clean(row[title_pos])
                    if clean_title:
                        matched_trie = europresse_title_dict.get(clean_title)
                        if matched_trie:
                            match = matched_trie.match(url)
                            matched_on = "europresse_title_exact"
                            europresse_title = row[title_pos]
                        else:
                            for title in titles:
                                if clean_title in title:
                                    matched_trie = europresse_title_dict.get(title)
                                    if matched_trie:
                                        match = matched_trie.match(url)
                                        europresse_title = title
                                        if match:
                                            matched_on = "europresse_title_fuzzy"
                                            break

                url_dict[url] = {
                    "first_shared": int(row[date_pos]),
                    "thread_ids": set([row[thread_pos]]),
                    "match": match,
                    "matched_on": matched_on,
                    "nb_tweets": 1,
                    "page_title": row[title_pos],
                    "europresse_title": europresse_title
                }


writer = csv.DictWriter(sys.stdout, fieldnames=columns)
writer.writeheader()

for url, stats in tqdm(url_dict.items(), desc="Write urls"):

    writer.writerow({
        "url": url,
        "domain_name": get_domain_name(url),
        "probably_homepage": is_homepage(url),
        "europresse_id": stats["match"],
        "matched_on": stats["matched_on"],
        "first_shared": datetime.fromtimestamp(stats["first_shared"]).isoformat(),
        "thread_ids": "|".join(stats["thread_ids"]),
        "nb_tweets": stats["nb_tweets"],
        "nb_threads": len(stats["thread_ids"]),
        "page_title": stats["page_title"],
        "europresse_title": stats["europresse_title"]
    })