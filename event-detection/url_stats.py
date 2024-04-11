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
from casanova.exceptions import UnknownNamedColumnError
import string
import sys

columns = ["url", "domain_name", "probably_homepage", "europresse_id", "matched_on",
           "first_shared", "thread_ids", "nb_tweets", "nb_retweets", "nb_threads",
           "page_title", "europresse_title",
           ]
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

def find_in_dict(url, url_dict):

    if is_url(url):
        normalized = normalize_url(url)
        if normalized in url_dict:
            return normalized
    return ""

def get_url_stats(url, row, url_scraped=True):
    try:
        url = normalize_url(url)
    except ValueError:
        return
    url_stats = url_dict.get(url)

    if url_stats:
        url_stats["nb_tweets"] += 1
        url_stats["nb_retweets"] += int(row[retweet_pos])
        if url_scraped:
            url_stats["thread_ids"].add(row[thread_pos])
    else:
        matched_on = None
        europresse_title = None
        try:
            match = europresse_url_trie.match(url)
        except AttributeError:
            match = None

        if match:
            matched_on = "europresse_url"
        elif url_scraped:
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
            "thread_ids": set([row[thread_pos]]) if url_scraped else None,
            "match": match,
            "matched_on": matched_on,
            "nb_tweets": 1,
            "nb_retweets": int(row[retweet_pos]),
            "page_title": row[title_pos] if url_scraped else None,
            "europresse_title": europresse_title
        }


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
        manual_url_pos = None
        if "manually_found_url" in europresse_headers:
            manual_url_pos = europresse_headers.manually_found_url

    for row in reader :
        if manual_url_pos is not None and row[manual_url_pos]:
            europresse_url_trie.set(row[manual_url_pos], row[id_pos])

        if row[resolved_url_pos] :
            europresse_url_trie.set(row[resolved_url_pos], row[id_pos])

        if row[url_pos] and is_url(row[url_pos]):
            europresse_url_trie.set(row[url_pos], row[id_pos])

        if row[media_pos] in media_homepages:
            media_homepage = media_homepages[row[media_pos]]
            europresse_title_dict[clean(row[title_pos])].set(media_homepage, row[id_pos])

titles = list(europresse_title_dict.keys())

count = casanova.count(infile_tweets, strip_null_bytes_on_read=True)
with casanova.reader(infile_tweets, strip_null_bytes_on_read=True) as reader:
    if reader.headers:
        try:
            retweet_pos = reader.headers.retweet_count
            date_pos = reader.headers.timestamp_utc
            url_pos = reader.headers.selected_url
            thread_pos = reader.headers.thread_id
            title_pos = reader.headers.page_title
            lead_pos = reader.headers.page_description

            url_scraped = True
        except UnknownNamedColumnError:
            url_scraped = False
            url_pos = reader.headers.links

    for row in tqdm(reader, total=count, desc="Read tweets"):
        if row[url_pos]:
            if url_scraped:
                get_url_stats(row[url_pos], row, url_scraped)
            else:
                for url in row[url_pos].split("|"):
                    get_url_stats(url, row, url_scraped)


with casanova.enricher(infile_europresse, sys.stdout, add=columns) as enricher:
    if enricher.headers:
        europresse_headers = enricher.headers
        url_pos = europresse_headers.url
        resolved_url_pos = europresse_headers.resolved_url
        id_pos = europresse_headers.id
        title_pos = europresse_headers.title
        media_pos = europresse_headers.media
        manual_url_pos = None
        if "manually_found_url" in europresse_headers:
            manual_url_pos = europresse_headers.manually_found_url
    for row in enricher:

        url = ""

        if manual_url_pos is not None and row[manual_url_pos]:
            url = find_in_dict(row[manual_url_pos], url_dict)

        if not url and row[resolved_url_pos]:
            url = find_in_dict(row[resolved_url_pos], url_dict)

        if not url and row[url_pos]:
            url = find_in_dict(row[url_pos], url_dict)

        if url:
            stats = url_dict[url]
            enricher.writerow(row, [
                url,
                get_domain_name(url),
                is_homepage(url),
                stats["match"],
                stats["matched_on"],
                datetime.fromtimestamp(stats["first_shared"]).isoformat(),
                "|".join(stats["thread_ids"]) if stats["thread_ids"] else "",
                stats["nb_tweets"],
                stats["nb_retweets"],
                len(stats["thread_ids"]) if stats["thread_ids"] else "",
                stats["page_title"],
                stats["europresse_title"]
            ])
            continue
        enricher.writerow(row, [""]*len(columns))

# for url, stats in tqdm(url_dict.items(), desc="Write urls"):

#     writer.writerow({
#         "url": url,
#         "domain_name": get_domain_name(url),
#         "probably_homepage": is_homepage(url),
#         "europresse_id": stats["match"],
#         "matched_on": stats["matched_on"],
#         "first_shared": datetime.fromtimestamp(stats["first_shared"]).isoformat(),
#         "thread_ids": "|".join(stats["thread_ids"]) if stats["thread_ids"] else "",
#         "nb_tweets": stats["nb_tweets"],
#         "nb_retweets": stats["nb_retweets"],
#         "nb_threads": len(stats["thread_ids"]) if stats["thread_ids"] else "",
#         "page_title": stats["page_title"],
#         "europresse_title": stats["europresse_title"]
#     })