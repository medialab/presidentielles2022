#!/usr/bin/env python
# coding: utf-8

"""
Compute some aggregated stats about events:
- nb of tweets
- nb of words
# - start date
# - end date
# - day with max nb of tweets
- top 5 words (according to chi2 metric)
# - top 5 users (idem)
# - top 5 media (idem)
# - first 5 users (in time)
# - first 5 media (in time)
"""

import os
import csv
import sys
import casanova
import pandas as pd
from tqdm import tqdm
from ural import get_domain_name
from collections import defaultdict
import re


csv.field_size_limit(sys.maxsize)
TOTAL_TWEETS = 6896842

medias = pd.read_csv('https://raw.githubusercontent.com/medialab/corpora/master/polarisation/medias.csv')
categories_sorted = ['Mainstream Media', 'Opinion Journalism', 'Counter-Informational Space', 'Periphery', None]


media_domains = dict()
for i, row in medias.iterrows():
    if row['wheel_category'] in categories_sorted:
        for prefix in row['prefixes'].split('|'):
            media_domains[get_domain_name(prefix)] = row['wheel_category']

for unwanted_domain in [None, 'facebook.com','twitter.com', 'youtube.com', 'dailymotion.com']:
    media_domains.pop(unwanted_domain)


def word_ngrams(tokens, stop_words=None, vocab=None, ngram_range=(1, 1)):
    """Turn tokens into a sequence of n-grams after stop words filtering.
    Source: https://github.com/scikit-learn/scikit-learn/blob/7db5b6a98ac6ad0976a3364966e214926ca8098a/sklearn/feature_extraction/text.py#L245
    """
    # handle stop words
    if stop_words is not None:
        tokens = [w for w in tokens if w not in stop_words]
    # handle vocab
    elif vocab is not None:
        tokens = [w for w in tokens if w in vocab]

    # handle token n-grams
    min_n, max_n = ngram_range
    if max_n != 1:
        original_tokens = tokens
        if min_n == 1:
            # no need to do any slicing for unigrams
            # just iterate through the original tokens
            tokens = list(original_tokens)
            min_n += 1
        else:
            tokens = []

        n_original_tokens = len(original_tokens)

        # bind method outside of loop to reduce overhead
        tokens_append = tokens.append
        space_join = " ".join

        range_ngrams = range(min_n, min(max_n + 1, n_original_tokens + 1))

        if vocab is not None:
            for n in range_ngrams:
                for i in range(n_original_tokens - n + 1):
                    w = space_join(original_tokens[i : i + n])
                    if w in vocab:
                        tokens_append(w)
        else:
            for n in range_ngrams:
                for i in range(n_original_tokens - n + 1):
                    tokens_append(space_join(original_tokens[i : i + n]))
    return tokens

def get_top_k_chi_squares(event_count, event_frequency, frequency, n, k):
    scores = []
    words = []

    for w, w_count in event_frequency.items():
        indep = event_count*frequency[w]/n
        chi_square = (w_count-indep)**2/indep
        
        if len(scores) < k:
            scores.append(chi_square)
            words.append(w)
        else:
            min_chi_square = min(scores)
            if chi_square > min_chi_square :
                index = scores.index(min_chi_square)
                scores[index] = chi_square
                words[index] = w
    return [word for score, word in sorted(zip(scores, words), reverse=True)]


def event_stats(source_file, vocab_file, outfile, format_thread_id, min_nb_docs=10):
    with casanova.reader(vocab_file) as reader:
        
        vocab = set(t for t in reader.cells("token"))

    with open(source_file, 'r') as f:

        reader = casanova.reader(csv.reader(l.replace('\0', '') for l in f))
        
        text_pos = reader.headers.text
        event_pos = reader.headers.thread_id
        
        term_frequency = defaultdict(int)
        events_stats = defaultdict(lambda: {"nb_words": 0, "nb_docs": 0, "tf": defaultdict(int)})
        n = 0
        token_pattern=re.compile(r'[a-z]+')

        for row in tqdm(reader, total=TOTAL_TWEETS):
            event_id = format_thread_id(row[event_pos])
            stats = events_stats[event_id]
            stats["nb_docs"] += 1
            for token in word_ngrams(token_pattern.findall(row[text_pos].lower()), vocab=vocab, ngram_range=(1, 2)):
                stats["tf"][token] += 1
                term_frequency[token] += 1
                stats["nb_words"] += 1
                n += 1
        
        with open(outfile, "w") as of:
            writer = csv.writer(of)
            writer.writerow(["thread_id", "nb_docs", "nb_words", "top_5_words"])
            total = len(events_stats)
            for event, stats in tqdm(events_stats.items(), total=total):
                nb_docs = stats["nb_docs"]
                if nb_docs >= min_nb_docs:
                    nb_words = stats["nb_words"]
                    top_5 = get_top_k_chi_squares(nb_words, stats["tf"], term_frequency, n, 5)
                    writer.writerow([event, nb_docs, nb_words, "|".join(top_5)])

                

if __name__ == '__main__':
    in_file = sys.argv[1]
    assert os.path.exists(in_file)

    vocab_file = sys.argv[2]
    assert os.path.exists(vocab_file)

    formated_file = sys.argv[3]
    assert formated_file.endswith('.csv')

    format_thread_id = int if len(sys.argv) == 5 else str

    event_stats(in_file, vocab_file, formated_file, format_thread_id)
