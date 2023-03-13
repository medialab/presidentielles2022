import os
import csv
import sys
from itertools import combinations
from collections import defaultdict

def inner_loop(row, pairs_dict):
    for event, value in pairs_dict[row["event"]].items():
        if not value:
            pairs_dict[row["event"]][event] = [row["id"], row["text"], row["links"]]
            return pairs_dict

def write_zero_score_pairs(frequency_file, hydrated_tweets_file):

    with open(frequency_file, "r") as f:
        event_ids = [row[1] for row in csv.reader(f)]
        pairs = defaultdict(dict)
        reversed_pairs = defaultdict(dict)

        for pair in combinations(event_ids, 2):
            pairs[pair[0]][pair[1]] = False
            reversed_pairs[pair[1]][pair[0]] = False

    with open(hydrated_tweets_file, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:

            update_pairs = inner_loop(row, pairs)
            
            if update_pairs:
                pairs = update_pairs

            update_reversed_pairs = inner_loop(row, reversed_pairs)

            if update_reversed_pairs:
                reversed_pairs = update_reversed_pairs

    writer = csv.writer(sys.stdout)
    writer.writerow(["similarity_score", "id1", "text1", "urls1", "event1", "id2", "text2", "urls2", "event2"])
    for event1, d in pairs.items():
        for event2, id1 in d.items():
            if id1:
               id2 = reversed_pairs[event2][event1]
               if id2:
                   writer.writerow(["0", *id1, event1, *id2, event2])
        
if __name__ == "__main__":
    frequency_file = sys.argv[1]
    if not os.path.isfile(frequency_file):
        raise FileNotFoundError(frequency_file)
    
    tweets_file = sys.argv[2]
    if not os.path.isfile(tweets_file):
        raise FileNotFoundError(tweets_file)
    
    write_zero_score_pairs(frequency_file, tweets_file)
