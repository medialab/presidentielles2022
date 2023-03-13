
#!/bin/sh
set -euo pipefail

# Find "leftovers" (tweets not associated to an event) in Event2018: 43115 tweets
xsv search -s label . ~/Documents/Data/Event2018/full_annotation.tsv -v > event2018_not_associated_to_an_event.csv

# Rehydrate full tweets from ids: 29192 tweets
minet twitter tweets id event2018_not_associated_to_an_event.csv > event2018_not_associated_to_an_event_tweets.csv
xsv search -s lang . event2018_not_associated_to_an_event_tweets.csv > event2018_not_associated_to_an_event_hydrated.csv
rm event2018_not_associated_to_an_event_tweets.csv

# Create pairs of tweets that have nothing to do with each other - label them as 0: 13752 pairs
xsv frequency -l 0 -s event event2018_not_associated_to_an_event_hydrated.csv > event2018_event_frequencies.csv
python make_training_sample.py event2018_event_frequencies.csv event21018_not_associated_to_an_event_hydrated.csv > event2018_training_sample.csv
