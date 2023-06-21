
#!/bin/sh
set -euo pipefail

eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
pyenv activate minet

# Rehydrate full tweets from ids: 94549 tweets hydrated out of 139804
xsv fmt ~/Documents/Data/Event2018/full_annotation.tsv | minet twitter tweets id -i - | xsv search -s lang . > ~/Documents/Data/Event2018/full_annotation_rehydrated.csv

# Find "leftovers" (tweets not associated to an event) in Event2018: 28978 tweets
xsv search -s label . -v ~/Documents/Data/Event2018/full_annotation_rehydrated.csv > event2018_not_associated_to_an_event_hydrated.csv

# Create pairs of tweets that have nothing to do with each other - label them as 0: 13747 pairs
xsv frequency -l 0 -s event event2018_not_associated_to_an_event_hydrated.csv > event2018_event_frequencies.csv
python make_training_sample.py event2018_event_frequencies.csv event2018_not_associated_to_an_event_hydrated.csv > event2018_training_sample.csv

# Run singerie on leftovers and extract pairs of tweets with a non zero distance
singerie vocab ~/Documents/Data/Event2018/full_annotation_rehydrated.csv --total 94549 --ngrams 2 > event2018_vocab.csv
WINDOW=`singerie window event2018_not_associated_to_an_event_hydrated.csv --raw --total 28978 --datecol timestamp_utc`
singerie nn event2018_vocab.csv event2018_not_associated_to_an_event_hydrated.csv -w $WINDOW --total 28978 --ngrams 2 --threshold 0.7 > event2018_leftovers_nn.csv
xsv search -s nearest_neighbor . event2018_leftovers_nn.csv | xsv search -s distance ^0$ -v | xsv sort -s distance -N > event2018_nnz_distance_tweet_pairs.csv

xsv join --left id event2018_nnz_distance_tweet_pairs.csv id event2018_not_associated_to_an_event_hydrated.csv | \
xsv select 1-4,13-16,60 > event2018_nnz_distance_tweet_pairs_first_join.csv
