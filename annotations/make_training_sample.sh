
#!/bin/sh
set -euo pipefail

eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
pyenv activate minet

# Rehydrate full tweets from ids: 94549 tweets hydrated out of 139804
xsv fmt ~/Documents/Data/Event2018/full_annotation.tsv | minet twitter tweets id -i - | xsv search -s lang . > ~/Documents/Data/Event2018/full_annotation_rehydrated.csv

# # Find "leftovers" (tweets not associated to an event) in Event2018: 28978 tweets
xsv search -s label . -v ~/Documents/Data/Event2018/full_annotation_rehydrated.csv | \
xsv replace -s text 'http\S+' '' > event2018_not_associated_to_an_event_hydrated.csv

# Create pairs of tweets that have nothing to do with each other
# (they were searched by anotators in relation to different events, probably with different keywords)
# and label them as 0: 13747 pairs
xsv frequency -l 0 -s event event2018_not_associated_to_an_event_hydrated.csv > event2018_event_frequencies.csv
python make_training_sample.py event2018_event_frequencies.csv event2018_not_associated_to_an_event_hydrated.csv > event2018_training_sample.csv

# Run singerie on leftovers and extract pairs of tweets with a non zero distance
singerie vocab ~/Documents/Data/Event2018/full_annotation_rehydrated.csv --total 94549 --ngrams 2 > event2018_vocab.csv
WINDOW=`singerie window event2018_not_associated_to_an_event_hydrated.csv --raw --total 28978 --datecol timestamp_utc`
singerie nn event2018_vocab.csv event2018_not_associated_to_an_event_hydrated.csv -w $WINDOW --total 28978 --ngrams 2 --threshold 0.7 > event2018_leftovers_nn.csv
xsv search -s nearest_neighbor . event2018_leftovers_nn.csv | xsv search -s distance ^0$ -v > event2018_nnz_distance_tweet_pairs.csv

# Join to get the text of the tweets
xsv sort -s id -u event2018_nnz_distance_tweet_pairs.csv | xsv sort -s nearest_neighbor -u | xsv sort -s distance -N > event2018_nnz_distance_tweet_pairs_unique.csv
xsv join --left id event2018_nnz_distance_tweet_pairs_unique.csv id event2018_not_associated_to_an_event_hydrated.csv --prefix-right "1_" |\
xsv select 1_id,1_text,1_event,nearest_neighbor,distance > event2018_nnz_distance_tweet_pairs_first_join.csv

xsv join --left nearest_neighbor event2018_nnz_distance_tweet_pairs_first_join.csv \
id event2018_not_associated_to_an_event_hydrated.csv --prefix-right "2_" | \
xsv select 1_id,1_text,1_event,2_id,2_text,2_event,distance | \
xsv sort -s 1_id -u | xsv sort -s distance -N > event2018_nnz_distance_tweet_pairs_second_join.csv


# Select tweet pairs with 0.6 <= distance < 0.7
xsv search -s distance ^0.6 event2018_nnz_distance_tweet_pairs_second_join.csv > event2018_tweet_pairs_distance_greater_06.csv