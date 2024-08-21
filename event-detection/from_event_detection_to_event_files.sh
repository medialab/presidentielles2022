
#!/bin/sh
set -euo pipefail

INPUT_FILE=$1
THRESHOLD=$2
WINDOW_SIZE=$3
STOPWORDS_FILE="stopwords_media_twitter.csv"
#KEYWORDS="soline|méga-bassine|mégabassine|megabassine|ecoterroris|écoterroris|eco-terroris|éco-terroris"
#KEYWORDS="lola"
#KEYWORDS="refus d'obtempérer|émeutes|cagnotte|l'haÿ-les-roses|lhaylesroses|violences policières|nanterre|zyed|bouna|clichy-sous-bois|lola|naël|nahel"
KEYWORDS="jeune conducteur|affrontements|refus d'obtempérer|émeutes|émeutiers|cagnotte|l'haÿ-les-roses|lhaylesroses|violences|nanterre|clichy-sous-bois|naël|nahel"

SAFE_KEYWORDS=$(echo $KEYWORDS | sed -r "s/^(.{30}).*$/\1/" | sed -r "s/[^a-z]/_/gi")

BASE_NAME=$(basename ${INPUT_FILE} | sed "s/\..*//")

eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
pyenv activate minet

echo "1. Compute events"
WINDOW=`singerie window --timezone "Europe/Paris"  $INPUT_FILE --datecol timestamp_utc --size $WINDOW_SIZE --raw`
singerie vocab --ngrams 2 --stopwords $STOPWORDS_FILE $INPUT_FILE > ${BASE_NAME}_vocab.csv
singerie nn ${BASE_NAME}_vocab.csv $INPUT_FILE \
--window $WINDOW \
--ngrams 2 \
--threshold $THRESHOLD \
> ${BASE_NAME}_nn_${THRESHOLD}_${WINDOW_SIZE}.csv

echo "2. Join with text and add local time"
xsv join --left id ${BASE_NAME}_nn_${THRESHOLD}_${WINDOW_SIZE}.csv id $INPUT_FILE | \
xsv datefmt timestamp_utc --outtz "Europe/Paris" --outfmt "%Y-%m-%d %H:%M:%S" -c "local_time" |\
xsv select id[1],thread_id,timestamp_utc,local_time,user_id,user_screen_name,retweet_count,links,domains,text,tweet_text,page_title,page_description,page_date,selected_url,nearest_neighbor \
> ${BASE_NAME}_nn_${THRESHOLD}_${WINDOW_SIZE}_with_text.csv

echo "3. Compute events stats"
python events_stats.py ${BASE_NAME}_nn_${THRESHOLD}_${WINDOW_SIZE}_with_text.csv ${BASE_NAME}_vocab.csv ${BASE_NAME}_${THRESHOLD}_${WINDOW_SIZE}_events_stats.csv int

echo "5. Find thread ids associated to keywords"
xsv map "lower(text)" lowered_text ${BASE_NAME}_nn_${THRESHOLD}_${WINDOW_SIZE}_with_text.csv | \
xsv search -s lowered_text "$KEYWORDS" | \
xsv frequency -s thread_id -l 0 \
> ${BASE_NAME}_${THRESHOLD}_${WINDOW_SIZE}_${SAFE_KEYWORDS}_threads.csv

echo "6. Filter events stats on keywords threads"
xsv join --left value ${BASE_NAME}_${THRESHOLD}_${WINDOW_SIZE}_${SAFE_KEYWORDS}_threads.csv thread_id ${BASE_NAME}_${THRESHOLD}_${WINDOW_SIZE}_events_stats.csv |\
xsv select count,thread_id,nb_docs,top_chi_square_words,top_chi_square_hashtags,top_hashtags,media_urls,tweets_by_media,start_date,end_date,max_docs_date,MPs \
> ${BASE_NAME}_${THRESHOLD}_${WINDOW_SIZE}_${SAFE_KEYWORDS}_events_stats.csv

echo "7. Filter on 20 tweets by cluster"
xsv filter "gte(count, 20)" ${BASE_NAME}_${THRESHOLD}_${WINDOW_SIZE}_${SAFE_KEYWORDS}_events_stats.csv \
> ${BASE_NAME}_${THRESHOLD}_${WINDOW_SIZE}_${SAFE_KEYWORDS}_events_stats_gt20.csv

echo "8. Write all tweets"
xsv join --left value ${BASE_NAME}_${THRESHOLD}_${WINDOW_SIZE}_${SAFE_KEYWORDS}_threads.csv thread_id ${BASE_NAME}_nn_${THRESHOLD}_${WINDOW_SIZE}_with_text.csv |\
xsv select value,count,user_screen_name,retweet_count,text,tweet_text,page_title,page_description,page_date,selected_url,id,nearest_neighbor,user_id,timestamp_utc,thread_id \
> ${BASE_NAME}_${THRESHOLD}_${WINDOW_SIZE}_original_tweets_from_events_containing_${SAFE_KEYWORDS}.csv

echo "9. Write one file per thread for threads greater than 20"
xsv join --left thread_id ${BASE_NAME}_${THRESHOLD}_${WINDOW_SIZE}_${SAFE_KEYWORDS}_events_stats_gt20.csv thread_id ${BASE_NAME}_nn_${THRESHOLD}_${WINDOW_SIZE}_with_text.csv |\
xsv select local_time,user_screen_name,retweet_count,text,tweet_text,page_title,page_description,page_date,selected_url,id,nearest_neighbor,user_id,timestamp_utc,thread_id |\
xsv partition thread_id ${BASE_NAME}_${THRESHOLD}_${WINDOW_SIZE}_files