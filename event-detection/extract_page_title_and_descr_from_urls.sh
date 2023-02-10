
#!/bin/sh
set -euo pipefail

INPUT_FILE=$1
BASE_NAME=$(basename ${INPUT_FILE} | sed "s/\..*//")

# Select original tweets (not retweets) that have been retweeted once or more
zcat $INPUT_FILE | \
xsv search -s retweeted_id . -v | \
xsv search -s retweet_count ^0$ -v | \
gzip -c > ${BASE_NAME}_originals_with_RT.csv.gz

# Select tweets that have one or several links, and normalize urls. Deduplicate on links column.
zcat ${BASE_NAME}_originals_with_RT.csv.gz | \
xsv select links | \
xsv search -s links . | \
minet url-parse links --separator \| | \
xsv search -s homepage . -v | \
xsv search -s typo . -v | \
xsv search -s domain_name "instagram.com|twitter.com|msn.com|1001rss.com|tiktok.com|gala.fr|lavoixdunord.fr|open.spotify.com" -v | \
xsv select links,normalized_url | \
xsv sort -u -s links | \
gzip -c > ${BASE_NAME}_links.csv.gz

# Deduplicate on normalized_url column
zcat ${BASE_NAME}_links.csv.gz | \
xsv select normalized_url | \
xsv sort -u > ${BASE_NAME}_normalized_url_sorted.csv

# Shuffle the result
COUNT=`xsv count ${BASE_NAME}_normalized_url_sorted.csv`
xsv sample $COUNT ${BASE_NAME}_normalized_url_sorted.csv | gzip -c > ${BASE_NAME}_normalized_url.csv.gz
rm ${BASE_NAME}_normalized_url_sorted.csv

# Fetch urls
zcat ${BASE_NAME}_normalized_url.csv.gz | \
minet fetch \
-o ${BASE_NAME}_fetch_report.csv \
--throttle 0 \
--domain-parallelism 4 \
--compress \
--total $COUNT \
--timeout 10 \
--resume \
-t 10 \
--folder-strategy prefix-4 \
normalized_url

# Extract title and description
minet extract ${BASE_NAME}_fetch_report.csv --total $COUNT | \
xsv select normalized_url,canonical_url,title,description,error,extract_error | gzip -c > ${BASE_NAME}_extraction.csv.gz
rm -r downloaded
gzip ${BASE_NAME}_fetch_report.csv

# Filter out empty titles
zcat ${BASE_NAME}_extraction.csv.gz | \
xsv select normalized_url,canonical_url,title,description,date | \
xsv search -s title,description . > ${BASE_NAME}_successful_extraction.csv

# Join
gzip -d ${BASE_NAME}_links.csv.gz

xsv join normalized_url ${BASE_NAME}_links.csv normalized_url ${BASE_NAME}_successful_extraction.csv | \
xsv select 1,2,4-7 > ${BASE_NAME}_links_with_extraction.csv
rm ${BASE_NAME}_successful_extraction.csv
rm ${BASE_NAME}_links.csv

gzip -d ${BASE_NAME}_originals_with_RT.csv.gz

xsv join --left links ${BASE_NAME}_originals_with_RT.csv links ${BASE_NAME}_links_with_extraction.csv | \
gzip -c > ${BASE_NAME}_originals_with_RT_extraction.csv.gz
rm ${BASE_NAME}_originals_with_RT.csv
rm ${BASE_NAME}_links_with_extraction.csv
