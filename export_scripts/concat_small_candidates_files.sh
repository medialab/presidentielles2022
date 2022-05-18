#!/bin/bash
set -e

for CANDIDATE in asselineau egger kazib koenig kuzmanovic taubira thouy
do
  echo "*********************"
  echo $CANDIDATE
  echo "*********************"
  echo

  echo "unzip files"
  echo
  sudo gzip -d presidentielle_${CANDIDATE}_20220201-20220322.csv.gz
  sudo gzip -d presidentielle_${CANDIDATE}.csv.gz

  echo "cut files"
  echo
  # Cut first file at 2022-01-31
  CUT_INDEX=`/home/boo/.cargo/bin/xsv select local_time "presidentielle_${CANDIDATE}.csv" | grep -n 2022-01-31 | tail -2 | cut -f1 -d: | head -1`
  /home/boo/.cargo/bin/xsv slice -e $CUT_INDEX "presidentielle_${CANDIDATE}.csv" > "presidentielle_${CANDIDATE}_20220101_20220201.csv"

  # Cut second file at 2022-03-04
  CUT_INDEX=`/home/boo/.cargo/bin/xsv select local_time "presidentielle_${CANDIDATE}_20220201-20220322.csv" | grep -n 2022-03-04 | tail -2 | cut -f1 -d: | head -1`
  /home/boo/.cargo/bin/xsv slice -e $CUT_INDEX "presidentielle_${CANDIDATE}_20220201-20220322.csv" > "presidentielle_${CANDIDATE}_20220201_20220305.csv"

  echo "concat files"
  echo
  /home/boo/.cargo/bin/xsv cat rows presidentielle_${CANDIDATE}_20220101_20220201.csv presidentielle_${CANDIDATE}_20220201_20220305.csv > "presidentielle_${CANDIDATE}_20220101_20220305.csv"

  echo "rezip files"
  echo
  sudo gzip presidentielle_${CANDIDATE}*.csv
done
