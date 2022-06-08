#!/bin/bash
set -e

for CANDIDATE in arthaud poutou hidalgo dupontaignan roussel lassalle jadot pecresse zemmour melenchon lepen main
do
  echo "*********************"
  echo $CANDIDATE
  echo "*********************"
  echo

  echo "unzip files"
  echo
  sudo gzip -d presidentielle_${CANDIDATE}_20220201-20220322.csv.gz
  sudo gzip -d presidentielle_${CANDIDATE}.csv.gz

  echo "cut first file"
  echo
  # Cut first file at 2022-01-31
  CUT_INDEX=`/home/boo/.cargo/bin/xsv select local_time "presidentielle_${CANDIDATE}.csv" | grep -n 2022-01-31 | tail -2 | cut -f1 -d: | head -1`
  /home/boo/.cargo/bin/xsv slice -e $CUT_INDEX "presidentielle_${CANDIDATE}.csv" > "presidentielle_${CANDIDATE}_20220101_20220201.csv"

  echo "concat files"
  echo
  /home/boo/.cargo/bin/xsv cat rows presidentielle_${CANDIDATE}_2022* > "presidentielle_${CANDIDATE}_20220101_20220425.csv"

  echo "rezip files"
  echo
  sudo gzip presidentielle_${CANDIDATE}*.csv
done
