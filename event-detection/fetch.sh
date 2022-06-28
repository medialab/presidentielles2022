#!/bin/sh


INPUT_FILE="$1"
if test -z "$INPUT_FILE" || ! test -s "$INPUT_FILE"; then
  echo "Please input as first argument the csv input file"
  exit 1
fi

OUTPUT_DIR="$2"
if test -z "$OUTPUT_DIR" || ! test -d "$OUTPUT_DIR"; then
  echo "Please input as second argument the path of a directory where to store reports and data"
  exit 1
fi

OUTPUT_FILE="$3"
if test -z "$OUTPUT_FILE" || ! test -d "$OUTPUT_FILE"; then
  echo "Please input as third argument the name of the csv file in which to write the report"
  exit 1
fi

minet fetch url $INPUT_FILE --throttle 0 --domain-parallelism 4 --total 1131867 --compress --output-dir $OUTPUT_DIR --folder-strategy prefix-4 -o $OUTPUT_FILE --resume

