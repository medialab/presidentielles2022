#!/bin/bash


INPUT_DIR="$1"
if test -z "$INPUT_DIR" || ! test -s "$INPUT_DIR"; then
  echo "Please input as first argument the input directory"
  exit 1
fi

INPUT_FILE="$2"
if test -z "$INPUT_FILE" || ! test -s "$INPUT_FILE"; then
  echo "Please input as second argument the csv input file"
  exit 1
fi

OUTPUT_FILE="$3"
if test -z "$OUTPUT_FILE" || ! test -d "$OUTPUT_FILE"; then
  echo "Please input as third argument the name of the csv file in which to write the report"
  exit 1
fi


minet extract -i $INPUT_DIR $INPUT_FILE -o $OUTPUT_FILE
