#!/bin/sh

minet fetch url unique_and_counted_urls.csv --throttle 0 --domain-parallelism 4 --total 1131867 --compress --output-dir FETCHED --folder-strategy prefix-4 -o FETCHED.CSV --resume
