#Fetching
minet fetch url unique_and_counted_urls.csv --throttle 0 --domain-parallelism 4 --total 1131867 --compress --output-dir FETCHED --folder-strategy prefix-4 -o FETCHED.CSV --resume


#Extracting without multiprocessing
minet extract -p 1 -i FETCHED FETCHED.CSV -o EXTRACTED-P1.CSV
