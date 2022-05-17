import csv
import gzip
import glob
import heapq
from itertools import groupby

import casanova
from tqdm import tqdm

def candidate_name_from_file_path(path):
    return path.replace('presidentielle_', '').split('.', 1)[0]


files = {
    candidate_name_from_file_path(path): gzip.open(path, 'rt')
    for path in sorted(glob.glob('presidentielle_*.csv.gz'))
}

readers = {
    name: casanova.reader(f, ignore_null_bytes=True)
    for name, f in files.items()
}

def yield_rows_with_candidate(name, reader):
    for row in reader:
        yield name, row

generators = [
    yield_rows_with_candidate(name, reader)
    for name, reader in readers.items()
]

headers = next(iter(readers.values())).headers
id_pos = headers.id
timestamp_pos = headers.timestamp_utc

# Reading
with open('presidentielle_compiled.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow([
        'id',
        'timestamp',
        'candidates'
    ])

    def timestamp_key(item):
        return item[1][timestamp_pos]

    def id_key(item):
        return item[1][id_pos]

    loading_bar = tqdm(desc='Reading', unit=' rows')

    # NOTE: rows are sorted by timestamp only, but timestamp is not precise enough.
    # This means that several different tweets might share a timestamp and this
    # can be an issue since ES's sort is not stable. So, in order to make sure
    # we merge the rows correctly, we group by timestamp first, then we
    # sort in memory to regroup correctly by tweet id. This is ok performance-wise
    # because the cardinality of tweet sets sharing a same timestamp is never
    # very large.
    for _, items_grouped_by_timestamp in groupby(heapq.merge(*generators, key=timestamp_key), key=timestamp_key):
        items_grouped_by_timestamp = sorted(items_grouped_by_timestamp, key=id_key)
        loading_bar.update(len(items_grouped_by_timestamp))

        for _, items in groupby(items_grouped_by_timestamp, key=id_key):
            items = list(items)
            candidates = [item[0] for item in items]
            tweet = items[0][1]

            writer.writerow([
                tweet[id_pos],
                tweet[timestamp_pos],
                '|'.join(candidates)
            ])

# Cleanup
for f in files.values():
    f.close()
