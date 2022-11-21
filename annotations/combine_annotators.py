import csv
from tqdm import tqdm
from os import listdir
from os.path import join
from fog.key import fingerprint
from collections import defaultdict




list_annotators = []
events = set()
annotations_count = defaultdict(lambda: defaultdict(int))
for file in listdir("annotations"):
    list_annotators.append(file)
    with open(join("annotations", file), "r") as f:
        reader = csv.DictReader(line.replace("\0", "") for line in f)
        for row in tqdm(reader):
            if not row["id"]:  # initial label
                continue
            event = fingerprint(row["event"])
            if "mckinsey" in event:
                event = "mckinsey"
                row["event"] = "mckinsey"
            if not event:
                event = "0"
            if event =="arambaru":  # mistype Béatrice
                event = "aramburu"
            
            events.add(event)

            annotations_count[row["id"]][event] += 1

events = list(events)
with open("presidentielle_2022_annotation_2022-03-20_2022-03-28.csv", "w") as f:
    writer = csv.DictWriter(f, fieldnames=["id", "label_full", "label"])
    writer.writeheader()
    for tweet_id, labels in annotations_count.items():
        if len(labels) == 1:
            label = list(labels.keys())[0]
        elif len(labels) == 2:
            label = max(labels, key=labels.get)
        # We ignore the case len(labels) == 3 (3 annotators chose 3 different annotations --> this probably
        # means that we cannot decide)

        if label != "0":
            writer.writerow({"id": tweet_id, "label_full": label, "label": events.index(label)})




