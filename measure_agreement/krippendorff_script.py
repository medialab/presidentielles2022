import os
import csv
import random
from tqdm import tqdm
from os import listdir
from os.path import join
from collections import defaultdict, Counter
from fog.key import fingerprint
import krippendorff
import numpy as np
import matplotlib
import matplotlib as mpl
import matplotlib.pyplot as plt

labels_annotator = defaultdict(list)    # {annotator 1 : [label 1, label 2, ...], annotator 2 : ...}
set_ids = set()
list_annotators = []
pairs_label_id_annotator = defaultdict(list)  # {annotator 1 : [(id 1, event), (id 2, event) ...], annotator 2 : ...}
labels_to_int = dict()  # {label 1 : id 1, label 2 : ...} ---> label is fingerprint(label)
int_to_labels = dict()  # {id 1 : label 1, id 2 : ...} ---> label is initial label
raw_text_labels = set()
for file in listdir("annotations"):
    list_annotators.append(file)
    with open(join("annotations", file), "r") as f:
        reader = csv.DictReader(line.replace("\0", "") for line in f)
        labels_annotator[file].append("0")
        labels_to_int["0"] = 1  # start at 1 to be counted in krippendorff
        int_to_labels[1] = "0"
        last_label_index = 0    # used to keep last position non-empty label for Sylvain
        index = 0   # used to count position for Sylvain
        label_index = 2     # used to convert label into int
        for line in tqdm(reader):
            event = fingerprint(line["event"])
            if "mckinsey" in event:
                event = "mckinsey"
                line["event"] = "mckinsey"
            if not line["id"]:  # initial label
                raw_text_labels.add(line["event"])
                labels_annotator[file].append(event)
                labels_to_int[event] = label_index
                int_to_labels[label_index] = line["event"]
                label_index += 1
                continue
            index += 1
            if file == "Sylvain.csv" and event:
                last_label_index = index
            if not event:
                event = "0"
            if event =="arambaru":  # mistype Béatrice
                event = "aramburu"
            if event not in labels_annotator[file]: # try to assign label not in initial labels
                raise Exception("Not initial label for " + file)
            pairs_label_id_annotator[file].append((line["id"], labels_to_int[event]))
            set_ids.add(line["id"])
        if file == "Sylvain.csv":   # cut every line after last label
            pairs_label_id_annotator[file] = pairs_label_id_annotator[file][:last_label_index]

# check if annotators all have the same labels
labels = labels_annotator[file]
for key, values in labels_annotator.items():
    if values != labels:
        diff = list(set(values) ^ set(labels))
        in_values_not_labels = []
        in_labels_not_values = []
        for el in diff:
            if el in values:
                in_values_not_labels.append(el)
            else:
                in_labels_not_values.append(el)
        raise Exception("In " + key + " not in initials", in_values_not_labels, "In initials not in " + key, in_labels_not_values,)

print("Total number of tweets: ", len(set_ids))
# print("Conversion for labels: ", labels_to_int)

# create dict where line is annotator, column is id and value is event : {id 1 : {{annotator 1 : event}, {annotator 2 : event} ...}, id 2 : ...}
id_to_annotator_to_event = defaultdict(dict)
for annotator, value in pairs_label_id_annotator.items():
    for id, event in value:
        id_to_annotator_to_event[id][annotator] = event

# for id in set_ids:
#     print(id_to_annotator_to_event[id])

reliability_data = np.empty([len(list_annotators), len(set_ids)])
for j, annotator in enumerate(list_annotators):
    for i, id in enumerate(id_to_annotator_to_event.keys()):
        reliability_data[j, i] = id_to_annotator_to_event[id].get(annotator) if id_to_annotator_to_event[id].get(annotator) else np.nan

# -------------
for annotator, value in pairs_label_id_annotator.items():
    nb_annoted_annotator = 0
    event_counters_annotator = Counter()
    for id, event in value:
        event_counters_annotator[int_to_labels[event]] += 1
        nb_annoted_annotator += 1
    print(annotator, "annotated (percentage of total)", nb_annoted_annotator*100/len(set_ids))
    print("Percentage per annotator out of total | out of self:")
    for event, nb in event_counters_annotator.most_common():
        print(event, ": ", nb*100/len(set_ids), "|", nb*100/nb_annoted_annotator)
    for label_int in int_to_labels.keys():
        if int_to_labels[label_int] not in event_counters_annotator.keys():
            print(int_to_labels[label_int], ": ", 0, "|", 0)
    print()
    print()

# ____________
# Count tweets where we agree / disagree. For tweets where we disagree, find another annotator.

annotated_by_one = set()
annotated_by_more = set()
count_agreed = 0
count_disagreed = 0
array_annotators = np.array(list_annotators)
reannotator_names = ["Béatrice", "Benjamin T", "Benjamin O"]
reannotator = {name: {origin_file: [] for origin_file in list_annotators} for name in reannotator_names}
events = [{"event": label} for label in raw_text_labels]

for i, id in enumerate(id_to_annotator_to_event.keys()):
    not_nan = ~np.isnan(reliability_data[:, i])
    count_annotated = np.count_nonzero(not_nan)
    if count_annotated == 0:
        raise Exception("No annotation for " + id)
    elif count_annotated == 1:
        annotated_by_one.add(id)
    else:
        annotated_by_more.add(id)
        annotation = reliability_data[:, i][not_nan]
        if np.all(annotation == annotation[0]):
            count_agreed += 1
        else:
            annotated_by = array_annotators[not_nan]
            origin_file = annotated_by[0]

            if "Benjamin.csv" in annotated_by:
                if "Béatrice.csv" in annotated_by:
                    reannotator["Benjamin O"][origin_file].append(id)
                else:
                    reannotator["Béatrice"][origin_file].append(id)
            elif "Béatrice.csv" in annotated_by:
                reannotator["Benjamin T"][origin_file].append(id)
            else:
                i = random.randint(0, 2)
                reannotator[reannotator_names[i]][origin_file].append(id)
            count_disagreed += 1

print("{} tweets were annotated by 2 persons. We agreed on {} tweets, and disagreed on {} tweets.".format(
    len(annotated_by_more),
    count_agreed,
    count_disagreed
))
print("{} were only annotated by one person.".format(len(annotated_by_one)))

print()
print()

os.makedirs("additional_annotations", exist_ok=True)
headers = ["event", "tweet_texts", "texts", "user_screen_names", "sum_retweet_count", "candidates", "urls", "id",
           "date", "ids"]

for name, value in reannotator.items():

    with open(os.path.join("additional_annotations", name + "_additional.csv"), "w") as new_file:
        writer = csv.DictWriter(new_file, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for event in events:
            writer.writerow(event)
        for file, tweets in value.items():

            if tweets:
                with open(os.path.join("annotations", file), "r") as origin_file:
                    reader = csv.DictReader(origin_file)
                    next(reader)
                    for row in reader:
                        row.pop("event")
                        if row["id"] in tweets:
                            writer.writerow(row)
                            tweets.remove(row["id"])

                if len(tweets) != 0:
                    raise Exception("These tweets were not found in {}: {}".format(file, tweets))


# --------------

event_counters= Counter()
for i in range(len(list_annotators)):
    for j in range(len(set_ids)):
        if reliability_data[i][j] in int_to_labels.keys():
            event_counters[int_to_labels[reliability_data[i][j]]] += 1
for event, nb in event_counters.most_common():
    print(event, nb)
for label_int in int_to_labels.keys():
    if int_to_labels[label_int] not in event_counters:
        print(int_to_labels[label_int], 0)
# -------------

# print(reliability_data)
# print(reliability_data[4, 2985])
print("Krippendorff's alpha with zeros: ", krippendorff.alpha(reliability_data=reliability_data, level_of_measurement="nominal"))

krippendorff_annotators = np.empty([len(list_annotators), len(list_annotators)])
for index_1, annotator_1 in enumerate(list_annotators):
    for index_2, annotator_2 in enumerate(list_annotators):
        reliability_data_pairs = np.vstack((reliability_data[index_1], reliability_data[index_2]))
        krippendorff_annotators[index_1][index_2] = round(krippendorff.alpha(reliability_data=reliability_data_pairs, level_of_measurement="nominal"), 3)
        # print("Krippendorff's alpha with zeros between", annotator_1, "and", annotator_2, ":", krippendorff_annotators[index_1][index_2])

# Plot heatmap
fig, ax = plt.subplots()
im = ax.imshow(krippendorff_annotators)

# Show all ticks and label them with the respective list entries
ax.set_xticks(np.arange(len(list_annotators)), labels=list_annotators)
ax.set_yticks(np.arange(len(list_annotators)), labels=list_annotators)

# Rotate the tick labels and set their alignment.
plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
         rotation_mode="anchor")

# Loop over data dimensions and create text annotations.
for i in range(len(list_annotators)):
    for j in range(len(list_annotators)):
        text = ax.text(j, i, krippendorff_annotators[i, j], ha="center", va="center", color="w")

ax.set_title("Krippendorff between annotators with 0")
fig.tight_layout()
plt.show()



# Without zeros
reliability_data[reliability_data == 1] = np.nan

# print(reliability_data)
# print(reliability_data[4, 2985])
print("Krippendorff's alpha without zeros: ", krippendorff.alpha(reliability_data=reliability_data, level_of_measurement="nominal"))
print()
print()

for index_1, annotator_1 in enumerate(list_annotators):
    for index_2, annotator_2 in enumerate(list_annotators):
        reliability_data_pairs = np.vstack((reliability_data[index_1], reliability_data[index_2]))
        krippendorff_annotators[index_1][index_2] = round(krippendorff.alpha(reliability_data=reliability_data_pairs, level_of_measurement="nominal"), 3)
        # print("Krippendorff's alpha without zeros between", annotator_1, "and", annotator_2, ":", krippendorff_annotators[index_1][index_2])


fig, ax = plt.subplots()
im = ax.imshow(krippendorff_annotators)

# Show all ticks and label them with the respective list entries
ax.set_xticks(np.arange(len(list_annotators)), labels=list_annotators)
ax.set_yticks(np.arange(len(list_annotators)), labels=list_annotators)

# Rotate the tick labels and set their alignment.
plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
         rotation_mode="anchor")

# Loop over data dimensions and create text annotations.
for i in range(len(list_annotators)):
    for j in range(len(list_annotators)):
        text = ax.text(j, i, krippendorff_annotators[i, j], ha="center", va="center", color="w")

ax.set_title("Krippendorff between annotators without 0")
fig.tight_layout()
plt.show()