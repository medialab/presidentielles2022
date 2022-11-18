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


print("---------------------------------")
# ____________
# Count tweets where we agree / disagree. For tweets where we disagree, find another annotator.

nb_annotators_buckets = [set() for i in range(5)]
count_agreed = 0
count_disagreed = 0
count_couldnt_find_agreement = 0
array_annotators = np.array(list_annotators)
reannotator_names = ["Béatrice", "Benjamin T", "Benjamin O"]
reannotator = {name: {origin_file: [] for origin_file in list_annotators} for name in reannotator_names}
events = [{"event": label} for label in raw_text_labels]

for i, id in enumerate(id_to_annotator_to_event.keys()):
    not_nan = ~np.isnan(reliability_data[:, i])
    count_annotated = np.count_nonzero(not_nan)
    
    if count_annotated == 0:
        raise Exception("No annotation for " + id)
    else:
        nb_annotators_buckets[count_annotated].add(id)
    
    if count_annotated >= 2:
        annotation = reliability_data[:, i][not_nan]
        annotated_by = array_annotators[not_nan]

        if count_annotated == 2:
            if np.all(annotation == annotation[0]):
                count_agreed += 1
            else:
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
        elif count_annotated == 3:
            if len(np.unique(annotation)) == 3:
                count_couldnt_find_agreement += 1
        else:
            raise Exception(
                "{} annotators for tweet {}: {}, {}".format(
                    count_annotated, id, annotation, annotated_by
                    )
                )

for i, bucket in enumerate(nb_annotators_buckets):
    if len(bucket) != 0:
        print("{} tweets were annotated by {} person{}".format(len(bucket), i, "s" if i > 1 else ""))

print("For tweets annotated by 3 persons, {} tweets received 3 different annotations"\
    .format(count_couldnt_find_agreement))


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

print("--------------------------------")

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

print("--------------------------------")
print("Krippendorff's alpha: ", krippendorff.alpha(reliability_data=reliability_data, level_of_measurement="nominal"))

krippendorff_annotators = np.empty([len(list_annotators), len(list_annotators)])
krippendorff_annotators_more_than_hundred = krippendorff_annotators.copy()
for index_1, annotator_1 in enumerate(list_annotators):
    for index_2, annotator_2 in enumerate(list_annotators):

        reliability_data_pairs = np.vstack((reliability_data[index_1], reliability_data[index_2]))

        nb_in_common = reliability_data_pairs[:, ~np.isnan(reliability_data_pairs).any(axis=0)].shape[1]
        
        krippendorff_annotators[index_1][index_2] = round(krippendorff.alpha(reliability_data=reliability_data_pairs, level_of_measurement="nominal"), 3)

        if nb_in_common >= 100:
            krippendorff_annotators_more_than_hundred[index_1][index_2] = round(krippendorff.alpha(reliability_data=reliability_data_pairs, level_of_measurement="nominal"), 3)
        else:
            krippendorff_annotators_more_than_hundred[index_1][index_2] = np.nan
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

ax.set_title("Krippendorff between annotators")
fig.tight_layout()
plt.savefig("kpf_heatmap.png")

# ---------------------------------

# Plot heatmap
fig, ax = plt.subplots()

not_BO = np.array(list_annotators) != "BenjaminO.csv"
kpf_matrix = krippendorff_annotators_more_than_hundred[not_BO][:,not_BO.T]
im = ax.imshow(kpf_matrix)

list_annotators.remove("BenjaminO.csv")

# Show all ticks and label them with the respective list entries
ax.set_xticks(np.arange(len(list_annotators)), labels=list_annotators)
ax.set_yticks(np.arange(len(list_annotators)), labels=list_annotators)

# Rotate the tick labels and set their alignment.
plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
         rotation_mode="anchor")

# Loop over data dimensions and create text annotations.
for i in range(len(list_annotators)):
    for j in range(len(list_annotators)):
        text = ax.text(j, i, kpf_matrix[i, j], ha="center", va="center", color="w")

ax.set_title("Krippendorff between annotators with more than 100 common tweets")
fig.tight_layout()
plt.savefig("kpf_heatmap_more_than_100_common_tweets.png")