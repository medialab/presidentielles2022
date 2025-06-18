import casanova
import sys
from collections import defaultdict


def main(threads_file_path, tweets_file_path, output_file_path):
    instructions = defaultdict(dict)

    with casanova.reader(threads_file_path) as reader:
        thread_id = reader.headers.thread_id
        merged_thread_id = reader.headers.thread_id_merge
        before = reader.headers.suppr_avant_inclus
        after = reader.headers.suppr_apres_inclus

        for row in reader:
            # If thread is composed of 2 distinct subevents, the names of the newly created threads must be different
            if "end" in row[merged_thread_id]:
                instructions[row[thread_id]]["switch_idx"] = int(row[before])

            else:
                # If some tweets must be deleted from thread
                if row[before] and row[after]:
                    instructions[row[thread_id]]["delete_between"] = [
                        int(row[after]),
                        int(row[before]) + 1,
                    ]
                elif row[before]:
                    instructions[row[thread_id]]["delete_before"] = int(row[before])
                elif row[after]:
                    instructions[row[thread_id]]["delete_after"] = int(row[after])

                # If the name of the thread has changed due to a merge or a tweet deletion
                if "_" in row[merged_thread_id]:
                    instructions[row[thread_id]]["new_thread_id"] = row[merged_thread_id]

    with open(tweets_file_path) as tweets_file, open(output_file_path, "w") as output_file:
        enricher = casanova.enricher(tweets_file, output_file, add=["merged_thread_id"])
        thread_id = enricher.headers.thread_id
        current_thread = None

        for row in enricher:
            if row[thread_id] != current_thread:
                counter = 0
                delete_between = []
                delete_before = 0
                delete_after = 0
                switch_at = 0
                current_thread = row[thread_id]
                if current_thread in instructions:
                    if "switch_idx" in instructions[current_thread]:
                        switch_at = instructions[current_thread]["switch_idx"]
                    else:
                        new_thread_id = instructions[current_thread]["new_thread_id"]
                        if "delete_before" in instructions[current_thread]:
                            delete_before = instructions[current_thread]["delete_before"]
                        elif "delete_after" in instructions[current_thread]:
                            delete_after = instructions[current_thread]["delete_after"]
                        elif "delete_between" in instructions[current_thread]:
                            delete_between = range(*instructions[current_thread]["delete_between"])
                else:
                    new_thread_id = current_thread

            counter += 1

            if switch_at:
                if counter < switch_at:
                    new_thread_id = row[thread_id] + "_start"
                else:
                    new_thread_id = row[thread_id] + "_end"

            elif delete_between and counter in delete_between:
                continue

            elif delete_before and counter <= delete_before:
                continue

            elif delete_after and counter >= delete_after:
                continue

            enricher.writerow(row, [new_thread_id])



if __name__ == "__main__":
    manual_thread_file = sys.argv[1]
    tweets_file = sys.argv[2]
    output_file = sys.argv[3]
    main(manual_thread_file, tweets_file, output_file)
