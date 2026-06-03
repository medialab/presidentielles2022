import casanova
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np
import argparse
import sys

parser = argparse.ArgumentParser(
    prog="Plot political users",
)
parser.add_argument("in_file_path")
parser.add_argument("out_file_path")
parser.add_argument("--ylabel", default="Nombre de tweets")
parser.add_argument("--title", default="Sans les non partisans")
parser.add_argument("--fence-sitters", action="store_true")


def plot_groups_per_day(
    file_path, out_file_path, ylabel, title, show_fence_sitters=False
):
    groups = [
        "non partisan",
        "partagé",
        "gauche radicale",
        "gauche",
        "gouvernement et centre",
        "droite",
        "extrême droite",
    ]
    colors = [
        "gainsboro",
        "lightgrey",
        "silver",
        "darkgray",
        "grey",
        "dimgrey",
        "black",
    ]
    days = [
        "2023-06-27",
        "2023-06-28",
        "2023-06-29",
        "2023-06-30",
        "2023-07-01",
        "2023-07-02",
        "2023-07-03",
        "2023-07-04",
        "2023-07-05",
    ]
    str_dates = [
        "27 juin",
        "28 juin",
        "29 juin",
        "30 juin",
        "1er juillet",
        "2 juillet",
        "3 juillet",
        "4 juillet",
        "5 juillet",
    ]

    if not show_fence_sitters:
        groups.pop(groups.index("partagé"))
        colors.pop(colors.index("lightgrey"))

    if file_path == "-":
        file_path = sys.stdin
    reader = casanova.reader(file_path)
    h = reader.headers
    group_dict = defaultdict(lambda: {day: 0 for day in days})

    for row in reader:
        if row[h.date] in days:
            group = row[h.user_candidate_group]
            if not group:
                if show_fence_sitters:
                    group = "partagé"
                else:
                    continue
            group_dict[group][row[h.date]] += int(row[h.count])

    x = np.arange(len(days))  # the label locations
    width = 1 / (len(groups) + 1)  # the width of the bars
    multiplier = 0

    fig, ax = plt.subplots(layout="constrained")
    plt.grid(visible=True, axis="y")

    for enum, (color, group) in enumerate(zip(colors, groups)):
        y = list(group_dict[group][day] for day in days)
        offset = width * multiplier
        rects = ax.bar(
            x + offset,
            y,
            width,
            label=group,
            color=color,
            hatch="/" * 2 * enum,
        )
        multiplier += 1

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xticks(x + 0.36, str_dates)
    ax.legend(loc="upper right")
    fig.set_size_inches(8, 10)

    handles, labels = plt.gca().get_legend_handles_labels()
    plt.legend(handles, labels)

    plt.savefig(out_file_path)


args = parser.parse_args()
plot_groups_per_day(
    args.in_file_path,
    args.out_file_path,
    args.ylabel,
    args.title,
    args.fence_sitters,
)
