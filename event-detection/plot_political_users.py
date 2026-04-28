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
parser.add_argument("--non-partisans", action="store_true")


def plot_groups_per_day(
    file_path, out_file_path, ylabel, title, show_non_partisans=True
):
    groups = [
        "non partisan",
        "gauche radicale",
        "gauche",
        "gouvernement et centre",
        "droite",
        "extrême droite",
    ]
    colors = ["whitesmoke", "lightgrey", "silver", "darkgray", "dimgrey", "black"]
    days = set(
        [
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
    )
    str_dates = [
        "5 juillet",
        "4 juillet",
        "3 juillet",
        "2 juillet",
        "1er juillet",
        "30 juin",
        "29 juin",
        "28 juin",
        "27 juin",
    ]

    if not show_non_partisans:
        groups = groups[1:]
        colors = colors[1:]

    if file_path == "-":
        file_path = sys.stdin
    reader = casanova.reader(file_path)
    h = reader.headers
    group_dict = defaultdict(list)

    for row in reader:
        if row[h.date] in days:
            group = row[h.user_candidate_group]
            if not group:
                if show_non_partisans:
                    group = "non partisan"
                else:
                    continue
            group_dict[group].append(int(row[h.count]))

    x = np.arange(len(days))  # the label locations
    width = 1 / (len(groups) + 1)  # the width of the bars
    multiplier = 0

    fig, ax = plt.subplots(layout="constrained")
    plt.grid(visible=True, axis="x")

    for enum, (color, group) in enumerate(zip(colors, groups)):
        y = list(reversed(group_dict[group]))
        offset = width * multiplier
        rects = ax.barh(
            x + offset,
            y,
            width,
            label=group,
            color=color,
            hatch="/" * 2 * (enum + int(not show_non_partisans)),
        )
        multiplier += 1

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_title(title)
    ax.set_xlabel(ylabel)
    ax.set_yticks(x + 0.36, str_dates)
    ax.legend(loc="upper right")
    fig.set_size_inches(8, 10)

    plt.savefig(out_file_path)


args = parser.parse_args()
plot_groups_per_day(
    args.in_file_path,
    args.out_file_path,
    args.ylabel,
    args.title,
    args.non - partisans,
)
