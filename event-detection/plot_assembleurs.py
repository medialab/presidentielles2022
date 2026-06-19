import pandas as pd
import matplotlib.pyplot as plt
import argparse
import matplotlib.ticker as mtick


parser = argparse.ArgumentParser(
    prog="Plot political users",
)
parser.add_argument("in_file_path")
parser.add_argument("out_file_path")
parser.add_argument("--color", action="store_true")
args = parser.parse_args()

groups = [
    "non partisan",
    "partagé",
    "gauche",
    "gouvernement et centre",
    "droite",
    "extrême droite",
]
if args.color:
    colors = [
        "#BDBDBD", # non partisan
        "#9467BD", # partagé
        "#E41A1C", # gauche
        "#FFD700", # centre
        "#377EB8", # droite
        "#08306B", # extrême droite
    ]
else:
    colors = [
        "whitesmoke",
        "lightgrey",
        "darkgray",
        "grey",
        "dimgrey",
        "black",
    ]

df = pd.read_csv(args.in_file_path).set_index("index")
df["gauche"] = df["gauche"] + df["gauche radicale"]
df.drop("gauche radicale", axis=1, inplace=True)
df.index.names = ["assembleur"]
df = df.rename(
    index={"Utilisateur militant et groupe d'intérêt": "Militant et groupe d'intérêt"}
)


rename_dict = {
    i: i + "\n {} sous-événements".format(row["nb_events"]) for i, row in df.iterrows()
}
df = df.rename(index=rename_dict)
df.drop("nb_events", axis=1, inplace=True)

ax = df.plot.barh(
    figsize=(6, 9),
    color={group: color for group, color in zip(groups, colors)},
    edgecolor="black",
    fontsize=8,
    legend=False,
    width=0.7,
)
bars = ax.patches

hatches = []
r = range(len(groups) - 1)
r = [0] + list(r)
for i in r:
    hatch = "/" * 2 * i
    for j in range(len(df)):
        hatches.append(hatch)

for bar, hatch in zip(bars, reversed(hatches)):
    bar.set_hatch(hatch)
    bar.set_linewidth(0.001)

ax.get_xaxis().get_major_formatter().set_scientific(False)
ax.tick_params(axis="y", labelsize=8)
ax.xaxis.set_major_formatter(
    mtick.FuncFormatter(lambda x, _: f"{int(x):,}".replace(",", " "))
)
ax.set_ylabel("")

handles, labels = plt.gca().get_legend_handles_labels()
plt.legend(reversed(handles), reversed(labels), fontsize=8)

fig = ax.figure
fig.tight_layout()
fig.subplots_adjust(left=0.35)
fig.savefig(args.out_file_path)
