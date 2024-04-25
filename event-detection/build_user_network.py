"""
Build a graph of users (who retweeted whom), based on a file of users and a file of tweets

"""
import csv
import sys
import casanova
import argparse
from tqdm import tqdm
from pelote import tables_to_graph
import networkx as nx

USERS = set()

def generate_edges(tweets, count=8407650):
    tweets_users = {}
    with casanova.reader(args.tweets) as reader:
        if reader.headers:
            user_id_pos = reader.headers.user_id
            id_pos = reader.headers.id
            retweeted_id_pos = reader.headers.retweeted_id


            for row in tqdm(reader, total=args.count):
                if not row[retweeted_id_pos]:
                    tweets_users[int(row[id_pos])] = int(row[user_id_pos])

    writer = csv.writer(sys.stdout)
    writer.writerow(["user_id", "retweeted_user_id", "tweet_local_time", "id", "retweeted_id"])

    with casanova.reader(tweets) as reader:
        if reader.headers:
            user_id_pos = reader.headers.user_id
            id_pos = reader.headers.id
            retweeted_id_pos = reader.headers.retweeted_id
            time_pos = reader.headers.tweet_local_time


            for row in tqdm(reader, total=count):
                if \
                row[retweeted_id_pos] and \
                int(row[retweeted_id_pos]) in tweets_users and \
                row[time_pos] < "2022-10-14":

                    writer.writerow([
                        row[user_id_pos],
                        tweets_users[int(row[retweeted_id_pos])],
                        row[time_pos],
                        row[id_pos],
                        row[retweeted_id_pos]
                        ])

                    yield {"user_id": int(row[user_id_pos]), "retweeted_user_id": tweets_users[int(row[retweeted_id_pos])]}


def generate_nodes(users):
    with casanova.reader(users) as reader:
        if reader.headers:
            user_id_pos = reader.headers.user_id
            description_pos = reader.headers.user_description
            screen_pos = reader.headers.user_screen_names
            tweet_count_pos = reader.headers.nb_original_tweets

            for row in reader:
                if int(row[tweet_count_pos]) >= 5:
                    USERS.add(int(row[user_id_pos]))
                yield {
                    "user_id": int(row[user_id_pos]),
                    "user_description": row[description_pos],
                    "user_screen_names": row[screen_pos]
                    }

parser = argparse.ArgumentParser(description="Build users network")
parser.add_argument("tweets", type=str, help="Path to the .csv file containing tweets")
parser.add_argument("users", type=str, help="Path to the .csv file containing users")
parser.add_argument("graph", type=str, help="Path of the .gexf file that will contain the resulting graph")
parser.add_argument("--count", "-c", type=int, help="Number of lines in tweets file")
args = parser.parse_args()

g = tables_to_graph(
    generate_nodes(args.users),
    generate_edges(args.tweets),
    node_col="user_id",
    edge_source_col="user_id",
    edge_target_col="retweeted_user_id",
    node_data=["user_description", "user_screen_names"],
    count_rows_as_weight=True,
    add_missing_nodes=False,
    directed=True
)

communities = nx.community.louvain_communities(g, resolution=0.7, seed=33)

for enum, c in enumerate(communities):
    if len(c) > 10:
        for node in c:
            if node in USERS:
                g.nodes[node]["color"] = enum
            else:
                g.remove_node(node)
    else:
        for node in c:
            if node not in USERS:
                g.remove_node(node)

nx.write_gexf(g, args.graph)





