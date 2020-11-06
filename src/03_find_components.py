import dask.bag as db
import networkx as nx
import pandas as pd
import time
import json
import community
import collections
import matplotlib.pyplot as plt
from dask.distributed import Client, LocalCluster
from karateclub import EgoNetSplitter


def not_none(edge):
    return edge is not None


def parse_edge(edge):
    try:
        return (int(edge[0]), int(edge[1]), pd.to_datetime(edge[2]))
    except ValueError:
        return None


def make_grouper(period):
    def grouper(edge):
        timestamp = (edge[2] - pd.Timestamp('1970-01-01')).total_seconds()
        return timestamp // period, edge[0], edge[1]

    def revert(tup):
        ts = pd.Timestamp(tup[0] * period, unit='s').isoformat()
        return (ts, tup[1])

    return grouper, revert


def get_components(t):
    # components = list(nx.connected_components(t[1]))
    # return t[0], components

    G = t[1]

    try:
        model = EgoNetSplitter()
        model.fit(G)
        partition = model.get_memberships()
        communities = collections.defaultdict(set)
        for node, groups in partition.items():
            for group in groups:
                communities[node].add(group)
    except:
        partition = community.community_louvain.best_partition(G)
        communities = collections.defaultdict(set)
        for node, group in partition.items():
            communities[node].add(group)
    
    return t[0], list(communities.values())


def combine_edges_to_graph(G: nx.Graph, edge):
    G = G.copy()
    G.add_edge(edge[0][1], edge[0][2], weight=edge[1])
    return G


def graph_combine(G1: nx.Graph, G2: nx.Graph):
    g1 = {(e[0], e[1]): e[2] for e in G1.edges(data='weight')}
    g2 = {(e[0], e[1]): e[2] for e in G2.edges(data='weight')}
    all_edges = set(g1.keys()) | set(g2.keys())
    G = nx.Graph()
    for edge in all_edges:
        G.add_edge(edge[0], edge[1], weight=g1.get(edge, 0) + g2.get(edge, 0))
    return G


def flatten(group):
    return [{
        'timestamp': group[0],
        'nodes': list(component)
    } for component in group[1]]


def find_components(inpath="data/02_normalize/*.csv", outpath="data/03_find_components/*.ndjson", period=24 * 60 * 60):
    with open("data/meta/period.txt", "w+") as f:
        f.write("<INVALID>")

    lines = db.read_text(inpath)
    edges = lines.str.strip().str.split(',')
    edges = edges.map(parse_edge).filter(not_none)

    key, revert = make_grouper(period)
    sliced_weighted_edges = edges.foldby(key, lambda x, _: x + 1, 0, lambda x, y: x + y, 0).repartition(30)
    slices = sliced_weighted_edges.foldby(lambda x: x[0][0], combine_edges_to_graph, nx.Graph(), graph_combine,
                                          nx.Graph())
    slices = slices.repartition(12).persist()

    # store the first graph for diagnostics
    example_ts, example_G = slices.take(1)[0]

    weights = [e[2] for e in example_G.edges(data='weight')]
    weights = [w/max(weights) for w in weights]
    nx.write_edgelist(example_G, "diagnostics/03_find_components/network_structure.txt")
    nx.draw_circular(example_G, with_labels=False, node_size=20, edge_color=weights)
    plt.title(example_ts)
    plt.savefig("diagnostics/03_find_components/network_structure.png")

    # compute connected components
    components = slices.map(get_components)
    flattened_components = components.map(flatten).flatten()
    flattened_components.map(json.dumps).to_textfiles(outpath)

    with open("data/meta/period.txt", "w+") as f:
        f.write(str(period))


if __name__ == '__main__':
    cluster = LocalCluster(n_workers=12, threads_per_worker=1, memory_limit='166GB')
    client = Client(cluster)
    print(f"Serving on {client.dashboard_link} with {client.cluster}")

    start = time.time()
    find_components()
    print(f"Ran in {time.time() - start:.2f}s")
