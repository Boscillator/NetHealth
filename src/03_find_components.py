import dask.bag as db
import networkx as nx
import pandas as pd
import time
import json
import community
import collections
import matplotlib.pyplot as plt
from dask.distributed import Client, LocalCluster


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
        return timestamp // period

    def revert(tup):
        ts = pd.Timestamp(tup[0] * period, unit='s').isoformat()
        return (ts, tup[1])

    return grouper, revert


def graph_binop(G: nx.Graph, edge):
    G.add_edge(edge[0], edge[1])
    return G


def graph_combine(G1: nx.Graph, G2: nx.Graph):
    g1 = set([e for e in G1.edges])
    g2 = set([e for e in G2.edges])
    G = nx.Graph()
    G.add_edges_from(g1 | g2)
    return G


def get_components(t):
    # components = list(nx.connected_components(t[1]))
    # return t[0], components

    G = t[1]
    partition = community.community_louvain.best_partition(G)
    communities = collections.defaultdict(set)
    for node,part in partition.items():
        communities[part].add(node)
    return t[0], list(communities.values())



def flatten(group):
    return [{
        'timestamp': group[0],
        'nodes': list(component)
    } for component in group[1]]


def find_components(inpath="data/02_normalize/*.csv", outpath="data/03_find_components/*.ndjson", period=60 * 60):

    with open("data/meta/period.txt", "w+") as f:
        f.write("<INVALID>")

    lines = db.read_text(inpath)
    edges = lines.str.strip().str.split(',')
    edges = edges.map(parse_edge).filter(not_none)

    key, revert = make_grouper(period)
    slices = edges.foldby(key, graph_binop, nx.Graph(), graph_combine, nx.Graph()).map(revert).repartition(12).persist()

    # store the first graph for diagnostics
    example_ts, example_G = slices.take(1)[0]
    nx.write_edgelist(example_G, "diagnostics/03_find_components/network_structure.txt")
    nx.draw_networkx(example_G, with_labels=False, node_size=20)
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
