from typing import List, FrozenSet
import dask.bag as db
import time
import json
import numpy as np
import os
from dataclasses import dataclass, field
from dask.distributed import Client, LocalCluster
import matplotlib.pyplot as plt
import seaborn as sns

sns.set()


@dataclass(frozen=True)
class Component:
    timestamp: str
    nodes: FrozenSet[int]

    def to_dict(self):
        return {'timestamp': self.timestamp, 'nodes': list(self.nodes)}


@dataclass(frozen=True)
class Group:
    nodes: FrozenSet[int] = field(default=frozenset())
    components: FrozenSet[Component] = field(default=frozenset())

    def to_dict(self):
        return {
            'nodes': list(self.nodes),
            'components': list(map(Component.to_dict, self.components))
        }


def json_to_component(doc):
    return Group(
        frozenset(doc['nodes']),
        frozenset([Component(doc['timestamp'], frozenset(doc['nodes']))]))


def merge_groups(g1: Group, g2: Group):
    return Group(
        g1.nodes | g2.nodes,
        g1.components | g2.components
    )


def main(inpath="data/03_find_components/*.ndjson", outpath="data/04_pre_merge/*.ndjson"):
    groups = db.read_text(inpath).map(json.loads).map(json_to_component)
    # groups = groups.foldby(lambda g: g.nodes, merge_groups, Group()).map(lambda t: t[1]).persist()

    group_sizes = groups.map(lambda g: len(g.nodes)).compute()
    number_of_meetings = groups.map(lambda g: len(g.components)).compute()


    with open("data/meta/period.txt") as f:
        period = int(f.read())//60

    hist = np.unique(group_sizes, return_counts=True)
    if not os.path.exists("diagnostics/04_premerge/hist"):
        os.makedirs("diagnostics/04_premerge/hist")
    with open(f"diagnostics/04_premerge/hist/{period}min.json", "w+") as f:
        json.dump({
            'groups': hist[0].tolist(),
            'freq': hist[1].tolist()
        }, f)
        print(hist)

    plt.hist(group_sizes)
    plt.title(f"Group sizes pre-merge. [period={period}min]")
    plt.xlabel("Size of group")
    plt.ylabel("Frequency")
    plt.savefig("diagnostics/04_premerge/group_size_hist.png")
    plt.clf()

    plt.hist(number_of_meetings)
    plt.title("Number of meetings pre-merge")
    plt.xlabel("Number of meetings")
    plt.ylabel("Frequency")
    plt.savefig("diagnostics/04_premerge/meeting_num_hist.png")
    plt.clf()

    results = groups.map(Group.to_dict)
    results.map(json.dumps).to_textfiles(outpath)


if __name__ == '__main__':
    client = Client()
    print(f"Serving on {client.dashboard_link} with {client.cluster}")

    start = time.time()
    main()
    print(f"Ran in {time.time() - start:.2f}s")
