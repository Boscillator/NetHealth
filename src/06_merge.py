import json
import time


def intersection(g1, g2):
    return len(g1 & g2) / len(g1 | g2)


def compute_score_pairwise(clusters):
    results = []
    for i in range(len(clusters)):
        for j in range(i + 1, len(clusters)):
            results.append((i, j, intersection(clusters[i], clusters[j])))
    return sorted(results, key=lambda x: x[2])


def format_cluster(cluster):
    return set(cluster['nodes'])


def main(inpath="data/05_filter/0.ndjson", outpath="data/06_merge/results.ndjson"):
    with open(inpath) as f:
        clusters = map(json.loads, f)
        clusters = map(format_cluster, clusters)
        clusters = list(clusters)

        scores = compute_score_pairwise(clusters)
        print(scores)


if __name__ == '__main__':
    start = time.time()
    main()
    print(f"Ran in {time.time() - start:.2f}s")
