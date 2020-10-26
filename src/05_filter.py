import json
import dask.bag as db
from dask.distributed import Client
import time

def main(inpath="data/04_pre_merge/*.ndjson", outpath="data/05_filter/*.ndjson"):
    groups = db.read_text(inpath).map(json.loads)
    groups = groups.filter(lambda x: len(x['nodes']) > 2).persist()
    print(groups.count().compute())
    groups.map(json.dumps).to_textfiles(outpath)


if __name__ == '__main__':
    client = Client()
    print(f"Serving on {client.dashboard_link} with {client.cluster}")

    start = time.time()
    main()
    print(f"Ran in {time.time() - start:.2f}s")
