import argparse
import dask.dataframe as dd
from dask.distributed import Client

MAX_ID = 99999

def normalize(inpath = "data/01_split_data/*.csv", outpath = "data/02_normalize/*.csv"):
    df = dd.read_csv(inpath, parse_dates=['timestamp'])
    df = df[df['egoid'] <= MAX_ID]
    df = df[df['alterid'] <= MAX_ID]
    df = df[df['egoid'] != df['alterid']]
    df.to_csv(outpath, index=False, line_terminator='\n')

if __name__ == '__main__':
    client = Client()
    print(f"Serving on {client.dashboard_link} with {client.cluster}")

    normalize()