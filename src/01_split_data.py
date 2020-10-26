import argparse
import dask.dataframe as dd
from dask.distributed import Client

def split(source_file, output_directory, npart, rename):
    df = dd.read_csv(source_file)
    df = df.repartition(npartitions=npart)
    if rename:
        df = df.rename(columns = {'from_egoid': 'egoid', 'to_egoid': 'alterid', 'timestamp':'epochtime'})
    df = df[['epochtime', 'egoid', 'alterid']]
    df['timestamp'] = dd.to_datetime(df['epochtime'], unit='ms')
    df = df.drop(columns='epochtime')
    df.to_csv(output_directory+ '/*.csv', index=False, line_terminator='\n')

if __name__ == '__main__':
    client = Client()
    print(f"Serving on {client.dashboard_link} with {client.cluster}")

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--npart', type=int, default=12)
    parser.add_argument('-r', '--rename', action='store_true')
    parser.add_argument('-o', '--output', default='data/01_split_data')
    parser.add_argument('source_file')
    args = parser.parse_args()
    split(args.source_file, args.output, args.npart, args.rename)