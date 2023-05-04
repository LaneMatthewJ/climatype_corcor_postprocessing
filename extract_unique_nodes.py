#!/usr/bin/env python3

import dask
import dask_cudf
import dask.dataframe as dd
import sys
from dask.distributed import Client
import os
from tqdm import tqdm 

def disconnect(client, workers_list):
    client.retire_workers(workers_list, close_workers=True)
    client.shutdown()

if __name__ == '__main__':

    sched_file = str(sys.argv[1]) #scheduler file
    num_workers = int(sys.argv[2]) # number of workers to wait for
    output_dir = str(sys.argv[3])

    # 1. Connects to the dask-cuda-cluster
    client = Client(scheduler_file=sched_file)
    print("client information ",client)

    # 2. Blocks until num_workers are ready
    print("Waiting for " + str(num_workers) + " workers...")
    client.wait_for_workers(n_workers=num_workers)


    workers_info=client.scheduler_info()['workers']
    connected_workers = len(workers_info)
    print(str(connected_workers) + " workers connected")

    # 3. Do computation
    #cudf_list = []
    #for file in tqdm(os.listdir(output_dir)):
    #    if os.path.isdir(f'{output_dir}/{file}'):
    #        continue
    #    cudf_list.append( dask_cudf.read_csv(f'{output_dir}/{file}', sep=',', header=None, dtype=['str','str'] ))

    
    #appended = dask_cudf.concat(cudf_list)

    df = dd.read_csv(f"{output_dir}/*", header=None, dtype='str', blocksize="400MB")
    appended= dask_cudf.from_dask_dataframe(df)

    unique_zero = appended[0].unique()
    unique_one = appended[1].unique()

    unique_both = dask_cudf.concat([unique_zero, unique_one]).unique()

    # curiously, dask_cudf.Series does not have a to_csv function
    # in this version: 
    ddf = unique_both.to_dask_dataframe()
    ddf.to_csv(f'{output_dir}/unique', header=None, index=False)

    # 4. Shutting down the dask-cuda-cluster
    print("Shutting down the cluster")
    workers_list = list(workers_info)
    disconnect (client, workers_list)
