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
    n = int(sys.argv[2]) # number of workers to wait for
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
    cudf_list = []
for file in tqdm(os.listdir(output_dir)):
    if 'top47' not in file:
        continue 
    if not os.path.isdir(f'{output_dir}/{file}') or file == "reformatted_network_values" or file == "unique_map" or file == "old_unique_map":
        continue
    cudf_list.append( dask_cudf.read_csv(f'{output_dir}/{file}/unique/0.part', sep=',', header=None, dtype=['str']))

    appended = dask_cudf.concat(cudf_list)

    unique_zero = appended['0'].unique()

    # curiously, dask_cudf.Series does not have a to_csv function
    # in this version: 
    ddf = unique_zero.to_dask_dataframe()
    ddf.to_csv(f'{output_dir}/unique_map', header=None, index=False)

    # 4. Shutting down the dask-cuda-cluster
    print("Shutting down the cluster")
    workers_list = list(workers_info)
    disconnect (client, workers_list)
