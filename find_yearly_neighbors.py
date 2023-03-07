#!/usr/bin/env python3

import dask
import dask.dataframe as dd
import sys
from dask.distributed import Client
import os
from tqdm import tqdm 
import cudf 
import dask_cudf 

def disconnect(client, workers_list):
    client.retire_workers(workers_list, close_workers=True)
    client.shutdown()

if __name__ == '__main__':

    sched_file = str(sys.argv[1]) #scheduler file
    num_workers = int(sys.argv[2]) # number of workers to wait for
    base_path = str(sys.argv[3])

    output_dir = f"{base_path.split('.txt')[0]}_top47k"
    

    # 1. Connects to the dask-cuda-cluster
    client = Client(scheduler_file=sched_file)
    print("client information ",client)

    # 2. Blocks until num_workers are ready
    print("Waiting for " + str(num_workers) + " workers...")
    client.wait_for_workers(n_workers=num_workers)


    workers_info=client.scheduler_info()['workers']
    connected_workers = len(workers_info)
    print(str(connected_workers) + " workers connected")

    fiftyk=cudf.read_csv("/gpfs/alpine/syb105/proj-shared/Projects/Climatype/incite/global_mean/corcor//downsampled_cluster_coords_global_mean_dates01-1958_12-2019_t0.964_i2_n400_10rand_reformatted.txt", header=None, dtype=['str'])


    
    # 3. Do computation
    df = dask_cudf.read_csv(base_path, sep=' ', header=None, dtype=['str','str'])

    zero_mask = df['0'].isin(fiftyk['0'])
    one_mask = df['1'].isin(fiftyk['0'])

    current_mask = zero_mask | one_mask

    sliced = df[current_mask]

    sliced.to_csv(output_dir, header=None, index=False)
        
    # 4. Shutting down the dask-cuda-cluster
    print("Shutting down the cluster")
    workers_list = list(workers_info)
    disconnect (client, workers_list)
