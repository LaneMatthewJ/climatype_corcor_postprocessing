#!/usr/bin/env python3

import dask
import dask.dataframe as dd
import sys
from dask.distributed import Client
import os
from tqdm import tqdm 
import cudf 
import dask_cudf 
import pandas as pd
import time

def disconnect(client, workers_list):
    try:
        client.retire_workers(workers_list, close_workers=True)
        client.shutdown()
    except Exception as ex: 
        print("Exception in disconnect: ", ex)

def remove_date_refset(x): 
    x[0] = x[0][:-16]
    return x

def remove_date_for_cols(x):
    x[0] = x[0][:-16]
    x[1] = x[1][:-16]
    x[2] = x[2]
    return x

if __name__ == '__main__':

    sched_file = str(sys.argv[1]) #scheduler file
    num_workers = int(sys.argv[2]) # number of workers to wait for
    base_path = str(sys.argv[3])

    output_dir = f"{base_path}_top47k" if base_path[-1] != '/' else f'{base_path[:-1]}_top47k'
    

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
    print("Reading base data")
    df = dd.read_csv(f'{base_path}/*.txt', sep=' ', header=None, blocksize="400MB", dtype=str)
#    df = df.apply(remove_date_for_cols, axis = 1, meta=df)
    df = dask_cudf.from_dask_dataframe(df) 

    print('Creating mask')
    zero_mask = df[0].isin(fiftyk['0'])
    one_mask = df[1].isin(fiftyk['0'])

    current_mask = zero_mask | one_mask

    sliced = df[current_mask]

    print("Saving df")
    start_time=time.time()
    sliced.to_csv(output_dir, header=None, index=False)
    print("Took a total of ", (time.time() - start_time )/60, " minutes")
    
    if len(os.listdir(output_dir)) != sliced.npartitions: 
        raise Exception("Not all partitions saved to dir.")
    else: 
        print("len(os.listdir(output_dir)) != sliced.npartitions")
        print(len(os.listdir(output_dir)), "!=", sliced.npartitions)

    # 4. Shutting down the dask-cuda-cluster
    print("Shutting down the cluster")
    workers_list = list(workers_info)
    disconnect (client, workers_list)

