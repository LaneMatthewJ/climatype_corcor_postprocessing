#!/usr/bin/env python3

import dask
import dask.dataframe as dd
import sys
from dask.distributed import Client

def disconnect(client, workers_list):
    client.retire_workers(workers_list, close_workers=True)
    client.shutdown()

if __name__ == '__main__':

    sched_file = str(sys.argv[1]) #scheduler file
    num_workers = int(sys.argv[2]) # number of workers to wait for

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
    # ...
    # ...

    df = dd.read_csv('/gpfs/alpine/syb105/proj-shared/Projects/Climatype/incite/global_yearly/comet_postprocessing_summit/postprocessed_txts_1958/out_*.txt')
    print(f"df.head = {df.head()}")
    print(f"df.shape = {df.shape}")
    print(f"df.memory_usage = {df.memory_usage()}")

    # 4. Shutting down the dask-cuda-cluster
    print("Shutting down the cluster")
    workers_list = list(workers_info)
    disconnect (client, workers_list)
