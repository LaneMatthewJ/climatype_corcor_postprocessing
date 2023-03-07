#!/usr/bin/env python3

import dask
import dask_cudf
import dask.dataframe as dd
import sys
from dask.distributed import Client
import networkx as nx
import os
from tqdm import tqdm 
import scipy
import pandas as pd 
from scipy.spatial import distance
from 

def disconnect(client, workers_list):
    client.retire_workers(workers_list, close_workers=True)
    client.shutdown()

# if __name__ == '__main__':
print("MAIN") 
sched_file = str(sys.argv[1]) #scheduler file
num_workers = int(sys.argv[2]) # number of workers to wait for
output_dir = str(sys.argv[3])

# 1. Connects to the dask-cuda-cluster
#     client = Client(scheduler_file=sched_file)
#     print("client information ",client)

#     # 2. Blocks until num_workers are ready
#     print("Waiting for " + str(num_workers) + " workers...")
#     client.wait_for_workers(n_workers=num_workers)


#     workers_info=client.scheduler_info()['workers']
#     connected_workers = len(workers_info)
#     print(str(connected_workers) + " workers connected")
fiftyk=pd.read_csv("/gpfs/alpine/syb105/proj-shared/Projects/Climatype/incite/global_mean/corcor//downsampled_cluster_coords_global_mean_dates01-1958_12-2019_t0.964_i2_n400_10rand_reformatted.txt", header=None, dtype=str)

total_unique_file="/gpfs/alpine/syb105/proj-shared/Projects/Climatype/incite/global_yearly/comet_postpostprocessing_summit/reformatted_merged_combined_txts/unique_map/0.part"
total_unique_cols = pd.read_csv(total_unique_file, header=None, dtype=str)

mask = total_unique_cols[0].isin(fiftyk[0])
desired_rows = total_unique_cols[mask]

idx_list = list(total_unique_cols[mask].index)

# 3. Do computation
print("Reading 1958 network") 
sparse_matrix_1958 = scipy.sparse.load_npz("/gpfs/alpine/syb105/proj-shared/Projects/Climatype/incite/global_yearly/comet_postpostprocessing_summit/reformatted_merged_combined_txts/reformatted_merged_combined_global_yearly_2w_1958_top47k/adjacency.npz")


print("Reading reference net network") 
sparse_matrix_SD_YEAR = scipy.sparse.load_npz(f"{output_dir}/adjacency.npz")

print("Calculating SD for all Cluster Values") 
dist_dict = {}
for i in tqdm(range(len(idx_list))):
    index = idx_list[i]
    key = total_unique_cols.loc[index].values[0]
    dist_dict[key] = 1 - distance.dice(sparse_matrix_1958[index, :].toarray(), sparse_matrix_SD_YEAR[index, :].toarray())
    

print("Converting to Series") 
distance_series = pd.Series(dist_dict)
print("Saving to CSV in ", output_dir)
distance_series.to_csv(f'{output_dir}_SD.csv', header=None)