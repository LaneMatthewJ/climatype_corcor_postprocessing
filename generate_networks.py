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

# 3. Do computation
df_list = []
print('generating list') 
for file in tqdm(os.listdir(output_dir)):
    if os.path.isdir(f'{output_dir}/{file}') or '.npz' in file:
        continue
    df_list.append( pd.read_csv(f'{output_dir}/{file}', sep=',', header=None, dtype=str))

appended = pd.concat(df_list)

print('reading unique columns') 
unique_cols =  pd.read_csv(f'{output_dir}/unique/0.part', sep=',', header=None, dtype=str)

print('reading unique_map')
total_unique_file="/gpfs/alpine/syb105/proj-shared/Projects/Climatype/incite/global_yearly/comet_postpostprocessing_summit/reformatted_merged_combined_txts/unique_map/0.part"
total_unique_cols = pd.read_csv(total_unique_file, header=None, dtype=str)

print('extracting mask') 
missing_values_mask = total_unique_cols[0].isin(unique_cols[0])
missing_elements = total_unique_cols[ ~missing_values_mask]

print('Creating network') 
G = nx.from_pandas_edgelist(appended, source=0, target=1)

print("Adding missing elements") 
G.add_nodes_from(missing_elements[0].values)

print("Generating Sparse Matrix") 
sparse = nx.to_scipy_sparse_matrix(G, nodelist=total_unique_cols[0].values)

print("Saving sparse matrix")
scipy.sparse.save_npz(f'{output_dir}/adjacency.npz', sparse)
