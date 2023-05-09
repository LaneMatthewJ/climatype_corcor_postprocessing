# climatype_corcor_postprocessing

Given the nature of the climatype dataset, calculating the Sorensen Dice coefficient between each year and 1958 required usage of distributed methods. Dask is python framework that allows for simple and easy scaling. For each yearly range, we used dask data frames to read in all 2880 cor-cor output files as a singular dask dataframe object.  Cluster representatives and their immediate neighbors were then extracted and saved to files (done via the individual workers). Unique points were then extracted from each year and collated into a singular list of unique nodes to ensure similar sized matrices for Sorensen Dice calculations. Networks were then generated from the cluster representatives and their neighbors for each given year and then saved as adjacency matrices, from which Sorensen Dice coefficients were calculated for all years with respect to the starting year.


### 1. Finding All Neighbors Within the Given Year:
#### Code
  - find_all_yearly_neighbors_multiprocess.py
#### Description
  - The Edge List Files are loaded via dask dataframes and filtered to down to contain only the 47 thousand representative cluster nodes (10 nodes per cluster) and their neighbors (defined by: "downsampled_cluster_coords_global_mean_dates01-1958_12-2019_t0.964_i2_n400_10rand_reformatted.txt") using dask_cudf.
  - The filtered dataset is saved to file within a new directory with the same name and a `top47k` suffix as dask dataframes save data per worker (and result in many `.part*` files).


### 2. Extract and Save Yearly Unique Elements:
#### Code
  - extract_unique_nodes.py
#### Description
  - Unique elements are extracted from all files within each `*top47k` directory and saved in the: `./*top47k/unique` directories.


### 3. Get A Union of All Unique Lists Per Year (for alignment of adjacency matrices)
#### Code
  - get_total_unique_nodes.py
#### Description
  - For each directory, all unique elements are extracted and then unioned to create a super-list of unique elements. 
  - super-list saved in element_map.txt


### 4. Generate Networks
#### Code
  - generate_networks.py

#### Description
  - Networks were developed for each unique node set per year.
  - Missing nodes within each individual netowrk (i.e. nodes that exist in other networks but not the one under immediate construction) are added to the networks (from the super-list: `element_map.txt`).
  - Scipy Sparse Adjacency Matrices are saved in specific order w/ respect to unique super-list.
  - Saved in the `./*top47k/adjacency.npz`



### 5. Calculate Dice Similarity 
#### Code
  - get_SD_multiprocessing.py
#### Description
  - Dice Similarity (1-SD Dissimilarity) calculated using scipy across each year for all 47k representative nodes and saved within the `./top47k/SD.csv`

    
    
### 6. Concatenate All SD values: 
#### Code
  - concatenate_all_SDs.py
#### Description
  - All `./*top47k/SD.csv` files were then combined with respect to their shared indices.
