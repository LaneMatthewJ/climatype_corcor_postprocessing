# climatype_corcor_postprocessing

## Finding All Neighbors Within the Given Year:
### Code
  - find_all_yearly_neighbors.py
### Description
  - The 1TB Edge List Files are filtered to down to contain only the 47 thousand representative cluster nodes (10 nodes per cluster) and their neighbors (defined by: "/gpfs/alpine/syb105/proj-shared/Projects/Climatype/incite/global_mean/corcor//downsampled_cluster_coords_global_mean_dates01-1958_12-2019_t0.964_i2_n400_10rand_reformatted.txt") using dask_cudf.
  - The filtered dataset is saved to file within the top47k directories.


## Extract and Save Yearly Unique Elements:
### Code
  - extract_unique_nodes.py
### Description
  - Unique elements are extracted from each directory and saved in the *top47k/unique directory.


## Get A Union of All Unique Lists Per Year (for alignment of adjacency matrices)
### Code
  - get_total_unique_nodes.py
### Description
  - For each directory, all unique elements are extracted and then unioned to create a super-list of elements. 
  - super-list saved in element_map.txt


## Generate Networks
### Code
  - generate_networks.py

### Description
  - Networks were developed for each particular node set.
  - Missing nodes within each individual netowrk (i.e. nodes that exist in other networks but not the one under immediate construction) are added to the networks.
  - Scipy Sparse Adjacency Matrices are saved in specific order w/ respect to unique super-list.
  - Saved in the *top47k/adjacency



## Calculate Dice Similarity 
### Code
  - get_SD_multiprocessing.py
### Description
  - Dice Similarity (1-SD Dissimilarity) calculated using scipy across each year for all 47k representative nodes and saved within the *top47k/SD.csv


All *top47k/SD.csv files were then combined with respect to their shared indices.
    concatenate_all_SDs.py
