from dask_cuda import LocalCUDACluster

cluster = LocalCUDACluster()

print("LCC: ", cluster.cuda_visible_devices)

