#!/bin/bash -l

module load ums
module load ums-gen119
module load nvidia-rapids

echo scheduler addendum is $1 

SCHEDULER_DIR=/gpfs/alpine/syb105/proj-shared/Personal/lanemj/projects/climatypes/dask_neighbors/dask_scheduler
WORKER_DIR=/gpfs/alpine/syb105/proj-shared/Personal/lanemj/projects/climatypes/dask_neighbors/worker_dir/$1

SCHEDULER_FILE=$SCHEDULER_DIR/my-scheduler$1.json

echo 'Running scheduler'
jsrun --nrs 1 --tasks_per_rs 1 --cpu_per_rs 2 --smpiargs="-disable_gpu_hooks" \
      dask-scheduler --interface ib0 --scheduler-file $SCHEDULER_FILE\
                     --no-dashboard --no-show &

#Wait for the dask-scheduler to start
sleep 10

jsrun --rs_per_host 6 --tasks_per_rs 1 --cpu_per_rs 2 --gpu_per_rs 1 --smpiargs="-disable_gpu_hooks" \
      dask-cuda-worker --nthreads 1 --memory-limit 82GB --device-memory-limit 16GB --rmm-pool-size=15GB \
                       --interface ib0 --scheduler-file $SCHEDULER_FILE --local-directory $WORKER_DIR \
                       --no-dashboard &

#Wait for WORKERS
sleep 10

WORKERS=12

#python -u $CONDA_PREFIX/examples/blazingsql/bsql_test_multi.py $SCHEDULER_FILE #WORKERS

#wait

#clean LOG files
#rm -fr $SCHEDULER_DIR
