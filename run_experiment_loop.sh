#!/bin/bash
LOGFILE_PREFIX="$HOME/mpi_logs/quadtree_search_mpi_"
echo "REPS: $3"
echo "WORKERS: $1"
echo "Logging to $LOGFILE_PREFIX$1-$3.log"
for IDX in $(seq 1 $3); do 
    echo "REP# $IDX"
    mpirun.openmpi -mca orte_base_help_aggregate 0 -np $1 -machinefile mpi_host_list_1slot \
 ~/.virtualenvs/thesis-py3-rio/bin/python3 quadtree_search_mpi.py \
 /mnt/mpi_repo/data/vector/lidar_coverage/lidar_coverage.shp \
 $2 > "$LOGFILE_PREFIX$1-$IDX.log" 2>&1
done
