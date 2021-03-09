from mpi4py import MPI

import shapely
from shapely.geometry import mapping, Polygon, shape, LineString
import fiona
from fiona.crs import from_epsg
from shapely.ops import unary_union

import rtree.index

import itertools, argparse, random

from pprint import pprint
from datetime import datetime
from itertools import islice

def split_every(n, iterable):
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))

def split_by_mod (k, iterable):
    n = len(iterable)
    return [iterable[(i*n)//k:((i+1)*n)//k] for i in range(k)]

def log_time_diff(start_time, end_time, label="EXEC_TIME"):
    time_diff = (end_time - start_time)
    exec_time_millis = time_diff.total_seconds() * 1000
    print(f"EXECTIME|{label}|{exec_time_millis}|milliseconds")

# comm = MPI.COMM_WORLD
# my_rank = cluster_comm.Get_rank()
# num_procs = cluster_comm.Get_size()
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Intersect GADM shapefile with coverage shapefile",
                                     epilog="Example: ...")

    parser.add_argument("cov_shp", help="Coverage shapefile")
    parser.add_argument("query_shp", help="Query shapefile")
    args = parser.parse_args()

    cluster_comm = MPI.COMM_WORLD
    cluster_size = cluster_comm.Get_size()
    cluster_worker_size = cluster_size-1
    cluster_rank = cluster_comm.Get_rank()
    node_name = MPI.Get_processor_name()
    print('cluster_size=%d, cluster_rank=%d, node:[%s]' % (cluster_size, cluster_rank, node_name))

    # Ping worker nodes
    # if cluster_rank != 0:
    #     message = f"Pong from [{cluster_rank}]"
    #     cluster_comm.send(message, dest=0)
    # else:
    #     for proc_id in range(1,cluster_size):
    #         message = cluster_comm.recv(source=proc_id)
    #         print(f"Process [0] receives from [{proc_id}]: {message}")

    #NOTE:Read query shapefile and broadcast
    query_feat_dict = None
    if cluster_rank == 0:
        with fiona.open(args.query_shp) as query_sh:
            query_feat_dict = dict(next(iter(query_sh)))

    query_feat_dict = cluster_comm.bcast(query_feat_dict, root=0)
    if cluster_rank != 0:
        print(f"R[{cluster_rank}] Broadcast received by [{cluster_rank}]:")
        # pprint(shape(query_feat_dict['geometry']))

    #NOTE:Read coverage shapefile and distribute geometry, then intersect with query shapefile
    # if cluster_rank == 0:
    #     with fiona.open(args.cov_shp) as cov_sh:
    #         # preserve the schema of the original shapefile, including the crs
    #         cov_meta = cov_sh.meta
    #         print("Read Shapefile:")
    #         pprint(cov_meta)

    #         #Send features 1 at a time
    #         for idx in range(1, cluster_size):
    #             print(f"Sending feature [{(idx-1)}] to Worker[{idx}]")
    #             cov_ft = cov_sh[idx-1]
    #             cluster_comm.send(dict(cov_ft), dest=idx, tag=1)
           
    # else:
    #     cov_ft = cluster_comm.recv(source=0, tag=1)
    #     cov_ft_geom = shape(cov_ft['geometry'])
    #     cov_ft_prop = cov_ft['properties']
        # print(f"Received by [{cluster_rank}]:")
        # pprint(cov_ft_geom)
        # pprint(cov_ft_prop)
    
    start_time=None
    end_time=None
    
    #NOTE: Scatter send features
    scatter_list = []
    if cluster_rank == 0:
        with fiona.open(args.cov_shp) as cov_sh:
            # preserve the schema of the original shapefile, including the crs
            cov_meta = cov_sh.meta
            print("R[{cluster_rank}] Read Shapefile:")
            pprint(cov_meta)
            
            start_time = datetime.now() if cluster_rank==0 else None
            
            print(f"R[{cluster_rank}] Indexing coverage features")
            rtree_idx = rtree.index.Index()
            
            # Index coverage feature bbox in rtree
            for pos, poly in enumerate(cov_sh):
                rtree_idx.insert(pos, shape(poly['geometry']).bounds)
            print(f"R[{cluster_rank}] Finished indexing")
            log_time_diff(start_time, datetime.now(),label="INDEX_TREE") if cluster_rank==0 else None
            
            query_geom = shape(query_feat_dict['geometry'])
            query_rtree_res = list(rtree_idx.intersection(query_geom.bounds))
            print(f"R[{cluster_rank}] BBOX query result: (len={len(query_rtree_res)}) {query_rtree_res}")
            
            # Shuffle list to randomly distribute workload
            random.shuffle(query_rtree_res)
            
            # Split list to scatter_list on (cluster_size) nodes, including root
            start_time = datetime.now() if cluster_rank==0 else None
            scatter_list = split_by_mod(cluster_worker_size, [ [idx, dict(cov_sh[idx])] for idx in query_rtree_res ])
            print(f"R[{cluster_rank}] Scatter list lens: {[len(ilist) for ilist in scatter_list]}")
    
    

    #NOTE: Receive scatter_list and process
    scatter_list.insert(0, None)    # None item at root, so no data is sent to root
    scatter_list = cluster_comm.scatter(scatter_list, root=0)
    if scatter_list is not None:
        print(f"R[{cluster_rank}] received scatter_list: {len(scatter_list)}")
    log_time_diff(start_time, datetime.now(),label="COMM_SCATTER_LIST") if cluster_rank==0 else None
    
    intersect_list = []
    if cluster_rank != 0: #Distribute workload to workers only
        start_time = datetime.now() if cluster_rank == 0 else None
        intersect_list = []
        query_geom = shape(query_feat_dict['geometry'])
        for idx, feature in scatter_list:
            feat_geom = shape(feature['geometry'])
            feat_dict = feature['properties']
            if query_geom.intersects(feat_geom):
                # intersect_list.append(feat_dict['UID'])
                intersect_list.append(idx)
        print(f"R[{cluster_rank}] Query results:  {len(intersect_list)} -- {intersect_list}")
    
    #NOTE: Gather results
    gathered_list = cluster_comm.gather(intersect_list, root=0)
    if cluster_rank == 0:
        gathered_list = list(itertools.chain.from_iterable(gathered_list))
        print(f"R[{cluster_rank}] Gathered list:  {len(gathered_list)} -- {gathered_list}")
    

    log_time_diff(start_time, datetime.now(),label="COMM_INTERSECT_GATHER") if cluster_rank==0 else None
    
    MPI.Finalize




