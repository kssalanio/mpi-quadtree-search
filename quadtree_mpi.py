from mpi4py import MPI

import shapely
from shapely.geometry import mapping, Polygon, shape, LineString
import fiona
from fiona.crs import from_epsg
from shapely.ops import unary_union

import rtree.index
from quadtree import *

import itertools, argparse, random
import local_config

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

def quadtree_tile_search(quadtree, query_shp_geom, query_shp_geom_boundary, qtile_properties_dict, qtile_length_limit=1024):
    pass

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

    #NOTE: Read query shapefile and broadcast
    CLUS_query_feat_dict = None
    
    if cluster_rank == 0:
        with fiona.open(args.query_shp) as query_sh:
            CLUS_query_feat_dict = dict(next(iter(query_sh)))

    CLUS_query_feat_dict         = cluster_comm.bcast(CLUS_query_feat_dict, root=0)
    CLUS_query_shp_geom          = shape(CLUS_query_feat_dict['geometry'])
    CLUS_query_shp_geom_boundary = Rect.from_extents(*CLUS_query_shp_geom.bounds)

    if cluster_rank != 0:
        print(f"R[{cluster_rank}] Broadcast received by [{cluster_rank}]:")

    """
        NOTE:
        Quadtree Intersection loop
        Executes only on master node
    """
    CLUS_qtree_scatter_list = []
    ROOT_qtree_scatter_list = []
    if cluster_rank == 0:
        min_x = local_config.BASE_QUADTREE["min_x"]
        min_y = local_config.BASE_QUADTREE["min_y"]
        max_x = local_config.BASE_QUADTREE["max_x"]
        max_y = local_config.BASE_QUADTREE["max_y"]
        width = max_x - min_x
        height = max_y - min_y

        print(f"BOUNDS: ({min_x}, {min_y}, {max_x}, {max_y})")
        print(f"WIDTH: [{width}]")
        print(f"HEIGHT: [{height}]")

        root_bbox = Rect.from_extents(min_x, min_y, max_x, max_y)
        
        print(f"ROOT_BBOX: {root_bbox}")

        qtree_root = QuadTree(root_bbox, None)
        ROOT_qtree_scatter_list = [qtree_root]
        qtree_accumulator_list = []

        #NOTE: Process on master until smallest quadrant fits query geometry bbox

        while len(ROOT_qtree_scatter_list) <= 4:
            tmp_scatter_list = []
            for qtree in ROOT_qtree_scatter_list:
                # print(f"R[{cluster_rank}] Dividing at QTree depth: {qtree.depth}")
                qtree.divide()

                if qtree.nw.boundary.intersects(CLUS_query_shp_geom_boundary):
                    tmp_scatter_list.append(qtree.nw)

                if qtree.ne.boundary.intersects(CLUS_query_shp_geom_boundary):
                    tmp_scatter_list.append(qtree.ne)
                    
                if qtree.se.boundary.intersects(CLUS_query_shp_geom_boundary):
                    tmp_scatter_list.append(qtree.se)
                    
                if qtree.sw.boundary.intersects(CLUS_query_shp_geom_boundary):
                    tmp_scatter_list.append(qtree.sw)
                
            ROOT_qtree_scatter_list = tmp_scatter_list
            pprint(ROOT_qtree_scatter_list)
            
        print("R[{cluster_rank}] LOOP FINISHED! Result Scatter List:")
        
        
    #NOTE: Receive scatter_list and process
    # start_time = datetime.now() if cluster_rank==0 else None
    # log_time_diff(start_time, datetime.now(),label="COMM_SCATTER_LIST") if cluster_rank==0 else None
    
    
    """
        Quadtree descent with scatter-gather
        NOTE: may need to be restricted to exclude master node
    """
    CLUS_qtree_gather_list = []
    ROOT_qtree_terminal_list = []
    loop_qtree_search = True
    #TODO: translate terminating conditions to loop break
    while loop_qtree_search:
        if cluster_rank ==0:
            # NOTE: split by number of workers, but put None in place of root before scattering
            if not ROOT_qtree_scatter_list:
                ROOT_qtree_scatter_list =  [ None for _ in range(cluster_size) ]
            else:
                ROOT_qtree_scatter_list = split_by_mod(cluster_size-1, [  qtree for qtree in ROOT_qtree_scatter_list ])
                ROOT_qtree_scatter_list.insert(0, [])
                print(f"R[{cluster_rank}] Scatter list lens: {[len(ilist) for ilist in ROOT_qtree_scatter_list[1:]]}")
    
        CLUS_qtree_scatter_list = cluster_comm.scatter(ROOT_qtree_scatter_list, root=0)
        if CLUS_qtree_scatter_list is None: # Terminating Condition: Root node scatters 'None' to each node, including itself
            if cluster_rank == 0:
                print(f"R[{cluster_rank}] ROOT_qtree_scatter_list len: {ROOT_qtree_scatter_list}")
                print(f"R[{cluster_rank}] ROOT_qtree_terminal_list len: {len(ROOT_qtree_terminal_list)}")
            break

        tmp_scatter_list = None
        tmp_terminal_list = None

        if cluster_rank > 0:
            print(f"R[{cluster_rank}] received scatter_list: {len(CLUS_qtree_scatter_list)}")
            tmp_scatter_list = []
            tmp_terminal_list = []
            # Evaluate each 
            for qtree in CLUS_qtree_scatter_list:
                if (qtree.boundary.w <= local_config.TILE_SIZE) \
                    or (qtree.boundary.h <= local_config.TILE_SIZE):
                
                    qtree.node_type = QuadTreeNodeType.INTERSECTS
                    tmp_terminal_list.append(qtree)

                elif (qtree.boundary.to_shapely_poly().within(CLUS_query_shp_geom)):
                    # Set QuadTreeNodeType
                    qtree.node_type = QuadTreeNodeType.INSIDE
                    tmp_terminal_list.append(qtree)
    
                else:
                    qtree.divide()

                    if qtree.nw.intersects_shapely_geom(CLUS_query_shp_geom):
                        tmp_scatter_list.append(qtree.nw)

                    if qtree.ne.intersects_shapely_geom(CLUS_query_shp_geom):
                        tmp_scatter_list.append(qtree.ne)
                        
                    if qtree.se.intersects_shapely_geom(CLUS_query_shp_geom):
                        tmp_scatter_list.append(qtree.se)
                        
                    if qtree.sw.intersects_shapely_geom(CLUS_query_shp_geom):
                        tmp_scatter_list.append(qtree.sw)
        elif cluster_rank == 0:
            tmp_scatter_list = []
            tmp_terminal_list = []
        
        CLUS_qtree_gather_list = cluster_comm.gather([tmp_scatter_list, tmp_terminal_list], root=0)
        
        if cluster_rank == 0:
            ROOT_qtree_scatter_list = []
        
            # Split each sublist into scatter lists and terminal lists
            for tmp_scatter_list, tmp_terminal_list in CLUS_qtree_gather_list:
                ROOT_qtree_scatter_list.extend(tmp_scatter_list)
                ROOT_qtree_terminal_list.extend(tmp_terminal_list)
            
            # NOTE: debug print
            pprint([qt.depth for qt in ROOT_qtree_scatter_list])
            # pprint([qt.depth for qt in ROOT_qtree_terminal_list])
        
        
    
    MPI.Finalize
            
        
        





        

    
