from mpi4py import MPI

import shapely
from shapely.geometry import mapping, Polygon, shape, LineString
import fiona
from fiona.crs import from_epsg
from shapely.ops import unary_union

import rtree.index
from quadtree import *
from quadtree_index_worker import *

import itertools, argparse, random
import local_config

from pprint import pprint
from datetime import datetime
from itertools import islice

def log_to_cluster(cluster_rank, msg):
    print(f"R[{cluster_rank}]>{msg}")

def split_every(n, iterable):
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))

def split_by_mod (k, iterable):
    n = len(iterable)
    return [iterable[(i*n)//k:((i+1)*n)//k] for i in range(k)]

def log_time_diff(cluster_rank, start_time, end_time, label="EXEC_TIME"):
    time_diff = (end_time - start_time)
    exec_time_millis = time_diff.total_seconds() * 1000
    print(f"R[{cluster_rank}]>EXECTIME|{label}|{exec_time_millis:.2f}|milliseconds")


def quadtree_tile_search(quadtree, query_shp_geom, query_shp_geom_boundary, qtile_properties_dict, qtile_length_limit=1024):
    pass

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Intersect GADM shapefile with coverage shapefile",
                                     epilog="Example: ...")

    parser.add_argument("cov_shp", help="Coverage shapefile")
    parser.add_argument("cov_out_dir", help="Coverage output directory")
    parser.add_argument("tile_size", type=int, help="Minimmum tile size in UTM51N")
#    parser.add_argument("raster_dir", help="Directory for rasters to pair with features in coverage shapefile")
    args = parser.parse_args()
    
    ROOT_coverage_output_dir = args.cov_out_dir
    CLUS_tile_size = args.tile_size
    
    cluster_comm = MPI.COMM_WORLD
    cluster_size = cluster_comm.Get_size()
    cluster_worker_size = cluster_size-1
    cluster_rank = cluster_comm.Get_rank()
    node_name = MPI.Get_processor_name()
    print('cluster_size=%d, cluster_rank=%d, node:[%s]' % (cluster_size, cluster_rank, node_name))

    ROOT_coverage_scatter_list = []
    if cluster_rank == 0:
        with fiona.open(args.cov_shp) as cov_sh:
            # preserve the schema of the original shapefile, including the crs
            cov_meta = cov_sh.meta
            
            # Split list to scatter_list on (cluster_size) nodes, including root
            cov_list = list(cov_sh)
            random.shuffle(cov_list)
            ROOT_coverage_scatter_list = [[]] + split_by_mod(cluster_worker_size, cov_list)
            log_to_cluster(cluster_rank, f"Coverage records: {len(cov_sh)}")
            log_to_cluster(cluster_rank, f"Scatter list len: {len(ROOT_coverage_scatter_list)}")
            
            for sublist in ROOT_coverage_scatter_list:
                print(len(sublist))

    CLUS_coverage_scatter_list = cluster_comm.scatter(ROOT_coverage_scatter_list, root=0)
    if cluster_rank > 0:
        log_to_cluster(cluster_rank, f"Received scatter_list: {len(CLUS_coverage_scatter_list)}")

        min_x = local_config.BASE_QUADTREE["min_x"]
        min_y = local_config.BASE_QUADTREE["min_y"]
        max_x = local_config.BASE_QUADTREE["max_x"]
        max_y = local_config.BASE_QUADTREE["max_y"]
        width = max_x - min_x
        height = max_y - min_y

        # print(f"R[{cluster_rank}] BOUNDS: ({min_x}, {min_y}, {max_x}, {max_y})")
        # print(f"R[{cluster_rank}] WIDTH: [{width}]")
        # print(f"R[{cluster_rank}] HEIGHT: [{height}]")

        root_bbox = Rect.from_extents(min_x, min_y, max_x, max_y)
        
        # print(f"R[{cluster_rank}] ROOT_BBOX: {root_bbox}")

        
        #pprint(count_feature_points(CLUS_coverage_scatter_list))
        start_time = datetime.now()
        feature_qtree_dict = dict()
        for ft_idx, feature in enumerate(CLUS_coverage_scatter_list):
            
            ft_geom = shape(feature["geometry"])
            ft_prop = feature["properties"]
            qtree_root = QuadTree(root_bbox, None)
            
            qtile_accumulator = []

            rec_qtree_decompose(qtree_root, ft_geom, qtile_accumulator, qtile_length_limit=CLUS_tile_size)
            ft_qtree_info = {  
                'block_name': ft_prop["BLOCK_NAME"],
                # 'qtree_tiles': [ qt.depth for qt in qtile_accumulator],
                'num_qtree_tiles': len(qtile_accumulator),
            }
            log_to_cluster(cluster_rank, f"Feature quadtree info: {ft_qtree_info}")
            feature_qtree_dict[ft_idx] = {
                'ft_idx': ft_idx,
                'ft_geom': ft_geom,
                'block_name': ft_prop["BLOCK_NAME"],
                'qtree_tiles': qtile_accumulator,
            }
        log_time_diff(cluster_rank, start_time, datetime.now(),label="INDEX-QUADTREE_DECOMPOSE")

        start_time = datetime.now()
        for key, ft_qtree in feature_qtree_dict.items():
            intersected_tiles = []
            for qtree_tile in ft_qtree["qtree_tiles"]:
                if qtree_tile.node_type == QuadTreeNodeType.INTERSECTS:
                    if ft_qtree["ft_geom"].is_valid:
                        intersected_tiles.append(qtree_tile.boundary.to_shapely_poly().intersection(ft_qtree["ft_geom"]))
                    else:
                        intersected_tiles.append(qtree_tile.boundary.to_shapely_poly().intersection(ft_qtree["ft_geom"].buffer(0)))

                else:
                    intersected_tiles.append(qtree_tile.boundary.to_shapely_poly())
            ft_qtree['intersected_tiles'] = intersected_tiles
            # log_to_cluster(cluster_rank, f"[{ft_qtree['block_name']}] Q tiles: {len(ft_qtree['qtree_tiles'])}")
            # log_to_cluster(cluster_rank, f"[{ft_qtree['block_name']}] I tiles: {len(ft_qtree['intersected_tiles'])}")
            num_qtree_tiles = len(ft_qtree['qtree_tiles'])
            num_inter_tiles = len(ft_qtree['intersected_tiles'])
            if num_qtree_tiles != num_inter_tiles:
                log_to_cluster(cluster_rank, f"[{ft_qtree['block_name']}] Mismatch found! Q:{num_qtree_tiles} vs I:{num_inter_tiles}")
            
            
                    
        log_time_diff(cluster_rank, start_time, datetime.now(),label="INDEX-QUADTREE_GET_TILE_INTERSECT")

    
        

        
        
    