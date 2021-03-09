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

def extract_poly_coords(geom):
    if geom.type == 'Polygon':
        exterior_coords = geom.exterior.coords[:]
        interior_coords = []
        for interior in geom.interiors:
            interior_coords += interior.coords[:]
    elif geom.type == 'MultiPolygon':
        exterior_coords = []
        interior_coords = []
        for part in geom:
            epc = extract_poly_coords(part)  # Recursive call
            exterior_coords += epc['exterior_coords']
            interior_coords += epc['interior_coords']
    else:
        raise ValueError('Unhandled geometry type: ' + repr(geom.type))
    return {'exterior_coords': exterior_coords,
            'interior_coords': interior_coords}

def count_feature_points(feature_list):
    point_count_dict_aggr = dict()
    for feature in feature_list:
        feat_geom = shape(feature["geometry"])
        point_count_dict = extract_poly_coords(feat_geom)
        point_count_dict_aggr[feature["properties"]["BLOCK_NAME"]] = {  
            'exterior_coords': len(point_count_dict['exterior_coords']),
            'interior_coords': len(point_count_dict['interior_coords']),
            'block_name': feature["properties"]["BLOCK_NAME"]}

    return point_count_dict_aggr

def decompose_to_quadtree(feat_geom, tile_size):
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

def rec_qtree_decompose(qtree, geom, qtile_acc, qtile_length_limit=1024):
    if (qtree.boundary.w <= qtile_length_limit) \
        or (qtree.boundary.h <= qtile_length_limit):
        # Return after hitting tile length limit
        
        qtree.node_type = QuadTreeNodeType.INTERSECTS
        qtile_acc.append(qtree)    
        return

    elif (qtree.boundary.to_shapely_poly().within(geom)):
        qtile_acc.append(qtree)
        return

    else:
        qtree.divide()

        if qtree.nw.intersects_shapely_geom(geom):
            rec_qtree_decompose(qtree.nw, geom, qtile_acc, qtile_length_limit)
        
        if qtree.ne.intersects_shapely_geom(geom):
            rec_qtree_decompose(qtree.ne, geom, qtile_acc, qtile_length_limit)
        
        if qtree.se.intersects_shapely_geom(geom):
            rec_qtree_decompose(qtree.se, geom, qtile_acc, qtile_length_limit)
        
        if qtree.sw.intersects_shapely_geom(geom):
            rec_qtree_decompose(qtree.sw, geom, qtile_acc, qtile_length_limit)
        