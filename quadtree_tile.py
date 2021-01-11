from quadtree import *
    

def write_rect_to_shp(rect, shp_fh, properties_dict):
    """
    Write a {rect} into {shp_fg}
    #NOTE: {properties_dict} must adhere to schema of {shp_fh}"""
    tile_nw = (rect.min_x, rect.max_y)
    tile_sw = (rect.min_x, rect.min_y)
    tile_se = (rect.max_x, rect.min_y)
    tile_ne = (rect.max_x, rect.max_y)
    shp_fh.write({
            'geometry': mapping(Polygon([tile_nw, tile_sw, tile_se, tile_ne])),
            'properties': properties_dict,
        })

def write_quadtree_to_shp(quadtree, shp_fh, qtile_properties_dict):
    # Write boundary of current quadtree node
    #print(f"Writing to SHP at depth: {quadtree.depth}")
        
    qtile_properties_dict["DEPTH"] = quadtree.depth
    qtile_properties_dict["CX"] = quadtree.boundary.cx
    qtile_properties_dict["CY"] = quadtree.boundary.cy
    qtile_properties_dict["MIN_X"] = quadtree.boundary.min_x
    qtile_properties_dict["MIN_Y"] = quadtree.boundary.min_y
    qtile_properties_dict["MAX_X"] = quadtree.boundary.max_x
    qtile_properties_dict["MAX_Y"] = quadtree.boundary.max_y
    
    write_rect_to_shp(quadtree.boundary, shp_fh, qtile_properties_dict)

    # Recursively write quadrant tiles
    if quadtree.divided:
        write_quadtree_to_shp(quadtree.ne, shp_fh, qtile_properties_dict)
        write_quadtree_to_shp(quadtree.se, shp_fh, qtile_properties_dict)
        write_quadtree_to_shp(quadtree.sw, shp_fh, qtile_properties_dict)
        write_quadtree_to_shp(quadtree.nw, shp_fh, qtile_properties_dict)
        
def rec_tile_search(quadtree, query_shp_geom, query_shp_geom_boundary, out_shp_fh, qtile_properties_dict, qtile_length_limit=1024):
    if (quadtree.boundary.to_shapely_poly().within(query_shp_geom)):
        # Return after tile within geometry or tile limit
        qtile_properties_dict["DEPTH"] = quadtree.depth
        qtile_properties_dict["CX"] = quadtree.boundary.cx
        qtile_properties_dict["CY"] = quadtree.boundary.cy
        qtile_properties_dict["MIN_X"] = quadtree.boundary.min_x
        qtile_properties_dict["MIN_Y"] = quadtree.boundary.min_y
        qtile_properties_dict["MAX_X"] = quadtree.boundary.max_x
        qtile_properties_dict["MAX_Y"] = quadtree.boundary.max_y
    
        write_rect_to_shp(quadtree.boundary, out_shp_fh, qtile_properties_dict)
        return
    elif (quadtree.boundary.w <= qtile_length_limit) \
        or (quadtree.boundary.h <= qtile_length_limit):
        if quadtree.boundary.to_shapely_poly().intersects(query_shp_geom):
            qtile_properties_dict["DEPTH"] = quadtree.depth
            qtile_properties_dict["CX"] = quadtree.boundary.cx
            qtile_properties_dict["CY"] = quadtree.boundary.cy
            qtile_properties_dict["MIN_X"] = quadtree.boundary.min_x
            qtile_properties_dict["MIN_Y"] = quadtree.boundary.min_y
            qtile_properties_dict["MAX_X"] = quadtree.boundary.max_x
            qtile_properties_dict["MAX_Y"] = quadtree.boundary.max_y
        
            write_rect_to_shp(quadtree.boundary, out_shp_fh, qtile_properties_dict)
        return

    else:
        quadtree.divide()

        if quadtree.nw.boundary.intersects(query_shp_geom_boundary):
            rec_tile_search(quadtree.nw, query_shp_geom, query_shp_geom_boundary, out_shp_fh, qtile_properties_dict, qtile_length_limit)
        
        if quadtree.ne.boundary.intersects(query_shp_geom_boundary):
            rec_tile_search(quadtree.ne, query_shp_geom, query_shp_geom_boundary, out_shp_fh, qtile_properties_dict, qtile_length_limit)
        
        if quadtree.se.boundary.intersects(query_shp_geom_boundary):
            rec_tile_search(quadtree.se, query_shp_geom, query_shp_geom_boundary, out_shp_fh, qtile_properties_dict, qtile_length_limit)
        
        if quadtree.sw.boundary.intersects(query_shp_geom_boundary):
            rec_tile_search(quadtree.sw, query_shp_geom, query_shp_geom_boundary, out_shp_fh, qtile_properties_dict, qtile_length_limit)
        

if __name__ == "__main__":
    #Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("query_shp", help="Output shapefile")
    parser.add_argument("out_shp", help="Output shapefile")
    args = parser.parse_args()

    
    min_x = local_config.BASE_QUADTREE["min_x"]
    min_y = local_config.BASE_QUADTREE["min_y"]
    max_x = local_config.BASE_QUADTREE["max_x"]
    max_y = local_config.BASE_QUADTREE["max_y"]
    width = max_x - min_x
    height = max_y - min_y

    print(f"BOUNDS: ({min_x}, {min_y}, {max_x}, {max_y})")
    print(f"WIDTH: [{width}]")
    print(f"HEIGHT: [{height}]")

    bbox = Rect.from_extents(min_x, min_y, max_x, max_y)
    
    print(f"BBOX: {bbox}")

    qtree = QuadTree(bbox, None)
    

    out_shp_schema = {
            'geometry': 'Polygon',
            'properties': dict([('TYPE', 'int:2'), ('DEPTH', 'int:5'),
                ('CX', 'float:19'), ('CY', 'float:19'), 
                ('MIN_X', 'float:19'), ('MIN_Y', 'float:19'), 
                ('MAX_X', 'float:19'), ('MAX_Y', 'float:19')])
            }
    
    with fiona.open(args.query_shp, 'r', 'ESRI Shapefile') as query_shp_fh:
        #shp_geom = shape(shp.next()['geometry'])
        shp_poly = shape(query_shp_fh.next()['geometry'])
        print(f"QUERY GEOMETRY: {len(shp_poly.geoms)}")
        print(f"QUERY BOUNDS: {shp_poly.bounds}")
        shp_poly_boundary = Rect.from_extents(*shp_poly.bounds)
        
        with fiona.open(args.out_shp, 'w','ESRI Shapefile', out_shp_schema, crs=from_epsg(32651), ) as output_shp_fh:
            base_qt_dict = {
                "TYPE" : 0,
                "DEPTH" : 0,
                "CX" : bbox.cx,  "CY" : bbox.cy,
                "MIN_X" : bbox.min_x, "MIN_Y" : bbox.min_y, 
                "MAX_X" : bbox.max_x, "MAX_Y" : bbox.max_y
            }
            rec_tile_search(qtree, shp_poly, shp_poly_boundary, output_shp_fh, base_qt_dict, qtile_length_limit=1024)
        
        
    """
    # TEST: Generate base quadtree
    with fiona.open(args.out_shp, 'w','ESRI Shapefile', out_shp_schema, crs=from_epsg(32651), ) as output_shp_fh:

        #qtree.rec_divide(depth_limit=3, qtile_length_limit=1024)
        qtree.rec_divide(depth_limit=50, qtile_length_limit=1024)
    
        base_qt_dict = {
            "TYPE" : 0,
            "DEPTH" : 0,
            "CX" : bbox.cx,  "CY" : bbox.cy,
            "MIN_X" : bbox.min_x, "MIN_Y" : bbox.min_y, 
            "MAX_X" : bbox.max_x, "MAX_Y" : bbox.max_y
        }
        #write_rect_to_shp(bbox, output_shp_fh, base_qt_dict)
        write_quadtree_to_shp(qtree, output_shp_fh, base_qt_dict)
    #"""

    