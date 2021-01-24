from contextlib import contextmanager
import os

from quadtree import *
import tile_raster_rio
import rasterio as rio
from rasterio import Affine, MemoryFile
import rasterio.mask
from rasterio.merge import merge


@contextmanager
def write_mem_raster(data, **profile):
    with MemoryFile() as memfile:
        with memfile.open(**profile) as dataset:  # Open as DatasetWriter
            dataset.write(data)

        with memfile.open() as dataset:  # Reopen as DatasetReader
            yield dataset  # Note yield not return

def write_mem_raster_no_yield(data, **profile):
    out_ds = None
    with MemoryFile() as memfile:
        with memfile.open(**profile) as dataset:  # Open as DatasetWriter
            dataset.write(data)
        out_ds = memfile.open() 
    
    return out_ds # return DatasetReader

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
        
    qtile_properties_dict["TYPE"] = quadtree.node_type
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
        
def rec_tile_search(quadtree, query_shp_geom, query_shp_geom_boundary, out_shp_fh, qtile_properties_dict, qtile_length_limit=1024, qtile_acc=None):
    if (quadtree.boundary.to_shapely_poly().within(query_shp_geom)):
        # Set QuadTreeNodeType
        quadtree.node_type = QuadTreeNodeType.INSIDE

        # Return and write to output shapefile if tile is within query shapefile geometry
        qtile_properties_dict["TYPE"] = QuadTreeNodeType.INSIDE
        qtile_properties_dict["DEPTH"] = quadtree.depth
        qtile_properties_dict["CX"] = quadtree.boundary.cx
        qtile_properties_dict["CY"] = quadtree.boundary.cy
        qtile_properties_dict["MIN_X"] = quadtree.boundary.min_x
        qtile_properties_dict["MIN_Y"] = quadtree.boundary.min_y
        qtile_properties_dict["MAX_X"] = quadtree.boundary.max_x
        qtile_properties_dict["MAX_Y"] = quadtree.boundary.max_y
    
        write_rect_to_shp(quadtree.boundary, out_shp_fh, qtile_properties_dict)

        if qtile_acc is not None:
            qtile_acc.append(quadtree)

        return

    elif (quadtree.boundary.w <= qtile_length_limit) \
        or (quadtree.boundary.h <= qtile_length_limit):
        # Return after hitting tile length limit
        if quadtree.boundary.to_shapely_poly().intersects(query_shp_geom):

            # Set QuadTreeNodeType
            quadtree.node_type = QuadTreeNodeType.INTERSECTS
            
            # Write tile if it intersects query shapefile geometry
            qtile_properties_dict["TYPE"] = QuadTreeNodeType.INTERSECTS
            qtile_properties_dict["DEPTH"] = quadtree.depth
            qtile_properties_dict["CX"] = quadtree.boundary.cx
            qtile_properties_dict["CY"] = quadtree.boundary.cy
            qtile_properties_dict["MIN_X"] = quadtree.boundary.min_x
            qtile_properties_dict["MIN_Y"] = quadtree.boundary.min_y
            qtile_properties_dict["MAX_X"] = quadtree.boundary.max_x
            qtile_properties_dict["MAX_Y"] = quadtree.boundary.max_y
        
            write_rect_to_shp(quadtree.boundary, out_shp_fh, qtile_properties_dict)

            if qtile_acc is not None:
                qtile_acc.append(quadtree)
    
        return

    else:
        quadtree.divide()

        if quadtree.nw.boundary.intersects(query_shp_geom_boundary):
            rec_tile_search(quadtree.nw, query_shp_geom, query_shp_geom_boundary, out_shp_fh, qtile_properties_dict, qtile_length_limit, qtile_acc)
        
        if quadtree.ne.boundary.intersects(query_shp_geom_boundary):
            rec_tile_search(quadtree.ne, query_shp_geom, query_shp_geom_boundary, out_shp_fh, qtile_properties_dict, qtile_length_limit, qtile_acc)
        
        if quadtree.se.boundary.intersects(query_shp_geom_boundary):
            rec_tile_search(quadtree.se, query_shp_geom, query_shp_geom_boundary, out_shp_fh, qtile_properties_dict, qtile_length_limit, qtile_acc)
        
        if quadtree.sw.boundary.intersects(query_shp_geom_boundary):
            rec_tile_search(quadtree.sw, query_shp_geom, query_shp_geom_boundary, out_shp_fh, qtile_properties_dict, qtile_length_limit, qtile_acc)
        

if __name__ == "__main__":
    #Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--query_shp", help="Output shapefile")
    parser.add_argument("--out_shp", help="Output shapefile")
    parser.add_argument("--in_raster", help="Input raster")
    parser.add_argument("--out_raster_dir", help="Input raster")
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
    qtile_acc = []

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
        query_shapes =  [feature["geometry"] for feature in query_shp_fh]
        try:
            print(f"QUERY GEOMETRY: {len(shp_poly.geoms)}")
        except:
            pass
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
            rec_tile_search(qtree, shp_poly, shp_poly_boundary, output_shp_fh, base_qt_dict, qtile_length_limit=1024, qtile_acc=qtile_acc)
        
        print(f"ACCUMULATED QTILES: {len(qtile_acc)}")
        # qtile_acc = [
        #     # W, H, CX, CY, MIN_X, MIN_Y, MAX_X, MAX_Y
        #     (1024, 1024, 304472, 1452072, 302424, 1450024, 306520, 1454120),
        #     (1024, 1024, 308568, 1452072, 306520, 1450024, 310616, 1454120),
        #     (1024, 1024, 312664, 1452072, 310616, 1450024, 314712, 1454120),
        #     (1024, 1024, 308568, 1447976, 306520, 1445928, 310616, 1450024),
        #     (1024, 1024, 312664, 1456168, 310616, 1454120, 314712, 1458216),
        #     (1024, 1024, 308568, 1456168, 306520, 1454120, 310616, 1458216),
        # ]

        query_shp_name = os.path.basename(args.query_shp)
        raster_file_name = os.path.basename(args.in_raster)
        raster_file_ext = "tif"
        with rio.open(args.in_raster) as raster_ds:

            # raster_prj  = raster_ds.GetProjection()
            # raster_gt   = raster_ds.GetGeoTransform()
            raster_meta = raster_ds.meta.copy()
            raster_ncols, raster_nrows = raster_ds.meta['width'], raster_ds.meta['height']
            raster_band_num = 1
            print(f"RASTER META:")
            print(f"RASTER COLS: {raster_ncols}")
            print(f"RASTER ROWS: {raster_nrows}")


            tile_window_list = []
            tile_ds_list = []
            for tile_num in range(len(qtile_acc)):
                #width, height, cx, cy, min_x, min_y, max_x, max_y = qtile_acc[tile_num]
                
                width   = qtile_acc[tile_num].boundary.w
                height  = qtile_acc[tile_num].boundary.h
                cx      = qtile_acc[tile_num].boundary.cx
                cy      = qtile_acc[tile_num].boundary.cy
                min_x   = qtile_acc[tile_num].boundary.min_x
                min_y   = qtile_acc[tile_num].boundary.min_y
                max_x   = qtile_acc[tile_num].boundary.max_x
                max_y   = qtile_acc[tile_num].boundary.max_y

                #tile_window = get_tile_window(raster_ds, cx, cy, tile_size=width)
                tile_window = tile_raster_rio.get_tile_window_from_extents(raster_ds, min_x, min_y, max_x, max_y, tile_size=1024)
                # print(f"TILE WINDOW: {tile_window}") 
                tile_window_list.append(tile_window)

                tile_name = f"{query_shp_name}_{tile_num}.{raster_file_ext}"    
                tile_output_path = os.path.join(args.out_raster_dir, tile_name)
                # print(f"Writing to: [{tile_output_path}]")

                # Read the data in the window
                # clip is a nbands * N * N numpy array
                clip = raster_ds.read(window=tile_window)

                # You can then write out a new file
                meta = raster_ds.meta
                meta['width'], meta['height'] = tile_window.width, tile_window.height
                temp_gt = rio.windows.transform(tile_window, raster_ds.transform)
                meta['transform'] = temp_gt
                

                # print("--------")
                # print(f"{qtile_acc[tile_num].node_type} -- {qtile_acc[tile_num]}")
                # pprint(raster_ds.transform.f)
                # pprint(raster_ds.window_transform(tile_window))
                # pprint(raster_meta['transform'])
                # pprint(meta['transform'])

                # pprint(clip) 
                
                """"" NOTE: Write tiles snippet
                if qtile_acc[tile_num].node_type == QuadTreeNodeType.INTERSECTS:
                    
                    with write_mem_raster(clip, **meta) as clip_ds:
                        masked_clip, masked_transform = rasterio.mask.mask(clip_ds, query_shapes, crop=True)
                        meta.update({"driver": "GTiff",
                                "height": masked_clip.shape[1],
                                "width": masked_clip.shape[2],
                                "transform": masked_transform})
                        
                        with rio.open(tile_output_path, 'w', **meta) as raster_out_ds:
                            raster_out_ds.write(masked_clip)
                else:
                    with rio.open(tile_output_path, 'w', **meta) as raster_out_ds:
                        raster_out_ds.write(clip)
                #"""

                if qtile_acc[tile_num].node_type == QuadTreeNodeType.INTERSECTS:
                    
                    with write_mem_raster(clip, **meta) as clip_ds:
                        masked_clip, masked_transform = rasterio.mask.mask(clip_ds, query_shapes, crop=True)
                        meta.update({"driver": "GTiff",
                                "height": masked_clip.shape[1],
                                "width": masked_clip.shape[2],
                                "transform": masked_transform})
                    
                        
                    masked_ds = write_mem_raster_no_yield(masked_clip, **meta)
                    tile_ds_list.append(masked_ds)
                    print("INTERSECT -- "+str(masked_ds))
                else:
                    clip_ds = write_mem_raster_no_yield(clip, **meta)
                    print("WITHIN >>> " + str(clip_ds))
                    tile_ds_list.append(clip_ds)
            
            merge_ds, merge_transform = merge(tile_ds_list)
            raster_meta.update({"driver": "GTiff",
                                "height": merge_ds.shape[1],
                                "width": merge_ds.shape[2],
                                "transform": merge_transform})
            
            raster_out_name = f"{query_shp_name}_merged.{raster_file_ext}"    
            raster_out_path = os.path.join(args.out_raster_dir, raster_out_name)
                
            with rio.open(raster_out_path, 'w', **raster_meta) as raster_out_ds:
                raster_out_ds.write(merge_ds)
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

        