import fiona
from shapely.geometry.geo import shape, mapping
from shapely.strtree import STRtree
from pprint import pprint
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from modquadtree import get_bounds_1x1km, get_pow2_extents, Node, QuadTree, write_tile_to_shape, get_tile_size
from fiona.crs import from_epsg
import argparse
from shapely.geometry.geo import shape, mapping
from osgeo import ogr


TILE_SIZE = 1024    #TODO: reconcile with Geotrellis tiling dimensions and units (pixels/mtr?)
FEATURE_COUNT_LIMIT = 2000


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("input_shp")
    parser.add_argument("output_shp")
    args = parser.parse_args()

    #Read CRS via GDAL osgeo library

    # infile = ogr.Open(args.input_shp)
    # layer = infile.GetLayer()
    # extent = layer.GetExtent()
    # # crs
    # spatialRef = layer.GetSpatialRef()
    # print(f"CRS: {spatialRef.ExportToWkt()}")
    # print(f"Extents: {extent}")

    #Transfer Variables
    pow2_bounds = None

    with fiona.open(args.input_shp, 'r', 'ESRI Shapefile') as input_shp_fh:
        print(f"CRS: {input_shp_fh.crs}")
        print(f"Features #: {str(len(input_shp_fh))}")
        
        count=0
        for data_feature in input_shp_fh:
            if count > FEATURE_COUNT_LIMIT:
                break 
            pprint(data_feature['properties'])
        
            #get feature geometry
            df_geom = shape(data_feature['geometry'])
            print(f"Shapefile Geom bounds: {df_geom.bounds}")

            pow2_bounds = get_pow2_extents(get_bounds_1x1km(df_geom.bounds), TILE_SIZE)
            print(f"Bounds^2: {str(pow2_bounds)}")

    """
        Gen QuadTree
    """
    rootrect = list(pow2_bounds)
    rootnode = Node(None, rootrect)
    tree = QuadTree(rootnode, TILE_SIZE, df_geom)
    print(f"Leaves: {str(len(tree.leaves))}")
    

    out_shp_schema = {
            'geometry': 'Polygon',
            'properties': dict([('EN_REF', 'str:254'), ('TYPE', 'int:1'),('MINX', 'float:19'), ('MINY', 'float:19'), ('MAXX', 'float:19'), ('MAXY', 'float:19')])
            }
    with fiona.open(args.output_shp, 'w','ESRI Shapefile', out_shp_schema, crs=from_epsg(32651), ) as output_shp_fh:
        min_x, min_y, max_x, max_y = pow2_bounds
        write_tile_to_shape((min_x, min_y, max_x, max_y), output_shp_fh, TILE_SIZE, Node.ROOT)
        for child in rootnode.children:
            print(f"{child.rect}")
            min_x, min_y, max_x, max_y = child.rect
            write_tile_to_shape((min_x, min_y, max_x, max_y), output_shp_fh, TILE_SIZE, Node.ROOT)
        
    
