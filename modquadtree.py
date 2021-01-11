# quadtree.py
# Implements a Node and QuadTree class that can be used as 
# base classes for more sophisticated implementations.
# Malcolm Kesson Dec 19 2012
import os
import argparse
import fiona
import math
import sys
import time
from shapely.geometry.geo import mapping, shape
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.linestring import LineString
from shapely.geometry.point import Point
from shapely.ops import cascaded_union
from fiona.crs import from_epsg

global TILE_SIZE
TILE_SIZE = 100
    
class Node():
    ROOT = 0
    BRANCH = 1
    LEAF = 2
    minsize = 1   # Set by QuadTree
    #_______________________________________________________
    # In the case of a root node "parent" will be None. The
    # "rect" lists the minx,minz,maxx,maxz of the rectangle
    # represented by the node.
    def __init__(self, parent, rect):
        self.parent = parent
        self.children = [None,None,None,None]
        if parent == None:
            self.depth = 0
        else:
            self.depth = parent.depth + 1
        self.rect = rect
        #print("NODE RECT: "+str(rect)+" DEPTH: "+str(self.depth))
        x0,z0,x1,z1 = rect
        if self.parent == None:
            self.type = Node.ROOT
        elif (x1 - x0) <= Node.minsize:
            self.mark_as_leaf()
        else:
            self.type = Node.BRANCH
    
    def mark_as_leaf(self):
        self.type = Node.LEAF
    #_______________________________________________________
    # Recursively subdivides a rectangle. Division occurs 
    # ONLY if the rectangle spans a "feature of interest".
    def subdivide(self, ref_geom):
        if self.type == Node.LEAF:
            return
        x0,z0,x1,z1 = self.rect
        h = (x1 - x0)/2
        
        # SPACE FILLING CURVE TRAVERSAL?
        rects = []
        #ul_quad = 
        #dl_quad
        #dr_quad
        #ur_quad
        rects.append( (x0, z0, x0 + h, z0 + h) )
        rects.append( (x0, z0 + h, x0 + h, z1) )
        rects.append( (x0 + h, z0 + h, x1, z1) )
        rects.append( (x0 + h, z0, x1, z0 + h) )
        for n in range(len(rects)):
            #Spanning check
            #~ span = self.spans_feature(rects[n], ref_shp)
            #~ print("DEPTH: "+str(self.depth) + " N-SON: "+str(n) + " SPAN: " + str(span))
            #~ if span == True:
                #~ #Creates node
                #~ self.children[n] = self.getinstance(rects[n])
                #~ #Subdivides node
                #~ self.children[n].subdivide(ref_shp) # << recursion
            
            #
            poly_int = self.get_intersection_geometry(rects[n], ref_geom)
            if poly_int:
                self.children[n] = self.getinstance(rects[n])
                if not is_square(poly_int):
                    self.children[n].subdivide(ref_geom) # << recursion
                else:   #Mark full squares as leaves
                    self.children[n].mark_as_leaf()
            
    #_______________________________________________________
    # A utility proc that returns True if the coordinates of
    # a point are within the bounding box of the node.
    def contains(self, x, z):
        x0,z0,x1,z1 = self.rect
        if x >= x0 and x <= x1 and z >= z0 and z <= z1:
            return True
        return False
    #_______________________________________________________
    # Sub-classes must override these two methods.
    def getinstance(self,rect):
        return Node(self, rect)      
        
    def spans_feature(self, rect, ref_shape):
        min_x, min_y, max_x, max_y = rect
        tile_ulp = (min_x, max_y)
        tile_dlp = (min_x, min_y)
        tile_drp = (max_x, min_y)
        tile_urp = (max_x, max_y)
        tile = Polygon([tile_ulp, tile_dlp, tile_drp, tile_urp])
        #return not tile.intersection(geom).is_empty
        for feature in ref_shape:
            shp_geom = shape(feature['geometry'])
            poly_int =tile.intersection(shp_geom)
            if not poly_int.is_empty:
                return True
        return False
    
    def get_intersection_geometry(self, rect, ref_geom):
        min_x, min_y, max_x, max_y = rect
        tile_ulp = (min_x, max_y)
        tile_dlp = (min_x, min_y)
        tile_drp = (max_x, min_y)
        tile_urp = (max_x, max_y)
        tile = Polygon([tile_ulp, tile_dlp, tile_drp, tile_urp])
        
        poly_int = tile.intersection(ref_geom)
        if not poly_int.is_empty:
            return poly_int
        
#         if ref_geom.type == 'Polygon':
#             print("Polygon")
#             poly_int = tile.intersection(ref_geom)
#             if not poly_int.is_empty:
#                 return poly_int
#         elif ref_geom.type == 'MultiPolygon':
#             print("MultiPolygon with ["+str(len(ref_geom))+"] polygons")
#             for part in ref_geom:
#                 poly_int = tile.intersection(ref_geom)
#                 if not poly_int.is_empty:
#                     return poly_int
#         else:
#             raise ValueError('Unhandled geometry type: ' + repr(ref_geom.type))
#         for feature in ref_geom:
#             shp_geom = shape(feature['geometry'])
#             poly_int = tile.intersection(shp_geom)
#             if not poly_int.is_empty:
#                 return poly_int    
        

#===========================================================            
class QuadTree():
    maxdepth = 1 # the "depth" of the tree
    
    leaves = []
    allnodes = []
    #_______________________________________________________
    def __init__(self, rootnode, minrect, ref_geom, out_shp_file=None):
        print("MINRECT: " + str(minrect))
        QuadTree.maxdepth = 1 # the "depth" of the tree
        QuadTree.leaves = []
        QuadTree.allnodes = []
        
        Node.minsize = minrect
        self.shp_file = out_shp_file
        
        #Timer
        start_time = time.time()
        
        rootnode.subdivide(ref_geom) # constructs the network of nodes
        self.prune(rootnode)
        
        end_time = time.time()
        self.elapsed_time=round(end_time - start_time,2)
        print('\nElapsed Time:', str("{0:.2f}".format(self.elapsed_time)), 'seconds')
        
        #~ schema = {
            #~ 'geometry': 'Polygon',
            #~ #'properties': dict([(u'EN_REF', 'tr:254'), (u'MINX', 'float:19'), (u'MINY', 'float:19'), (u'MAXX', 'float:19'), (u'MAXY', 'float:19'), (u'Tilename', 'str:254'), (u'File_Path', 'str:254')])
            #~ 'properties': dict([('EN_REF', 'str:254'), ('TYPE', 'int:1'),('MINX', 'float:19'), ('MINY', 'float:19'), ('MAXX', 'float:19'), ('MAXY', 'float:19')])
            #~ }
        #~ if out_shp_file is not None:
            #~ with fiona.open(out_shp_file, 'w','ESRI Shapefile', schema, crs=from_epsg(32651), ) as out_shp:
                #~ self.traverse(rootnode, out_shp)
        #~ else:
        self.traverse(rootnode)
    
    def get_leaf_nodes(self):
        return list(self.leaves)
    #_______________________________________________________
    # Sets children of 'node' to None if they do not have any
    # LEAF nodes.       
    def prune(self, node):
        if node.type == Node.LEAF:
            return 1
        leafcount = 0
        removals = []
        for child in node.children:
            if child != None:
                leafcount += self.prune(child)
                if leafcount == 0:
                    removals.append(child)
        for item in removals:
            n = node.children.index(item)
            node.children[n] = None     
        return leafcount
    #_______________________________________________________
    # Appends all nodes to a "generic" list, but only LEAF 
    # nodes are appended to the list of leaves.
    def traverse(self, node, shp_handler=None):
        if shp_handler is not None:
            #print("Writing {0}".format(node.rect))
            #if node.type == Node.LEAF:
            write_tile_to_shape(node.rect, shp_handler, TILE_SIZE, node.type) 
        QuadTree.allnodes.append(node)
        if node.type == Node.LEAF:
            QuadTree.leaves.append(node)
            if node.depth > QuadTree.maxdepth:
                QuadTree.maxdepth = node.depth
        for child in node.children:
            if child != None:
                self.traverse(child, shp_handler) # << recursion

def get_georefs(rect, tile_size):
    min_x, min_y, max_x, max_y = get_bounds_1x1km(rect)
    for tile_y in xrange(min_y + tile_size,
                         max_y + tile_size,
                         tile_size):
        for tile_x in xrange(min_x,
                             max_x,
                             tile_size):
            #code here
            pass
    
def is_square(test_geom):
    if isinstance(test_geom, Polygon):
        if len(test_geom.exterior.coords) is 5:
            x_pts, y_pts = test_geom.exterior.coords.xy
            perimeter = zip(x_pts, y_pts)
            len_sides = []
            for i in range(len(perimeter)):
                len_sides.append(Point(perimeter[i]).distance(Point(perimeter[i-1])))
            len_sides.remove(0.0)
            if len(set(len_sides)) <= 1:
                #print(">>> SQUARE: "+ str(perimeter)+ " | SIDES: " + str(len_sides))
                return True
    
                
def write_tile_to_shape(tile_extents,shp_file, tile_size, node_type):
    min_x, min_y, max_x, max_y = tile_extents
    tile_ulp = (min_x, max_y)
    tile_dlp = (min_x, min_y)
    tile_drp = (max_x, min_y)
    tile_urp = (max_x, max_y)
    gridref = "E{0}N{1}".format(min_x / tile_size, max_y / tile_size,)
    shp_file.write({
                #'geometry': mapping(Polygon([tile_ulp, tile_dlp, tile_drp, tile_urp])),
                'geometry': mapping(Polygon([tile_ulp, tile_dlp, tile_drp, tile_urp])),
                'properties': {'EN_REF': gridref,
                               'TYPE' : node_type,
                               'MINX' : min_x,
                               'MINY' : min_y,
                               'MAXX' : max_x,
                               'MAXY' : max_y,
                               },
            })

def tile_floor(x):
    return int(math.floor(x / float(TILE_SIZE)) * TILE_SIZE)

def tile_ceiling(x):
    return int(math.ceil(x / float(TILE_SIZE)) * TILE_SIZE)

def get_tile_size(tile_extents):
    min_x, min_y, max_x, max_y = tile_extents
    dx = max_x-min_x
    dy = max_y-min_y
    if dx != dy:
        raise Exception("Not a square tile! dx={0}, dy={1}".format(dx,dy))
    else:
        return dx


def get_bounds_1x1km(extents):
    min_x, min_y, max_x, max_y = extents
    min_x = tile_floor(min_x)
    min_y = tile_floor(min_y)
    max_x = tile_ceiling(max_x)
    max_y = tile_ceiling(max_y)
    return min_x, min_y, max_x, max_y

def get_pow2_extents(extents, tile_size):
    min_x, min_y, max_x, max_y = extents
    width_1x1km = int((max_x-min_x)/tile_size)
    height_1x1km = int((max_y-min_y)/tile_size)
    min_sqr_len = (1<<(max(height_1x1km,width_1x1km)-1).bit_length())*tile_size
    print("P2SQR: [{0},{1}] - {2}".format(height_1x1km*tile_size, width_1x1km*tile_size, min_sqr_len))
    
    pow2_extents = (int(min_x),int(min_y),int(min_x+min_sqr_len),int(min_y+min_sqr_len))
    #write_tile_to_shape(pow2_extents,shp_file)
    return pow2_extents

#_______________________________________________________
# Returns a string containing the rib statement for a
# four sided polygon positioned at height "y".
def RiPolygon(rect, y): 
    x0,z0,x1,z1 = rect
    verts = []
    verts.append(' %1.3f %1.3f %1.3f' % (x0,y,z0))
    verts.append(' %1.3f %1.3f %1.3f' % (x0,y,z1))
    verts.append(' %1.3f %1.3f %1.3f' % (x1,y,z1))
    verts.append(' %1.3f %1.3f %1.3f' % (x1,y,z0))
    rib =  '\tPolygon "P" ['
    rib += ''.join(verts)
    rib += ']\n'
    return rib
        
def count_polygons(shp):
    count=0
    for feature in shp:
        geom=shape(feature['geometry'])
        print( feature['properties'])
        if geom.type == 'Polygon':
            count+=1
        elif geom.type == 'MultiPolygon':
            for part in geom:
                count+=1
        else:
            raise ValueError('Unhandled geometry type: ' + repr(geom.type))
        print( count)
    return count
    

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("shpfile", help="Shapefile location")
    parser.add_argument('-t', '--tilesize', help="Size of tile in meters", required=True)

    args = parser.parse_args()
    with fiona.open(args.shpfile, 'r', 'ESRI Shapefile') as shp:
    
        print( "Creating QuadTree...")
                
        resolution = tilesize = int(args.tilesize)
        print( "Bounds:"+ str(shp.bounds))
        print( "1x1   :"+ str(get_bounds_1x1km(shp.bounds)))
        
        pow2_bounds = get_pow2_extents(get_bounds_1x1km(shp.bounds), tilesize)

        rootrect = list(pow2_bounds)
        rootnode = Node(None, rootrect)
        out_shp_file = args.shpfile.replace('.shp', '_quadtree_'+args.tilesize+'km.shp')
        tree = QuadTree(rootnode, resolution, shp, out_shp_file)
        
        schema = {
            'geometry': 'Polygon',
            #'properties': dict([(u'EN_REF', 'tr:254'), (u'MINX', 'float:19'), (u'MINY', 'float:19'), (u'MAXX', 'float:19'), (u'MAXY', 'float:19'), (u'Tilename', 'str:254'), (u'File_Path', 'str:254')])
            'properties': dict([('EN_REF', 'str:254'), ('TYPE', 'int:1'),('MINX', 'float:19'), ('MINY', 'float:19'), ('MAXX', 'float:19'), ('MAXY', 'float:19')])
            }
        if out_shp_file is not None:
            with fiona.open(out_shp_file, 'w','ESRI Shapefile', schema, crs=from_epsg(32651), ) as out_shp:
                for leaf in tree.get_leaf_nodes():
                    write_tile_to_shape(leaf.rect, out_shp, tilesize, leaf.type)
        print( '------END------')








#~ shp=fiona.open("/home/ken/LAStools/shapefiles/panay_test/PanayIsland.shp", 'r', 'ESRI Shapefile')
#~ shp=fiona.open("/home/ken/LAStools/shapefiles/adnu2_coverage/suc_coverage_adnu2.shp", 'r', 'ESRI Shapefile')
#~ shp=fiona.open("/home/ken/LAStools/shapefiles/ph_muni_bounds/wvi_municities.shp", 'r', 'ESRI Shapefile')
#~ nohup python modquadtree.py -t=1000 /home/ken/LAStools/shapefiles/adnu2_coverage/suc_coverage_adnu2.shp > modquad_adnu2.out 2>&1 &
#~ count_polygons(shp)

# for link in grid.link_set.all():
#     print(link.url)


# for link in grid.link_set.all():
#     if "http://" not in link.url:
#         if "lipad" in link.url:
#             link.url = "http://"+link.url
#             link.save()
#     print(link.url)
