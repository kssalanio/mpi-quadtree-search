import math
import numpy
from pprint import pprint
import argparse
import itertools
from enum import IntEnum

import shapely
from shapely.geometry import mapping, Polygon, shape, LineString
import fiona
from fiona.crs import from_epsg
from shapely.ops import unary_union

import local_config

_TILE_SIZE = 1024
_DEPTH_LIMIT = 8

import numpy as np

class Point:
    """A point located at (x,y) in 2D space.

    Each Point object may be associated with a payload object.

    """

    def __init__(self, x, y, payload=None):
        self.x, self.y = x, y
        self.payload = payload

    def __repr__(self):
        return '{}: {}'.format(str((self.x, self.y)), repr(self.payload))
    def __str__(self):
        return 'P({:.2f}, {:.2f})'.format(self.x, self.y)

    def distance_to(self, other):
        try:
            other_x, other_y = other.x, other.y
        except AttributeError:
            other_x, other_y = other
        return np.hypot(self.x - other_x, self.y - other_y)

class Rect:
    """A rectangle centred at (cx, cy) with width w and height h."""

    def __init__(self, cx, cy, w, h):
        self.cx, self.cy = cx, cy
        self.w, self.h = w, h
        self.west_edge, self.east_edge = cx - w/2, cx + w/2
        self.south_edge, self.north_edge = cy - h/2, cy + h/2
        self.min_x = self.west_edge
        self.max_x = self.east_edge
        self.min_y = self.south_edge
        self.max_y = self.north_edge

    def __repr__(self):
        return str((self.west_edge, self.east_edge, self.south_edge,
                self.north_edge))

    def __str__(self):
        return '({:.2f}, {:.2f}, {:.2f}, {:.2f})'.format(self.west_edge,
                    self.south_edge, self.east_edge, self.north_edge)

    @classmethod
    def from_extents(cls, min_x, min_y, max_x, max_y):
        w = max_x - min_x
        h = max_y - min_y
        cx = min_x + w/2
        cy = min_y + h/2
        return cls(cx,cy, w, h)

    def contains(self, point):
        """Is point (a Point object or (x,y) tuple) inside this Rect?"""

        try:
            point_x, point_y = point.x, point.y
        except AttributeError:
            point_x, point_y = point

        return (point_x >= self.west_edge and
                point_x <  self.east_edge and
                point_y >= self.south_edge and
                point_y < self.north_edge)

    def intersects(self, other):
        """Does Rect object other interesect this Rect?"""
        return not (other.west_edge > self.east_edge or
                    other.east_edge < self.west_edge or
                    other.south_edge > self.north_edge or
                    other.north_edge < self.south_edge)

    def to_shapely_poly(self):
        tile_nwp = (self.min_x, self.max_y)
        tile_nep = (self.max_x, self.max_y)
        tile_sep = (self.max_x, self.min_y)
        tile_swp = (self.min_x, self.min_y)
        
        return Polygon([tile_nwp, tile_nep, tile_sep, tile_swp])                

    def draw(self, ax, c='k', lw=1, **kwargs):
        x1, y1 = self.west_edge, self.north_edge
        x2, y2 = self.east_edge, self.south_edge
        ax.plot([x1,x2,x2,x1,x1],[y1,y1,y2,y2,y1], c=c, lw=lw, **kwargs)

class QuadTreeNodeType(IntEnum):
    OUTSIDE     = 0
    INSIDE      = 1
    INTERSECTS  = 2

class QuadTree:
    """A class implementing a quadtree."""

    def __init__(self, boundary, parent, depth=0):
        """Initialize this node of the quadtree.

        boundary is a Rect object defining the region from which points are
        placed into this node; max_points is the maximum number of points the
        node can hold before it must divide (branch into four more nodes);
        depth keeps track of how deep into the quadtree this node lies.

        """

        self.boundary = boundary
        self.parent = parent
        self.points = []
        self.depth = depth
        # A flag to indicate whether this node has divided (branched) or not.
        self.divided = False
        self.node_type = QuadTreeNodeType.OUTSIDE

    def __str__(self):
        """Return a string representation of this node, suitably formatted."""
        sp = ' ' * self.depth * 2
        s = str(self.boundary) + '\n'
        s += sp + ', '.join(str(point) for point in self.points)
        if not self.divided:
            return f"{self.node_type.name}:{s}"
        return "QN-TYPE: "+ self.node_type.name +"\n" + s + '\n' + '\n'.join([
                sp + 'nw: ' + str(self.nw), sp + 'ne: ' + str(self.ne),
                sp + 'se: ' + str(self.se), sp + 'sw: ' + str(self.sw)])
    
    def __repr__(self):
        """Return a string representation of this node, suitably formatted."""
        sp = ' ' * self.depth * 2
        s = str(self.boundary) + '\n'
        s += sp + ', '.join(str(point) for point in self.points)
        if not self.divided:
            return f"{self.node_type.name}:{s}"
        return "QN-TYPE: "+ self.node_type.name +"\n" + s + '\n' + '\n'.join([
                sp + 'nw: ' + str(self.nw), sp + 'ne: ' + str(self.ne),
                sp + 'se: ' + str(self.se), sp + 'sw: ' + str(self.sw)])
    
    def to_string(self):
        """Return a string representation of this node, suitably formatted."""
        sp = ' ' * self.depth * 2
        s = str(self.boundary) + '\n'
        s += sp + ', '.join(str(point) for point in self.points)
        if not self.divided:
            return s
        return "QN-TYPE: "+ self.node_type +"\n" + s + '\n' + '\n'.join([
                sp + 'nw: ' + str(self.nw), sp + 'ne: ' + str(self.ne),
                sp + 'se: ' + str(self.se), sp + 'sw: ' + str(self.sw)])

    def divide(self):
        """Divide (branch) this node by spawning four children nodes."""

        cx, cy = self.boundary.cx, self.boundary.cy
        w, h = self.boundary.w / 2, self.boundary.h / 2
        # The boundaries of the four children nodes are "northwest",
        # "northeast", "southeast" and "southwest" quadrants within the
        # boundary of the current node.
        self.nw = QuadTree(Rect(cx - w/2, cy + h/2, w, h),
                                    self, self.depth + 1)
        self.ne = QuadTree(Rect(cx + w/2, cy + h/2, w, h),
                                    self, self.depth + 1)
        self.se = QuadTree(Rect(cx + w/2, cy - h/2, w, h),
                                    self, self.depth + 1)
        self.sw = QuadTree(Rect(cx - w/2, cy - h/2, w, h),
                                    self, self.depth + 1)
        self.divided = True
    
    def rec_divide(self, depth_limit=50, qtile_length_limit=256):
        #print(f"Dividing at depth: {self.depth}")
        if (self.depth > depth_limit) or (self.boundary.w <= qtile_length_limit) or (self.boundary.h <= qtile_length_limit) :
            # Return after reaching depth or tile limit
            return
        else:
            # Divide self
            self.divide()

            # Recursively subdivide on quadrant nodes
            self.nw.rec_divide(depth_limit, qtile_length_limit)
            self.ne.rec_divide(depth_limit, qtile_length_limit)
            self.se.rec_divide(depth_limit, qtile_length_limit)
            self.sw.rec_divide(depth_limit, qtile_length_limit)
    
    def intersects_shapely_geom(self, shapely_geom):
        return self.boundary.to_shapely_poly().intersects(shapely_geom)
