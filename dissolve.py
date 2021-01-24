from shapely.geometry import shape, mapping
from shapely.ops import unary_union
import fiona
import itertools
in_shp = "/home/ken/git/thesis-repos/00_Data/thesis/vector/32651/mindoro/mindoro_merged/mindoro_merged.shp"
out_shp = "/home/ken/git/thesis-repos/00_Data/thesis/vector/32651/mindoro/mindoro_merged/mindoro_dissolved_"
with fiona.open(in_shp) as input:
    # preserve the schema of the original shapefile, including the crs
    meta = input.meta
    # groupby clusters consecutive elements of an iterable which have the same key so you must first sort the features by the 'STATEFP' field
    e = sorted(input, key=lambda k: k['properties']['PROVINCE'])
    # group by the 'STATEFP' field 
    for key, group in itertools.groupby(e, key=lambda x:x['properties']['PROVINCE']):
        properties, geom = zip(*[(feature['properties'],shape(feature['geometry'])) for feature in group])
        # write the feature, computing the unary_union of the elements in the group with the properties of the first element in the group
        with fiona.open(out_shp+key+".shp", 'w', **meta) as output:
            output.write({'geometry': mapping(unary_union(geom)), 'properties': properties[0]})