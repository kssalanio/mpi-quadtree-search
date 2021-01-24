import logging
import numpy
import os
import random
import sys
import gdalconst

_BUFFER = 50  # meters


try:
    #from osgeo import ogr, osr, gdal, gdalnumeric, gdalconst
    #hacked because importing gdalnumeric causes error
    from osgeo import ogr, osr, gdal, gdalconst
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')


def pixel2world(gt, col_id, row_id):
    x, y = gdal.ApplyGeoTransform(gt, col_id, row_id)
    return x, y


def world2pixel(gt, x, y):
    # try-except block to handle different output of InvGeoTransform with gdal versions
    try:
        inv_gt_success, inv_gt = gdal.InvGeoTransform(gt)
    except:
        inv_gt = gdal.InvGeoTransform(gt)
    pixel_loc = gdal.ApplyGeoTransform(inv_gt, x, y)
    col_id, row_id = tuple([int(round(i, 0)) for i in pixel_loc])
    return col_id, row_id

def open_raster(raster_filepath):
    name = os.path.basename(isexists(raster_filepath))
    dataset = gdal.Open(path)
    if dataset is None:
        raise Exception(f"Cannot open raster in path [{raster_filepath}]!")
    raster_dct = {"dataset": dataset,
              "projection": dataset.GetProjection(),
              "geotransform": dataset.GetGeoTransform(),
              "cols": dataset.RasterXSize,
              "rows": dataset.RasterYSize,
              "name": name}
    print(f"Opened raster [{raster_dct['name']}] with projection [{raster_dct['projection']}]")
    return raster_dct

def get_band_array_tile(raster_band, raster_band_arr,
    raster_geotransform, raster_band_nodata, raster_band_unit_type,
    min_x, min_y, max_x, max_y):
    # Assumes a single band raster dataset
    # xoff, yoff - coordinates of upper left corner
    # xsize, ysize - tile size
    """
    # Get tile of band array
    tile_data = osgeotools.get_band_array_tile(resampled_dem,
                                                raster_band,
                                                tile_x, tile_y,
                                                _TILE_SIZE)
    """

    # Add buffers
    # ul_x, ul_y = xoff - _BUFFER, yoff + _BUFFER
    # lr_x, lr_y = xoff + size + _BUFFER, yoff - size - _BUFFER

    # Get tile bounding box pixel coordinates
    # ul_col, ul_row = world2pixel(raster["geotransform"], ul_x, ul_y)
    # lr_col, lr_row = world2pixel(raster["geotransform"], lr_x, lr_y)
    
    ul_col, ul_row = world2pixel(raster_geotransform, min_x, max_y)
    lr_col, lr_row = world2pixel(raster_geotransform, max_x, min_y)
    print(f"min_x = {min_x} max_y = {max_y} max_x = {max_x} min_y = {min_y}")
    print(f"ul_col = {ul_col} ul_row = {ul_row} lr_col = {lr_col} lr_row = {lr_row}")

    # Get tile subset
    tile = raster_band_arr[ul_row:lr_row, ul_col:lr_col]

    # Check if band subset has data
    if raster_band_nodata == tile.min() == tile.max():
        print("Tile has no data! Skipping.")
        return None

    # return tile, tile_cols, tile_rows, ul_x, ul_y
    return tile, min_x, max_y

def write_raster(path, driver_name, new_band_array, data_type, geotransform,
                 raster_prj, raster_band_nodata, raster_band_unit_type):
    """
    # Save new GeoTIFF
    osgeotools.write_raster(tile_path, "GTiff", tile,
                            osgeotools.gdalconst.GDT_Float32,
                            tile_gt, dem, raster_band)
    """
    # Assumes 1-band raster

    # Check if driver exists
    driver = gdal.GetDriverByName(driver_name)

    rows, cols = new_band_array.shape

    # Create new raster dataset
    raster_dataset = driver.Create(path, cols, rows, 1, data_type)

    # Set geotransform and prjection of raster dataset
    raster_dataset.SetGeoTransform(geotransform)
    raster_dataset.SetProjection(raster_prj)

    # Get the first raster band and write the band array data
    raster_dataset.GetRasterBand(1).WriteArray(new_band_array)
    # Also set the no data value, unit type and compute statistics
    raster_dataset.GetRasterBand(1).SetNoDataValue(raster_band_nodata)
    raster_dataset.GetRasterBand(1).SetUnitType(raster_band_unit_type)
    raster_dataset.GetRasterBand(1).ComputeStatistics(False)
    # Flush data
    del raster_dataset

def isexists(path):
    normpath = os.path.normpath(path)
    # Check if path exists
    if not os.path.exists(normpath):
        _logger.error("%s path does not exist! Exiting.", path)
        exit(1)
    return normpath

if __name__ == "__main__":
    #Parse CLI arguments
    parser = argparse.ArgumentParser(description="Generates 1k x 1k GeoTIFF \
    tiles from input DEM.",
                                     epilog="Example: ./tile_dem.py input_raster output_dir")
    
    parser.add_argument("in_raster", help="Input raster")
    parser.add_argument("out_dir", help="Output directory")
    
