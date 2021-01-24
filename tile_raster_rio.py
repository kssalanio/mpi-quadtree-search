import os, argparse, math
from itertools import product
from pprint import pprint

import rasterio as rio
from rasterio import windows
from affine import Affine

def get_tiles(ds, width=256, height=256):
    ncols, nrows = ds.meta['width'], ds.meta['height']
    offsets = product(range(0, ncols, width), range(0, nrows, height))
    big_window = windows.Window(col_off=0, row_off=0, width=ncols, height=nrows)
    for col_off, row_off in  offsets:
        window =windows.Window(col_off=col_off, row_off=row_off, width=width, height=height).intersection(big_window)
        transform = windows.transform(window, ds.transform)
        yield window, transform

def test():
    with rio.open(os.path.join(in_path, input_filename)) as inds:
        tile_width, tile_height = 256, 256

        meta = inds.meta.copy()

        for window, transform in get_tiles(inds):
            print(window)
            meta['transform'] = transform
            meta['width'], meta['height'] = window.width, window.height
            outpath = os.path.join(out_path,output_filename.format(int(window.col_off), int(window.row_off)))
            with rio.open(outpath, 'w', **meta) as outds:
                outds.write(inds.read(window=window))

def get_tile_window(raster_ds, tile_cx, tile_cy, tile_size=1024):
    # Get pixel coordinates from map coordinates
    #py, px = raster_ds.index(tile_cy, tile_cx)
    ul_row, ul_col = raster_ds.index(tile_cx-(tile_size/2), tile_cy+(tile_size/2))
    lr_row, lr_col = raster_ds.index(tile_cx+(tile_size/2), tile_cy-(tile_size/2))
    #print('Pixel Y, X coords: {}, {}'.format(py, px))
    win_size = lr_col - ul_col
    #print(f"ul_col = {ul_col} ul_row = {ul_row} lr_col = {lr_col} lr_row = {lr_row}")

    # Build an NxN window
    # window = rio.windows.Window(px - tile_size//2, py - tile_size//2, tile_size, tile_size)
    window = rio.windows.Window(ul_col, ul_row, win_size, win_size)
    return window

def get_tile_window_from_extents(raster_ds, min_x, min_y, max_x, max_y, tile_size=1024):
    ul_row, ul_col = raster_ds.index(min_x, max_y, precision=5)
    lr_row, lr_col = raster_ds.index(max_x, min_y, precision=5)
    win_size = lr_col - ul_col
    #print(f"ul_col = {ul_col} ul_row = {ul_row} lr_col = {lr_col} lr_row = {lr_row}")
    
    # NOTE: adds a 5-pixel buffer to every side, prevent pixel gaps from round-off errors
    window = rio.windows.Window(ul_col-5, ul_row-5, win_size+10, win_size+10)
    return window


if __name__ == "__main__":
    #Parse CLI arguments
    parser = argparse.ArgumentParser(description="Generates 1k x 1k GeoTIFF \
    tiles from input DEM.",
                                     epilog="Example: ./tile_dem.py input_raster output_dir")
    
    parser.add_argument("in_raster", help="Input raster")
    parser.add_argument("out_raster_dir", help="Output directory")
    args = parser.parse_args()

    quadtree_tile_list = [
        # W, H, CX, CY, MIN_X, MIN_Y, MAX_X, MAX_Y
        (1024, 1024, 304472, 1452072, 302424, 1450024, 306520, 1454120),
        (1024, 1024, 308568, 1452072, 306520, 1450024, 310616, 1454120),
        (1024, 1024, 312664, 1452072, 310616, 1450024, 314712, 1454120),
        (1024, 1024, 308568, 1447976, 306520, 1445928, 310616, 1450024),
        (1024, 1024, 312664, 1456168, 310616, 1454120, 314712, 1458216),
        (1024, 1024, 308568, 1456168, 306520, 1454120, 310616, 1458216),
    ]
    

    # Open raster
    with rio.open(args.in_raster) as raster_ds:
        raster_meta = raster_ds.meta.copy()
        raster_ncols, raster_nrows = raster_ds.meta['width'], raster_ds.meta['height']
        pprint(raster_meta)
        print(f"RASTER WIDTH : {raster_ncols}")
        print(f"RASTER HEIGHT: {raster_nrows}")


        tile_window_list = []
        name1 = os.path.basename(args.in_raster)
        name2 = "tif"
            

        # Create rasterio windows
        #for test_tile in test_tile_list:
        for tile_num in range(len(quadtree_tile_list)):
            width, height, cx, cy, min_x, min_y, max_x, max_y = quadtree_tile_list[tile_num]
            #tile_window = get_tile_window(raster_ds, cx, cy, tile_size=width)
            tile_window = get_tile_window_from_extents(raster_ds, min_x, min_y, max_x, max_y, tile_size=1024)
            # print(f"TILE WINDOW: {tile_window}") 
            tile_window_list.append(tile_window)

            tile_name = f"{name1}_{tile_num}.{name2}"    
            tile_output_path = os.path.join(args.out_raster_dir, tile_name)
            # print(f"Writing to: [{tile_output_path}]")

            # Read the data in the window
            # clip is a nbands * N * N numpy array
            clip = raster_ds.read(window=tile_window)

            # You can then write out a new file
            meta = raster_ds.meta
            meta['width'], meta['height'] = tile_window.width, tile_window.height
            #meta['width'], meta['height'] = 409, 409
            #meta['transform'] = rio.windows.transform(tile_window, raster_ds.transform)
            temp_gt = rio.windows.transform(tile_window, raster_ds.transform)
            meta['transform'] = temp_gt
            #meta['transform'] = Affine(temp_gt.a,temp_gt.b, temp_gt.f, temp_gt.d, temp_gt.e, temp_gt.c)
            #meta['transform'] = Affine(temp_gt.a,temp_gt.b, min_x, temp_gt.d, temp_gt.e, max_y)


            print("--------")
            # pprint(raster_ds.transform.f)
            # pprint(raster_ds.window_transform(tile_window))
            # pprint(raster_meta['transform'])
            # pprint(meta['transform'])

            with rio.open(tile_output_path, 'w', **meta) as raster_out_ds:
                raster_out_ds.write(clip)

    
        

    
