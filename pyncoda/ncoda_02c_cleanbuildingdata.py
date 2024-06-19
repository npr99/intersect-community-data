
import matplotlib.pyplot as plt
import math as math
import numpy as np
import geopandas as gpd
import pandas as pd
import shapely
import descartes

import folium as fm # folium has more dynamic maps - but requires internet connection

import os # For saving output to path
import sys

from shapely.geometry import Point # Shapely for converting latitude/longitude to geometry
from shapely.wkt import loads
from pyproj import CRS

''' Code to convert to functions
# Store output files in set folder
output_folder = "hua_workflow"
programname = "IN_CORE_1cv2_Galveston_CleanBuildingInventory_2021-10-20"

## Read in Building Inventory
The building inventory provide basic understanding of where address points can be located.
current_dir = os.getcwd()

# Galveston Building inventory
filename = 'Nofal_Galveston_Buildings_2021-10-20\\Galveston_Bldgs_Points_Damage.shp'
bldg_inv_gdf = gpd.read_file(filename)
bldg_inv_gdf.head()



# creating a geometry column 
geometry = [Point(xy) for xy in zip(bldg_inv_gdf['X_Longit'], bldg_inv_gdf['Y_Latit'])]
# Coordinate reference system : WGS84
crs = CRS('epsg:4326')
# Creating a Geographic data frame 
bldg_inv_gdf = gpd.GeoDataFrame(bldg_inv_gdf, crs=crs, geometry=geometry)

# bldg_inv_gdf[['F_Arch','Bldg_Area']].groupby(['F_Arch']).describe()
# bldg_inv_gdf[['W_Arch','N_Units','Prop_Use','Occupancy']].head()
# pd.pivot_table(bldg_inv_gdf, values='OBJECTID', index=['W_Arch','F_Arch'],
#                           margins = True, margins_name = 'Total',
#                           columns=['N_Units','Prop_Use'], aggfunc='count')

## Check Unique Building ID
# Building ID will be important for linking Address Point Inventory to Buildings and Critical Infrastructure Inventories to Buildings.

## ID must be unique and non-missing.
## Count the number of Unique Values
# bldg_inv_gdf.OBJECTID.astype(str).describe()
##  Count the number of Unique Values
# bldg_inv_gdf.OBJECTID.nunique()
# bldg_inv_gdf.loc[bldg_inv_gdf['OBJECTID'] == 568208]
## Are there any missing values for the unique id?
# bldg_inv_gdf.loc[bldg_inv_gdf.OBJECTID.isnull()]
# Move Primary Key Column Building ID to first Column
# cols = ['guid']  + [col for col in bldg_inv_gdf if col != 'guid']
# cols
# bldg_inv_gdf = bldg_inv_gdf[cols]
# bldg_inv_gdf.head()
# Confirm Building ID is Unique and Non-Missing
#bldg_inv_gdf.guid.describe()
## Read in Census Block Data
# Census Blocks provide an estimate of how many residential 
# address points (housing units) should be located in each block.

source_file = 'tl_2010_48167_tabblockplacepuma10'
census_blocks_csv = output_folder+"/"+source_file+"EPSG4269.csv"
census_blocks_df = pd.read_csv(census_blocks_csv)
census_blocks_gdf = gpd.GeoDataFrame(census_blocks_df)
census_blocks_gdf.head()

# Use shapely.wkt loads to convert WKT to GeoSeries
census_blocks_gdf['geometry'] = census_blocks_gdf['geometry'].apply(lambda x: loads(x))
#census_blocks_gdf['geometry'].geom_type.describe()
census_blocks_gdf = census_blocks_gdf.set_geometry(census_blocks_gdf['geometry'])
census_blocks_gdf.crs = CRS('epsg:4269')
#census_blocks_gdf.head()
## Check CRS for Building Centroid and Block
# census_blocks_gdf.crs
# bldg_inv_gdf.crs
# Convert Census Block CRS to Buildings CRS
census_blocks_gdf = census_blocks_gdf.to_crs(bldg_inv_gdf.crs)
#census_blocks_gdf.crs

# Check change in Geometry
census_blocks_gdf['blk104269'] = /
    census_blocks_gdf['blk104269'].apply(lambda x: loads(x))
#census_blocks_gdf[['geometry','blk104269']]./
#    loc[census_blocks_gdf['geometry'] != census_blocks_gdf['blk104269']]

## Convert BLOCKID10 to a string
census_blocks_gdf['BLOCKID10'] = /
   census_blocks_gdf['GEOID10'].apply(lambda x : str(int(x)))

## Add State, County, and Census Block ID to Each Footprint
## Select Blocks within Bounding Box of Buildings
# Find the bounds of the Buildings to select Census Blocks
# Add Small Buffer for blocks on the edges
buffer = 0.001
minx = bldg_inv_gdf.bounds.minx.min() - buffer # subtract buffer from minimum values
miny = bldg_inv_gdf.bounds.miny.min() - buffer # subtract buffer from minimum values
maxx = bldg_inv_gdf.bounds.maxx.max() + buffer
maxy = bldg_inv_gdf.bounds.maxy.max() + buffer
building_gdf_bounds = [minx, miny, maxx, maxy]
building_gdf_bounds

# Select pumas within Bounds of Study Area
# build the r-tree index - for census blocks
sindex_census_blocks_gdf = census_blocks_gdf.sindex
possible_matches_index = list(sindex_census_blocks_gdf.intersection(building_gdf_bounds))
building_census_blocks_gdf = census_blocks_gdf.iloc[possible_matches_index]
building_census_blocks_gdf['BLOCKID10'].describe()

## Add Census Geography Details to Buildings
# Significant help from: https://geoffboeing.com/2016/10/r-tree-spatial-index-python/
# Significant help from: https://github.com/gboeing/urban-data-science/blob/master/19-Spatial-Analysis-and-Cartography/rtree-spatial-indexing.ipynb
# build the r-tree index - Using Representative Point
sindex_bldg_inv_gdf = bldg_inv_gdf.sindex
sindex_bldg_inv_gdf

# find the points that intersect with each subpolygon and add ID to Point
for index, block in building_census_blocks_gdf.iterrows():
    if index%100==0:
        print('.', end ="")

    # find approximate matches with r-tree, then precise matches from those approximate ones
    possible_matches_index = list(sindex_bldg_inv_gdf.intersection(block['geometry'].bounds))
    possible_matches = bldg_inv_gdf.iloc[possible_matches_index]
    precise_matches = possible_matches[possible_matches.intersects(block['geometry'])]
    bldg_inv_gdf.loc[precise_matches.index,'BLOCKID10'] = block['BLOCKID10']
    bldg_inv_gdf.loc[precise_matches.index,'placeGEOID10'] = block['placeGEOID10']
    bldg_inv_gdf.loc[precise_matches.index,'placeNAME10'] = block['placeNAME10']

# Move Foreign Key Columns Block ID State, County, Tract to first Columns
first_columns = ['OBJECTID','BLOCKID10','placeGEOID10','placeNAME10']
cols = first_columns + [col for col in bldg_inv_gdf if col not in first_columns]
bldg_inv_gdf = bldg_inv_gdf[cols]
bldg_inv_gdf.head()

### How many buildings do not have block id information?
bldg_noblock_gdf = bldg_inv_gdf.loc[(bldg_inv_gdf['BLOCKID10'].isnull())]
bldg_noblock_gdf

# if there are missing buildings this code will help identify where they are -
# every building should have a block
# plot the building with missing block data
# Find the bounds of the Census Block File
minx = bldg_inv_gdf.bounds.minx.min()
miny = bldg_inv_gdf.bounds.miny.min()
maxx = bldg_inv_gdf.bounds.maxx.max()
maxy = bldg_inv_gdf.bounds.maxy.max()

blockstyle_function = lambda x: {'color':'green','fillColor': 'transparent' }

bldg_inv_gdf_map = fm.Map(location=[(miny+maxy)/2,(minx+maxx)/2], zoom_start=10)
fm.GeoJson(bldg_noblock_gdf).add_to(bldg_inv_gdf_map)
fm.GeoJson(census_blocks_gdf['geometry'],name='All Census Blocks',style_function=blockstyle_function).add_to(bldg_inv_gdf_map)
fm.GeoJson(building_census_blocks_gdf['geometry'],name='Selected Census Blocks').add_to(bldg_inv_gdf_map)
bldg_inv_gdf_map.save(output_folder + '/' + 'buildings_noblocks.html')
# Error Displaying Map display(neosho_place_gdf_map)

### Save Work
# Check Columns
cols = [col for col in bldg_inv_gdf]
# Save Work at this point as CSV
savefile = os.path.join(os.getcwd(), output_folder, f"{programname}_EPSG4326.csv" )
bldg_inv_gdf.to_csv(savefile)

'''