import matplotlib.pyplot as plt
import math as math
import numpy as np
import geopandas as gpd
import pandas as pd
import shapely
import descartes

import folium as fm # folium has more dynamic maps - but requires internet connection
import sys
import os # For saving output to path


def read_in_zip_shapefile_data(geolevel, year, url_list):
    # Read data from www2.census.gov 
    census_url = url_list[geolevel][year]
    print(f'Obtaining Census {geolevel} data from:',census_url)
    gdf = gpd.read_file(census_url)
    
    return gdf

def add_representative_point(gdf,year):
    yr = year[2:4]
    test_crs = gdf.crs
    if test_crs == "EPSG:4269":
        # EPSG 4269 uses NAD 83 which will have slightly different lat lon points 
        # when compared to EPSG 4326 which uses WGS 84.
        # Add Representative Point
        gdf.loc[gdf.index, f'rppnt{yr}4269'] = gdf['geometry'].representative_point()
        gdf[f'rppnt{yr}4269'].label = f"Representative Point {year} EPSG 4269 (WKT)"
        gdf[f'rppnt{yr}4269'].notes = f"Internal Point within census block {year} poly EPSG 4269"

        # Add Column that Duplicates Polygon Geometry - 
        # allows for switching between point and polygon geometries for spatial join
        gdf.loc[gdf.index, f'blk{yr}4269'] = gdf['geometry']
        gdf[f'blk{yr}4269'].label = f"{year} Census Block Polygon EPSG 4269 (WKT)"
        gdf[f'blk{yr}4269'].notes = f"Polygon Shape Points for {year} Census Block EPSG 4269"
    else:
        print("Check Census Geography CRS - Expected EPSG 4269")

    return gdf

def spatial_join_points_to_poly(points_gdf, 
                                poly_gdf, 
                                point_var, 
                                poly_var, 
                                geolevel, 
                                varlist):
    """
    Function adds polygon varaibles to block points.
    point_var: Variable with WKT Point Geometry for Block Polygon
    poly_var: Variable with WKT Polygon Geometry for Block Polygon
    geolevel: Geography level of the polygon - example Place or PUMA
    varlist: Variable list to be transfere from Polygon File to Block File
    """

    # TO DO - Add CRS Check - Assumes that both block and poly gdf are in the same CRS

    # Find the bounds of the Census Block File
    minx = points_gdf.bounds.minx.min()
    miny = points_gdf.bounds.miny.min()
    maxx = points_gdf.bounds.maxx.max()
    maxy = points_gdf.bounds.maxy.max()
    points_gdf_bounds = [minx, miny, maxx, maxy]

    # Select polygons within Bounds of Study Area
    # build the r-tree index - for Places
    sindex_poly_gdf = poly_gdf.sindex
    possible_matches_index = list(sindex_poly_gdf.intersection(points_gdf_bounds))
    area_poly_gdf = poly_gdf.iloc[possible_matches_index]
    print("Identified",area_poly_gdf.shape[0],geolevel,"polygons to spatially join.")

    # build the r-tree index - Using Representative Point
    points_gdf.loc[points_gdf.index,'geometry'] = points_gdf[point_var]
    sindex_points_gdf = points_gdf.sindex

    # find the points that intersect with each subpolygon and add ID to Point
    for index, poly in area_poly_gdf.iterrows():
        # find approximate matches with r-tree, then precise matches from those approximate ones
        possible_matches_index = list(sindex_points_gdf.intersection(poly['geometry'].bounds))
        possible_matches = points_gdf.iloc[possible_matches_index]
        precise_matches = possible_matches[possible_matches.intersects(poly['geometry'])]
        for var in varlist:
            points_gdf.loc[precise_matches.index,geolevel+var] = poly[var]

    # Switch Geometry back to Polygon
    points_gdf.loc[points_gdf.index,'geometry'] = points_gdf[poly_var]

    return points_gdf

def add_address_point_counts(addpt_df, 
                            block_gdf, 
                            merge_id_old, 
                            merge_id):
    # Convert blockid Parcel ID to a String
    addpt_df[merge_id] = addpt_df[merge_id_old].apply(lambda x : str((x)))
    
    # Merge Address Point Count with Block Data 
    block_gdf = pd.merge(block_gdf, addpt_df,
                        left_on=merge_id, right_on=merge_id, how='left')

    return block_gdf

def obtain_join_block_place_puma_data(county_fips: str = '48167',
                            state: str = 'TEXAS',
                            year: str = '2010',
                            output_folder: str = 'hua_workflow'):
    """
    Function obtains and cleans Census Block Data
    Function uses County FIPS Code and URL list to look up Census ZIP Files
    Census TIGER ZIP Files include GIS Shapefiles.

    county_fips: 5 character string county FIPS code
    """

    # Find State FIPS Code from County FIPS Code
    # TO DO - Check that State FIPS code is 2 characters - example California should be '06'
    state_fips = county_fips[0:2]
    # TO DO - Use US package to get the State name - in all caps
    # Find 2 digit year - used in variable names
    yr = year[2:4]

    # List of URLS for Census Geography Files
    base_url = 'https://www2.census.gov/geo/tiger/'
    block_mid_url = f'TIGER2020PL/STATE/{state_fips}_{state}/{county_fips}/tl_2020_'
    url_list = \
    {'block' : 
        {'2010' : f'{base_url}{block_mid_url}{county_fips}_tabblock10.zip',
        '2020' : f'{base_url}{block_mid_url}{county_fips}_tabblock20.zip'},
    'place' :
        {'2010' : f'{base_url}TIGER2010/PLACE/2010/tl_2010_{state_fips}_place10.zip',
        '2020' : f'{base_url}TIGER2020/PLACE/tl_2020_{state_fips}_place.zip'},
    'puma'  : 
        {'2010' : f'{base_url}TIGER2010/PUMA5/2010/tl_2010_{state_fips}_puma10.zip',
        '2020' : f'{base_url}TIGER2020/PUMA/tl_2020_{state_fips}_puma10.zip'}}

    # start empty geodataframe dictionary to store geolevel gdfs
    gdf = {}
    join_cols = {}
    geolevels = ['block','place','puma']
    for geolevel in geolevels:
        gdf[geolevel] = read_in_zip_shapefile_data(geolevel=geolevel, year = year, url_list = url_list)
        join_cols[geolevel] =  [col for col in gdf[geolevel] if col.startswith("GEOID")]
        join_cols[geolevel] =  join_cols[geolevel] + [col for col in gdf[geolevel] if col.startswith("NAME")]
   
    gdf['block'] = add_representative_point(gdf = gdf['block'],year = year)

    # Add Place Name (Cities) and PUMA ids To Blocks
    # Place names provide link to population demographics for cities and places defined by the Census. 
    # The Census communicates with cities and updates city boundaries based on policitical boundaries set by communities.

    geolevels = ['place','puma']
    for geolevel in geolevels:
        print('Spatially Join',geolevel,'Columns',join_cols[geolevel],'with Block Data.')
        gdf['block'] = spatial_join_points_to_poly(
                    gdf['block'],
                    gdf[geolevel] ,
                    f'rppnt{yr}4269',f'blk{yr}4269',
                    geolevel,
                    join_cols[geolevel])

    # Save Work at this point as CSV
    savefile = sys.path[0]+"/"+output_folder+"/"+f"tl_{year}_{county_fips}_tabblockplacepuma{yr}"+"EPSG4269.csv"
    gdf['block'].to_csv(savefile)

    return gdf['block']

def single_layer_folium_map(gdf,layer_name, output_folder):   
    # Find the bounds of the Census Block File
    minx = gdf.bounds.minx.min()
    miny = gdf.bounds.miny.min()
    maxx = gdf.bounds.maxx.max()
    maxy = gdf.bounds.maxy.max()

    # plot the intersections and the city
    gdf_map = fm.Map(location=[(miny+maxy)/2,(minx+maxx)/2], zoom_start=10)
    #fm.GeoJson(gdf).add_to(gdf_map)

    style_function = lambda x: {'color':'green','fillColor': 'transparent' }

    fm.GeoJson(gdf[['geometry']],
              name=layer_name,
              style_function=style_function,).add_to(gdf_map)
    
    fm.LayerControl().add_to(gdf_map)
    
    gdf_map.save(output_folder+'/'+layer_name+'.html')
    
    return gdf_map


''' example code

census_block_place_puma_gdf = obtain_join_block_place_puma_data(county_fips = '48167',
                            state = 'TEXAS',
                            year = '2010',
                            output_folder = output_folder)
census_block_place_puma_gdf[['GEOID10','placeGEOID10','pumaGEOID10']].astype(str).describe()

census_block_place_puma_gdf = obtain_join_block_place_puma_data(county_fips = '48167',
                            state = 'TEXAS',
                            year = '2020',
                            output_folder = output_folder)

census_block_place_puma_gdf[['GEOID20','placeGEOID','pumaGEOID10']].astype(str).describe()

map = single_layer_folium_map(census_block_place_puma_gdf,'Census Blocks 2010')
'''