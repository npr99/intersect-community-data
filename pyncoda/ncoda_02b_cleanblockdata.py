import geopandas as gpd
import pandas as pd
import sys
import os
from pyncoda.ncoda_00e_geoutilities import *

def read_in_zip_shapefile_data(geolevel, year, url_list):
    # Read data from www2.census.gov 
    census_url = url_list[geolevel][year]
    print(f'Obtaining Census {geolevel} data from:',census_url)
    gdf = gpd.read_file(census_url)
    
    return gdf

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
                            output_folder: str = 'hua_workflow',
                            replace: bool = False):
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

    # Check if outputfile already exists
    filename = f"tl_{year}_{county_fips}_tabblockplacepuma{yr}"+"EPSG4269.csv"
    print(f'Checking if file exists: {output_folder}/{filename}')
    # Checing if file exists
    if os.path.exists(f'{output_folder}/{filename}') and replace == False:
        print(f'File exists. Skipping. Use replace=True to overwrite.')
        print("Block data already exists for ",county_fips)
        blockdata_df = pd.read_csv(output_folder+'/'+filename)
        # Convert df to gdf
        blockdata_gdf = df2gdf_WKTgeometry(df = blockdata_df, 
                       projection = "epsg:4269", 
                       reproject ="epsg:4269",
                       geometryvar = 'blk104269')
        return blockdata_gdf
    else:
        print("Creating block data for ",county_fips)

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
        gdf[geolevel] = read_in_zip_shapefile_data(geolevel=geolevel, 
                                                   year = year, 
                                                   url_list = url_list)
        join_cols[geolevel] =  [col for col in gdf[geolevel] if col.startswith("GEOID")]
        join_cols[geolevel] =  join_cols[geolevel] + \
            [col for col in gdf[geolevel] if col.startswith("NAME")]
   
    gdf['block'] = add_representative_point(polygon_gdf = gdf['block'],year = year)

    # Add Place Name (Cities) and PUMA ids To Blocks
    # Place names provide link to population demographics for 
    # cities and places defined by the Census. 
    # The Census communicates with cities and updates city boundaries 
    # based on political boundaries set by communities.

    geolevels = ['place','puma']
    for geolevel in geolevels:
        print('Spatially Join',geolevel,'Columns',join_cols[geolevel],'with Block Data.')
        gdf['block'] = spatial_join_points_to_poly(
                    points_gdf = gdf['block'],
                    polygon_gdf =gdf[geolevel] ,
                    point_var = f'rppnt{yr}4269',
                    poly_var = f'blk{yr}4269',
                    geolevel = geolevel,
                    join_column_list = join_cols[geolevel])

    # Add block id string to block data
    gdf['block'][f'BLOCKID{yr}'] = \
        gdf['block'][f'GEOID{yr}'].apply(lambda x : str(int(x)))
    # Add Block ID as string with "CB" prefix = protects block id
    # from losing the leading zero when converted to integer
    gdf['block'][f'BLOCKID{yr}_str'] = \
        gdf['block'][f'GEOID{yr}'].apply(lambda x : "B"+str(int(x)).zfill(15))
    # move blockid to the start of the dataframe
    first_cols = [f'BLOCKID{yr}',f'BLOCKID{yr}_str'] 
    cols = first_cols  + \
           [col for col in gdf['block'] if col not in first_cols]
    gdf['block'] = gdf['block'][cols]

    # Save Work at this point as CSV
    savefile = sys.path[0]+"/"+output_folder+"/"+filename
    gdf['block'].to_csv(savefile)

    return gdf['block']


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