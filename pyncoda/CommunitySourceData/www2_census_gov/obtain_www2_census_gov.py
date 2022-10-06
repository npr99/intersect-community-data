# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import os # For saving output to path
import sys
import numpy as np
import geopandas as gpd
import pandas as pd
import folium as fm
import shapely

class obtain_www2_census_gov():
    """
    Functions to obtain data from www2.census.gov
    Example data obtain:
        - Shapefiles for Census Geography
    """

    def __init__(self,
                geolevel,
                year,
                outputfolder,
                county_fips: str = '48167',
                state: str = 'TEXAS'):
    
        self.geolevel = geolevel
        self.year = year
        self.outputfolder = outputfolder
        self.county_fips = county_fips,
        self.state = state
        self.state_fips = county_fips[0:2].zfill(2)


    # List of URLS for Census Geography Files
    def make_url_list(self):
        base_url = {'2010' :   'https://www2.census.gov/geo/tiger/TIGER2010/',
                    '2020' :   'https://www2.census.gov/geo/tiger/TIGER2020/',
                    '2020PL' : f'https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/{self.state_fips}_{self.state}'}
        file_name = {'county' : {'2010'   : f'tl_2010_{self.state_fips}_tabblock10.zip',
                                 '2020'   : 'note can not find easy way to get 2020 county boundary. \
                                             use 2010 for now.'}}
        url_list = {
                'county' : 
                        {'2010' : f"{base_url['2010']}/COUNTY/2010/{file_name['county']['2010']}",
                         '2020' : f"{base_url['2010']}/COUNTY/2010/{file_name['county']['2010']}"}
                    }

        return url_list

    def read_in_zip_shapefile_data_as_gdf(self):
        # Read data from www2.census.gov 
        census_url = self.url_list[self.geolevel][self.year]
        print(f'Obtaining Census {self.geolevel} data from:',self.census_url)
        gdf = gpd.read_file(census_url)
        
        return gdf

    def add_representative_point(self,gdf):
        """
        Representative points are lat lon inside the polygon
        """
        yr = self.year[2:4]
        test_crs = gdf.crs
        if test_crs == "EPSG:4269":
            # EPSG 4269 uses NAD 83 which will have slightly different lat lon points when compared to EPSG 4326 which uses WGS 84.
            # Add Representative Point
            gdf.loc[gdf.index, f'rppnt{yr}4269'] = gdf['geometry'].representative_point()
            gdf[f'rppnt{yr}4269'].label = f"Representative Point {self.year} EPSG 4269 (WKT)"
            gdf[f'rppnt{yr}4269'].notes = f"Internal Point within census block {self,year} poly EPSG 4269"

            # Add Column that Duplicates Polygon Geometry - allows for swithcing between point and polygon geometries for spatial join
            gdf.loc[gdf.index, f'blk{yr}4269'] = gdf['geometry']
            gdf[f'blk{yr}4269'].label = f"{self.year} Census Block Polygon EPSG 4269 (WKT)"
            gdf[f'blk{yr}4269'].notes = f"Polygon Shape Points for {self.year} Census Block EPSG 4269"
        else:
            print("Check Census Geography CRS - Expected EPSG 4269")

        return gdf

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
        url_list = {'block' : {'2010' : f'https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/{state_fips}_{state}/{county_fips}/tl_2020_{county_fips}_tabblock10.zip',
                            '2020' : f'https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/{state_fips}_{state}/{county_fips}/tl_2020_{county_fips}_tabblock20.zip'},
                    'place' : {'2010' : f'https://www2.census.gov/geo/tiger/TIGER2010/PLACE/2010/tl_2010_{state_fips}_place10.zip',
                            '2020' : f'https://www2.census.gov/geo/tiger/TIGER2020/PLACE/tl_2020_{state_fips}_place.zip'},
                    'puma'  : {'2010' : f'https://www2.census.gov/geo/tiger/TIGER2010/PUMA5/2010/tl_2010_{state_fips}_puma10.zip',
                            '2020' : f'https://www2.census.gov/geo/tiger/TIGER2020/PUMA/tl_2020_{state_fips}_puma10.zip'}}

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
            gdf['block'] = spatial_join_points_to_poly(gdf['block'] ,gdf[geolevel] ,f'rppnt{yr}4269',f'blk{yr}4269',geolevel,join_cols[geolevel])

        # Save Work at this point as CSV
        savefile = sys.path[0]+"/"+output_folder+"/"+f"tl_{year}_{county_fips}_tabblockplacepuma{yr}"+"EPSG4269.csv"
        gdf['block'].to_csv(savefile)

        return gdf['block']