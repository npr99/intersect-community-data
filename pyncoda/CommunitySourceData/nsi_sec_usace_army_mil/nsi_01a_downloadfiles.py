# Copyright (c) 2023 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import requests # Census API Calls
import os  # Operating System (os) For folders and finding working directory
import pandas as pd
import geopandas as gpd
import sys  # saving CSV files



def setup_nsi_directory():
    # Define output directories
    output_folder = 'OutputData'
    output_sourcedata = os.path.join(output_folder, '00_SourceData')
    output_directory = os.path.join(output_sourcedata, 'nsi_sec_usace_army_mil')

    # Create output directory if it does not exist
    def create_directory(path):
        if not os.path.exists(path):
            print(f"Making new directory to save output: {path}")
            os.makedirs(path)
        else:
            print(f"Directory {path} already exists.")

    create_directory(output_folder)
    create_directory(output_sourcedata)
    create_directory(output_directory)

    return output_directory


def download_nsi_files(county_fips,
                       unique_id = 'fd_id_bid',
                       unique_id_vars = ['fd_id','bid'],
                       keepvars = ['occtype','sqft'],
                       replace = False):
    '''
    Download files from the National Structure Inventory (NSI) website.
    Returns a geopandas dataframe with the data.
    '''

    output_directory = setup_nsi_directory()

    # Set file path where file will be downloaded
    folderpath = output_directory+'\\'
    filename = f"nsi_01av1_hua_{county_fips}.shp"

    # Check if file exists - if not then download
    if os.path.exists(folderpath + filename):
        print("   File",filename,"has already been downloaded.")
        nsi_gdf_hua = gpd.read_file(folderpath + filename)
        # Check what the download date was
        print("   Download date was",nsi_gdf_hua['bldgsource'].unique())
        if replace:
            print("   Replacing file with new download.")
            # exit if statement
        else:
            return nsi_gdf_hua

    # Read in file with NSI API call    
    nsi_api_call = 'https://nsi.sec.usace.army.mil/nsiapi/structures?fips=' + county_fips
    nsi_gdf = gpd.read_file(nsi_api_call)

    # Create string with current date
    from datetime import date
    today = date.today()
    today_string = today.strftime("%Y-%m-%d")

    # prepare file for HUA
    # add column for unique id based on fd_id and bid
    # loop through unique_id_vars and add to unique_id
    nsi_gdf[unique_id] = 'nsi'
    for var in unique_id_vars:
        nsi_gdf[unique_id] = nsi_gdf[unique_id]+'-'+nsi_gdf[var].astype(str)

    # add source column
    nsi_gdf['bldgsource'] = 'NSI_'+today_string
    # Make new dataframe with key columns
    nsi_gdf_hua = nsi_gdf[[unique_id]+unique_id_vars+keepvars+['bldgsource','geometry']]

    # Save file in Source Data folder
    print("   Saving file",folderpath + filename)
    nsi_gdf_hua.to_file(folderpath + filename)

    return nsi_gdf_hua