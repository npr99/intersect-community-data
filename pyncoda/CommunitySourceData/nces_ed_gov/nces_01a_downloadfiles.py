import os
import wget
import zipfile
import pandas as pd
import geopandas as gpd
import numpy as np
from pyncoda.CommunitySourceData.nces_ed_gov.nces_00c_cleanutils \
    import *

def setup_directory():
    # Create output directory if it does not exist
    output_sourcedata = 'Outputdata\\00_SourceData'
    output_directory = 'Outputdata\\00_SourceData\\nces_ed_gov'
    # Make directory to save output
    if not os.path.exists(output_sourcedata):
        print("Making new directory to save output: ",
            output_sourcedata)
        os.mkdir(output_sourcedata)
    if not os.path.exists(output_directory):
        print("Making new directory to save output: ",
            output_directory)
        os.mkdir(output_directory)
    else:
        print("Directory",output_directory,"Already exists.")

    unzipped_output_directory = output_directory+'\\unzipped'
    # Make directory to save output
    if not os.path.exists(unzipped_output_directory):
        print("Making unzipped_output_directory directory"+
            " to save output: ",unzipped_output_directory)
        os.mkdir(unzipped_output_directory)
    else:
        print("Directory",unzipped_output_directory,
            "Already exists.")

    return output_directory, unzipped_output_directory

def download_unzip_nces_file(downloadfiles, 
                        output_directory, 
                        unzipped_output_directory):
    '''
    Download files from the National Center for Education Statistics
    (NCES) website.  The filelist_df is a dataframe with the URLs
    and file names for the data files and documentation files.
    The output_directory is the folder where the files will be
    downloaded.
    The unzipped_output_directory is the folder where
    the unzipped files will be saved.
    '''

    for file in downloadfiles:
        # Set file path where file will be downloaded
        filepath = output_directory+"/"+file
        print("   Checking to see if file",file,
            "has been downloaded...")
        
        # set URL for where the file is located
        url = downloadfiles[file]+file
        
        # Check if file exists - if not then download
        if not os.path.exists(filepath):
            print("   Downloading: ",file, "from \n",url)
            wget.download(url, out=output_directory)

            # check if files is a zip file
            if file.endswith('.zip'):
                print("   file is a zip file")
                # unzip the downloaded file
                print("   Unzipping",file)
                with zipfile.ZipFile(filepath, 'r') as zip_ref:
                    zip_ref.extractall(unzipped_output_directory)
        else:
            print("   file already exists in folder ")
            

def download_nces_files(downloadlistcsv):
    '''
    Download files from the National Center for Education Statistics
    given a list of files to download, download the files.

    # Expected columns in downloadlistcsv
    Program
    Output
    School Year
    File Description
    Documentation File Name
    Data File Name
    Unzipped Shapefile File Location
    DataPortalURL
    Documentation File URL
    Data File URL
    copyvars
    level
    schtype
    NonIntegerVars
    '''

    output_directory, unzipped_output_directory = setup_directory()

    # Read in the file list
    filelist_df = pd.read_csv(downloadlistcsv)
    
    # Loop through file list and download the data file
    # and documentation for each file.
    for index, files in filelist_df.iterrows():
        '''
        Loop through file list and download the data file
        and documentation for each file.
        '''
        #print("\nDownloading",files['File Description'],"Files for School Year",files['School Year'])
        
        # Create dictionary with documentation and 
        # data file names and associated URL
        downloadfiles = \
            {files['Documentation File Name']:
                files['Documentation File URL'],
            files['Data File Name']:
                files['Data File URL']}
        # Download files
        download_unzip_nces_file(downloadfiles, 
                                  output_directory,
                                  unzipped_output_directory)

    return filelist_df

def create_schoolist_community(downloadlistcsv,
                                county_list,
                                communityname,
                                outputfolder,
                                year
                                ):

    # Setup output file name
    root_filename = "EDGES_GEOCODED_SCHOOLDATA_"+year
    stem_filename = '_'+communityname+'.shp'
    output_filename = root_filename+stem_filename
    outputfilepath = outputfolder+"/"+output_filename
    # Check if output file already exists
    if os.path.exists(outputfilepath):
        print("File",output_filename,"already exists.")
        print("File will not be overwritten.")
        # Read output file into geopandas geodataframe
        schoollist = gpd.read_file(outputfilepath)

        return schoollist
                        
    # Check that all files are downloaded
    filelist_df = download_nces_files(downloadlistcsv)
    # Setup directory
    output_directory, unzipped_output_directory = setup_directory()

    # Create dictionary to hold geopandas dataframes
    schooldata = {}
    select_schooldata = {} 

    # Loop through file list and download the data file
    # and documentation for each file.
    for index, files in filelist_df.iterrows():
        # Convert shapefiles to geopandas dataframe
        #where is unzipped shapefile
        # Check if output type is for the school list
        if files['Output'] == 'SchoolList':
            shapefile = files['Unzipped Shapefile File Location']
            filepath = unzipped_output_directory+"/"+shapefile
            # Set keys
            key1 = files['File Description']
            key2 = files['School Year']

            # check keys
            print("key1",key1)
            print("key2",key2)

            schooldata[(key1,key2)] = gpd.read_file(filepath)
            # Set Coordinate Reference System to to WGS84
            schooldata[(key1,key2)] = \
                schooldata[(key1,key2)].to_crs("epsg:4326")
            # Select data for a single county        
            select_schooldata[(key1,key2)] = \
                select_var(schooldata[(key1,key2)],
                    'CNTY15',county_list)
            # prepare data for appending
            # convert variables to copy into a list
            copyvars = list(files['copyvars'].split(","))
            select_schooldata[(key1,key2)] = \
                prepare_nces_data_for_append( 
                    select_schooldata[(key1,key2)],
                    copyvars,
                    files['level'],
                    files['schtype'],
                    files['School Year'])

    # Append School Data
    schoollist_community = pd.concat(select_schooldata, 
                              ignore_index=True, sort=False)
    # save as shapefile
    schoollist_community.to_file(outputfilepath)
    # save as csv
    schoollist_community.to_csv(outputfilepath[:-4]+'.csv')

    return schoollist_community


def create_sab_community(downloadlistcsv,
                        schoollist_community,
                        communityname,
                        outputfolder,
                        year
                        ):
    # Setup output file name
    root_filename = "EDGES_SAB_"+year
    stem_filename = '_'+communityname+'.shp'
    output_filename = root_filename+stem_filename
    outputfilepath = outputfolder+"/"+output_filename
    # Check if output file already exists
    if os.path.exists(outputfilepath):
        print("File",output_filename,"already exists.")
        print("File will not be overwritten.")
        # Read output file into geopandas geodataframe
        SAB_community = gpd.read_file(outputfilepath)

        return SAB_community

    # Check that all files are downloaded
    filelist_df = download_nces_files(downloadlistcsv)
    # Setup directory
    output_directory, unzipped_output_directory = setup_directory()

    # Loop through file list and download the data file
    # and documentation for each file.
    for index, files in filelist_df.iterrows():
        # Convert shapefiles to geopandas dataframe
        #where is unzipped shapefile
        # Check if output type is for the school list
        if files['Output'] == 'SAB':
            shapefile = files['Unzipped Shapefile File Location']
            filepath = unzipped_output_directory+"/"+shapefile
            # Set keys
            key1 = files['File Description']
            key2 = files['School Year']

            # check keys
            print("key1",key1)
            print("key2",key2)

            SAB_file = gpd.read_file(filepath)
            
    ## Select NCES SAB data for a single county
    # Create list of School identification numbers (NCESSCH)
    # `NCESSCH` values
    # The appended school list file renamed
    # the `NCESSCH` variable to `ncesid`
    NCESSCH_list = schoollist_community.ncesid.tolist()
    # Create list of `LEAID` values
    # Local education agency identification numbers (LEAID) 
    # The appended school list file renamed
    # the `LEAID` variable to `ncesid`
    LEAID_list = NCESSCH_list

    SAB_community = \
        select_NCES_sabs(SAB_file,NCESSCH_list,LEAID_list)

    # save as shapefile
    SAB_community.to_file(outputfilepath)

    return SAB_community