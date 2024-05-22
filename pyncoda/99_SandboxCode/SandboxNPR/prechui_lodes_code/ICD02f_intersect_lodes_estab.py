import numpy as np
import pandas as pd
import os # For saving output to path
import sys
"""

Python Version      3.8.12 | packaged by conda-forge | (default, Oct 12 2021, 21:22:46) [MSC v.1916 64 bit (AMD64)]
numpy version:      1.22.0
pandas version:     1.3.5

"""

# open, read, and execute python program with reusable commands
from pyincore_data_addons.SourceData.api_census_gov.tidy_censusapi \
    import tidy_censusapi
from pyincore_data_addons.SourceData.api_census_gov.hui_add_categorical_char \
     import add_new_char_by_random_merge_2dfs

from pyincore_data_addons.ICD00b_directory_design import directory_design
from pyincore_data_addons.ICD01a_obtain_sourcedata import obtain_sourcedata
from pyincore_data_addons.ICD02b_tidy import icd_tidy
from pyincore_data_addons.ICD02a_clean import clean_comm_data_intrsctn
from pyincore_data_addons.ICD03a_results_table import pop_results_table as viz

from pyincore_data_addons.SourceData.www_data_axle_com.dataaxle_01a_obtain \
    import obtain_dataaxel
from pyincore_data_addons.SourceData.www_data_axle_com.dataaxle_02a_clean \
    import add_lodes_industrycode

from pyincore_data_addons.ICD02e_intersect_location import df2gdf_WKTgeometry
from pyincore_data_addons.ICD02e_intersect_location import nearest_pt_search
from pyincore_data_addons.ICD02e_intersect_location import spatial_join_polygon_point


from pyincore_data_addons.ICD02b_tidy \
    import icd_tidy as icdtidy 

def intersect_lodes_estab(communities, outputfolder, 
                            seed, 
                            basevintage,
                            estab_data_folder_path,
                            lodes_file_name,
                            posted_relative_path):
    """
    Example Datastructure inputs:
    communities = {'Lumberton_NC' : {
                        'community_name' : 'Lumberton, NC',
                        'counties' : { 
                            1 : {'FIPS Code' : '37155', 'Name' : 'Robeson County, NC'}}},                   
                    }


    seed = 9876
    basevintage = '2010'
    outputfolder = "ICD_workflow_2022-01-19"

    estab_data_folder_path = 'C:\\MyProjects\\HRRCProjects\\IN-CORE\\WorkNPR\\'
        establishment data is based on data-axel data which is not public
    lodes_file_name = "joblist_v010_JT07_37155_2010_rs133234_prechui.csv"
    posted_relative_path = '\\..\\Posted\\Labor_Market_Allocation_Output'

    """
    # Setup directory design
    for community in communities.keys():
        print("Intersect Community Data for:",\
            communities[community]['community_name'])
        for county in communities[community]['counties'].keys():
            state_county = communities[community]['counties'][county]['FIPS Code']
            state_county_name  = communities[community]['counties'][county]['Name']
            print(state_county_name,': county FIPS Code',state_county)
        
            outputfolders = directory_design(state_county_name = state_county_name,
                                                outputfolder = outputfolder)

    estab_df = obtain_dataaxel(year = '2010',
                FIPS_Code = 37155,
                State = 'NC',
                outputfolder = outputfolders["SourceData"],
                folderpath = estab_data_folder_path)

    # Add lodes industry code
    estab_df = add_lodes_industrycode(estab_df)

    """ Explore results
    estab_df.groupby(['IndustryCode','NAICS2D']).\
        aggregate({'Employee_Size_Location':np.sum})

    from pyincore_viz.geoutil import GeoUtil as viz
    map = viz.plot_gdf_map(estab_block_gdf,column='IndustryCode', category=True)
    print("Map of data-axel Establishments")
    map
    """

    ## Add Census Block ID
    # Read in Block Data
    block_df = obtain_sourcedata.block_place_csv_to_df()

    # Prepare data for spatial join
    block_gdf = df2gdf_WKTgeometry(df = block_df, \
     projection = "epsg:4326",reproject="epsg:4326")

    estab_gdf = df2gdf_WKTgeometry(df = estab_df, \
     projection = "epsg:4326",reproject="epsg:4326")

    # Run spatial join between points and polygons
    print("******************************")
    print(" Spatial Join Establishments with blocks")
    print("******************************")
    estab_block_gdf = spatial_join_polygon_point(points_gdf = estab_gdf,
                           polygon_gdf = block_gdf,
                           epsg = 4326,
                           join_column_list = ['Block2010','PLCNAME10']) 
                
    # Check missing block values
    missing_block = estab_block_gdf.loc[estab_block_gdf['Block2010'].isnull()]
    print(len(missing_block),"Establishments have missing block data")
    print("Double check to ensure that missing block data is due to")
    print("establishment geocode being just outside of county boundary.")
    """ Explore     
    map = viz.plot_gdf_map(missing_block,column='IndustryCode', category=True)
    print("Map of data-axel Establishments Missing Block ID")
    map
    """

    # Drop observations missing block - on the outside of the county boundary
    estab_block_gdf = estab_block_gdf.loc[~(estab_block_gdf['Block2010'].isnull())]

    print("******************************")
    print(" Expand Dataset")
    print("******************************")
    estab_block_gdf['jobcount'] = estab_block_gdf['Employee_Size_Location']
    df_expand = icdtidy.expand_df(estab_block_gdf,'jobcount')

    print("     Add Counter")
    df_expand['jestabid_counter'] = df_expand.groupby(['estabid']).cumcount() + 1
    print("     Generate unique ID")
    counter_var = 'jestabid_counter'
    primary_key = 'jestabid'
    estabid = 'estabid'
    id_type = "J"
    datayear = '2010'
    counter_var_max = df_expand[counter_var].max()
    counter_var_maxdigits = len(str(counter_var_max))
    print("     Counter max length",counter_var_maxdigits)
    df_expand.loc[:,primary_key] = df_expand.apply(lambda x: \
                                    id_type + x[estabid] + 
                                    'yr' + datayear + 'c' +
                                    str(x[counter_var]).\
                                        zfill(counter_var_maxdigits), axis=1)

    estab_job_list_df = df_expand.copy()

    print("******************************")
    print(" Read in LODES Dataset")
    print("******************************")
    #lodes_file_name = "joblist_v010_JT07_37155_2010_rs133234_prechui.csv"
    #posted_relative_path = '\\..\\Posted\\Labor_Market_Allocation_Output'
    lodes_folderpath = sys.path[0]+posted_relative_path
    lodes_filepath = lodes_folderpath+'\\'+lodes_file_name
    jobi_df = pd.read_csv(lodes_filepath)

    # Add Block ID based on work location
    jobi_df['Block2010'] = jobi_df['w_geocode_str'].astype(str)

    # Clean up jobid
    jobi_df.loc[:,'uniqueid_part2'] = \
    jobi_df.groupby(['uniqueid_part1']).cumcount() + 1
    # Need to zero pad part2 find the max number of characters
    part2_max = jobi_df['uniqueid_part2'].max()
    part2_maxdigits = len(str(part2_max))
    jobi_df.loc[:,'jobid'] = jobi_df['uniqueid_part1'] + \
        "C" + jobi_df['uniqueid_part2'].apply(lambda x : \
            str(int(x)).zfill(part2_maxdigits))

    ## Attempt Random Merge by Blockid 
    ## Set up random merge
    print("\n***************************************")
    print("    Random merge between Job Records and Establishment.")
    print("***************************************\n")
    jrec_estab = add_new_char_by_random_merge_2dfs(
        dfs = {'primary'  : {'data': jobi_df, 
                        'primarykey' : 'jobid',
                        'geolevel' : 'Block',
                        'geovintage' :'2010',
                        'sort_vars' : [], # Merge shorter commute distance first
                        'sort_vars_ascending' : [],
                        'notes' : 'Job records with industry code.'},
        'secondary' : {'data': estab_job_list_df, 
                        'primarykey' : 'jestabid', # primary key needs to be different from new char
                        'geolevel' : 'Block',
                        'geovintage' :'2010',
                        'sort_vars' : ['Employee_Size_Location'], # Merge small establishments first
                        'sort_vars_ascending' : [True],
                        'notes' : 'Job List based on Establishment List Data.'}},
        seed = seed, #self.seed,
        common_group_vars = ['IndustryCode'],
        new_char = 'estabid',
        extra_vars = ['Latitude','Longitude','NAICS2D','NAICS4D','SIName','Employee_Size_Location'],
        geolevel = "Block",
        geovintage = "2010",
        by_groups = {'NA' : {'by_variables' : []}},
        fillna_value= '-999',
        state_county = state_county, #self.state_county,
        outputfile = "jrec_randomestab",
        outputfolder = outputfolders['RandomMerge']) #self.outputfolders['RandomMerge'])

    # Set up round options
    rounds = {'options': {
        'IndustryCode' : {'notes' : 'Attempt to merge based on Industry Code.',
                        'common_group_vars' : 
                                jrec_estab.common_group_vars,
                        'by_groups' :
                                jrec_estab.by_groups},
                        },
        'geo_levels' : ['Block','BlockGroup','Tract','County']                         
        }

    jrec_estab_df = jrec_estab.run_random_merge_2dfs(rounds)

    return jrec_estab_df