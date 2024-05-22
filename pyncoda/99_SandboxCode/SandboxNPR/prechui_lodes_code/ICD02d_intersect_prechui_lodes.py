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


def intersect_prechui_lodes(communities, 
                                outputfolder, 
                                seed, 
                                basevintage,
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

    # Read in Housing Unit Inventory and Person Records
    # Read in Housing Unit Inventory and Person Records
    obtain_dfs = obtain_sourcedata(communities=communities,
                                outputfolder = outputfolder,
                                seed = seed,
                                basevintage = basevintage)

    hui_df, prechui_df = obtain_dfs.read_hui_prechui_data_csv_to_df()


    """
    ## Set up person record inventory for random merge
    Need age group categories that match LODES
    """
    lodes_agegroups = {0: {'minageyrs': 0, 'maxageyrs': 13},
                    1: {'minageyrs': 18, 'maxageyrs': 29},
                    2: {'minageyrs': 30, 'maxageyrs': 54},
                    3: {'minageyrs': 55, 'maxageyrs': 65},
                    4: {'minageyrs': 66, 'maxageyrs': 110}}

    prechui_df = tidy_censusapi.add_age_groups(prechui_df,
                                agegroup_dict = lodes_agegroups,
                                group = "LODES",
                                randage_var = "randagePCT12")

    # Possible workers are in LODES age groups 1,2, or 3
    prechui_df['PossibleWorker'] = 0
    prechui_df.loc[prechui_df['agegroupLODES'].isin([1,2,3]),'PossibleWorker'] = 1

    # Add Estimate of Total Workers in Household
    prechui_df = icd_tidy.add_total_sum_byvar(df = prechui_df,  
                            values_to_sum = 'PossibleWorker',
                            by_vars = ['huid'],
                            values_to_sum_col_rename = 'EstTotalWorkers1')
    # Reduce Est Total Workers by 1 if possible
    # This gives second earnings group for future merge
    prechui_df['EstTotalWorkers2'] = 0
    prechui_df.loc[prechui_df['EstTotalWorkers1'] > 1, \
            'EstTotalWorkers2'] = prechui_df['EstTotalWorkers1'] - 1
    prechui_df.loc[prechui_df['EstTotalWorkers1'] == 1, \
            'EstTotalWorkers2'] = 1
    prechui_df['EstTotalWorkers3'] = 0
    prechui_df.loc[prechui_df['EstTotalWorkers2'] > 1, \
            'EstTotalWorkers3'] = prechui_df['EstTotalWorkers2'] - 1
    prechui_df.loc[prechui_df['EstTotalWorkers2'] == 1, \
            'EstTotalWorkers3'] = 1

    # Merge Household Income
    prechui_df = pd.merge(left = prechui_df,
                        right = hui_df[['huid','randincome']],
                        on = 'huid')

    for option in range(1,4):
        # Earnings with all workers
        prechui_df['randAnnualEarnings'+str(option)] =  \
                prechui_df['randincome'] / prechui_df['EstTotalWorkers'+str(option)]
        prechui_df['randMonthEarnings'+str(option)] =  \
                prechui_df['randAnnualEarnings'+str(option)] / 12
        prechui_df = clean_comm_data_intrsctn.\
                add_earningsgroupLODES(input_df = prechui_df,
                earningsvar = 'randMonthEarnings'+str(option),
                groupvar = 'earngingsgroupLODES'+str(option))

    """" # Explore results
        table_df = prechui_lodes_df
        pd.pivot_table(table_df, 
                        values = ['precid','randMonthEarnings3'],
                        index=['earngingsgroupLODES3'], 
                        aggfunc={'precid':'count',
                                'randMonthEarnings3': np.min}, 
                        margins=True, margins_name = 'Total')
        table_df = prechui_lodes_df
        pd.pivot_table(table_df, 
                        values = ['precid','randMonthEarnings3'],
                        index=['earngingsgroupLODES1',
                                'earngingsgroupLODES2',
                                'earngingsgroupLODES3'], 
                        aggfunc={'precid':'count',
                                'randMonthEarnings3': np.min}, 
                        margins=True, margins_name = 'Total')
    """


    """
    ### Read in Cleaned LODES data
    From : G:\Shared drives\HRRC_IN-CORE\Tasks\P4.9 Testebeds\WorkNPR\lodes_workflow_outputv4\

    Source Program:
    LODES_1av4_CleanLODESdata_2022-01-11.ipynb

    File name:
    joblist_v010_JT11_37155_2010_rs133234_prechui.csv

    Posted to:
    G:\Shared drives\HRRC_IN-CORE\Tasks\M5.2-01 Pop inventory\Posted\Labor_Market_Allocation_Output\
    """
    print("******************************")
    print(" Read in LODES Dataset")
    print("******************************")
    #lodes_file_name = "joblist_v010_JT07_37155_2010_rs133234_prechui.csv"
    #posted_relative_path = '\\..\\Posted\\Labor_Market_Allocation_Output'
    lodes_folderpath = sys.path[0]+posted_relative_path
    lodes_filepath = lodes_folderpath+'\\'+lodes_file_name
    jobi_df = pd.read_csv(lodes_filepath)

    # Add Block ID
    jobi_df['Block2010'] = jobi_df['h_geocode_str'].astype(str)

    # Clean up jobid
    jobi_df.loc[:,'uniqueid_part2'] = \
    jobi_df.groupby(['uniqueid_part1']).cumcount() + 1
    # Need to zero pad part2 find the max number of characters
    part2_max = jobi_df['uniqueid_part2'].max()
    part2_maxdigits = len(str(part2_max))
    jobi_df.loc[:,'jobid'] = jobi_df['uniqueid_part1'] + \
        "C" + jobi_df['uniqueid_part2'].apply(lambda x : \
            str(int(x)).zfill(part2_maxdigits))

    print("\n***************************************")
    print("    Try to clean geometry lat lon.")
    print("***************************************\n")
        
    # Clean geometry column and add lat lon
    clean_df = clean_comm_data_intrsctn()
    jobi_df = clean_df.clean_geometry(jobi_df,
                    projection = "epsg:4326",
                    reproject  = "epsg:4326", 
                    geometryvar = 'w_geometry',
                    latlontype  = 'wcb')

    jobi_df = clean_df.clean_geometry(jobi_df,
                    projection = "epsg:4326",
                    reproject  = "epsg:4326", 
                    geometryvar = 'h_geometry',
                    latlontype  = 'hcb')

    print("******************************")
    print(" Split LODES Dataset")
    print(" To append workers that live outside of county to Prechui")
    print("******************************")

    # Identify workers that live outside of county
    jobi_df_outside = jobi_df[jobi_df['h_geocode_countyid'] != int(state_county)]
    jobi_df_inside = jobi_df[jobi_df['h_geocode_countyid'] == int(state_county)]
    
    # Add person record id to outside workers
    # Generate unique ID
    counter_var = 'prec_counter'
    id_type = "PJ"
    # Add Counter
    jobi_df_outside[counter_var] = \
        jobi_df_outside.groupby(['Block2010']).cumcount() + 1
    counter_var_max = jobi_df_outside[counter_var].max()
    counter_var_maxdigits = len(str(counter_var_max))
    jobi_df_outside.loc[:,'precid'] = \
        jobi_df_outside.\
            apply(lambda x: x['Block2010'] + id_type +
            str(x[counter_var]).zfill(counter_var_maxdigits), axis=1)

    jobi_df_outside.loc[:,'Block2010str'] = \
        jobi_df_outside.\
            apply(lambda x: 'B'+ x['Block2010'].zfill(15), axis=1)

    # Append workers that live outside of county to Prechui
    keep_vars = ['precid','Block2010str','Block2010','jobid','sex','race','hispan',
        'agegroupLODES','Earnings','Education',
        'jobtype','IndustryCode',
        'w_geocode_str','h_stabbr','od_distance','w_geometry','h_geometry',
        'hcb_lat','hcb_lon','wcb_lat','wcb_lon']
    jobi_df_outside_prechui = jobi_df_outside[keep_vars]       

 
    print("******************************")
    print(" Prepare LODES Dataset for Merge.")
    print(" Merge workers with persons living in county.")
    print("******************************")

    #keep only required columns
    jobi_df_mergevars = ['Block2010','race','sex','hispan','agegroupLODES']
    jobi_df_new_char = ['jobid']
    jobi_df_extravars = ['IndustryCode','w_geometry','wcb_lat','wcb_lon','h_stabbr', \
                        'Earnings','Education','jobtype','od_distance']
    keep_vars= jobi_df_new_char+jobi_df_mergevars+jobi_df_extravars
    # use jobs that live and work inside county to merge with prechui
    jobi_df_rmerge = jobi_df_inside[keep_vars].copy()

    # Create 3 levels of earnings groups
    jobi_df_rmerge['earngingsgroupLODES1'] = jobi_df_rmerge['Earnings']
    jobi_df_rmerge['earngingsgroupLODES2'] = jobi_df_rmerge['Earnings']
    jobi_df_rmerge['earngingsgroupLODES3'] = jobi_df_rmerge['Earnings']


    ## Set up random merge
    print("\n***************************************")
    print("    Random merge between Person Records and Jobs.")
    print("***************************************\n")
    # intersect by each earnings level over jobs options
    prechui_intersect_job_df = prechui_df.copy()
    for earnings in ['earngingsgroupLODES1','earngingsgroupLODES2','earngingsgroupLODES3']:
        prec_hui_job = add_new_char_by_random_merge_2dfs(
                dfs = {'primary'  : {'data': prechui_intersect_job_df, 
                                'primarykey' : 'precid',
                                'geolevel' : 'Block',
                                'geovintage' :'2010',
                                'notes' : 'Person records with race, hispan, age, sex.'},
                'secondary' : {'data': jobi_df_rmerge, 
                                'primarykey' : 'uniqueid_part1', # primary key needs to be different from new char
                                'geolevel' : 'Block',
                                'geovintage' :'2010',
                                'notes' : 'Job Inventory Data.'}},
                seed = seed, #self.seed,
                common_group_vars = ['agegroupLODES','sex','race','hispan',earnings],
                new_char = 'jobid',
                extra_vars = ['w_geometry','wcb_lat','wcb_lon','IndustryCode','jobtype',
                        'Earnings','Education','h_stabbr','od_distance'],
                geolevel = "Block",
                geovintage = "2010",
                by_groups = {'NA' : {'by_variables' : []}},
                fillna_value= -999,
                state_county = state_county, #self.state_county,
                outputfile = "prec_hui_randomjob",
                outputfolder = outputfolders['RandomMerge']) #self.outputfolders['RandomMerge'])

        # Set up round options
        rounds = {'options': {
                'householderH17' : {'notes' : 'Attempt to merge householder \
                        on all common group vars.',
                                'common_group_vars' : 
                                        prec_hui_job.common_group_vars,
                                'by_groups' :
                                        prec_hui_job.by_groups},
                                },
                'geo_levels' : ['Block','BlockGroup','Tract','County']                         
                }

        prec_hui_job_df = prec_hui_job.run_random_merge_2dfs(rounds)
        prechui_intersect_job_df = prec_hui_job_df['primary']
        jobi_df_rmerge = prec_hui_job_df['secondary']

    # Append jobs outside county
    print("\n***************************************")
    print("    Append jobs for workers that live outside county.")
    print("***************************************\n")
    print("The person record file has {} records.".\
        format(prec_hui_job_df['primary'].shape[0]))
    
    output_df = pd.concat([prec_hui_job_df['primary'],jobi_df_outside_prechui])
    #prec_hui_job_df['primary'] = \
    #    prec_hui_job_df['primary'].concat(jobi_df_outside_prechui)
    print("With workers outside county, the person record file has {} records.".\
        format(output_df.shape[0]))



    """
    ### Explore commute distance
    Interesting anomaly is that the commute distance can be very large. This could be due to several factors. 
    1. The person could have recently moved to the community and the address on their 
        unemployment insurance form could be their old address.
    2. The work location could be a national franchise that has their administrative offices in the community but hires workers from all over the country.
    3. The geocode of the home or work addresses could be incorrect.

    Units in km

    One solution would be to select outliers (3 sd from the mean) and 
    change the County2010 variable (used in randome merge) to the 
    local county. This would mean that the job would be allocated 
    randomly to a person in the county.

    jobi_df.od_distance.describe().T

    """

    return output_df