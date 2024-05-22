
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

from pyincore_data_addons.ICD01a_obtain_sourcedata import obtain_sourcedata
from pyincore_data_addons.ICD02a_clean import clean_comm_data_intrsctn
from pyincore_data_addons.ICD03a_results_table import pop_results_table as viz
from pyincore_data_addons.SourceData.nces_ed_gov.nces_01a_obtain \
    import nces_obtain_ccd0910
from pyincore_data_addons.SourceData.nces_ed_gov.nces_02a_tidy \
    import tidy_nces
from pyincore_data_addons.ICD00b_directory_design import directory_design
from pyincore_data_addons.SourceData.api_census_gov.hui_add_categorical_char \
     import add_new_char_by_random_merge_2dfs
     

def intersect_prechui_nces(communities, outputfolder, seed, basevintage):
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
    """
    version_text = "v0-2-0"

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
    obtain_dfs = obtain_sourcedata(communities=communities,
                                outputfolder = outputfolder,
                                seed = seed,
                                basevintage = basevintage)

    hui_df, prechui_df = obtain_dfs.read_hui_prechui_data_csv_to_df()

    # Read in Place Names 
    place_df = obtain_dfs.block_place_csv_to_df()

    # Merge Person Records with place names
    ## Add Place to Person Record Data
    # Place (city) is a helpful variable for 
    # exploring results of person records
    keep_vars = ['Block2010','PLCGEOID10','PLCNAME10','PUMGEOID10','rppnt4269']
    prechui_df = pd.merge(left = prechui_df,
                          right = place_df[keep_vars],
                          on = 'Block2010')
    # fill in missing placenames
    prechui_df.loc[prechui_df['PLCNAME10'].isnull(),'PLCNAME10'] = 'Unincorporated'
    """ Explore data
    hui_df.head()
    prechui_df.head()
    prechui_df.describe().T
    """

    # Add Grade Level
    clean_df = clean_comm_data_intrsctn(seed=seed,
                                    randage_var = 'randagePCT12',
                                    kgentage = 5,       # Kindergarten entrance age
                                    kgageby  = '08-31', # Kindergarten age by date
                                    schoolyear = '2009',
                                    census_day = '2010-04-01')
    gradelevel_df = clean_df.add_gradelevel_to_prechui(
                                    input_df = prechui_df)

    # Add race by 5 categories to match NCES
    gradelevel_df = clean_df.add_racecat5(gradelevel_df)

    # Obtain School Attendance Boundary Data
    sabs_df = obtain_dfs.read_nces_sab_csv_to_df()

    # Merge Person Records with SABS
    sabs_gradelevel_df = pd.merge(left = gradelevel_df,
                                right = sabs_df,
                                on = 'Block2010')

    # Manual fix for Robeson County SABS
    sabs_gradelevel_df = clean_df.manual_fix_RobesonCounty(sabs_gradelevel_df)

    # Identify Community of Interest
    # For Lumberton the community of interest is Lumberton Junior High SABS
    sabs_gradelevel_df['CommunityFocus'] = 'Outside Community'
    sabs_gradelevel_df.loc[sabs_gradelevel_df['ncessch_2']== '370393002236' ,\
            'CommunityFocus'] = 'Inside Community'


    ## Read in NCES Student Data
    srec_df = tidy_nces(outputfolder = outputfolders['TidySourceData'])

    #Random Merge with Person Records
    """
    Check data types before merge
    geo_levels = ['ncessch_1','ncessch_2','ncessch_3','ncessch_5','ncessch_6']  
    common_group_vars = ['gradelevel1','gradelevel2','sex','racecat5']
    sabs_gradelevel_df[geo_levels+common_group_vars].dtypes
    srec_df[geo_levels+common_group_vars].dtypes
    """
    print("\n***************************************")
    print("    Random merge between Person Records and Student records.")
    print("***************************************\n")
   
    # intersect by each grade level over school options
    prechui_intersect_srec_df = sabs_gradelevel_df.copy()
    for gradelevel in ['gradelevel1','gradelevel2','gradelevel3']:
        prechui_srec = add_new_char_by_random_merge_2dfs(
                dfs = {'primary'  : {'data': prechui_intersect_srec_df, 
                                'primarykey' : 'precid',
                                'geolevel' : 'School Attendance Boundary',
                                'geovintage' :'2010',
                                'notes' : 'Person records with race, hispan, gradelevel, schoolid, sex.'},
                'secondary' : {'data': srec_df, 
                                'primarykey' : 'srecid', # primary key needs to be different from new char
                                'geolevel' : 'School Attendance Boundary',
                                'geovintage' :'2010',
                                'notes' : 'Student Record Data.'}},
                seed = seed, #self.seed,
                common_group_vars = [gradelevel,'sex','racecat5'],
                new_char = 'NCESSCH',
                extra_vars = ['SCHNAM09','gradelevel','LATCOD09','LONCOD09'],
                geolevel = "Block",
                geovintage = "2010",
                by_groups = {'NA' : {'by_variables' : []}},
                fillna_value= '-999',
                state_county = state_county, #self.state_county,
                outputfile = "prec_srec_schoolid",
                outputfolder = outputfolders['RandomMerge']) #self.outputfolders['RandomMerge'])

        # Set up round options
        rounds = {'options': {
                'studentall' : {'notes' : 'Attempt to merge students on all common group vars.',
                                'common_group_vars' : 
                                        prechui_srec.common_group_vars,
                                'by_groups' :
                                        prechui_srec.by_groups},
                'studentnorace' : {'notes' : 'Attempt to merge students without racecat5.',
                                'common_group_vars' : 
                                        [gradelevel,'sex'],
                                'by_groups' :
                                        prechui_srec.by_groups},
                'studentnosex' : {'notes' : 'Attempt to merge students without sex.',
                                'common_group_vars' : 
                                        [gradelevel],
                                'by_groups' :
                                        prechui_srec.by_groups},
                                },
                'geo_levels' : ['ncessch_1','ncessch_2','ncessch_3','ncessch_5','ncessch_6']                         
                }

        # Update person record school record file for next merge
        prechui_srec_df = prechui_srec.run_random_merge_2dfs(rounds)
        prechui_intersect_srec_df = prechui_srec_df['primary']
        srec_df = prechui_srec_df['secondary']

    """ Explore data
    prechui_srec_df['primary'].head()
    table_df = prechui_srec_df['primary']
    results_df = pd.pivot_table(table_df, 
                    values = ['precid'],
                    index=['NCESSCH','SCHNAM09'], 
                    aggfunc={'precid':'count'}, 
                    margins=True, margins_name = 'Total')
    
    county_condition = (ccd_df['CONUM09'] == '37155')
    keep_vars = ['NCESSCH','SCHNAM09','MEMBER09','CONUM09']
    ccd_county_select = ccd_df[keep_vars].loc[county_condition]

    # Merge CCD data with person record results
    check_results = pd.merge(left = results_df,
                            right = ccd_county_select,
                            on = ['NCESSCH','SCHNAM09'])
                    
    check_results['check_diff'] = check_results['MEMBER09'] - check_results['precid']
    check_results['prct_check_diff'] = check_results['check_diff'] / check_results['MEMBER09']  
    check_results['check_diff'].describe()
    np.array(check_results['precid']).sum()
    np.array(check_results['MEMBER09']).sum()
    np.array(check_results['check_diff']).sum()
    check_results['prct_check_diff'].describe()
    check_results.loc[check_results['prct_check_diff'] > .05]
    gradelevel_condition = (prechui_srec_df['primary']['gradelevel1'] != 'NA')
    school_missing = (prechui_srec_df['primary']['NCESSCH'] == '-999')
    conditions = gradelevel_condition & school_missing
    table_df = prechui_srec_df['primary'].loc[conditions]
    pd.pivot_table(table_df, 
                    values = ['precid'],
                    index=['gradelevel1'], 
                    columns = 'race',
                    aggfunc={'precid':'count'}, 
                    margins=True, margins_name = 'Total')

    table_df = prechui_srec_df['secondary']
    pd.pivot_table(table_df, 
                    values = ['srecid'],
                    index=['NCESSCH_flagsetrm'], 
                    columns = 'LEVEL09',
                    aggfunc={'srecid':'count'}, 
                    margins=True, margins_name = 'Total')
    table_df = prechui_srec_df['secondary']
    pd.pivot_table(table_df, 
                    values = ['srecid'],
                    index=['gradelevel'], 
                    columns = 'NCESSCH_flagsetrm',
                    aggfunc={'srecid':'count'}, 
                    margins=True, margins_name = 'Total')
    table_df = prechui_srec_df['secondary']
    pd.pivot_table(table_df, 
                    values = ['srecid'],
                    index=['racecat5'], 
                    columns = 'NCESSCH_flagsetrm',
                    aggfunc={'srecid':'count'}, 
                    margins=True, margins_name = 'Total')
    table_df = prechui_srec_df['primary']
    pd.pivot_table(table_df, 
                    values = ['precid'],
                    index=['racecat5','gradelevel'], 
                    columns = 'NCESSCH_flagsetrm',
                    aggfunc={'precid':'count'}, 
                    margins=True, margins_name = 'Total')
    """

    """ Explore data
    sabs_gradelevel_df.head(1).T                              
    pd.pivot_table(sabs_gradelevel_df, 
                values = ['precid'],
                index=['high_schnm','ncessch_3'], 
                aggfunc={'precid':'count'}, 
                margins=True, margins_name = 'Total')

    pd.pivot_table(sabs_gradelevel_df, 
                values = ['precid'],
                index=['mid_schnm','ncessch_2'], 
                aggfunc={'precid':'count'}, 
                margins=True, margins_name = 'Total')

    pd.pivot_table(sabs_gradelevel_df, 
                values = ['precid'],
                index=['primary_schnm','ncessch_1'], 
                aggfunc={'precid':'count'}, 
                margins=True, margins_name = 'Total')

    pd.pivot_table(sabs_gradelevel_df.loc[sabs_gradelevel_df['ncessch_2'].isnull()], 
                values = ['precid'],
                index=['high_schnm','ncessch_3','primary_schnm','ncessch_1'], 
                aggfunc={'precid':'count'}, 
                margins=True, margins_name = 'Total')
    
    from pyincore_data_addons.ICD03a_results_table import pop_results_table as viz
    viz.pop_results_table(sabs_gradelevel_df,
                    who = "Total Population by Persons", 
                    what = "by Race, Ethnicity",
                    where = "Robeson County, NC",
                    when = "2010",
                    row_index = 'Race Ethnicity',
                    col_index = 'Family Type',
                    row_percent = "1 Family Household")
    """
    print("\n***************************************")
    print("    Try to polish final prechui srec data.")
    print("***************************************\n")
        
    # Sort data by huid and person counter
    prechui_srec_df['primary'] = \
            prechui_srec_df['primary'].sort_values(by = ['huid','pernum'])
    # move huid to second column
    # Create column list to move primarykey to first column
    primary_key_names = ['precid','huid','pernum','Block2010str']
    columnlist = [col for col in prechui_srec_df['primary'] if col not in primary_key_names]
    new_columnlist = primary_key_names + columnlist
    prechui_srec_df['primary']  = prechui_srec_df['primary'][new_columnlist]

    # Clean geometry column and add lat lon
    prechui_srec_df['primary'] = clean_df.clean_geometry(prechui_srec_df['primary'],
                    projection = "epsg:4269",
                    reproject  = "epsg:4326", 
                    geometryvar = 'rppnt4269',
                    latlontype  = 'hcb')
    # drop extra columns
    prec_srec_df = clean_df.drop_extra_columns(prechui_srec_df['primary'])
    prec_srec_df = clean_df.clean_gradelevel(prec_srec_df)


    print("\n***************************************")
    print("    Save cleaned data file.")
    print("***************************************\n")
    
    output_filename = f'prechui_srec_{version_text}_{state_county}_{basevintage}_rs{seed}'
    csv_filepath = outputfolders['top']+"/"+output_filename+'.csv'
    savefile = sys.path[0]+"/"+csv_filepath
    prec_srec_df.to_csv(savefile, index=False)
    print("File saved:",savefile)
    
    return prechui_srec_df


