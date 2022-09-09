import matplotlib.pyplot as plt
from scipy.stats import pearsonr
import pandas as pd
from pyncoda.ncoda_00d_cleanvarsutils import *

def predict_residential_addresspoints(building_to_block_gdf,
                                     hui_df,
                                     hui_blockid,
                                     bldg_blockid,
                                     bldg_uniqueid,
                                     placename_var,
                                     archetype_var, 
                                     residential_archetypes,
                                     building_area_var,
                                     building_area_cutoff):
    """
    Function that attempts to predict housing units in a structure.
    Function runs three rounds of checks and updates the housing unit counts in each round.
    To Do - This function repeats significant blocks of code that could 
    be split into sub functions.
    """
    
    # Create a copy of the building data frame
    bldg_df = building_to_block_gdf.copy(deep=True)
    # start by assuming that buildings have 0 residential address points
    bldg_df['residentialAP1'] = 0
    bldg_df['residentialAP1'].label = "Residential Address Point Round 1"
    bldg_df['residentialAP1'].note = \
        "Residential Address Point - first attempt to assign residential address points to buildings"
    
    bldg_df['residential'] = 0
    bldg_df['residentialAP1'].label = "Residential Address Point Round 1"
    bldg_df['residentialAP1'].note = \
        "Residential Address Point - first attempt to assign residential address points to buildings"
    

    # Update residential address points by assigning 
    # residential archetypes with 1 housing unit
    for residential_archetype in residential_archetypes:
        condition = (bldg_df[archetype_var] == residential_archetype)
        bldg_df.loc[condition,'residentialAP1'] = 1
        # Length of data frame
        len_bldg_df = bldg_df.loc[condition].shape[0]
        print(len_bldg_df,"Buildings have Residential Archetype",residential_archetype)

    # remove small buildings from residential building list
    condition = (bldg_df[building_area_var] < building_area_cutoff)
    bldg_df.loc[condition,'residentialAP1'] = 0
    # Length of data frame
    len_bldg_df = bldg_df.loc[condition].shape[0]
    print(len_bldg_df,"Buildings have building_area_var less than",building_area_cutoff)

    # Look at shape area divided by the number of address points
    bldg_df[building_area_var+'_by_AP'] = \
        bldg_df[building_area_var] / bldg_df['residentialAP1']

    # remove small buildings from residential building list
    condition = (bldg_df[building_area_var+'_by_AP'] < building_area_cutoff)
    bldg_df.loc[condition,'residentialAP1'] = 0
    # Length of data frame
    len_bldg_df = bldg_df.loc[condition].shape[0]
    print(len_bldg_df,"Buildings have ",building_area_var,"_by_AP less than",building_area_cutoff)

    # Check residential address points count
    condition = (bldg_df['residentialAP1'] == 1)
    len_bldg_df = bldg_df.loc[condition].shape[0]
    print(len_bldg_df,"Buildings assigned residential.")

    # Set residential binary using residentialAP1
    bldg_df['residential'] = bldg_df['residentialAP1']
    bldg_df['bldgcount'] = 1
    
    # Look at address point count by block
    hua_block_counts = hui_df[[hui_blockid,'huid']].groupby(hui_blockid).agg('count')
    hua_block_counts.reset_index(inplace = True)
    hua_block_counts = hua_block_counts.\
        rename(columns={'huid': "apcount", hui_blockid : bldg_blockid })
    # Sum apcount
    hua_apcount = hua_block_counts['apcount'].sum()
    print("Total number of expected housing unit address points in county:",hua_apcount)

    # merge address point counts by block with building data
    bldg_df = pd.merge(right = bldg_df,
                       left = hua_block_counts,
                       right_on = bldg_blockid,
                       left_on =  bldg_blockid,
                       how = 'outer')   

    # fill in missing apcounts with 0 values
    bldg_df['apcount'] = bldg_df['apcount'].fillna(value=0)

    # If Address Point Count (Housing10 + Group Quarters) from Census 
    # is 0 then Residential Address Point is also 0
    condition = (bldg_df['apcount'] == 0)
    len_bldg_df = bldg_df.loc[condition].shape[0]
    print(len_bldg_df,"buildings are in blocks with no housing units.")
    bldg_df.loc[condition,'residentialAP1'] = 0


    # sum Buildings over the column BLOCKID.
    # https://www.geeksforgeeks.org/python-pandas-dataframe-sum/
    block_rap1_gdf = bldg_df[[bldg_blockid,'residentialAP1','bldgcount']].copy(deep=True)
    block_rap1_gdf_sum = block_rap1_gdf.groupby([bldg_blockid]).sum()
    block_rap1_gdf_sum = block_rap1_gdf_sum.rename(columns={
                                            "residentialAP1": "residentialAP1_sum", 
                                            "bldgcount": "bldgcount1_sum"})

    # Merge Sum of Residential Address Points with Census Counts
    census_blocks_df_rap1 = pd.merge(left = hua_block_counts, 
                                    right = block_rap1_gdf_sum,
                                    left_on=bldg_blockid, 
                                    right_on=bldg_blockid, 
                                    how='outer')

    # fill in missing values
    census_blocks_df_rap1['residentialAP1_sum'] = \
        census_blocks_df_rap1['residentialAP1_sum'].fillna(value=0)
    census_blocks_df_rap1['apcount'] = \
        census_blocks_df_rap1['apcount'].fillna(value=0)
    # Compare Bldg Count to Housing 10 Correlation
    # calculate Pearson's correlation
    data1 = census_blocks_df_rap1['residentialAP1_sum']
    data2 = census_blocks_df_rap1['apcount']
    r, prob = pearsonr(data1, data2)
    print('For Round 1 Estimated Residential Address Points correlation: %.3f' % r)

    # Look at error issues in housing unit counts
    census_blocks_df_rap1 = \
        block_error_check_addresspoints(
            census_blocks_df = census_blocks_df_rap1,
            expected_count   = 'apcount',
            estimated_count  = 'residentialAP1_sum',
            building_count   = 'bldgcount1_sum',
            ErrorCheck       = 'ErrorCheck1')

    # Identify difference between expected housing unit count and 
    # sum of estimated address points
    census_blocks_df_rap1['DiffCount1'] = \
        census_blocks_df_rap1['apcount'] - census_blocks_df_rap1['residentialAP1_sum']

    # Merge Block level data with building level data
    keepcolumns = [bldg_blockid,'residentialAP1_sum',
                  'bldgcount1_sum','DiffCount1','ErrorCheck1_int']

    bldg_df_round2 = pd.merge(left = bldg_df, 
                              right = census_blocks_df_rap1[keepcolumns], 
                              left_on=bldg_blockid, 
                              right_on=bldg_blockid, 
                              how='left')


    # For Error Code 5 Make each Building a Residential Building
    condition = (bldg_df_round2['ErrorCheck1_int'] == 5)
    bldg_df_round2.loc[condition,'residentialAP1']=1
    len_bldg_df = bldg_df_round2.loc[condition].shape[0]
    print(len_bldg_df,"buildings with error 5. HU > 0, AP = 0.")

    # Calculate sum of Residential Area By Block
    bldg_df_round2['Res_Area'] = 0
    condition = (bldg_df_round2['residentialAP1'] == 1)
    bldg_df_round2.loc[condition, 'Res_Area'] = bldg_df_round2[building_area_var]
    bldg_df_round2_area = bldg_df_round2[[bldg_blockid,'Res_Area']].copy(deep=True)
    bldg_df_round2_area_sum = bldg_df_round2_area.groupby([bldg_blockid]).sum()
    bldg_df_round2_area_sum = bldg_df_round2_area_sum.\
        rename(columns={"Res_Area": "Sum_Res_Area"})
    bldg_df_round2 = pd.merge(left = bldg_df_round2, 
                              right = bldg_df_round2_area_sum, 
                              left_on=bldg_blockid, 
                              right_on=bldg_blockid, 
                              how='outer')

    # Calculate Number of Address Points Each Building Should Have 
    # based on Housing Unit Count Difference
    bldg_df_round2['residentialAP2'] = \
        (bldg_df_round2['Res_Area'] / bldg_df_round2['Sum_Res_Area']) * \
            bldg_df_round2['DiffCount1'] 

    ### Estimate new Housing Unit Count in Building
    # Using the distributed difference in address points, 
    # round the value and add to the initial estimate.
    # For blocks where the housing unit and address points are equal (Error 2) or 
    # for blocks where the number of address points is greater than 
    # the number of Housing Units (Error 3) 
    # use the estimate from Round 1.
    bldg_df_round2['residentialAP2v2'] = \
        round(bldg_df_round2['residentialAP2'],0) + bldg_df_round2['residentialAP1']
    # If Round 1 had a match between Address Points and Housing Units use Round 1
    condition = (bldg_df_round2['ErrorCheck1_int'] == 2)
    bldg_df_round2.loc[condition,'residentialAP2v2'] = bldg_df_round2['residentialAP1']
    len_bldg_df = bldg_df_round2.loc[condition].shape[0]
    print(len_bldg_df,"buildings with error 2. HU=AP")
    # If Round 1 had a more Address Points than Housing Units Use Round 1
    condition = (bldg_df_round2['ErrorCheck1_int'] == 3)
    bldg_df_round2.loc[condition,'residentialAP2v2'] = bldg_df_round2['residentialAP1']
    len_bldg_df = bldg_df_round2.loc[condition].shape[0]
    print(len_bldg_df,"buildings with error 3. HU<AP.")
    # If Round 1 had no buildings Use Round 1
    condition = (bldg_df_round2['ErrorCheck1_int'] == 9)
    bldg_df_round2.loc[condition,'residentialAP2v2'] = bldg_df_round2['residentialAP1']
    len_bldg_df = bldg_df_round2.loc[condition].shape[0]
    print(len_bldg_df,"buildings with error 9. no buildings for hu.")

    # If Estimated number of Census Housing Units is 0 make AP 0
    condition = (bldg_df_round2['ErrorCheck1_int'] == 1)
    bldg_df_round2.loc[condition,'residentialAP2v2'] = 0
    len_bldg_df = bldg_df_round2.loc[condition].shape[0]
    print(len_bldg_df,"buildings with error 1. HU=0.")

    # sum Buildings over the column BLOCKID.
    # https://www.geeksforgeeks.org/python-pandas-dataframe-sum/
    block_rap2_df = bldg_df_round2[[bldg_blockid,'residentialAP2v2','bldgcount']].copy(deep=True)
    #block_rap2_df['bldgcountv2'] = 1
    block_rap2_df_sum = block_rap2_df.groupby([bldg_blockid]).sum()
    block_rap2_df_sum = \
        block_rap2_df_sum.rename(columns={
                                "residentialAP2v2": "residentialAP2v2_sum", 
                                "bldgcount": "bldgcountv2_sum"})
    # Merge Sum of Residential Address Points with Census Counts
    census_blocks_df_rap2 = pd.merge(
                                  left = census_blocks_df_rap1, 
                                  right = block_rap2_df_sum,
                                  left_on=bldg_blockid, 
                                  right_on=bldg_blockid, 
                                  how='left')

    # Compare Bldg Count to Housing 10 Correlation
    # calculate Pearson's correlation
    data1 = census_blocks_df_rap2['residentialAP2v2_sum']
    data2 = census_blocks_df_rap2['apcount']
    r, prob = pearsonr(data1, data2)
    print('For Round 2 Estimated Residential Address Points correlation: %.3f' % r)

    # Error check round 2
    census_blocks_df_rap2 = \
        block_error_check_addresspoints(
            census_blocks_df = census_blocks_df_rap2,
            expected_count   = 'apcount',
            estimated_count  = 'residentialAP2v2_sum',
            building_count   = 'bldgcountv2_sum',
            ErrorCheck       = 'ErrorCheck2')
    # Identify difference between expected housing unit count and
    # sum of estimated address points
    census_blocks_df_rap2['DiffCount2'] = \
        census_blocks_df_rap2['apcount'] - census_blocks_df_rap2['residentialAP2v2_sum']


    # Merge Block level data with building level data
    keepcolumns = [bldg_blockid,'residentialAP2v2_sum', \
        'bldgcountv2_sum','DiffCount2','ErrorCheck2_int']

    bldg_df_round3 = pd.merge(bldg_df_round2, 
                              census_blocks_df_rap2[keepcolumns], 
                              left_on=bldg_blockid, 
                              right_on=bldg_blockid, 
                              how='left')

    # update estimated address point count
    bldg_df_round3['huestimate'] = bldg_df_round3['residentialAP2v2']
    condition1 = (bldg_df_round3['ErrorCheck2_int']==4)
    condition2 = (bldg_df_round3['residentialAP2v2']>=1)
    condition = condition1 & condition2
    bldg_df_round3.loc[condition,'huestimate'] = bldg_df_round3['huestimate'] + 1
    len_bldg_df = bldg_df_round3.loc[condition].shape[0]
    print(len_bldg_df,"buildings updated huestimate by 1.")

    # Check for condition 5 - too few AP for HUs
    condition1 = (bldg_df_round3['ErrorCheck2_int']==5)
    condition2 = (bldg_df_round3['huestimate'].isna())
    condition = condition1 & condition2
    bldg_df_round3.loc[condition,'huestimate'] = 1
    len_bldg_df = bldg_df_round3.loc[condition].shape[0]
    print(len_bldg_df,"buildings updated huestimate by 1.")

    # sum Buildings over the column BLOCKID.
    # https://www.geeksforgeeks.org/python-pandas-dataframe-sum/
    block_rap3_df = bldg_df_round3[[bldg_blockid,'huestimate','bldgcount']].copy(deep=True)
    #block_rap3_df['bldgcountv3'] = 1
    block_rap3_df_sum = block_rap3_df.groupby([bldg_blockid]).sum()
    block_rap3_df_sum = block_rap3_df_sum.rename(columns={
                                "huestimate": "residentialAP2v3_sum", 
                                "bldgcount": "bldgcountv3_sum"})
    # Merge Sum of Residential Address Points with Census Counts
    census_blocks_df_rap3 = pd.merge(left = census_blocks_df_rap2,
                                    right = block_rap3_df_sum,
                                    left_on=bldg_blockid, 
                                    right_on=bldg_blockid, 
                                    how='left')
    # Compare Bldg Count to Housing 10 Correlation
    # calculate Pearson's correlation
    data1 = census_blocks_df_rap3['residentialAP2v3_sum']
    data2 = census_blocks_df_rap3['apcount']
    r, prob = pearsonr(data1, data2)
    print('For Round 3 Estimated Residential Address Points correlation: %.3f' % r)

    # Error check round 3
    census_blocks_df_rap3 = \
        block_error_check_addresspoints(
            census_blocks_df = census_blocks_df_rap3,
            expected_count   = 'apcount',
            estimated_count  = 'residentialAP2v3_sum',
            building_count   = 'bldgcountv3_sum',
            ErrorCheck       = 'ErrorCheck3')
    # Identify difference between expected housing unit count and 
    # sum of estimated address points
    census_blocks_df_rap3['DiffCount3'] = \
        census_blocks_df_rap3['apcount'] - census_blocks_df_rap3['bldgcountv3_sum']

    # Merge Block level data with building level data
    keepcolumns = [bldg_blockid,'DiffCount3','ErrorCheck3_int','bldgcountv3_sum']

    bldg_df_round3 = pd.merge(left = bldg_df_round3, 
                              right = census_blocks_df_rap3[keepcolumns], 
                              left_on=bldg_blockid, 
                              right_on=bldg_blockid, 
                              how='left')                                

    return_cols1 = [bldg_blockid,bldg_uniqueid,placename_var,
                    archetype_var,'residential','apcount','bldgcount',
                    'huestimate','DiffCount3',"bldgcountv3_sum",
                    'ErrorCheck1_int','ErrorCheck2_int','ErrorCheck3_int']
    return_cols = return_cols1
    return bldg_df_round3[return_cols]


def block_error_check_addresspoints(census_blocks_df, 
                                    expected_count, 
                                    estimated_count,
                                    building_count, 
                                    ErrorCheck):
    """
    Code looks at the difference between the expected address points and the
    estimated address points.
    Returns 10 possible error codes

    Error Codes:
    1. HU = 0 - no housing unit in the block
    2. HU=AP -  housing unit count is equal to address points
    3. HU<AP  - housing unit count is less than address points
    4. HU>AP  - housing unit count is greater than address points
    """

    # Create Error Variable
    census_blocks_df[ErrorCheck+'_int'] = 0
    
    # Set up holders for variables to check
    hu_count =    f"df['{expected_count}']"
    addpt_count = f"df['{estimated_count}']"
    bldg_count =  f"df['{building_count}']"

    # Set up base condition
    base_condition = f"(df['{ErrorCheck}_int'] == 0)"

    # Set up dictionary for error codes
    error_codes_conditions = \
    { 'cat_var' : 
        {'variable_label' : ErrorCheck,
                         'notes' : 'Compares the difference between' + \
                            ' the expected address points and the' + \
                            ' estimated address points.'},
         'condition_list' : 
        {   1 : {'condition': f"({hu_count}.isna()) & " + \
                              f"{base_condition}",
                 'value_label': "1. HU=0",
                 'notes' : 'Block has no housing units based on the US Census.'},
            1 : {'condition': f"({hu_count}==0) & " + \
                              f"{base_condition}",
                 'value_label': "1. HU=0",
                 'notes' : 'Block has no housing units based on the US Census.'},
            2 : {'condition': f"({hu_count} == {addpt_count}) & " + \
                              f"{base_condition}",
                 'value_label': "2. HU=AP",
                 'notes' : 'Best case scenario - Housing Units match Address Points.'},
            3 : {'condition': f"({hu_count} < {addpt_count}) & " + \
                              f"{base_condition}",
                 'value_label': "3. HU<AP",
                 'notes' : 'Second best case scenario - More Address Points than HUs.'},
            8 : {'condition': f"({hu_count} > 0) & " + \
                              f"({bldg_count}.isna()) & " + \
                              f" {base_condition}",
                 'value_label': "8. HU > 0, Building Count = Missing",
                 'notes' : 'No Error - No buildings for expected HUs.'},
            9 : {'condition': f"({hu_count} > 0) & " + \
                              f"({bldg_count} == 0) & " + \
                              f" {base_condition}", 
                 'value_label': "9. HU > 0, Building Count = 0",
                 'notes' : 'No error - No housing units or buildings.'},
            10 : {'condition': f"({hu_count} == 0) & " + \
                              f"({bldg_count}.isna()) & " + \
                              f" {base_condition}", 
                 'value_label': "10. HU = 0, Building Count = Missing",
                 'notes' : 'No error - No housing units or buildings.'},
            5 : {'condition': f"({hu_count} > 0) & " + \
                              f"({addpt_count}==0) & " + \
                              f" {base_condition}",
                 'value_label': "5. HU > 0, AP = 0",
                 'notes' : 'Error to fix - No address points for expected HUs.'},
            4 : {'condition': f"({hu_count} > {addpt_count}) & " + \
                              f"{base_condition}",
                 'value_label': "4. HU>AP",
                 'notes' : 'Error to fix - not enough address points for HUs.'},
            6 : {'condition': f"({hu_count} > 0) & " + \
                              f"({addpt_count}.isna()) & " + \
                              f"({bldg_count} > 0) & " + \
                              f" {base_condition}",
                 'value_label': "6. HU > 0, AP = Missing, But buildings present",
                 'notes' : 'Error to fix - No address points for expected HUs.'},
            7 : {'condition': f"({hu_count} > 0) &" + \
                              f"({addpt_count}.isna()) & " + \
                              f" {base_condition}",
                 'value_label': "7. HU > 0, AP = Missing",
                 'notes' : 'Error to fix - No address points for expected HUs.'}
        }
    }

    pd_df = add_label_cat_conditions_df(
                        df = census_blocks_df, 
                        conditions = error_codes_conditions)

    return pd_df