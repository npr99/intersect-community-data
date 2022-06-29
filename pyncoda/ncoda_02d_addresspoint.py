import matplotlib.pyplot as plt
from scipy.stats import pearsonr
import pandas as pd

def predict_residential_addresspoints(bldg_df,
                                     hui_df,
                                     building_to_block_df,
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
    
    # start by assuming that buildings have 0 residential address points
    bldg_df.loc[:,'residentialAP1'] = 0
    bldg_df['residentialAP1'].label = "Residential Address Point Round 1"
    bldg_df['residentialAP1'].note = \
        "Residential Address Point - first attempt to assign residential address points to buildings"
    
    bldg_df.loc[:,'residential'] = 0
    bldg_df['residentialAP1'].label = "Residential Address Point Round 1"
    bldg_df['residentialAP1'].note = \
        "Residential Address Point - first attempt to assign residential address points to buildings"
    

    # Update residential address points by assigning residential archetypes with 1 housing unit
    for residential_archetype in residential_archetypes:
        bldg_df.loc[bldg_df[archetype_var] == residential_archetype,'residentialAP1'] = 1

    # remove small buildings from residential building list
    bldg_df.loc[bldg_df[building_area_var] < building_area_cutoff,'residentialAP1'] = 0

    # Look at shape area divided by the number of address points
    bldg_df.loc[:,'gsq_foot_by_AP'] = bldg_df[building_area_var] / bldg_df['residentialAP1']

    # remove small buildings from residential building list
    bldg_df.loc[bldg_df['gsq_foot_by_AP'] < building_area_cutoff,'residentialAP1'] = 0

    # Set residential binary using residentialAP1
    bldg_df.loc[:,'residential'] = bldg_df['residentialAP1']
    
    # Look at address point count by block
    hua_block_counts = hui_df[['Block2010','huid']].groupby('Block2010').agg('count')
    hua_block_counts.reset_index(inplace = True)
    hua_block_counts = hua_block_counts.rename(columns={'huid': "apcount", 'Block2010' : 'BLOCKID10' })

    # What columns are shared by the building and building to block dataframes
    building_column_list = [col for col in bldg_df]
    builidng_to_block_merge_columns = \
        [col for col in building_to_block_df if col in building_column_list]
    
    # add block id to building inventory
    bldg_df = pd.merge(right = bldg_df,
                       left = building_to_block_df,
                       right_on = builidng_to_block_merge_columns,
                       left_on = builidng_to_block_merge_columns,
                       how = 'outer')
    # merge address point counts by block with building data
    bldg_df.loc[:,'BLOCKID10'] = bldg_df['BLOCKID10'].astype(str)
    hua_block_counts.loc[:,'BLOCKID10'] = hua_block_counts['BLOCKID10'].astype(str)
    bldg_df = pd.merge(right = bldg_df,
                       left = hua_block_counts,
                       right_on = ['BLOCKID10'],
                       left_on = ['BLOCKID10'],
                       how = 'outer')   
    # fill in missing apcounts with 0 values
    bldg_df.loc[:,'apcount'] = bldg_df['apcount'].fillna(value=0)

    # If Address Point Count (Housing10 + Group Quarters) from Census 
    # is 0 then Residential Address Point is also 0
    condition = (bldg_df['apcount'] == 0)
    count_of_buildings_in_block_with_nohousingunits = bldg_df.loc[condition].shape[0]
    print(count_of_buildings_in_block_with_nohousingunits,\
        "buildings are in blocks with no housing units.")
    bldg_df.loc[condition,'residentialAP1'] = 0

    # sum Buildings over the column BLOCKID.
    # https://www.geeksforgeeks.org/python-pandas-dataframe-sum/
    block_rap1_gdf = bldg_df[['BLOCKID10','residentialAP1']]
    block_rap1_gdf.loc[:,'bldgcount'] = 1
    block_rap1_gdf_sum = block_rap1_gdf.groupby(['BLOCKID10']).sum()

    # Merge Sum of Residential Address Points with Census Counts
    census_blocks_df_rap1 = pd.merge(hua_block_counts, block_rap1_gdf_sum,
                                    left_on='BLOCKID10', right_on='BLOCKID10', how='left')

    # Compare Bldg Count to Housing 10 Correlation
    census_blocks_df_rap1[['residentialAP1','apcount']].corr()
    # calculate Pearson's correlation
    data1 = census_blocks_df_rap1['residentialAP1']
    data2 = census_blocks_df_rap1['apcount']
    corr, _ = pearsonr(data1, data2)
    print('For Round 1 Estimated Residential Address Points correlation: %.3f' % corr)

    # Look at error issues in housing unit counts
    census_blocks_df_rap1 = \
        block_error_check_addresspoints(
            census_blocks_df = census_blocks_df_rap1,
            expected_count   = 'apcount',
            estimated_count  = 'residentialAP1',
            building_count   = 'bldgcount',
            ErrorCheck       = 'ErrorCheck1')

    # Identify difference between expected housing unit count and 
    # sum of estimated address points
    census_blocks_df_rap1.loc[:,'DiffCount1'] = \
        census_blocks_df_rap1['apcount'] - census_blocks_df_rap1['residentialAP1']
    census_blocks_df_rap1 = census_blocks_df_rap1.rename(columns={
                                            "residentialAP1": "residentialAP1_sum", 
                                            "bldgcount": "bldgcount1_sum"})

    # Merge Block level data with building level data
    keepcolumns = ['BLOCKID10','residentialAP1_sum','bldgcount1_sum','DiffCount1','ErrorCheck1']

    bldg_df_round2 = pd.merge(bldg_df, census_blocks_df_rap1[keepcolumns], 
                                    left_on='BLOCKID10', right_on='BLOCKID10', how='left')

    # For Error Code 5 Make each Building a Residential Building
    bldg_df_round2.loc[(bldg_df_round2['ErrorCheck1'] == "5. HU > 0, AP = 0"),'residentialAP1']=1

    # Calcuate sum of Residential Area By Block
    bldg_df_round2.loc[:,'Res_Area'] = 0
    bldg_df_round2.loc[(bldg_df_round2['residentialAP1']>=1), 'Res_Area'] = \
        bldg_df_round2[building_area_var]
    bldg_df_round2_area = bldg_df_round2[['BLOCKID10','Res_Area']]
    bldg_df_round2_area_sum = bldg_df_round2_area.groupby(['BLOCKID10']).sum()
    bldg_df_round2_area_sum = bldg_df_round2_area_sum.rename(columns={"Res_Area": "Sum_Res_Area"})
    bldg_df_round2 = pd.merge(bldg_df_round2, bldg_df_round2_area_sum, 
                                  left_on='BLOCKID10', right_on='BLOCKID10', how='outer')

    # Calculate Number of Address Points Each Building Should Have 
    # based on Housing Unit Count Difference
    bldg_df_round2.loc[:,'residentialAP2'] = (bldg_df_round2['Res_Area'] / bldg_df_round2['Sum_Res_Area']) * bldg_df_round2['DiffCount1'] 

    ### Estimate new Housing Unit Count in Building
    # Using the distributed difference in address points, 
    # round the value and add to the initial estimate.
    # For blocks where the housing unit and address points are equal (Error 2) or 
    # for blocks where the number of address points is greater than 
    # the number of Housing Units (Error 3) 
    # use the estimate from Round 1.
    bldg_df_round2.loc[:,'residentialAP2v2'] = \
        round(bldg_df_round2['residentialAP2'],0) + bldg_df_round2['residentialAP1']
    # If Round 1 had a match between Address Points and Housing Units use Round 1
    bldg_df_round2.loc[(bldg_df_round2['ErrorCheck1'] == \
        "2. HU=AP"),'residentialAP2v2'] = bldg_df_round2['residentialAP1']
    # If Round 1 had a more Address Points than Housing Units Use Round 1
    bldg_df_round2.loc[(bldg_df_round2['ErrorCheck1'] == \
        "3. HU<AP"),'residentialAP2v2'] = bldg_df_round2['residentialAP1']

    # If Estiamted number of Census Housing Units is 0 make AP 0
    bldg_df_round2.loc[(bldg_df_round2['ErrorCheck1'] == \
        "1. HU=0"),'residentialAP2v2'] = 0

    # sum Buildings over the column BLOCKID.
    # https://www.geeksforgeeks.org/python-pandas-dataframe-sum/
    block_rap2_df = bldg_df_round2[['BLOCKID10','residentialAP2v2']]
    block_rap2_df['bldgcountv2'] = 1
    block_rap2_df_sum = block_rap2_df.groupby(['BLOCKID10']).sum()
    # Merge Sum of Residential Address Points with Census Counts
    census_blocks_df_rap2 = pd.merge(
                                  left = census_blocks_df_rap1, 
                                  right = block_rap2_df_sum,
                                  left_on='BLOCKID10', 
                                  right_on='BLOCKID10', 
                                  how='left')

    # Compare Bldg Count to Housing 10 Correlation
    # calculate Pearson's correlation
    data1 = census_blocks_df_rap2['residentialAP2v2']
    data2 = census_blocks_df_rap2['apcount']
    corr, _ = pearsonr(data1, data2)
    print('For Round 2 Estimated Residential Address Points correlation: %.3f' % corr)

    # Error check round 2
    census_blocks_df_rap2 = \
        block_error_check_addresspoints(
            census_blocks_df = census_blocks_df_rap2,
            expected_count   = 'apcount',
            estimated_count  = 'residentialAP2v2',
            building_count   = 'bldgcountv2',
            ErrorCheck       = 'ErrorCheck2')
    # Identify difference between expected housing unit count and
    # sum of estimated address points
    census_blocks_df_rap2.loc[:,'DiffCount2'] = \
        census_blocks_df_rap2['apcount'] - census_blocks_df_rap2['residentialAP2v2']
    census_blocks_df_rap2 = \
        census_blocks_df_rap2.rename(columns={
                                "residentialAP2v2": "residentialAP2v2_sum", 
                                "bldgcountv2": "bldgcountv2_sum"})

    # Merge Block level data with building level data
    keepcolumns = ['BLOCKID10','residentialAP2v2_sum', \
        'bldgcountv2_sum','DiffCount2','ErrorCheck2']

    bldg_df_round3 = pd.merge(bldg_df_round2, census_blocks_df_rap2[keepcolumns], 
                                    left_on='BLOCKID10', right_on='BLOCKID10', how='left')

    # update estimated address point count
    bldg_df_round3.loc[:,'huestimate'] = bldg_df_round3['residentialAP2v2']
    bldg_df_round3.loc[(bldg_df_round3['ErrorCheck2']=="4. HU>AP") &
                          (bldg_df_round3['residentialAP2v2']>=1),
                           'huestimate'] = bldg_df_round3['huestimate'] + 1

    # sum Buildings over the column BLOCKID.
    # https://www.geeksforgeeks.org/python-pandas-dataframe-sum/
    block_rap3_df = bldg_df_round3[['BLOCKID10','huestimate']]
    block_rap3_df.loc[:,'bldgcountv3'] = 1
    block_rap3_df_sum = block_rap3_df.groupby(['BLOCKID10']).sum()
    # Merge Sum of Residential Address Points with Census Counts
    census_blocks_df_rap3 = pd.merge(census_blocks_df_rap2, block_rap3_df_sum,
                                    left_on='BLOCKID10', right_on='BLOCKID10', how='left')
    # Compare Bldg Count to Housing 10 Correlation
    # calculate Pearson's correlation
    data1 = census_blocks_df_rap3['huestimate']
    data2 = census_blocks_df_rap3['apcount']
    corr, _ = pearsonr(data1, data2)
    print('For Round 3 Estimated Residential Address Points correlation: %.3f' % corr)

    # Error check round 3
    census_blocks_df_rap3 = \
        block_error_check_addresspoints(
            census_blocks_df = census_blocks_df_rap3,
            expected_count   = 'apcount',
            estimated_count  = 'huestimate',
            building_count   = 'bldgcountv3',
            ErrorCheck       = 'ErrorCheck3')
    # Identify difference between expected housing unit count and 
    # sum of estimated address points
    census_blocks_df_rap3.loc[:,'DiffCount3'] = \
        census_blocks_df_rap3['apcount'] - census_blocks_df_rap3['huestimate']
    census_blocks_df_rap3 = census_blocks_df_rap3.rename(columns={
                                            "residentialAP2v2": "residentialAP2v2_sum", 
                                            "bldgcountv2": "bldgcountv2_sum"})

    # Merge Block level data with building level data
    keepcolumns = ['BLOCKID10','DiffCount3','ErrorCheck3']

    bldg_df_round3 = pd.merge(left = bldg_df_round3, 
                              right = census_blocks_df_rap3[keepcolumns], 
                              left_on='BLOCKID10', 
                              right_on='BLOCKID10', 
                              how='left')                                

    return_cols1 = ['BLOCKID10','placeNAME10',archetype_var,'residential',
                    'huestimate','DiffCount3','ErrorCheck3']
    return_cols = builidng_to_block_merge_columns + return_cols1
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
    """
    # Create Error Variable
    census_blocks_df.loc[:,ErrorCheck] = "0. Not Checked"
    housing_unit_count = census_blocks_df[expected_count]
    addpt_count = census_blocks_df[estimated_count]
    bldg_count = census_blocks_df[building_count]

    base_condition = (census_blocks_df[ErrorCheck] == "0. Not Checked")
    census_blocks_df.loc[(housing_unit_count.isna()) &
                         base_condition
                         , ErrorCheck] = "1. HU=0"
    census_blocks_df.loc[(housing_unit_count == addpt_count) &
                         base_condition
                         , ErrorCheck] = "2. HU=AP"
    census_blocks_df.loc[(housing_unit_count < addpt_count) &
                        base_condition
                         , ErrorCheck] = "3. HU<AP"
    census_blocks_df.loc[(housing_unit_count > addpt_count)
                         & base_condition
                         , ErrorCheck] = "4. HU>AP"
    census_blocks_df.loc[(housing_unit_count > 0) & 
                         (addpt_count == 0) &
                         base_condition, ErrorCheck] = "5. HU > 0, AP = 0"
    census_blocks_df.loc[(housing_unit_count > 0) & 
                         (addpt_count.isna()) & 
                         (bldg_count > 0)
                         , ErrorCheck] = "6. HU > 0, AP = Missing, But buildings present"
    census_blocks_df.loc[(housing_unit_count > 0) & 
                         (addpt_count.isna())
                         , ErrorCheck] = "7. HU > 0, AP = Missing"
    census_blocks_df.loc[(housing_unit_count > 0) & 
                         (bldg_count.isna())
                         , ErrorCheck] = "8. HU > 0, Building Count = Missing"
    census_blocks_df.loc[(housing_unit_count.isna()) & 
                         (bldg_count.isna())
                         , ErrorCheck] = "9. HU = 0, Building Count = Missing"

    return census_blocks_df


''' Example code

builidng_to_block_df  = pd.read_csv(filename, 
                            usecols = ['OBJECTID','Parcel_ID','BLOCKID10','placeNAME10','placeGEOID10'],
                            dtype={'BLOCKID10': str} )
residential_archetypes = { 1 : 'One-story residential building on a crawlspace foundation',
                           2 : 'One-story residential building on a slab-on-grade foundation',
                           3 : 'Two-story residential building on a crawlspace foundation',
                           4 : 'Two-story residential building on a slab-on-grade foundation'}
huesimate_df = predict_residential_addresspoints(bldg_df = bdmg_df,
                                            hui_df = hui_df,
                                            builidng_to_block_df = builidng_to_block_df,
                                            archetype_var = 'F_Arch',
                                            residential_archetypes = residential_archetypes,
                                            building_area_var = 'Bldg_Area',
                                            building_area_cutoff = 30,
                                            )
pd.crosstab(huesimate_df['F_Arch'], huesimate_df['huestimate'], margins=True, margins_name="Total")
huesimate_df.columns

huesimate_df['huestimate'].groupby(huesimate_df['ErrorCheck3']).describe()
huesimate_df.head()

# add dummy single family variable
huesimate_df.loc[(huesimate_df["huestimate"] > 1),'d_sf'] = 0
huesimate_df.loc[(huesimate_df["huestimate"] == 1), 'd_sf'] = 1 

pd.crosstab(huesimate_df['huestimate'], huesimate_df['d_sf'], margins=True, margins_name="Total")

# What columns are shared by the building and building to block dataframes
building_column_list = [col for col in bdmg_df]
builidng_to_block_merge_columns = [col for col in huesimate_df if col in building_column_list]
    
# add block id to building inventory
dislocation_data_df = pd.merge(right = bdmg_df[builidng_to_block_merge_columns+['X_Longit','Y_Latit','Loss_Fr']],
                    left = huesimate_df,
                    right_on = builidng_to_block_merge_columns,
                    left_on = builidng_to_block_merge_columns,
                    how = 'outer')

## Step 4 - Get Block Group Data
# Use the Census Utility function to get block group data required for dislocation model

from pyincore_data.censusutil import CensusUtil
state_counties = ['48167']
blockgroup_df, bgmap = CensusUtil.get_blockgroupdata_for_dislocation(state_counties,
                                                                 out_csv=True,
                                                                 out_shapefile=False,
                                                                 out_html=False)
blockgroup_df.head()

# Add Block Group ID String
dislocation_data_df.loc[:,'bgidstr'] = dislocation_data_df['BLOCKID10'].apply(lambda x: "BG"+str(x)[0:12].zfill(12))
dislocation_data_df = pd.merge(right = dislocation_data_df,
                    left = blockgroup_df,
                    right_on = 'bgidstr',
                    left_on = 'bgidstr',
                    how = 'outer')

'''