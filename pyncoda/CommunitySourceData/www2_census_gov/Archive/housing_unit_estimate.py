
import matplotlib.pyplot as plt
from scipy.stats import pearsonr

residential_archetypes = { 1 : 'One-story residential building on a crawlspace foundation',
                           2 : 'One-story residential building on a slab-on-grade foundation',
                           3 : 'Two-story residential building on a crawlspace foundation',
                           4 : 'Two-story residential building on a slab-on-grade foundation'}
                           
def predict_residential_addresspoints(bldg_df,
                                     hui_df,
                                     builidng_to_block_df,
                                     archetype_var: str = 'F_Arch', 
                                     residential_archetypes = residential_archetypes,
                                     building_area_var: str = 'Bldg_Area',
                                     building_area_cutoff: int = 30):
    """
    Function that attempts to predict housing units in a structure.
    Function runs three rounds of checks and updates the housing unit counts in each round.
    To Do - This function repeats signficant blocks of code that could 
    be split into sub functions.
    """
    
    # start by assuming that buidlings have 0 residential address points
    bldg_df.loc[:,'residentialAP1'] = 0
    bldg_df['residentialAP1'].label = "Residential Address Point Round 1"
    bldg_df['residentialAP1'].note = "Residential Address Point - first attempt to assign residential address points to buildings"
    
    bldg_df.loc[:,'residential'] = 0
    bldg_df['residentialAP1'].label = "Residential Address Point Round 1"
    bldg_df['residentialAP1'].note = "Residential Address Point - first attempt to assign residential address points to buildings"
    

    # Update residential address points by assigning residential archetypes with 1 housing unit
    for residential_archetype in residential_archetypes:
        bldg_df.loc[bldg_df[archetype_var] == residential_archetype,'residentialAP1'] = 1

    # remove small buildings from residential bulding list
    bldg_df.loc[bldg_df[building_area_var] < building_area_cutoff,'residentialAP1'] = 0

    # Look at shape area divided by the number of address points
    bldg_df.loc[:,'gsq_foot_by_AP'] = bldg_df[building_area_var] / bldg_df['residentialAP1']

    # remove small buildings from residential bulding list
    bldg_df.loc[bldg_df['gsq_foot_by_AP'] < building_area_cutoff,'residentialAP1'] = 0

    # Set residential binary using residentialAP1
    bldg_df.loc[:,'residential'] = bldg_df['residentialAP1']
    
    # Look at address point count by block
    hua_block_counts = hui_df[['Block2010','huid']].groupby('Block2010').agg('count')
    hua_block_counts.reset_index(inplace = True)
    hua_block_counts = hua_block_counts.rename(columns={'huid': "apcount", 'Block2010' : 'BLOCKID10' })

    # What columns are shared by the building and building to block dataframes
    building_column_list = [col for col in bdmg_df]
    builidng_to_block_merge_columns = [col for col in builidng_to_block_df if col in building_column_list]
    
    # add block id to building inventory
    bldg_df = pd.merge(right = bldg_df,
                       left = builidng_to_block_df,
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

    # If Address Point Count (Housing10 + Group Quarters) from Census is 0 then Residential Address Point is also 0
    condition = (bldg_df['apcount'] == 0)
    count_of_buildings_in_block_with_nohousingunits = bldg_df.loc[condition].shape[0]
    print(count_of_buildings_in_block_with_nohousingunits,"buildings are in blocks with no housing units.")
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
    census_blocks_df_rap1 = block_error_check_addresspoints(census_blocks_df_rap1,'apcount','residentialAP1','bldgcount','ErrorCheck1')

    # Identify difference between expected housing unit count and sum of estimated address points
    census_blocks_df_rap1.loc[:,'DiffCount1'] = census_blocks_df_rap1['apcount'] - census_blocks_df_rap1['residentialAP1']
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
    bldg_df_round2.loc[(bldg_df_round2['residentialAP1']>=1), 'Res_Area'] = bldg_df_round2[building_area_var]
    bldg_df_round2_area = bldg_df_round2[['BLOCKID10','Res_Area']]
    bldg_df_round2_area_sum = bldg_df_round2_area.groupby(['BLOCKID10']).sum()
    bldg_df_round2_area_sum = bldg_df_round2_area_sum.rename(columns={"Res_Area": "Sum_Res_Area"})
    bldg_df_round2 = pd.merge(bldg_df_round2, bldg_df_round2_area_sum, 
                                  left_on='BLOCKID10', right_on='BLOCKID10', how='outer')

    # Calculate Number of Address Points Each Building Should Have based on Housing Unit Count Difference
    bldg_df_round2.loc[:,'residentialAP2'] = (bldg_df_round2['Res_Area'] / bldg_df_round2['Sum_Res_Area']) * bldg_df_round2['DiffCount1'] 

    ### Estimate new Housing Unit Count in Building
    # Using the distributed difference in address points, round the value and add to the intial estimate.
    # For blocks where the housing unit and address points are equal (Error 2) or 
    # for blocks where the number of address points is greater than the number of Housing Units (Error 3) 
    # use the estimate from Round 1.
    bldg_df_round2.loc[:,'residentialAP2v2'] = round(bldg_df_round2['residentialAP2'],0) + bldg_df_round2['residentialAP1']
    # If Round 1 had a match between Address Points and Housing Units use Round 1
    bldg_df_round2.loc[(bldg_df_round2['ErrorCheck1'] == "2. HU=AP"),'residentialAP2v2'] = bldg_df_round2['residentialAP1']
    # If Round 1 had a more Address Points than Housing Units Use Round 1
    bldg_df_round2.loc[(bldg_df_round2['ErrorCheck1'] == "3. HU<AP"),'residentialAP2v2'] = bldg_df_round2['residentialAP1']

    # If Estiamted number of Census Housing Units is 0 make AP 0
    bldg_df_round2.loc[(bldg_df_round2['ErrorCheck1'] == "1. HU=0"),'residentialAP2v2'] = 0

    # sum Buildings over the column BLOCKID.
    # https://www.geeksforgeeks.org/python-pandas-dataframe-sum/
    block_rap2_df = bldg_df_round2[['BLOCKID10','residentialAP2v2']]
    block_rap2_df['bldgcountv2'] = 1
    block_rap2_df_sum = block_rap2_df.groupby(['BLOCKID10']).sum()
    # Merge Sum of Residential Address Points with Census Counts
    census_blocks_df_rap2 = pd.merge(census_blocks_df_rap1, block_rap2_df_sum,
                                  left_on='BLOCKID10', right_on='BLOCKID10', how='left')

    # Compare Bldg Count to Housing 10 Correlation
    # calculate Pearson's correlation
    data1 = census_blocks_df_rap2['residentialAP2v2']
    data2 = census_blocks_df_rap2['apcount']
    corr, _ = pearsonr(data1, data2)
    print('For Round 2 Estimated Residential Address Points correlation: %.3f' % corr)

    # Error check round 2
    census_blocks_df_rap2 = block_error_check_addresspoints(census_blocks_df_rap2,
                        'apcount','residentialAP2v2','bldgcountv2','ErrorCheck2')
    # Identify difference between expected housing unit count and sum of estimated address points
    census_blocks_df_rap2.loc[:,'DiffCount2'] = census_blocks_df_rap2['apcount'] - census_blocks_df_rap2['residentialAP2v2']
    census_blocks_df_rap2 = census_blocks_df_rap2.rename(columns={
                                            "residentialAP2v2": "residentialAP2v2_sum", 
                                            "bldgcountv2": "bldgcountv2_sum"})

    # Merge Block level data with building level data
    keepcolumns = ['BLOCKID10','residentialAP2v2_sum','bldgcountv2_sum','DiffCount2','ErrorCheck2']

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
    census_blocks_df_rap3 = block_error_check_addresspoints(census_blocks_df_rap3,
                        'apcount','huestimate','bldgcountv3','ErrorCheck3')
    # Identify difference between expected housing unit count and sum of estimated address points
    census_blocks_df_rap3.loc[:,'DiffCount3'] = census_blocks_df_rap3['apcount'] - census_blocks_df_rap3['huestimate']
    census_blocks_df_rap3 = census_blocks_df_rap3.rename(columns={
                                            "residentialAP2v2": "residentialAP2v2_sum", 
                                            "bldgcountv2": "bldgcountv2_sum"})

    # Merge Block level data with building level data
    keepcolumns = ['BLOCKID10','DiffCount3','ErrorCheck3']

    bldg_df_round3 = pd.merge(bldg_df_round3, census_blocks_df_rap3[keepcolumns], 
                                    left_on='BLOCKID10', right_on='BLOCKID10', how='left')                                

    return bldg_df_round3[builidng_to_block_merge_columns+['BLOCKID10','placeNAME10',archetype_var,'residential','huestimate','DiffCount3','ErrorCheck3']]

def block_error_check_addresspoints(census_blocks_df, expected_count, estimated_count,building_count, ErrorCheck):
    # Create Error Varaible
    census_blocks_df.loc[:,ErrorCheck] = "0. Not Checked"

    basecondition = (census_blocks_df[ErrorCheck] == "0. Not Checked")
    census_blocks_df.loc[(census_blocks_df[expected_count].isna())
                         & basecondition, ErrorCheck] = "1. HU=0"
    census_blocks_df.loc[(census_blocks_df[expected_count] == census_blocks_df[estimated_count])
                         & basecondition, ErrorCheck] = "2. HU=AP"
    census_blocks_df.loc[(census_blocks_df[expected_count] < census_blocks_df[estimated_count])
                         & basecondition, ErrorCheck] = "3. HU<AP"
    census_blocks_df.loc[(census_blocks_df[expected_count] > census_blocks_df[estimated_count])
                         & basecondition, ErrorCheck] = "4. HU>AP"
    census_blocks_df.loc[(census_blocks_df[expected_count] > 0) & (census_blocks_df[estimated_count] == 0)
                         & basecondition, ErrorCheck] = "5. HU > 0, AP = 0"
    census_blocks_df.loc[(census_blocks_df[expected_count] > 0) & (census_blocks_df[estimated_count].isna()) & 
                         (census_blocks_df[building_count] > 0)
                         , ErrorCheck] = "6. HU > 0, AP = Missing, But buildings present"
    census_blocks_df.loc[(census_blocks_df[expected_count] > 0) & (census_blocks_df[estimated_count].isna())
                         , ErrorCheck] = "7. HU > 0, AP = Missing"
    census_blocks_df.loc[(census_blocks_df[expected_count] > 0) & (census_blocks_df[building_count].isna())
                         , ErrorCheck] = "8. HU > 0, Building Count = Missing"
    census_blocks_df.loc[(census_blocks_df[expected_count].isna()) & (census_blocks_df[building_count].isna())
                         , ErrorCheck] = "9. HU = 0, Building Count = Missing"

    return census_blocks_df

def add_d_sf(df):
    """
    d_sf = dummy single family
    variable required for Lin et al 2009 dislocation model
    
    """
    df.loc[(df["huestimate"] > 1),'d_sf'] = 0
    df.loc[(df["huestimate"] == 1), 'd_sf'] = 1 

    return df


def add_addptid(df,blockid_var,bldgid_var): 
    
    # find length of id vars
    if df.dtypes[bldgid_var] != object and df.dtypes[bldgid_var] != str:
        bldgid_var_len = len(df[bldgid_var].max().astype(str))
    else:
        bldgid_var_len = len(df[bldgid_var])
    if df.dtypes[blockid_var] != object and df.dtypes[blockid_var] != str:
        blockid_var_len = len(df[blockid_var].max().astype(str))
    else:
        blockid_var_len = len(df[blockid_var].max())
    max_len = max([bldgid_var_len,blockid_var_len]) 

    # Add counter by block id - use cummulative count method
    df['blockidcounter'] = df.groupby(blockid_var).cumcount()
    df.loc[(df[bldgid_var].isna()),
                'strctid'] = df.apply(lambda x: "CB"+ str(x[blockid_var]).zfill(max_len), axis=1)
    df.loc[(df[bldgid_var].notna()),
                'strctid'] = df.apply(lambda x: "ST"+ str(x[bldgid_var]).zfill(max_len), axis=1)
        
    # Sort Address Points by The first part of the address point 
    df.sort_values(by=['strctid'])
    # Add Counter by Building
    df['apcounter'] = df.groupby('strctid').cumcount()
    apcounter_len = len(df['apcounter'].max().astype(str))

    df['addrptid'] = df.apply(lambda x: x['strctid'] + "AP" +
                                str(int(x['apcounter'])).zfill(apcounter_len), axis=1)
    # Move Primary Key Column to first Column
    cols = ['addrptid']  + [col for col in df if col != 'addrptid']
    df = df[cols]

    return df