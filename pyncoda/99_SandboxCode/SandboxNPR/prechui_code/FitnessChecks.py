
"""
Compare values

https://data.census.gov/cedsci/table?g=0500000US48167&tid=DECENNIALSF12010.H15

https://data.census.gov/cedsci/table?g=0500000US48167&tid=DECENNIALSF12010.H7

https://data.census.gov/cedsci/table?g=0500000US48167&tid=DECENNIALSF12010.P5

https://api.census.gov/data/2010/dec/sf1?get=GEO_ID,P005011&in=state:48&for=county:167

Check data against 

https://data.census.gov/cedsci/table?g=0500000US48167&tid=ACSDT5Y2012.B19001I

https://data.census.gov/cedsci/table?g=0500000US48167&tid=ACSDT5Y2012.B19101I

https://data.census.gov/cedsci/table?g=0500000US48167&tid=ACSDT5Y2012.B19101H

https://data.census.gov/cedsci/table?g=0500000US48167&tid=ACSDT5Y2012.B19101B

All observations by family match Census totals for the county.
There are additional non-family observations which may be explained by the
suprising anonamoly that the data does not always match up.
"""

table_df = block_df['precihispan']
table = pd.pivot_table(table_df, values='precid', index=['race'],
                           margins = True, margins_name = 'Total',
                           columns=['hispan'], aggfunc='count')
table

table_df = block_df['hispan']
table = pd.pivot_table(table_df, values='numprec', index=['race'],
                           margins = True, margins_name = 'Total',
                           columns=['family'], aggfunc='count')
table

table_df = block_df['hispan']
table = pd.pivot_table(table_df, values='numprec', index=['race'],
                           margins = True, margins_name = 'Total',
                           columns=['family'], aggfunc='count')
table

# https://data.census.gov/cedsci/table?g=0500000US48167&tid=ACSDT5Y2012.B19001I
# https://data.census.gov/cedsci/table?g=0500000US48167&tid=ACSDT5Y2012.B19101I
table = pd.pivot_table(table_df, values = 'Tract2010', 
                         index=['incomegroup'], columns=['family'],
                        margins = True, margins_name = 'Total',
                        aggfunc='count')
table

table_df = block_income_df['primary']
table = table_df[['incomegroup','randincome']].groupby(['incomegroup']).describe()
table

table_df = block_income_df['primary']
table = pd.pivot_table(table_df, values = 'randincome', 
                         index=['numprec'], columns=['family'],
                        margins = True, margins_name = 'Total',
                        aggfunc=np.median)
table

table_df = block_income_df['primary']
table = pd.pivot_table(table_df, values = 'randincome', 
                         index=['race'], columns=['ownershp'],
                        margins = True, margins_name = 'Total',
                        aggfunc=np.median)
table

table_df = block_income_df['primary']
table = pd.pivot_table(table_df, values = 'randincome', 
                         index=['race'], columns=['hispan'],
                        margins = True, margins_name = 'Total',
                        aggfunc=np.median)
table

table_df = hhage_df
condition = (table_df['Block2010str'] == 'B371559601011007')
table_df = table_df.loc[condition]
table = pd.pivot_table(table_df, values='uniqueidH17', index=['race'],
                           margins = True, margins_name = 'Total',
                           columns=['agegroupH17'], aggfunc='count')
table

"""
https://data.census.gov/cedsci/table?g=0500000US37155&tid=ACSDT5Y2012.B19013

https://data.census.gov/cedsci/table?g=0500000US37155&tid=ACSDT5Y2012.B19013A

https://data.census.gov/cedsci/table?g=0500000US37155&tid=ACSDT5Y2012.B19013B

https://data.census.gov/cedsci/table?g=0500000US37155&tid=ACSDT5Y2012.B19013C
"""

def draft_function_check_fitness():
    """
    Code from notebook that works on fitness check problem
    """
    fitness_df = {}
    check_df = {}

    predict_df = block_df["precihispan"]
    new_char = 'hispan'
    values_to_sum = new_char
    geo_var = ['Block2010str']
    values_to_sum_col_rename = 'sumby_'+values_to_sum
    output_df = {}
    keep_vars = []
    errorcount_var_list = []

    for varstems_roots_dictionary in [sexbyage_P12HAI_varstem_roots, hispan_byrace_P5_varstem_roots]:
        group = varstems_roots_dictionary['metadata']['group']   
        char_vars = varstems_roots_dictionary['metadata']['char_vars']
        graft_chars = varstems_roots_dictionary['metadata']['graft_chars']

        # remove by race hispanic - variable - this is replaced by race and hispan
        if 'byracehispan' in char_vars:
            print(char_vars)
            # Remove byrace hispan from variable list
            char_vars_v2 = [var for var in char_vars if var != 'byracehispan']
            # Add missing graft characteristics
            missing_graft_chars = [var for var in graft_chars if var not in char_vars_v2]
            # Add race and hispan
            char_vars_v2 = char_vars_v2 + missing_graft_chars
            # remove newchar by variable if exists
            newcharby_var = [var for var in char_vars_v2 if var.startswith(new_char+"by")]
            char_vars_v2 = [var for var in char_vars_v2 if var not in newcharby_var]
            print(char_vars_v2)
        else:
            char_vars_v2 = graft_chars
        by_vars = geo_var+char_vars_v2
        print(by_vars)

        # Obtain fitness data
        fitness_df[group] = BaseInventory.get_data_based_on_varstems_and_roots(state_county = state_county,
                                                            varstems_roots_dictionary = \
                                                            varstems_roots_dictionary,
                                                            outputfolder = outputfolder)
        # Add block ID string
        fitness_df[group]  = BaseInventory.add_block_geoidstr(fitness_df[group] , 
                        geolevel="Block", year = "2010")
        # Keep columns for merge
        fitness_df[group]  = fitness_df[group][by_vars+['preccount']]
        # Group fitness dataframe by variables - ensures 1 observation per by variable set
        #newchar_var = [col for col in char_vars_v2 if col.startswith(new_char)]
        fitnes_values_to_sum = 'preccount'
        if new_char in by_vars:
                # By vars can not include the value to sum
                by_vars = [var for var in by_vars if var not in [new_char]]
                print("   Fix by vars :",by_vars)
        values_to_sum_col_rename = 'check_'+new_char
        check_df[group] = pd.pivot_table(fitness_df[group], 
                                        values=fitnes_values_to_sum, 
                                        index=by_vars,
                                        aggfunc=np.sum)
        check_df[group].reset_index(inplace = True)
        check_df[group] = check_df[group].rename(columns = \
            {fitnes_values_to_sum : values_to_sum_col_rename})

        # For Fitness based on Hispanic not white - H - A - I
        # Need to limit total sum to races not white
        if 'HAI' in group:
            update_predict_df = predict_df[predict_df['race'] != 1]
        else:
            update_predict_df = predict_df

        # Check total sums for predicted table
        total_sum_df = pd.pivot_table(update_predict_df, 
                                    values=new_char, 
                                    index=by_vars,
                                    aggfunc=np.sum)
        total_sum_df.reset_index(inplace = True)
        total_sum_df = total_sum_df.rename(columns = {new_char : 'predict_'+new_char})

        # add probabilty denomninator to the original data frame 
        output_df[group] = pd.merge(left = check_df[group],
                        right = total_sum_df,
                        left_on = by_vars,
                        right_on = by_vars,
                        copy=True, indicator=True, validate="1:1",
                        how = 'outer')

        # calculate error count
        errorcount_var = new_char+'errorcount_'+group
        errorcount_var_list.append(errorcount_var)
        output_df[group][errorcount_var] = output_df[group]['check_'+new_char] - \
                                                output_df[group]['predict_'+new_char]

        # add errror count to original dataframe
        predict_df = pd.merge(left = predict_df,
                    right = output_df[group][by_vars+[errorcount_var]],
                    left_on = by_vars,
                    right_on = by_vars,
                    how = 'outer')

        # Make list of variables to keep
        keep_vars = keep_vars + [var for var in by_vars if var not in keep_vars]

    keep_vars = ['precid']+keep_vars + [new_char] + ['totalprob_'+new_char] + errorcount_var_list
    predict_df = predict_df[keep_vars]

    outputfile = "preci"+"fitness"
    basevintage = '2010'
    csv_filename = f'BaseInventory_{outputfile}_{new_char}_{state_county}_{basevintage}'
    csv_filepath = outputfolder+"/"+csv_filename+'.csv'
    savefile = sys.path[0]+"/"+csv_filepath
    predict_df.to_csv(savefile, index=False)

    return predict_df

from pyincore_data_addons.SourceData.api_census_gov.CreateAPI_DataStructure \
    import createAPI_datastructure
vintage = '2010'
dataset_name = 'dec/sf1' 
group = 'H17'
hhagetenure_H17 = createAPI_datastructure.obtain_api_metadata(
                    vintage = vintage,
                    dataset_name = dataset_name,
                    group = group,
                    outputfolder = outputfolder,
                    version_text = version_text)

table_df = hui_df
table = pd.pivot_table(table_df, values='huid', index=['numprec'],
                           margins = True, margins_name = 'Total',
                           columns=['ownershp','sex'], aggfunc='count')
table

table_df = hui_df
table = pd.pivot_table(table_df, values='huid', index=['agegroupH18','numprec'],
                           margins = True, margins_name = 'Total',
                           columns=['ownershp','family','sex'], aggfunc='count')
table

table_df = h17_h18_df['primary']
table = pd.pivot_table(table_df, values='uniqueidH17', index=['agegroupH18'],
                           margins = True, margins_name = 'Total',
                           columns=['ownershp','family','sex','numprec'], aggfunc='count')
table