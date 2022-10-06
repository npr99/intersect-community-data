"""
Set of functions to setup and run a Markov Chain Monte Carlo Simulated Annealing Process
The number of possible job combinations between a single work block and its residential blocks
can be very large.
The MCMC SA process uses a random path to test different job cominations 
Each set of combinations is compared to the expected job count provided
in the LODES data.
"""

from posixpath import defpath
import pandas as pd
import numpy as np
# Use LODES Data Structure set of dictionaires to setup loops
from . import all_segstems
from . import all_charstems
from . import all_stems

# open, read, and execute python program with reusable commands
import pyincoredata_addons.SourceData.lehd_ces_census_gov.lodes_datautil as data_util

def add_random_number(df, seed_i, by_vars = ['w_geocode','h_geocode','jobidod','jobidod_counter']):
    df = df.sort_index()
    # Set random seed for reproducibility
    np.random.seed(seed_i)
    # How many random numbers do you need?
    random_count = df.shape[0]
    # add random number list to dataframe for sorting
    df.loc[:,'randomsort'] =  np.random.uniform(1, random_count*100, random_count)
    
    return df

def rand_select_jobs(df, seed, by_vars = \
    ['w_geocode','h_geocode','jobidod','jobidod_counter']):
    
    # Make sure dataframe has a random sort
    df = add_random_number(df,seed)
    df = df.sort_values(by=by_vars+['randomsort'])
    #df.reset_index(inplace = True, drop = True)
    
    # Add total number of possible matches - within the jobid and jobid counter set
    df.loc[:,'jobidod_total_rand'] = df.groupby(by_vars).cumcount()

    col_list = [col for col in df if col not in by_vars + ['jobidod_total_rand']]
    reorder_cols = by_vars +  ['jobidod_total_rand'] + col_list
    df = df[reorder_cols]
    
    # Select jobs based on jobidod_counter
    # this step will select the first job within the selection group    
    df['select_job'] = 0
    df.loc[df['jobidod_total_rand'] == 0, 'select_job'] = 1

    # update selected job if probability of selection is 1
    # this applies to updating the probability of selection based on initial MCMC
    try:
        df.loc[df['prob_selected'] == 1, 'select_job'] = 1
    except:
        column_list = list(df.columns)
        print("Check columns ",column_list)

    # Check to make sure selection is not overselecting jobs
    # add a check based on S000
    # update by variables for expected totals
    by_vars = ['w_geocode','h_geocode','jobtype']
    check_S000 = \
        df[by_vars+['select_job']].groupby(by_vars, as_index=False).sum()
    check_S000 = check_S000.rename(columns = \
            {'select_job' : 'check_S000'})
    # add Check S000 to joblist
    # remove check_S000 from current list
    if 'check_S000' in list(df.columns):
        print('Removing check_S000 from column list')
        df = df.drop(columns = ['check_S000'])
    df = pd.merge(left = df,
                right = check_S000,
                left_on = by_vars,
                right_on = by_vars)
    # sort values
    df = df.sort_values(by=by_vars+['randomsort'])

    # Add total number of possible matches - within the jobid and jobid counter set
    df['checkS000_counter'] = df.groupby(by_vars+['select_job']).cumcount()+1

    # Reset job select to 0 if counter greater than S000
    condition1 = (df['S000'] != df['check_S000'])
    condition2 = (df['select_job'] == 1)
    condition3 = (df['checkS000_counter'] > df['S000'])
    conditions = condition1 & condition2 & condition3 
    df.loc[conditions, 'select_job'] = 0

    # Set select job to 1 if counter less than expected
    # This might happen if two jobs are expected to be selected
    # But the jobidod_counter is not set correctly 
    condition1 = (df['S000'] != df['check_S000'])
    condition2 = (df['select_job'] == 0)
    condition3 = (df['checkS000_counter'] <= df['S000'])
    conditions = condition1 & condition2 & condition3 
    df.loc[conditions, 'select_job'] = 1    


    return df


def get_single_characteristic_fitness(df1, 
                     df2, 
                     char,
                     charstems = all_charstems,
                     by_char = 'Earnings',
                     by_stem = all_segstems['Earnings'],
                     by_vars = ['w_geocode','jobtype'], 
                     ):
    """
    df1 = dataframe with predicted individual values to sum
    df2 = dataframe with known totals to match
    """
    # Get the characteristic stem from dictionary of possible stems
    char_stem = charstems[char]
    #print(char_stem)
    if by_char != '':
        by_vars = by_vars + [by_char]
    
     # Add missing education category before subtracting dataframe
    if char_stem == 'CD':
        df2 = data_util.add_missingeducation(df2)
    # make list of columns that have the characteristic stem
    fitness_cols = [col for col in df2 if col.startswith(char_stem)]
    
    # Based on dataframe 1 - wac-rac-od job list create count of jobs by characteritic
    char_list = pd.pivot_table(df1.loc[df1['select_job'] == 1], 
                                values='jobidac', index=by_vars,
                                columns=[char], aggfunc='count')
    # Clean up new characteristic count dataframe
    char_list.reset_index(inplace = True)
    # Create a dictionary with values to rename columns values
    # the character column values match the rename values
    rename_dict = {}
    for rename_cols in fitness_cols:
        startchar = len(char_stem)
        endchar = startchar+2
        char_int = int(str(rename_cols[startchar:endchar]))
        rename_dict[char_int] = rename_cols
    char_list = char_list.rename(columns = rename_dict)
    char_list = char_list.fillna(value = 0)
    
    # make sure fitness columns are integer values
    for var in fitness_cols:
        df2.loc[:,var] = df2[var].astype(int)
    # make a comparable dataframe using the ac job list
    ac_joblist = df2[by_vars+fitness_cols].sort_values(by=by_vars)
    ac_joblist = ac_joblist.groupby(by_vars).sum()
    ac_joblist.reset_index(inplace = True)
            
    # compare two summary job lists by subtrancting the dataframes
    compare_df = data_util.subtract_df(df1 = char_list, 
                                       df2 = ac_joblist, 
                                       index_col =by_vars )

    # Fill in missing values - possible that df1 does not have 
    #  columns for some charactersitics 
    compare_df = compare_df.fillna(value=0)

    # caluculate a fitness total by the characteristic
    fitness_var = 'fitness_'+char_stem+by_stem
    compare_df.loc[:,fitness_var] = 0
    # issue with some data not being saved as an integer - force the data type to be integer
    compare_df.loc[:,fitness_var] = compare_df[fitness_var].astype(int)

    for var in fitness_cols:
        #length_missing_var = len(compare_df.loc[compare_df[var].isnull()])
        #print("Error check - var is missing:",length_missing_var)
        compare_df.loc[:,var] = compare_df[var].astype(int)
        compare_df.loc[:,fitness_var] = compare_df[fitness_var] + abs(compare_df[var])

    #compare_char_fitness_df = pd.pivot_table(compare_df, values=fitness_var, index=['w_geocode'],
    #                        aggfunc='sum')
    #compare_char_fitness_df.reset_index(inplace= True)

    compare_char_fitness = np.sum(compare_df[fitness_var])

    return compare_char_fitness

def calculate_combined_fitness(df1,
                               df2,
                         iteration, 
                         seed,
                         accept = True, 
                         charstems = all_charstems,
                         segstems = all_segstems,
                         by_chars = ['Earnings','Age','SuperSector']):
    """
    Combined fitness looks at totals by segment and characterstic.
    """
    # Update charstems based on columns in data frame - alows for fewer charactersitics
    possible_charstems = charstems
    charstems_available = [col for col in df1 if col in possible_charstems]

    # Update charstems dictionary
    charstems_available_dict = {}
    for charstem in charstems_available:
        charstems_available_dict[charstem] = possible_charstems[charstem]
    fitness = {}
    #char_count = 1
    for by_char in by_chars:
        for characteristic in charstems_available:
            if characteristic != by_char:
                #print(characteristic,by_char)
                if by_char in df2.keys():
                    compare_df = df2[by_char]
                    by_char = by_char
                    by_stem = segstems[by_char]
                else:
                    first_key = list(df2.keys())[0]
                    compare_df = df2[first_key]
                    by_char = ''
                    by_stem = ''
                fitness[characteristic+by_char] = \
                     get_single_characteristic_fitness(df1, compare_df, 
                                char = characteristic, 
                                charstems = charstems_available_dict,
                                by_char = by_char,
                                by_stem = by_stem)

            #char_count = char_count + 1
            #print(char_count)
    
    #compare_fitness_df = fitness[1]
    #for i in range(2,char_count):
    #    #print(i)
    #    compare_fitness_df = pd.merge(left = compare_fitness_df,
    #                                 right = fitness[i],
    #                                 left_on = 'w_geocode',
    #                                 right_on = 'w_geocode')

    compare_fitness_df = pd.DataFrame(fitness, index=[iteration])

    fitness_cols = [col for col in compare_fitness_df]
    # Add total fitness value
    compare_fitness_df['fitness'] = 0
    for cols in fitness_cols:
        compare_fitness_df['fitness'] = \
            compare_fitness_df['fitness'] +  compare_fitness_df[cols] 
    compare_fitness_df['iteration'] = iteration
    compare_fitness_df['seed'] = seed
    compare_fitness_df['accept'] = accept
    
    return compare_fitness_df

def calculate_total_fitness(df1,
                               df2,
                         iteration, 
                         seed,
                         accept = True, 
                         charstems = all_stems,
                         segstems = all_segstems,
                         by_chars = ['jobtype']):
    
    """
    Total fitness is the primary fitness to check - this looks at totals of jobs counts 
    in the block by jobtype.
    This fitness needs to be met first before the combined fitness is tested.
    Combined fitness looks at totals by segment and characterstic.
    """
    fitness = {}
    # Update charstems based on columns in data frame - alows for fewer charactersitics
    possible_charstems = charstems
    charstems_available = [col for col in df1 if col in possible_charstems]

    # Update charstems dictionary
    charstems_available_dict = {}
    for charstem in charstems_available:
        charstems_available_dict[charstem] = possible_charstems[charstem]
    #print("   Check aviable characteristics",charstems_available_dict)
    #char_count = 1
    for by_char in by_chars:
        for characteristic in charstems_available:
            if characteristic != by_char:
                #print(characteristic,by_char)
                if by_char in df2.keys():
                    compare_df = df2[by_char]
                    by_char_check = by_char
                    by_stem = segstems[by_char]
                else:
                    first_key = list(df2.keys())[0]
                    compare_df = df2[first_key]
                    by_char_check = ''
                    by_stem = ''
                #print(first_key,characteristic,by_char_check,by_stem)
                fitness[characteristic+by_char] = \
                    get_single_characteristic_fitness(df1, compare_df, 
                                char = characteristic, 
                                charstems = charstems_available_dict,
                                by_char = by_char_check,
                                by_stem = by_stem)
    compare_total_fitness_df = pd.DataFrame(fitness, index=[iteration])

    fitness_cols = [col for col in compare_total_fitness_df]
    # Add total fitness value
    compare_total_fitness_df.loc[:,'fitness'] = 0
    for cols in fitness_cols:
        compare_total_fitness_df.loc[:,'fitness'] = \
            compare_total_fitness_df['fitness'] +  compare_total_fitness_df[cols] 
    compare_total_fitness_df.loc[:,'iteration'] = iteration
    compare_total_fitness_df.loc[:,'seed'] = seed
    compare_total_fitness_df.loc[:,'accept'] = accept
    
    return compare_total_fitness_df

def markov_chain_monte_carlo_simanneal(df, 
                                       wac_joblist_df,
                                       random_accept_threshold,
                                       reduction_threshold,
                                       seedi,
                                       seedj,
                                       seedk):

    i = 2
    j = 1 # outer loop
    k = 2

    # Store results of Markov Chain Monte Carlo Process
    total_mcmcsa = {}
    combined_mcmcsa = {}
    total_mcmcsa[1,1] = calculate_total_fitness(df, 
                            wac_joblist_df, 
                            iteration =1, seed = seedi)
    combined_mcmcsa[1] = calculate_combined_fitness(df, 
                        wac_joblist_df, 
                        iteration =1, seed = seedi)
    old_fitness = total_mcmcsa[1,1]['fitness'].squeeze()
    first_fitness = old_fitness
    combined_fitness = combined_mcmcsa[1]['fitness'].squeeze()
    print('The starting total fitness =',first_fitness, \
        'and combined fitness =',combined_fitness)

    # Set threshold
    precent_fitness_threshold = int(first_fitness - \
        (first_fitness*reduction_threshold))

    percent_combined_fitness = int(combined_fitness - \
        (combined_fitness*reduction_threshold))  
    # Check if threshold set below 0  
    if precent_fitness_threshold < 0:
        precent_fitness_threshold = 0
    if percent_combined_fitness < 0:
        percent_combined_fitness = 0
    print("Total fitness set to:",precent_fitness_threshold)
    print("Combined fitness set to:",percent_combined_fitness)

    # Create a list of od block pairs that have multiple job types to select from
    od_block_selectlist = df.loc[df['prob_selected'] != 1] 
    od_block_list =  pd.pivot_table(od_block_selectlist, values='prob_selected', \
        index=['w_geocode','h_geocode','jobidod','jobidod_counter'],
                                aggfunc='count')
    od_block_list.reset_index(inplace= True)


    # Store previous combined fitness to check if loop gets stuck
    previous_combined_fitness = combined_fitness
    stuck_outer_loop = 0
    while (combined_fitness > percent_combined_fitness):
        print(j,',',i,',',k,' Combined Fitness = ',combined_fitness)
        # after the first loop update fitness
        if k > 2:
            seed_k = seedk + k
            df = rand_select_jobs(df, seed_k)
            # Check if number of jobs selected for each od pair 
            # matches the expected number of jobs

            # New function here

            total_mcmcsa[k,1] = calculate_total_fitness(\
                df, wac_joblist_df, iteration =k, seed = seed_k)
            old_fitness = total_mcmcsa[k,1]['fitness'].squeeze()
            print('Loop',k,'Restart Total fitness',old_fitness)

            i = 2
            j = 1
        
        # Store previous old fitness to check if loop gets stuck
        previous_old_fitness = old_fitness
        stuck_loop = 0
        while (old_fitness > precent_fitness_threshold):
            print(j,',',i,',',k,' Total Fitness = ',old_fitness)

            # randomly sort the od_block_list
            seed_j = seedj + j
            od_block_list = add_random_number(od_block_list,seed_j, by_vars = [])
            od_block_list = od_block_list.sort_values(['randomsort'])
            od_block_list.reset_index(inplace= True, drop = True)
            # Increase increment for inside loop
            # Inside loop is loop that checks total fitness
            j = j + 1


            # loop through each od pair and select a new job
            # if the new job selected increases the fitness 
            # Then keep the new selection - if not move on to the next pair
            for index, odpair in od_block_list.iterrows():
                # Increase increment for loop over block pairs
                i = i+1
                # flip one job in od pair
                odpair_joblist = df.loc[(df['jobidod']    == odpair['jobidod']) &
                        (df['w_geocode']  == odpair['w_geocode']) &
                        (df['h_geocode']  == odpair['h_geocode']) &
                        (df['jobidod_counter'] == odpair['jobidod_counter'])].copy()

                # increment seed for random sort
                seed_i = seedi + i

                # Use numpy arrays to flip job
                array_IDs = np.array(odpair_joblist.index)
                # store array IDs to shuffle
                shuffle_array = array_IDs
                # shuffle the unique array
                np.random.seed(seed_i)
                np.random.shuffle(shuffle_array)

                # Select new job
                flip_job_index = shuffle_array[0:1]
                flip_job_index = np.asscalar(flip_job_index)

                # select new job based on new index
                odpair_joblist['select_job'] = 0
                odpair_joblist.loc[odpair_joblist.index == \
                    flip_job_index, 'select_job'] = 1
                
                # update wac_rac_od_joblist_df - with new job selection
                new_df = df.copy(deep=True)
                new_df.update(odpair_joblist['select_job'])

                # Compare old fitness to new fitness
                old_fitness_df = calculate_total_fitness(\
                    df, wac_joblist_df, iteration = i, seed = seed_i)
                old_fitness = old_fitness_df['fitness'].squeeze()

                # check new fitness
                update_fitness_df = calculate_total_fitness(\
                    new_df, wac_joblist_df, iteration = i, seed = seed_i)
                new_fitness = update_fitness_df['fitness'].squeeze()

                # Update job list if fitness is "better"
                if new_fitness < old_fitness:
                    #print("accept new fitness")
                    df = new_df.copy(deep=True)
                    total_mcmcsa[i] = update_fitness_df
                    old_fitness = new_fitness
                else:
                    # pick a random number and compare to threshold
                    np.random.seed(seed_i)
                    random_select = np.random.random_sample()
                    # if random number is greater than threshold update the data frame
                    # introduction of randomness is part of the monte carlo method
                    # MCMC Simulated Annealling requires willingness to accept error
                    if random_select < random_accept_threshold:
                        #print("randomly accept new fitness ",random_select)
                        df = new_df.copy(deep=True) 
                        total_mcmcsa[k,i] = update_fitness_df
                        old_fitness = new_fitness
                    else:
                        total_mcmcsa[k,i] = update_fitness_df
                        total_mcmcsa[k,i]['accept'] = False
                        old_fitness = old_fitness

                if new_fitness == precent_fitness_threshold:
                    old_fitness = new_fitness
                    #print('Total fitness threshold met ',new_fitness)
                    break
                    
            # Check if Total fitness is stuck 
            if old_fitness < previous_old_fitness:
                previous_old_fitness = old_fitness
            else:
                stuck_loop += 1
            if stuck_loop > 2:
                print("   Total fitness Loop seems stuck at fitness",old_fitness)
                precent_fitness_threshold = \
                    precent_fitness_threshold + (1 / reduction_threshold)
                print("   Increased total fitness threshold to",precent_fitness_threshold)
                break   

        k = k+1

        combined_mcmcsa[k] = calculate_combined_fitness(df, 
                    wac_joblist_df, iteration =1, seed = seedi)
        combined_fitness = combined_mcmcsa[k]['fitness'].squeeze()
        #print('Combined fitness',combined_fitness)

        # Check if Combined fitness is stuck 
        if combined_fitness < previous_combined_fitness:
            previous_combined_fitness = combined_fitness
        elif combined_fitness >= previous_combined_fitness:
            stuck_outer_loop += 1
        if stuck_outer_loop > 3:
            print("  Combined Fitness Loop seems stuck at fitness",combined_fitness)
            percent_combined_fitness = \
                percent_combined_fitness + (4 / reduction_threshold)
            print("  Increased combined fitness threshold to",percent_combined_fitness)
            break   
            

    # Stack mcmcsa observations
    total_mcmcsa_df = pd.concat(total_mcmcsa.values(), ignore_index=True)
    combined_mcmcsa_df = pd.concat(combined_mcmcsa.values(), ignore_index=True)

    return df, total_mcmcsa_df

def check_flip(df,odpair,odpair_joblist,j,i):
    """
    helpful code to check flip for odpair

    j = counter for fitness loop
    i = counter for odpair loop
    """
    # # Check flip
    initial_jobselect_list = df.loc[(df['jobidod']    == odpair['jobidod']) &
            (df['w_geocode']  == odpair['w_geocode']) &
            (df['h_geocode']  == odpair['h_geocode']) &
            (df['select_job'] == 1) &
            (df['jobidod_counter'] == odpair['jobidod_counter'])].copy()

    flip_jobselect_list = odpair_joblist.loc[\
        (odpair_joblist['select_job'] == 1)].copy()

    initial_jobidac = initial_jobselect_list['jobidac'].squeeze()
    flipped_jobidac = flip_jobselect_list['jobidac'].squeeze()

    print(j,',',i,' : Initial job selected ',initial_jobidac,
        ' was flipped to ',flipped_jobidac)


def prepare_joblist_columns(joblist_df):
    """
    Drop and add columns for output
    Steps to prepare joblist and clean up columns
    Add geometry
    """

    output_df = joblist_df.copy()
    drop_if_starts_with = ['prob_','randomsort','Unnamed','check','probability','possible']
    for substring in drop_if_starts_with:
        drop_vars = [col for col in output_df if col.startswith(substring)]
        #print(drop_vars)
        # Drop columns
        output_df = output_df.drop(drop_vars, axis=1)
    drop_if_ends_with = ['_counter','_total_rand','pair']
    for substring in drop_if_ends_with:
        drop_vars = [col for col in output_df if col.endswith(substring)]
        #print(drop_vars)
        # Drop columns
        output_df = output_df.drop(drop_vars, axis=1)
    drop_if_equals = ['SA','SE','SI','S000','select_job']
    for substring in drop_if_equals:
        drop_vars = [col for col in output_df if col == substring]
        #print(drop_vars)
        # Drop columns
        output_df = output_df.drop(drop_vars, axis=1)

    for geocodevar in ['w_geocode','h_geocode']:
        output_df.loc[:,geocodevar+'_str'] = \
            output_df[geocodevar].apply(lambda x : str(int(x)).zfill(15))

    # rename race - lowercase R - to match Person Record Inventory
    output_df = output_df.rename(columns={"Race": "race"})
    # rename Ethnicity - hispan - to match Person Record Inventory
    #output_df = output_df.rename(columns={"Ethnicity": "hispan"})
    # Add hispan to match v0.2.0 person record inventory
    output_df.loc[output_df['Ethnicity']==1,'hispan'] = 0
    output_df.loc[output_df['Ethnicity']==2,'hispan'] = 1
    # rename other variables to prepare for merge with person record inventory
    output_df = output_df.rename(columns={"Age": "agegroupLODES"})
    output_df = output_df.rename(columns={"Sex": "sex"})
    output_df = output_df.rename(columns={"year": "yearLODES"})

    # Add geometry details
    output_df = data_util.add_latlon(output_df)
    output_df = data_util.add_distance(output_df, \
        'blklatdd_w', 'blklatdd_h', 'blklondd_w', 'blklondd_h' )
    output_df = data_util.add_coarse_geovar(output_df)
    # Convert CSV LAT LON to WKT and Geodataframe
    output_df['w_geometry'] = output_df.apply(lambda x: 'POINT ( ' +
                                    str(x['blklondd_w']) + ' ' +
                                    str(x['blklatdd_w']) + ')', axis = 1)

    output_df['h_geometry'] = output_df.apply(lambda x: 'POINT ( ' +
                                    str(x['blklondd_h']) + ' ' +
                                    str(x['blklatdd_h']) + ')', axis = 1)

    return output_df