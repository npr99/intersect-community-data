import sys
import os
import us
import pandas as pd
import numpy as np

# Tools for concurrent execution of code
import concurrent.futures
from itertools import repeat

# open, read, and execute python program with reusable commands
import pyincoredata_addons.SourceData.lehd_ces_census_gov.lodes_datautil as data_util
import pyincoredata_addons.SourceData.lehd_ces_census_gov.lodes_fullloop as lodes_loop

# Use Markov Chain Monte Carlo Uitilites 
from . import rand_select_jobs
from . import markov_chain_monte_carlo_simanneal
from . import calculate_total_fitness
from . import calculate_combined_fitness

# Use LODES Data Structure set of dictionaires to setup loops
from  . import all_stems

def mcmc_sa_loop(work_block: int, 
                 year,
                 county_jobcounts_df,
                 possible_block_joblist_df,
                 seedk,
                 seedi,
                 seedj,
                 random_accept_threshold,
                 reduction_threshold,
                 num_procs
                ):
    """
    MCMC SA = Markov Chain Monte Carlo Simulated Anneal
    
    The fitness of the wac-od-rac joblist can be check by attempting to recreate the WAC file summary of jobs.

    This loop splits the MCMC into 2 parts. 
    First the job list is split by earnings and age segements. 
    These smaller joblists are tested for their total fitness and then combined fitness. 
    The combined fitness for the split files appears to allways be 0 if the total fitness is 0.

    Once the split files are completed they are recombined and then compared.

    All jobs that appear in both lists are assumed to have 100% probability of selection. 
    This new joblist with updated probabilities will then be used 
    for a secondary MCMC that looks at combined fitness.

    num_procs is a parameter that must be set by users
    """

    block_str = str(int(work_block)).zfill(15)
    stfips = block_str[0:2]
    stabbr = str.lower(us.states.lookup(stfips).abbr)
    countyfips = block_str[0:5]

    # Start MCMC SA with a random selection of jobs
    possible_block_joblist_df = data_util.add_probability_job_selected(
        df = possible_block_joblist_df,
        prob_value = 'jobidac', 
        by_vars = ['w_geocode','h_geocode','jobidod','jobidod_counter',\
            'jobtype','jobidod_pair'])
    rand_select_alljobs_df = rand_select_jobs(possible_block_joblist_df, seedk)

    # Compare random selection of jobs with the WAC jobs list by Earnings, Age, and SuperSector
    wac_joblist_df = {}
    wac_joblist_df['Earnings'] = data_util.explorebyblock(county_jobcounts_df[stabbr,countyfips,\
        'wac',year,'SE'],'w_geocode',[work_block])
    wac_joblist_df['Age'] = data_util.explorebyblock(county_jobcounts_df[stabbr,countyfips,\
        'wac',year,'SA'],'w_geocode',[work_block])
    wac_joblist_df['SuperSector'] = data_util.explorebyblock(county_jobcounts_df[stabbr,countyfips,\
        'wac',year,'SI'],'w_geocode',[work_block])

    # Loop needs to focus on single jobtype and single segment value
    # get list of unique jobtypes
    jobtype_list = rand_select_alljobs_df['jobtype'].unique().tolist()

    # Create ouptput df dictionary to save jobtype data
    output_df_list = mcma_sa_job_concurrent_futures(mcmc_sa_process_job, num_procs, jobtype_list,
                                                    repeat(wac_joblist_df),
                                                    repeat(rand_select_alljobs_df),
                                                    repeat(seedi),
                                                    repeat(seedj),
                                                    repeat(seedk),
                                                    repeat(reduction_threshold),
                                                    repeat(random_accept_threshold))

    output_df = pd.concat(output_df_list, ignore_index=True)
    
    return output_df, wac_joblist_df

def mcma_sa_job_concurrent_futures(function_name, num_workers, *args):
    """Utilizes concurrent.future module to parallelize the per-job computation at the core of `mcmc_sa_loop`.
    Args:
        function_name (function): The function to be parallelized.
        num_workers (int): Maximum number workers in parallelization.
        *args: All the arguments in order to pass into parameter function_name.
    Returns:
        dict: Ordered dictionaries with MCMC outcomes for all job types.
    """
    output_dfs = []

    with concurrent.futures.ProcessPoolExecutor(
            max_workers=num_workers) as executor:
        for ret1 in executor.map(function_name, *args):
            output_dfs.append(ret1)

    return output_dfs

def mcmc_sa_process_job(jobtype, wac_joblist_df, rand_select_alljobs_df, seedi, seedj, seedk,
                        reduction_threshold, random_accept_threshold):
    # Save wac job list by jobtype
    wac_jobtypelist_df = {}
    # Look at wac joblist for jobtype only
    for segment in ['Earnings', 'Age', 'SuperSector']:
        wac_jobtypelist_df[segment] = \
            wac_joblist_df[segment].loc[ \
                wac_joblist_df[segment]['jobtype'] == jobtype]
    # Select jobs in job type category first
    # Job types have indepent distributions and totals
    rand_select_jobtype_df = rand_select_alljobs_df.loc[ \
        (rand_select_alljobs_df['jobtype'] == jobtype)]
    # Establish baseline fitness
    # randomly select a new job list
    length_wac_rac_od_joblist = len(rand_select_jobtype_df)
    selected_wac_rac_od_joblist = rand_select_jobtype_df.loc[
        rand_select_jobtype_df['select_job'] == 1]
    length_expected = len(selected_wac_rac_od_joblist)
    print("****************************")
    print("The Starting joblist has ", length_wac_rac_od_joblist, "observations")
    print("****************************")
    print("    ****************************")
    print("     The expected joblist has ", length_expected, "observations")
    print("    ****************************")
    start_totalfitness = calculate_total_fitness(rand_select_jobtype_df,
                                                 wac_jobtypelist_df,
                                                 iteration=1, seed=seedi)
    start_combinedfitness = calculate_combined_fitness(rand_select_jobtype_df,
                                                       wac_jobtypelist_df,
                                                       iteration=1, seed=seedi)
    start_fitness = start_totalfitness['fitness'].squeeze()
    combined_fitness = start_combinedfitness['fitness'].squeeze()
    #print('The starting total fitness =', start_fitness, \
    #      'and combined fitness =', combined_fitness)

    # Reduce error by desired %
    difference_between_current_and_expected = \
        length_wac_rac_od_joblist - length_expected
    reduce_by = difference_between_current_and_expected - \
                (difference_between_current_and_expected * reduction_threshold)
    threshold_length = int(length_expected + reduce_by)
    #print("Initial threshold length set to:", threshold_length)

    precent_fitness_threshold = int(start_fitness - \
                                    (start_fitness * reduction_threshold))

    percent_combined_fitness = int(combined_fitness - \
                                   (combined_fitness * reduction_threshold))
    #print("Initial total fitness threshold set to:", precent_fitness_threshold)
    #print("Initial combined fitness thresholds set to:", percent_combined_fitness)

    # How many times will the loop run?
    # Each time the loop runs the number of jobs in the joblist should shrink
    # Run loop that splits joblist by segments
    loop = 1
    # Store previous lenght to check if loop gets stuck
    previous_length_wac_rac_od_joblist = length_wac_rac_od_joblist
    stuck_length_loop = 0

    while (length_wac_rac_od_joblist > threshold_length):
        print("Split MCMCSA loop", loop)
        split_mcmcsa_df = {}  # This needs to be generated later by the concurrent futures function
        concat_split_df = {}  # Idem

        # Parallelize this section
        for segment in ['Earnings', 'Age']:
            seg_stem = all_stems[segment]
            split_rand_select_jobtype_df = {}
            split_wac_jobtypelist_df = {}
            split_df = {}

            # look at specific totals by earnings and age - segement vars are CE01, CE02, CE03
            # range(1,4) produces values 1,2,3 for segement variable name
            for i in range(1, 4):
                seg_var = seg_stem + "0" + str(i)
                # the wac joblist is a dictionary of dataframes based on segements earnings and age
                split_wac_jobtypelist_df[seg_var, jobtype] = {}

                # split the wac joblist by segment and jobtype
                split_wac_jobtypelist_df[seg_var, jobtype][segment] = \
                    wac_jobtypelist_df[segment].loc[
                        (wac_jobtypelist_df[segment][seg_var] != 0)]
                # length_wac_joblist = len(split_wac_jobtypelist_df[seg_var,jobtype][segment])
                # split the wac-rac-od joblist by segment and jobtype
                split_rand_select_jobtype_df[seg_var, jobtype] = \
                    rand_select_jobtype_df.loc[
                        (rand_select_jobtype_df[segment] == i) &
                        (rand_select_jobtype_df['jobtype'] == jobtype)]
                length_df = len(split_rand_select_jobtype_df[seg_var, jobtype])
                # print(length_df)
                if length_df > 0:
                    split_df[seg_var, jobtype], split_mcmcsa_df[seg_var, jobtype] = \
                        markov_chain_monte_carlo_simanneal(
                            split_rand_select_jobtype_df[seg_var, jobtype],
                            split_wac_jobtypelist_df[seg_var, jobtype],
                            random_accept_threshold=random_accept_threshold,
                            reduction_threshold=reduction_threshold,
                            seedi=seedi,
                            seedj=seedj,
                            seedk=seedk)

            # combine split segment dataframes
            concat_split_df[segment] = pd.concat(split_df.values(), ignore_index=True)

        # look at jobs selected by earnings and age
        selected_wac_rac_od_joblist = {}
        for segment in ['Earnings', 'Age']:
            selected_wac_rac_od_joblist[segment] = concat_split_df[segment].loc[
                concat_split_df[segment]['select_job'] == 1].copy(deep=True)

        # merge together the 2 sets of selected jobs
        by_vars = ['w_geocode', 'h_geocode', 'jobidod', 'jobidod_counter', 'jobidac']
        compare_joblist = pd.merge(
            left=selected_wac_rac_od_joblist['Earnings'][ \
                by_vars + ['prob_selected', 'select_job']],
            right=selected_wac_rac_od_joblist['Age'][ \
                by_vars + ['prob_selected', 'select_job']],
            left_on=by_vars,
            right_on=by_vars,
            how='outer')
        compare_joblist = compare_joblist.fillna(value=0)

        # update the probability of selection based on jobs selected in both lists
        update_prob_list = compare_joblist.loc[(compare_joblist.select_job_x == 1) &
                                               (compare_joblist.select_job_y == 1) &
                                               (compare_joblist.prob_selected_x != 1)]

        # Check if the update probability list has any matching obervations
        if len(update_prob_list) > 0:
            # clean up list to help with merge
            update_prob_list = update_prob_list.drop(columns= \
                                                         ['prob_selected_x', 'select_job_x', 'prob_selected_y',
                                                          'select_job_y'])
            # add a flag variable that is unique to the loop
            update_prob_list.loc[:, 'prob_selected_flag_' + str(loop)] = 1
            # update the name of the id variable - this will help with the merge
            update_prob_list = update_prob_list.rename(columns= \
                                                           {'jobidac': 'jobidac_selected'})
            # print("Comparison of earnings and ages has updated",len(update_prob_list),"jobs to probabilty 1")

            # merge in updated probability list to original wac-rac-od joblist
            by_vars = ['w_geocode', 'h_geocode', 'jobidod', 'jobidod_counter']
            update_rand_select_jobtype_df = pd.merge(
                left=rand_select_jobtype_df,
                right=update_prob_list,
                left_on=by_vars,
                right_on=by_vars,
                how='outer')

            # Save the original probability of selection, update flag
            update_rand_select_jobtype_df.loc[:, 'prob_select_' + str(loop)] = \
                update_rand_select_jobtype_df['prob_selected']
            # if the loop flag is 1 and the jobidac matches the selected \
            # jobid flip pobability selected to 1
            update_rand_select_jobtype_df.loc[
                (update_rand_select_jobtype_df['prob_selected_flag_' + str(loop)] == 1) &
                (update_rand_select_jobtype_df['jobidod_counter'] == \
                 update_rand_select_jobtype_df['jobidod_counter']) &
                (update_rand_select_jobtype_df['jobidac'] == \
                 update_rand_select_jobtype_df['jobidac_selected']),
                'prob_selected'] = 1
            # if jobid was not selected then set probability to 0
            update_rand_select_jobtype_df.loc[
                (update_rand_select_jobtype_df['prob_selected_flag_' + str(loop)] == 1) &
                (update_rand_select_jobtype_df['jobidod_counter'] == \
                 update_rand_select_jobtype_df['jobidod_counter']) &
                (update_rand_select_jobtype_df['jobidac'] != \
                 update_rand_select_jobtype_df['jobidac_selected']),
                'prob_selected'] = 0

            # jobidac_selected variable no longer need - drop - helps with future loops
            update_rand_select_jobtype_df = \
                update_rand_select_jobtype_df.drop(columns=['jobidac_selected'])

            # reset the job list with all observations that have a probabilty of selection
            # this will reset the selected job list to include all jobs with 100% probabiity of selection
            rand_select_jobtype_df = update_rand_select_jobtype_df.loc[ \
                update_rand_select_jobtype_df['prob_selected'] != 0]
        else:
            print("MCMC SA using split data did not find any matching jobs \n",
                  "between Earnings and Age search.")

        # randomly select a new job list
        rand_select_jobtype_df = rand_select_jobs(rand_select_jobtype_df, seedk + loop)

        length_wac_rac_od_joblist = len(rand_select_jobtype_df)
        print("****************************")
        print("The updated joblist has ", length_wac_rac_od_joblist, "observations")
        print("****************************")
        loop = loop + 1

        # Check if length loop is stuck
        if length_wac_rac_od_joblist < previous_length_wac_rac_od_joblist:
            previous_length_wac_rac_od_joblist = length_wac_rac_od_joblist
        elif length_wac_rac_od_joblist >= previous_length_wac_rac_od_joblist:
            stuck_length_loop += 1
        if stuck_length_loop > 2:
            print("  Loop is not reducing length of joblist")
            print("  Breaking loop.")
            break

    return rand_select_jobtype_df

def drop_extra_columns(input_df):
    """
    Need to drop columns used to predict varaibles
    """

    output_df = input_df.copy()
    drop_if_starts_with = ['prob_','randomsort']
    for substring in drop_if_starts_with:
        drop_vars = [col for col in output_df if col.startswith(substring)]
        #print(drop_vars)
        # Drop columns
        output_df = output_df.drop(drop_vars, axis=1)
    
    return output_df

def split_stack_df_byjobtype(stacked_df):
    """
    Split stack df of wac, rac, and od jobs by jobtype
    """
    fisrt_key=list(stacked_df.keys())[1]
    jobtype_list = stacked_df[fisrt_key]['jobtype'].unique().tolist()
    stacked_jobtype_df = {}
    for jobtype in jobtype_list:
        stacked_jobtype_df[jobtype] = {}
        # Look at wac joblist for jobtype only
        for key in stacked_df.keys():
            #print("Splitting stacked df",key,"by jobytpe",jobtype)
            stacked_jobtype_df[jobtype][key] = \
                stacked_df[key].loc[\
                    stacked_df[key]['jobtype'] == jobtype]
    
    return stacked_jobtype_df

def get_workblock_list(joblist):
    # Create list of work blocks to focus on
    work_blocks = joblist['w_geocode'].unique().tolist()
    print('There are',len(work_blocks),'Blocks')

    return work_blocks

def outer_mcmc_sa_input(year, years,
                 focus_jobtype,
                 stacked_jobtype_df,
                 outputfolder,
                 countyfips,
                 seed
                ):
    """
    Outer loop takes the joblist and tries to focus the MCMC SA process
    The goal is to work with one jobtype and run the process on a 
    smaller set of data.

    Part 1 splits up data by jobtype
    Returns a list of work blocks and stacked data by jobtype
    """

    # Check if file already made 
    filename = "joblist_v010_"+focus_jobtype+"_"+countyfips+"_"+year
    mcmcsa_filepath = sys.path[0]+"/"+outputfolder+"/"+filename
    mcmcsainput_filepath = mcmcsa_filepath+"_mcmcsainput.csv"
    if os.path.exists(mcmcsainput_filepath):
        print("Job list for MCMC SA input already exists.")
        joblist_df = pd.read_csv(mcmcsainput_filepath)
        return joblist_df, mcmcsa_filepath

    # Create list of work blocks to focus on
    wac_key = list(stacked_jobtype_df[focus_jobtype].keys())[1]
    work_blocks = get_workblock_list(stacked_jobtype_df[focus_jobtype][wac_key])

    # create job list based on work blocks
    joblist_wblock_df = {}
    joblist_wblock_all_years_df = {}
    for work_block in work_blocks:
        print("***********************************")
        print("Creating block list for",work_block)
        print("*********************************** \n")

        joblist_wblock_all_years_df[work_block] = \
            lodes_loop.block_to_joblist(work_block = work_block,
                                        years = years, 
                                        outputfoldername = outputfolder,
                                        #reshape_vars = reshape_varsv2, 
                                        stacked_df = stacked_jobtype_df[focus_jobtype])

        # Combine all years
        joblist_wblock_df[work_block] = pd.concat(\
                joblist_wblock_all_years_df[work_block].values(), ignore_index=True)
        
    # Combine all work blocks
    joblist_df = pd.concat(joblist_wblock_df.values(), ignore_index=True)
    # Add unique id
    joblist_df['uniqueid_part1'] =  joblist_df.apply(lambda x: 
                            'WB' +str(x['w_geocode']) +
                            'HB' +str(x['h_geocode']) +
                            str(x['jobidac'])
                                            , axis = 1)
    # move unique id to first column
    joblist_df = data_util.add_primarykey(joblist_df, 'uniquejobid', 'uniqueid_part1')

    # Save output
    joblist_df.to_csv(mcmcsainput_filepath)

    return joblist_df, mcmcsa_filepath

def outer_mcmc_sa_loop(years,
                 focus_jobtype,
                 stacked_jobtype_df,
                 joblist_df,
                 seed,
                 random_accept_threshold,
                 mcmcsa_filepath,
                 start_reduction_threshold,
                 num_procs=1
                ):
    """
    Part2 takes the output of part1 and attempts to run the MCMC SA on 
    all work blocks for the jobtype
    """

    np.random.seed(seed)
    seed_array = np.random.randint(1, 100000, 3)
    # program requires three random seeds based on the initial random seed
    seedi = seed_array[0]
    seedk = seed_array[1]
    seedj = seed_array[2]
    print("3 Random seeds set",seedi,seedk,seedj)

    # How many jobs have a 100% probability of being selected
    joblist_df['initprobability_selected'] = joblist_df['S000'] / joblist_df['possible_S000']
    joblist_df.loc[joblist_df['initprobability_selected']==1,'select_job_beforemcmcsa'] = 1
    joblist_df.loc[joblist_df['initprobability_selected']!=1,'select_job_beforemcmcsa'] = 0
    possible_job_list = joblist_df['uniquejobid'].count()
    percent_already_selcted = int((joblist_df['select_job_beforemcmcsa'].mean())*100)
    print("The",focus_jobtype,"joblist has",possible_job_list,"possible jobs.")
    print(percent_already_selcted,"% of the jobs have a 100% probability of selection.")
    print("The MCMC SA will attempt to predict the remaining job.")

    # start with blocks that have high probability of selection first
    joblist_df = joblist_df.sort_values(by='initprobability_selected', ascending=False)

    wac_rac_od_joblist_df = {}
    output_df_dict = {}    
    work_blocks = get_workblock_list(joblist_df)

    for year in years:
        #for work_block in work_blocks[0:4]: # to test on part of work block list
        for work_block in work_blocks:
            print("Running MCMC SA on block",work_block)
            wac_rac_od_joblist_df[work_block] = {}
            wac_joblist_df = {}

            possible_block_joblist_df = joblist_df.loc[\
                joblist_df['w_geocode']==work_block].copy()

            # Start MCMC SA with a random selection of jobs
            possible_block_joblist_df =data_util.add_probability_job_selected(
                df = possible_block_joblist_df,
                prob_value = 'jobidac', 
                by_vars = ['w_geocode','h_geocode','jobidod','jobidod_counter',\
                    'jobtype','jobidod_pair'])
            rand_select_alljobs_df = rand_select_jobs(possible_block_joblist_df, seedk)

            county_jobcounts_df = stacked_jobtype_df[focus_jobtype]
            block_str = str(int(work_block)).zfill(15)
            stfips = block_str[0:2]
            stabbr = str.lower(us.states.lookup(stfips).abbr)
            countyfips = block_str[0:5]
            # Compare random selection of jobs with the WAC jobs list by Earnings, Age, and SuperSector
            wac_joblist_df = {}
            wac_joblist_df['Earnings'] = data_util.explorebyblock(county_jobcounts_df[stabbr,countyfips,\
                'wac',year,'SE'],'w_geocode',[work_block])
            wac_joblist_df['Age'] = data_util.explorebyblock(county_jobcounts_df[stabbr,countyfips,\
                'wac',year,'SA'],'w_geocode',[work_block])
            wac_joblist_df['SuperSector'] = data_util.explorebyblock(county_jobcounts_df[stabbr,countyfips,\
                'wac',year,'SI'],'w_geocode',[work_block])

            start_totalfitness = calculate_total_fitness(rand_select_alljobs_df, 
                                wac_joblist_df, 
                                iteration =1, seed = seedi)
            start_combinedfitness = calculate_combined_fitness(rand_select_alljobs_df, 
                                wac_joblist_df, 
                                iteration =1, seed = seedi)
            outer_start_fitness = start_totalfitness['fitness'].squeeze()
            outer_combined_fitness = start_combinedfitness['fitness'].squeeze()
            print('The starting outer total fitness =',outer_start_fitness,\
                'and outer combined fitness =',outer_combined_fitness)
            # Store previous combined fitness to check if loop gets stuck
            previous_outer_combined_fitness = outer_combined_fitness
            stuck_outer1_loop = 0

            # Start with a low reduction threshold
            reduction_threshold = start_reduction_threshold
            while (stuck_outer1_loop < 3):
                print("#################################")
                print('Attempt to reduce size of possible job list by',reduction_threshold)
                print("#################################")
                wac_rac_od_joblist_df[work_block][year], wac_joblist_df[work_block] = \
                    mcmc_sa_loop(work_block = work_block, 
                                        year = year,
                                        county_jobcounts_df = stacked_jobtype_df[focus_jobtype],
                                        possible_block_joblist_df = possible_block_joblist_df,
                                        seedk = seedk,
                                        seedi = seedi,
                                        seedj = seedj,
                                        random_accept_threshold = random_accept_threshold,
                                        reduction_threshold = reduction_threshold,
                                        num_procs=num_procs)
            
                # Clean up possible joblist for next run
                possible_block_joblist_df = \
                    drop_extra_columns(wac_rac_od_joblist_df[work_block][year])

                outer_combined_fitness_df = calculate_combined_fitness(
                        possible_block_joblist_df, 
                        wac_joblist_df[work_block], iteration =1, seed = seedi)
                outer_combined_fitness = outer_combined_fitness_df['fitness'].squeeze()
                #print('Combined fitness',combined_fitness)

                # Check if Combined fitness is stuck
                if outer_combined_fitness < previous_outer_combined_fitness:
                    previous_outer_combined_fitness = outer_combined_fitness
                elif outer_combined_fitness >= previous_outer_combined_fitness:
                    stuck_outer1_loop += 1
                if stuck_outer1_loop > 3:
                    print("  Outer Combined Fitness Loop seems stuck at fitness",\
                        outer_combined_fitness)
                    print("  Outer loop will end.")  

                # Check if length of possible joblist is equal to expected length
                length_possible_joblist = len(possible_block_joblist_df)
                length_expected_joblist = \
                    np.array(wac_joblist_df[work_block]['SuperSector']['C000']).sum()
                if length_possible_joblist == length_expected_joblist:
                    print("Possible and expected job lists have the same length:",\
                        length_possible_joblist)
                    if outer_combined_fitness != 0:
                        print("If desired fitness not met \n",
                        "Consider changing random seed and starting over to get lower fitness.")
                    break
                # Increase reduction threshold
                reduction_threshold = reduction_threshold + 0.1
                # increase acceptance of randomness
                #random_accept_threshold = random_accept_threshold + 0.05
            
            if (outer_combined_fitness != 0) & \
                (length_possible_joblist > length_expected_joblist):
                # Run MCMC SA on reduced joblist - without splitting the data
                # adjust threshholds for final MCMC SA loop 
                ## NEED A BETTER WAY TO DEFINE THESE THRESHOLDS
                print("****************************")
                print("Combined fitness",outer_combined_fitness)
                print("Try to run MCMC SA without split")
                print("****************************")
                random_accept_threshold = 0.05
                reduction_threshold = 0.9
                    # Start MCMC SA with a random selection of jobs
                possible_block_joblist_df = data_util.add_probability_job_selected(
                    df = possible_block_joblist_df,
                    prob_value = 'jobidac', 
                    by_vars = ['w_geocode','h_geocode','jobidod','jobidod_counter',\
                        'jobtype','jobidod_pair'])

                # Start MCMC SA with a new random selection of jobs - round 2
                rand_select_alljobsr2_df = rand_select_jobs(
                        possible_block_joblist_df, seedk)

                output_df_dict[work_block], mcmcsa_df = markov_chain_monte_carlo_simanneal(
                                    rand_select_alljobsr2_df, 
                                    wac_joblist_df[work_block],
                                    random_accept_threshold = random_accept_threshold,
                                    reduction_threshold = reduction_threshold,
                                    seedi = seedi,
                                    seedj = seedj,
                                    seedk = seedk)
            else:
                output_df_dict[work_block] = possible_block_joblist_df
            
            # What is the final fitness
            final_totalfitness = calculate_total_fitness(rand_select_alljobs_df, 
                                wac_joblist_df, 
                                iteration =1, seed = seedi)
            final_combinedfitness = calculate_combined_fitness(rand_select_alljobs_df, 
                                wac_joblist_df, 
                                iteration =1, seed = seedi)
            outer_final_fitness = final_totalfitness['fitness'].squeeze()
            outer_combined_final_fitness = final_combinedfitness['fitness'].squeeze()
            print('The final outer total fitness =',outer_final_fitness,\
                'and outer combined fitness =',outer_combined_final_fitness)

            # Save file for workblock
            work_block_string = str(work_block).zfill(15)
            savefile = mcmcsa_filepath+"_"+work_block_string+"_rs"+str(seed)+".csv"
            output_df_dict[work_block].to_csv(savefile)

        # combine split dataframes
        output_df = pd.concat(output_df_dict.values(), ignore_index=True)

        savefile = mcmcsa_filepath+"_rs"+str(seed)+"_alljobs.csv"
        output_df.to_csv(savefile)
        
        selected_wac_rac_od_joblist = output_df.loc[\
            output_df['select_job'] ==  1]

        savefile = mcmcsa_filepath+"_rs"+str(seed)+".csv"
        selected_wac_rac_od_joblist.to_csv(savefile)
    
    return output_df

def compare_expected_possible(joblist_mcmcsa_df, 
                                stacked_jobtype_df,
                                year = '2010',
                                focus_jobtype = 'JT11'):
    """
    Code to check the expected number of jobs with the possible number of jobs
    ### Check to make sure that expected joblist and possible job have same length
    Issue found where possible job list did not have the same number of 
    potential jobs as the expected job list. 
    This issue led to the fitness not able to converge to 0 - 
    because there would always be missing values.
    """
    work_blocks = get_workblock_list(joblist_mcmcsa_df)

    # Selected data by block and check fitness
    wac_joblist_df_block = {}
    block_total_fitness = {}
    block_combined_fitness = {}
    #wac_joblist_df_block['Earnings'] = {}
    #wac_joblist_df_block['Age'] = {}
    #wac_joblist_df_block['SuperSector'] = {}
    # Check difference and fitness at the block level
    for work_block in work_blocks:
        county_jobcounts_df = stacked_jobtype_df[focus_jobtype]
        block_str = str(int(work_block)).zfill(15)
        stfips = block_str[0:2]
        stabbr = str.lower(us.states.lookup(stfips).abbr)
        countyfips = block_str[0:5]
        wac_joblist_df_block['Earnings'] = \
            data_util.explorebyblock(county_jobcounts_df[stabbr,countyfips,\
            'wac',year,'SE'],'w_geocode',[work_block])
        wac_joblist_df_block['Age'] = \
            data_util.explorebyblock(county_jobcounts_df[stabbr,countyfips,\
            'wac',year,'SA'],'w_geocode',[work_block])
        wac_joblist_df_block['SuperSector'] = \
            data_util.explorebyblock(county_jobcounts_df[stabbr,countyfips,\
            'wac',year,'SI'],'w_geocode',[work_block])
        # Add fitness check
        selected_wac_rac_od_joblist_block = joblist_mcmcsa_df.loc[\
            (joblist_mcmcsa_df['select_job'] ==  1) &
            (joblist_mcmcsa_df['w_geocode'] == work_block)]

        block_total_fitness[work_block] = \
            calculate_total_fitness(selected_wac_rac_od_joblist_block, 
            wac_joblist_df_block, iteration = 1, seed = 'na')
        # Add workblock to dataframe
        block_total_fitness[work_block]['w_geocode'] = work_block

        block_combined_fitness[work_block] = \
            calculate_combined_fitness(selected_wac_rac_od_joblist_block, 
            wac_joblist_df_block, iteration = 1, seed = 'na')
        # Add workblock to dataframe
        block_combined_fitness[work_block]['w_geocode'] = work_block


    # Combine block level data for fitness check
    total_fitness = pd.concat(block_total_fitness.values(), ignore_index=True)
    total_fitness = total_fitness.rename(columns={'fitness': "Total Fitness"})
    combined_fitness = pd.concat(block_combined_fitness.values(), ignore_index=True)
    combined_fitness = combined_fitness.rename(columns={'fitness': "Combined Fitness"})

    # Try this option to compare by block
    selected_wac_rac_od_joblist = joblist_mcmcsa_df.loc[\
        joblist_mcmcsa_df['select_job'] ==  1]

    expected_job_count = county_jobcounts_df[stabbr,countyfips,\
            'wac',year,'SE'][['w_geocode','jobtype','C000']].\
            groupby(['w_geocode','jobtype'],as_index=False).sum()
    
    by_vars = ['w_geocode']
    mcmcsa_C000 = selected_wac_rac_od_joblist[by_vars+['select_job']].groupby(\
        by_vars, as_index=False).count()
    
    compare_C000 = pd.merge(left = expected_job_count,
                            right = mcmcsa_C000,
                            on = ['w_geocode'],
                            how = 'right')
    compare_C000 = compare_C000.rename(columns={'C000': "expected count"})
    compare_C000 = compare_C000.rename(columns={'select_job': "MCMC SA count"})
    compare_C000['difference'] = compare_C000["expected count"] - compare_C000["MCMC SA count"]

    # Merge in total and combined fitness
    compare_C000 = pd.merge(left = compare_C000,
                            right = total_fitness[['w_geocode','Total Fitness']],
                            on = ['w_geocode'])
    compare_C000 = pd.merge(left = compare_C000,
                            right = combined_fitness[['w_geocode','Combined Fitness']],
                            on = ['w_geocode'])

    return compare_C000, total_fitness, combined_fitness
