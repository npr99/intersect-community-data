import sys
import us
import os
import numpy as np
import pandas as pd

# open, read, and execute python program with reusable commands
from lodes_datautil import get_homeblocklist
from lodes_datautil import import_lodes
from lodes_datautil import new_jobtypes
from lodes_datautil import fix_char_vars
from lodes_datautil import stack_jobset
from lodes_datautil import keep_nonzeros
from lodes_datautil import explorebyblock
from lodes_datautil import check_out_of_state_rac
from lodes_datautil import reshapecascade
from lodes_datautil import reshapecascade_revised
from lodes_datautil import expand_df
from lodes_datautil import add_jobids
from lodes_datautil import wac_rac_joblist
from lodes_datautil import wac_rac_od_joblist
from lodes_datautil import remove_duplicate_block_error
from lodes_datautil import add_total_count_byvar
from lodes_datautil import add_jobidod_pair
from lodes_datautil import combine_wac_rac_joblist

# Use LODES Data Structure set of dictionaires to setup loops
from _lodes_data_structure import all_ods, all_segparts, all_jobtypes, all_segstems, all_mxjobtypes

def obtain_lodes_county_loop(countylist, 
                    years, 
                    outputfoldername,
                    ods = all_ods,
                    segparts = all_segparts,
                    jobtypes = all_jobtypes,
                    mxjobtypes = all_mxjobtypes,
                    segstems = all_segstems,
                    blocklist = ''
                    ):
    """
    Obtain State Level Files and Select One Counties Data.
    Convert Segements and jobtypes into mutually exclusive block level lists.

    TO DO: Consider breaking loop into 
        1. obtain and select county data
        2. create mutually exculusive block level data

    The LODES data file is a structured collection of hundreds of files, 
    across many years (2002-2018), all US states, 
    that covers area characteristics (work and residential), and 
    origin-destination data.

    This loop works to obtain the Source Data, save it to your local machine,
    and then convert 120 files into 7 files for a single county.

    The output of this loop will be used by other modules to further reduce
    the number of files down to 1 file.
    """
    # Start an empty dictionary to save LODES data
    df = {}

    # Create dictionary for stacked job counts
    stacked_df = {} # start empty dictionary to collect dataframes
    # Loop through file group type
    for od in ods:                
        # check file group 
        if od in ['wac','rac']:
            # For Segements to Loop Through
            list_of_segements_or_parts = stem = segparts['Segments']
            
            # Set Job Count Variable
            jobcount = 'C000'
            
        if od == 'od':
            # For Parts to Loop Through
            list_of_segements_or_parts = segparts['Parts']
            
            # Set Job Count Variable
            jobcount = 'S000'
        
        for countyfips in countylist:

            #Get state abbrevation based on county fips
            # County Fips needs to be 5 character string that is 0 padded (zfill)
            countyfips_str = str(int(countyfips)).zfill(5)
            stfips = countyfips_str[0:2]
            # Create lower case state abbrevation based on fips code
            state = str.lower(us.states.lookup(stfips).abbr)
            for year in years:
                print('Importing and cleaning LODES data for the following files in the '+od+' group for '+year+'.')
                # Set areas to select -
                if od in ['od']:
                    # The Origin-Destiantion file uses the county list 
                    # to select jobs the start or end in the selected county
                    county = [countyfips]
                    blocks = ""
                if od in ['wac']:
                    # The Work Area File only looks at workers in county
                    county = [countyfips]
                    blocks = ""
                if od in ['rac']:
                    # The Residential area file looks at home blocks from the OD file
                    # RAC data can come from outside of the source county
                    county = [countyfips]
                    if blocklist == '':
                        blocks = get_homeblocklist(stacked_df[state,countyfips,'od',year,'na'])
                    else:
                        blocks = blocklist
                
                # Loop through Segement or Part List
                for newvar in list_of_segements_or_parts:

                    # Create dictionary with fixed characteristics data
                    fixed_df = {} # start empty dictionary to collect dataframes
                    # Create dictionary to store new job types
                    newjobtypes_df = {} # start empty dictionary to collect new job types by segement dataframes
                    seg_stem = segstems[newvar]
                    # Check if block level list has already been made
                    filename = f'{state}_{countyfips}_{od}_{year}_{seg_stem}'
                    savefile_path = sys.path[0]+"/"+outputfoldername+"/"+filename+".csv"

                    # Check if selected data already exists - if yes break out of function
                    if os.path.exists(savefile_path):
                        # read in existing CSV - make sure to check data types 
                        # year is a string
                        stacked_df[state,countyfips,od,year,seg_stem] = pd.read_csv(savefile_path, 
                                                                                    dtype={'year' : str})
                    else:
                        segparts_list = list_of_segements_or_parts[newvar]
                        for segpart in segparts_list:           
                            # replace segpart with segement id if wac or rac file group
                            if od in ['wac','rac']: 
                                # What characteristics make the list of jobs unique
                                seg_stem = segpart[0:2] 
                            if od in ['od']:
                                # For OD file group segement stem is not applicaple
                                seg_stem = 'na'


                            # Import LODES data by Job Type
                            # Create dictionary with jobtype data
                            jobtype_df = {} # start empty dictionary to collect dataframes
                            for jobtype in jobtypes:
                                print("  "+state+'_'+od+'_'+segpart+'_'+jobtype+'_'+year, end =",")
                                try:
                                    df[state,od,segpart,jobtype,year] = import_lodes(year = year, 
                                                                                od = od,                                       
                                                                                segpart = segpart, 
                                                                                jobtype = jobtype, 
                                                                                state = state, 
                                                                                countylist = county,
                                                                                blocklist = blocks)
                                except FileNotFoundError as e:
                                    print(f"File not found error: {e}")
                                    # Handle the error as needed or simply pass to continue without doing anything
                                    pass
                                # save selected jobtype data to disctionary
                                try: 
                                    jobtype_df[jobtype] = df[state,od,segpart,jobtype,year]
                                
                                except KeyError as e:
                                    print(f"Key error: {e}")
                                    # Handle the error as needed or simply pass to continue without doing anything
                                    pass

                            # create mutually exclusive job types
                            newjobtypes_df[state,od,segpart,year] = new_jobtypes(jobtype_df)

                            # Loop through Mututally exclusive job type to set fixed characteristic variables
                            for mxjobtype in mxjobtypes:
                                #print(state,od,segpart,mxjobtype,year)
                                try:
                                    fixed_df[state,od,segpart,mxjobtype,year] = fix_char_vars(
                                    df = newjobtypes_df[state,od,segpart,year][mxjobtype],
                                    mxjobtype = mxjobtype,
                                    jobcount = jobcount, 
                                    segpart = segpart, 
                                    seg_stem = seg_stem,
                                    newvar = newvar)
                                    
                                except KeyError as e:
                                    print(f"Key error: {e}")
                                    # Handle the error as needed or simply pass to continue without doing anything
                                    pass

                        # Stack mututally exculsive job types by characteristic and segement
                        stacked_df[state,countyfips,od,year,seg_stem] = stack_jobset(unstacked_df = fixed_df)
                        # add segement stem to dataframe
                        stacked_df[state,countyfips,od,year,seg_stem]['seg_stem']  = seg_stem
                        # add year variable
                        stacked_df[state,countyfips,od,year,seg_stem]['year'] = str(year)
                        # Drop observations with zero job counts
                        stacked_df[state,countyfips,od,year,seg_stem] = keep_nonzeros(stacked_df[state,countyfips,od,year,seg_stem],'jobcount')

                        # Save stacked file
                        stacked_df[state,countyfips,od,year,seg_stem].to_csv(savefile_path)

    return stacked_df


def out_of_state_rac_blocks(work_block: int, 
                    years, 
                    outputfoldername,
                    stacked_df,
                    segstems = ['SE','SI','SA']
                    ):
    """
    Loop uses full_lodes_loop to collect county data for out-of-state RAC files

    Loop runs for a single work block and saves county level data

        :param work_block: work block to select out of state RAC data for
        :type work_block: 14 or 15 digit integer
    """
    
    block_str = str(int(work_block)).zfill(15)
    stfips = block_str[0:2]
    stabbr = str.lower(us.states.lookup(stfips).abbr)
    countyfips = block_str[0:5]


    for year in years:
        outofstate_rac_dict = {} # create dictionary to store out of state blocks for work block
        out_of_state_rac_block = {} # create container to store blocks that are out of state
        out_of_state_blocks_list = {}
        list_of_blocks = []

        dfselect = {}
        # Using the Origin Destination file make a list of out of state blocks
        dfselect['od','na'] = \
            explorebyblock(stacked_df[stabbr,countyfips,'od',year,'na'],
                            'w_geocode',[work_block])
        out_of_state_blocks_list[year] = \
            check_out_of_state_rac(dfselect['od','na'][['w_geocode','h_geocode']])
        
        # Check length of out of state block list
        length_out_of_state_blocks_list = len(out_of_state_blocks_list[year])
        if length_out_of_state_blocks_list == 0:
            print("No out of state blocks")
            # Create empty list to append later
            df = {}
            return df

        for index, row in out_of_state_blocks_list[year].iterrows():
            county_str = str(row['h_countyfips'])
            h_geocode = row['h_geocode']
            h_stabbr = row['h_stabbr']

            list_of_blocks = list_of_blocks + [h_geocode]

            county_list_dict = {}
            county_list_dict[county_str] = 'Out of state county'
            print('Building LODES data for out of state county',county_list_dict)

            # Collect LODES data by county
            outofstate_rac_dict[h_geocode] = obtain_lodes_county_loop(countylist = county_list_dict, 
                                                years = [year], 
                                                outputfoldername = outputfoldername,
                                                blocklist = [h_geocode],
                                                ods = { 'rac' : 'Residence Area Characteristic data'})

            # Select individual blocks from county files
            for seg in segstems:
                out_of_state_rac_block[year,h_geocode,seg] =  explorebyblock(
                    outofstate_rac_dict[h_geocode][h_stabbr,county_str,'rac',year,seg],
                    'h_geocode',[h_geocode])

    # Append out of state blocks into one dataframe by segment
    df = {}
    for year in years:
        for seg in segstems:
            i = 1
            dfs_to_concat = []
            
            for block in list_of_blocks:
                #print(block)
                # if first block
                if i == 1:
                    df[year,seg] = out_of_state_rac_block[year,block,seg].\
                        copy(deep = True)
                else:
                    # df[year,seg] = df[year,seg].\
                    #     append(out_of_state_rac_block[year,block,seg])
                    dfs_to_concat.append(out_of_state_rac_block[year, block, seg])

                i = i + 1
            
            if len(dfs_to_concat) > 0:
                df[year, seg] = pd.concat([df[year, seg]] + dfs_to_concat, ignore_index=True)
            
            df[year,seg] = remove_duplicate_block_error(df[year,seg])

    return df


def block_to_joblist(stacked_df,
                    work_block: int,
                    years,
                    outputfoldername,
                    reshape_vars = {'CE' : 'Earnings',
                                    'CNS': 'IndustryCode',
                                    'CA' : 'Age',
                                    'CR' : 'Race',
                                    'CT' : 'Ethnicity',
                                    'CD' : 'Education',
                                    'CS' : 'Sex'
                                },
                    odreshape_vars = {'SA' : 'Age',
                                    'SE' : 'Earnings',
                                    'SI' : 'SuperSector'},
                    segstems = ['SE','SI','SA']
                    ):
    """
    Function works through a single work block to create a list of jobs
    with detail characteristics. 
    Each job has a work and or a home block.

    The following section applies functions that will reshape the stacked files. 
    This step will transform the unit of observation from blocks to jobtype charactersitics. 
    The three segement files will be combined to make one joblist file that keeps jobs 
    that appear in all three files. 
    The final step combines the WAC, RAC and OD files to make one job list.

    stacked_df is the output of the function obtain_lodes_county_loop

    reshape_vars: dictionary set in _lodes_data_structure.py
    odreshape_vars: dictionary set in _lodes_data_structure.py

    version: 2021-08-21T0951

    """

    wac_rac_od_joblist_df = {}
    reduced_joblist_df = {}

    for year in years:
        # obtain out of state rac blocks for single block
        out_of_state_rac_blocks_df = out_of_state_rac_blocks(work_block = work_block,
                                                                years = [year], 
                                                                outputfoldername = outputfoldername,
                                                                stacked_df = stacked_df)

        df = {} # create empty container to store data for block to joblist function

        # Function runs for single block - need state abbreviation and county fips 
        block_str = str(int(work_block)).zfill(15)
        stfips = block_str[0:2]
        stabbr = str.lower(us.states.lookup(stfips).abbr)
        countyfips = block_str[0:5]

        print("Reshaping stacked joblists from block level data \n", \
            "to joblist with characteristics.")
        df['od','na'] = stacked_df[stabbr,countyfips,'od',year,'na'].copy(deep = True)
        df['od','na'] = remove_duplicate_block_error(df['od','na'])

        for seg in segstems:
            df['rac',seg] = stacked_df[stabbr,countyfips,'rac',year,seg].copy(deep = True)
            ### Append out of state block data to RAC file
            ### If no out of state blocks the df will be empty
            if len(out_of_state_rac_blocks_df) == 3:
                # As of pandas 2.0, append (previously deprecated) was removed, so using concat to do the same command.
                # df['rac',seg] = df['rac',seg].append(out_of_state_rac_blocks_df[year,seg])
                df['rac', seg] = pd.concat([df['rac', seg], out_of_state_rac_blocks_df[year, seg]], ignore_index=True)

            df['wac',seg] = stacked_df[stabbr,countyfips,'wac',year,seg].copy(deep = True)

            # check duplicate blocks
            df['wac',seg] = remove_duplicate_block_error(df['wac',seg])
            df['rac',seg] = remove_duplicate_block_error(df['rac',seg])

        dfselect = {}
        # First select blocks from OD list that have work block
        dfselect['od','na'] = explorebyblock(df['od','na'],'w_geocode',[work_block])
        # Use the od block list to select home blocks associated with work block
        home_blocks = dfselect['od','na']['h_geocode'].unique().tolist()

        # Create a list of unique jobtypes - this can be used to reduce size of RAC file
        jobtype_list = dfselect['od','na']['jobtype'].unique().tolist()

        for seg in segstems:
            dfselect['wac',seg] = explorebyblock(df['wac',seg],'w_geocode',[work_block])
            # dfselect['rac',seg] = explorebyblock(df['rac',seg],'h_geocode',[home_blocks])
            dfselect['rac',seg] = explorebyblock(df['rac',seg],'h_geocode',[work_block])
            # reduce rac blocklist to blocks that have jobtypes in OD block pairs
            # this will help reduce the number of jobs in the job list
            dfselect['rac',seg] = explorebyblock(dfselect['rac',seg],'jobtype',jobtype_list)

        # Create dictionary with reshaped data
        reshapecascade_df = {} # start empty dictionary to collect dataframes
        for od in ['wac','rac']:
            for seg in ['SE','SI','SA']:
                reshaped = reshapecascade_revised(dfselect[od,seg],reshape_vars)
                expanded = expand_df(reshaped,'jobcount')
                reshapecascade_df[od,seg] = add_jobids(expanded)

        # reshape od data frame
        reshaped = reshapecascade_revised(dfselect['od','na'],odreshape_vars)
        expanded = expand_df(reshaped,'jobcount')
        reshapecascade_df['od','na'] = add_jobids(expanded)
        

        # clean up dataframe - reset index
        #reshapecascade_df.reset_index(inplace=True)
        print("Combing WAC RAC and OD joblists.")
        # Combine WAC and RAC Joblists
        df_append_wide_keep = wac_rac_joblist(reshapecascade_df)

        # Combine WAC RAC and OD joblists
        od_df = reshapecascade_df['od','na'].copy()
        wac_df = df_append_wide_keep['wac'].copy()
        rac_df = df_append_wide_keep['rac'].copy()
        # Dataset may not have all possible characteristics
        char_vars = ['Education', 'Ethnicity', 'IndustryCode', 'Race', 'Sex']
        wac_rac_od_joblist_df[year] = wac_rac_od_joblist(od_df, wac_df,rac_df, char_vars)

        # Add expected job total to data frame
        merge_vars = ['w_geocode','h_geocode','jobtype']
        keep_var = ['S000']
        wac_rac_od_joblist_df[year] = pd.merge(left = wac_rac_od_joblist_df[year] ,
                                right = dfselect['od','na'][merge_vars+keep_var],
                                on = merge_vars,
                                how = 'left')
        
        # What is the expected number of jobs
        expected_job_count = np.array(dfselect['od','na']['S000']).sum()
        print("The expected number of jobs in od-pair:",expected_job_count)
        # Remove impossible pairs
        print("Check for impossible pairs.")        
        possible_pairs = wac_rac_od_joblist_df[year]
        home_block_list = possible_pairs['h_geocode'].unique().tolist() 

        reduce_od_pairs = {}
        for home_block in home_block_list:
            #print
            # (work_block,home_block)
            reduce_od_pairs[home_block] = \
                add_jobidod_pair(expected_counts_df = dfselect['od','na'], 
                                possible_pairs = wac_rac_od_joblist_df[year],
                                block_check = home_block)
        # Recombine reduced od pairs
        reduced_joblist_df[year] = pd.concat(\
            reduce_od_pairs.values(), ignore_index=True)
        # How much did the process reduce the pair?
        length_possible_pairs = len(wac_rac_od_joblist_df[year])
        length_reduce_od_pairs = len(reduced_joblist_df[year])
        print("Possible job list reduced from",\
            length_possible_pairs,"to",length_reduce_od_pairs)

        # Add total possible jobs in block pair
        reduced_joblist_df[year] = \
            add_total_count_byvar(reduced_joblist_df[year], \
                values_to_count= 'jobidac', by_vars = merge_vars, 
                values_to_count_col_rename = 'possible_S000')

    return reduced_joblist_df

def explore_jobcounts(stacked_df):
    """
    Create a table that compares the total number of jobs
    in the OD WAC and RAC stacked dataframes.
    The total for the WAC file should match onthemap.ces.census.gov
    The totals in the OD file represent the jobs for workers
    that live and work in the county and the same state.
    The totals in the RAC file represent the jobs in the blocks
    that are either in the county or in the same state that are 
    linked to jobs in the county. The RAC total will be very large.    
    """

    job_count = {}
    for key in stacked_df.keys():
        if 'od' in key:
            count_var = 'S000'
        else:
            count_var = 'C000'
        key_string = '_'.join(key)
        job_count[key] = pd.pivot_table(stacked_df[key], values=count_var, index='jobtype',
                                aggfunc=np.sum, margins = True)
        job_count[key].reset_index(inplace = True)
        job_count[key] = job_count[key].rename(columns = \
            {count_var : key_string})
        job_count[key].assign(total=job_count[key].sum()).stack().to_frame(key_string)

    # Merge the first od file, the first wac file and the first rac file
    # The second and third WAC and RAC files have the same totals
    key_list = list(job_count.keys())
    i = 0
    for key in key_list:
        if i == 0: # first roun of merge
            job_count_check = job_count[key_list[i]]
        elif i in [1,4]:
            job_count_check = pd.merge(
                left = job_count_check,
                right = job_count[key_list[i]],
                on = 'jobtype'
            )
        i += 1
    
    return job_count_check
   
def wac_rac_block_to_joblist(stacked_df,
                         block_fips: int,
                         years,
                         seed_value,
                         outputfoldername,
                         od,
                         reshape_vars = {'CE' : 'Earnings',
                                         'CNS': 'IndustryCode',
                                         'CA' : 'Age',
                                         'CR' : 'Race',
                                         'CT' : 'Ethnicity',
                                         'CD' : 'Education',
                                         'CS' : 'Sex'},                     
                         segstems = ['SE','SI','SA'],
                         ):
    """
    Function works through a single work block to create a list of jobs/employees
    with detail characteristics. 
    Each job has a work/home block.

    The following section applies functions that will reshape the stacked files. 
    This step will transform the unit of observation from blocks to jobtype charactersitics. 

    stacked_df is the output of the function obtain_lodes_county_loop
    reshape_vars: dictionary set in _lodes_data_structure.py

    version: 2024-03-03

    """

    joblist_df = {}

    for year in years:

        df = {} # create empty container to store data for block to joblist function

        # Function runs for single block - need state abbreviation and county fips 
        block_str = str(int(block_fips)).zfill(15)
        stfips = block_str[0:2]
        stabbr = str.lower(us.states.lookup(stfips).abbr)
        countyfips = block_str[0:5]

        print("Reshaping stacked joblists from block level data \n", \
            "to joblist with characteristics.")

        for seg in segstems:
            df[od,seg] = stacked_df[stabbr,countyfips,od,year,seg].copy(deep = True)
            # if od == "rac":
            # # obtain out of state rac blocks for single block
            # ### Append out of state block data to RAC file
            # ### If no out of state blocks the df will be empty
            #     out_of_state_rac_blocks_df = out_of_state_rac_blocks(work_block = block_fips,
            #                                                     years = [year], 
            #                                                     outputfoldername = outputfoldername,
            #                                                     stacked_df = stacked_df)
            #     if len(out_of_state_rac_blocks_df) == 3:
            #         df[od, seg] = pd.concat([df[od, seg], out_of_state_rac_blocks_df[year, seg]], ignore_index=True)
           
            # check duplicate blocks
            df[od,seg] = remove_duplicate_block_error(df[od,seg])

        dfselect = {}

        geo_var = {'wac':'w_geocode', 'rac':'h_geocode'}

        for seg in segstems:
            dfselect[od,seg] = explorebyblock(df[od,seg], geo_var[od], [block_fips])

        # Create dictionary with reshaped data
        reshapecascade_df = {} # start empty dictionary to collect dataframes
        for seg in ['SE','SI','SA']:
            if not dfselect[od,seg].empty:
                reshaped = reshapecascade_revised(dfselect[od,seg],reshape_vars, seed_value)
                expanded = expand_df(reshaped,'jobcount')
                reshapecascade_df[od,seg] = add_jobids(expanded)       

        print(f"Combing {od} joblists.......")
        # Combine WAC / RAC Joblists
        df_append_wide_keep = combine_wac_rac_joblist(reshapecascade_df, od, seed_value)

        joblist_df[year,od] = df_append_wide_keep.copy()

        return joblist_df