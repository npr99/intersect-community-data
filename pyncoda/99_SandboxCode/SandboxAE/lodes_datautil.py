# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

# New functions in that could be added to https://github.com/IN-CORE/pyincore/blob/master/pyincore

import pandas as pd
import numpy as np
import random
import os                 # Operating System (os) For folders and finding working directory
import wget # for importing data from the web
import sys  # saving CSV files
import us   # A package for easily working with US and state metadata - used to convert FIPS to state abbreviation
import geopandas as gpd 
import datetime
import itertools # Used in creating list of possible pairs of jobs
from urllib.error import HTTPError


from _lodes_data_structure import all_segstems

def download_lodes(year: str, 
                 od : str,
                 segpart: str,
                 jobtype: str, 
                 state: str):
    """Function downloads LODES data from web and saves to local machine. LODES data provides
    a work area charctersitics by block and orgin-destination data by block. 
    The LODES data is based on the QCEW for the state unemployment insurance 
    forms. They take Quarter 1 and Quarter 2 to indentify jobs. 
    A job is counted if there is Q1 and Q2 forms. Which makes the data roughly 
    related to April 1 jobs.
    
    Source file names have the strucutre [ST][OD][SEG/PART][TYPE][YEAR].csv.gz
    
    Args:
        :param year: year to select data for
        :type year: four digit integer as a string
        :help year: list of possible years by state p. 3 https://lehd.ces.census.gov/data/lodes/LODES7/LODESTechDoc7.5.pdf
        :param od: Origin or Destination 
            Origin = rac – Residence Area Characteristic data, jobs are totaled by home Census Block
            Destination = wac – Workplace Area Characteristic data, jobs are totaled by work Census Block
        :type od: "wac" or "rac" or "od"
        :param segpart: Segment of workforce or Part of Origin Destination File
            [SEG] = Segment of the workforce
            [PART] = Part of the state OD File
        :type segpart: String
        :param jobtype: Job Type
        :type jobtype: String
        :param state: United States State - 2 character abreviation

        Returns:
        filepath: filepath to locally saved version of selected blocks as csv file
        xwalk_filepath: filepath for the Geography Crosswalk
        
    @author: Nathanael Rosenheim - nrosenheim@arch.tamu.edu
    @version: 2021-07-08
    """
    
    #Set up local machine to save large state files
    # This will minmize webpage pings for exploration
    # Make directory to save output
    datapath  = 'data_LODES'
    if not os.path.exists(datapath):
        print("Making new directory to save output: ",datapath)
        os.mkdir(datapath)
    state_datapath = datapath+'/'+state
    if not os.path.exists(state_datapath):
        print("Making new directory to save output: ",state_datapath)
        os.mkdir(state_datapath)
    od_datapath = datapath+'/'+state+'/'+od
    if not os.path.exists(od_datapath):
        print("Making new directory to save output: ",od_datapath)
        os.mkdir(od_datapath)
    
    # set URL for where the file is located
    soucerurl = 'https://lehd.ces.census.gov/data/lodes/LODES8'
    
    # Check if Geography Cross Walk has been downloaded
    # The geography cross walk cleans up minor issues with geocodes
    # download geography crosswalk
    xwalk_filename = f'{state}_xwalk'
    xwalk_url = soucerurl+'/'+state+'/'+xwalk_filename+'.csv.gz'
    # Set file path where file will be downloaded
    xwalk_filepath = state_datapath+"/"+xwalk_filename+'.csv.gz'
    # Check if file exists - if not then download
    if not os.path.exists(xwalk_filepath):
        try:
            wget.download(xwalk_url, out=state_datapath)
        except HTTPError as e:
            print(f"Error downloading file: {e}")       # Handle the error as needed, e.g., set xwalk_filepath to None or log the error.

    #structure LODES file name
    filename = f'{state}_{od}_{segpart}_{jobtype}_{year}'   
    url = soucerurl+'/'+state+'/'+od+'/'+filename+'.csv.gz'
    print(url)

    # Set file path where file will be downloaded
    filepath = od_datapath+"/"+filename+'.csv.gz'
    
    # Check if file exists - if not then download
    if not os.path.exists(filepath):
        try:
            wget.download(url, out=od_datapath)
        except HTTPError as e:
            print(f"Error downloading file: {e}")       # Handle the error as needed, e.g., set filepath to None or log the error.

    return filepath, xwalk_filepath

# draft function
def check_csv_selected():
    """
    Check to see if the selected.csv file already exists with the correct 
    selected area. this will cut down on the processing
    
    Return the dataframe with the selected area
    """
    return

# draft function
def create_downloadreport(od, segs, summary_rows = []):
    """
    The LODES download process includes many files
    It would be nice to create a CSV file that records the summary of
    files that have been dowloaded and processed
    
    This report could make future loops through the data faster  

    Return the dataframe and save CSV that has download report
    """
    # UTC to ISO 8601 with TimeZone information without microsecond  (Python 3)
    iso8601now = datetime.datetime.utcnow()\
                    .replace(tzinfo=datetime.timezone.utc)\
                    .replace(microsecond=0)\
                    .isoformat()
    summary_rows.append([od,segs,iso8601now])
    summary_df = pd.DataFrame(summary_rows,columns=['filegroup','segement','downloadtime'])
    summary_df

    return summary_df


def get_homeblocklist(df):
    """
    Function returns list of unique blocks. Used for selecting
    Residential Area Characteristics blocks associated with a work block.
    """
    home_blocks = df['h_geocode'].unique().tolist()

    return home_blocks

def get_statelist(df, geocodevar):
    """ 
    Function returns list of unique states. 
    Used for importing LODES data by states associated with work blocks.
    LODES data folder structure is based on lower case state abbreviation
    Example: Alabama's folder is al 
    Notes - uese the package US
    """
    # Create a 15 character string based on Census Block that is zero padded
    # States such as Alabama have a FIPS code of 01 - without zfill it would be 1
    df[geocodevar+'_str'] = df[geocodevar].apply(lambda x : str(int(x)).zfill(15))
    df['stfips'] = df[geocodevar+'_str'].str[0:2]
    # Create lower case state abbrevation based on fips code
    df['stabbr'] = df['stfips'].apply(lambda x :  str.lower(us.states.lookup(x).abbr))

    home_states = df['stabbr'].unique().tolist()

    return home_states

def remove_duplicate_block_error(df):
        """
        ISSUE TO FIX
        The full loop appears to be producing duplicate blocks - 
        this is for the out of state blocks and also the initial loop. 
        This does not seem to be an issue with the WAC or OD files. 
        The duplicates appear in the data saved in the program folder -
        but do not seem to be in the source data.

        """
        # Check for duplicates 
        #### ERRROR IN FULL LOOP ####
        # For some reason the full loop code is producing duplicates
        #print("Checking for duplicates")
        geo_var_by = [col for col in df if col.endswith("geocode")]
        fixed_var_by = [col for col in df if col in ['Earnings','Age','SuperSector']]
        original_col_list = geo_var_by + fixed_var_by + ['jobtype','seg_stem','year']
        #print(original_col_list)
        check_df = df[original_col_list]
        duplicates = check_df.loc[check_df.duplicated()]
        if len(duplicates) != 0:
            print("Error in code - block list had duplicates ",len(duplicates))
            # reset out of stte rac block list to be the observations without duplicates
            df  = df.loc[~df[original_col_list].duplicated()]
            print("Duplicate blocks removed")
        if len(duplicates) == 0:
            #print("No duplicate blocks")
            df = df
        
        return df


def check_out_of_state_rac(df):
    """
    The OD aux file inlcudes jobs connected to a county with residence that are 
    out of the state
    This function helps to find jobs that need out of state RAC data

    function depends on python package us 
    """
    
    # using the home and work geocodes (blockid) find the 2 digit state fips code
    # using the 2 digit state fips code find the state abbreviation
    for prefix in ['h','w']:
        df.loc[:, prefix+'_geocode'+'_str'] = df[prefix+'_geocode'].astype(str).apply(lambda x : str(int(x)).zfill(15))
        df.loc[:, prefix+'_stfips'] = df[prefix+'_geocode'+'_str'].str[0:2]
        # Create lower case state abbrevation based on fips code
        df.loc[:, prefix+'_stabbr'] = df[prefix+'_stfips'].apply(lambda x: str.lower(us.states.lookup(x).abbr) if us.states.lookup(x) is not None else '')
        df.loc[:, prefix+'_geocode'+'_str'] = df[prefix+'_geocode'].astype(str).apply(lambda x : str(int(x)).zfill(15))
        df.loc[:, prefix+'_countyfips'] = df[prefix+'_geocode'+'_str'].str[0:5]



        
    # create out-of-state varaible 
    df.loc[:, 'out-of-state'] = 0
    # df['out-of-state'] = 0
    # if home state does not equal work state job is an out of state job
    df.loc[df['h_stabbr'] != df['w_stabbr'], 'out-of-state'] = 1

    # create a list of out os state jobs each observation is a different block
    df = df.loc[df['out-of-state'] == 1]
    
    return df

def make_block_list_by_county(df):
    """
    This function does not work correctly
    need to make a list of blocks where each block is an integer 
    """

    df.loc[:, 'h_blocklist'] = df[['h_geocode_str','h_countyfips']].groupby(
        ['h_countyfips'])['h_geocode_str'].transform(lambda x: ','.join(int(x)))
    
    return df


def import_lodes(year: str, 
                 od : str,
                 segpart: str,
                 jobtype: str, 
                 state: str,
                 countylist = "", 
                 blocklist = "", 
                 columnfilter = "C"):
    """Function reads in LODES data from web to dataframe. LODES data provides
    a work area charctersitics by block and orgin-destination data by block. 
    The LODES data is based on the QCEW for the state unemployment insurance 
    forms. They take Quarter 1 and Quarter 2 to indentify jobs. 
    A job is counted if there is Q1 and Q2 forms. Which makes the data roughly 
    related to April 1 jobs.
    
    Source file names have the strucutre [ST][OD][SEG/PART][TYPE][YEAR].csv.gz
    
    Args:
        :param year: year to select data for
        :type year: four digit integer as a string
        :help year: list of possible years by state p. 3 https://lehd.ces.census.gov/data/lodes/LODES7/LODESTechDoc7.5.pdf
        :param od: Origin or Destination 
            Origin = rac – Residence Area Characteristic data, jobs are totaled by home Census Block
            Destination = wac – Workplace Area Characteristic data, jobs are totaled by work Census Block
        :type od: "wac" or "rac" or "od"
        :param segpart: Segment of workforce or Part of Origin Destination File
            [SEG] = Segment of the workforce
            [PART] = Part of the state OD File
        :type segpart: String
        :param jobtype: Job Type
        :type jobtype: String
        :param state: United States State - 2 character abreviation
        :param countylist: list of County FIPS code - 5 character string
        :param blocklist: list of census blocks (15 character string)
        :param columnfilter: string for selecting specific columns

    
    Returns:
        pandas dataframe: LODES data for selected area
        csv file: Saved version of selected blocks as csv file
        
    @author: Nathanael Rosenheim - nrosenheim@arch.tamu.edu
    @version: 2021-07-07T2126
    """
    # set geocode variable and check od parameter is correct
    if od == 'rac':
        geocode_vars = ['h_geocode']
    elif od == 'wac':
        geocode_vars = ['w_geocode']
    elif od == 'od':
        # OD has both home and work locations that could be in study area
        geocode_vars = ['w_geocode','h_geocode'] 
    else:
        raise ValueError("OD parameter should equal wac, rac, or od")
    
    # Check if final CSV file has aleady been selected
    datapath  = 'data_LODES'
    od_datapath = datapath+'/'+state+'/'+od
    #structure LODES file name
    for county in countylist:
        selected = county
    csv_filename = f'{state}_{od}_{segpart}_{jobtype}_{year}_{selected}'
    csv_filepath = od_datapath+"/"+csv_filename+'.csv'

    # Check if selected data already exists - if yes break out of function
    if os.path.exists(csv_filepath):
       df = pd.read_csv(csv_filepath)

       return df

    filepaths = download_lodes(year = year, 
                            od = od,                                       
                            segpart = segpart, 
                            jobtype = jobtype, 
                            state = state)

    df = pd.read_csv(filepaths[0])
        
    # Read in Crosswalk between blocks and County FIPS code
    xwalk_df = pd.read_csv(filepaths[1], usecols = ['tabblk2020','cty'])

    # add year to dataframe
    df['year'] = year
    
    # add segment type
    df['seg'] = segpart
    
    # add job type
    df['jobtype'] = jobtype
        
    # loop through geovars to locate possible jobs in study area
    # create empty container to store study area jobs
    append_studyarea = []
    for geocode_var in geocode_vars:
        # Convert BLOCKID10 to a string - zero padded 15 characters long
        df[geocode_var+'str'] = df[geocode_var].apply(lambda x : str(int(x)).zfill(15))
        
        # Add County Variable - from Geography Crosswalk
        # Note can not call the variable County - will be included in column list
        df = pd.merge(left = df,
                      right = xwalk_df,
                      left_on = geocode_var,
                      right_on = 'tabblk2020',
                      how = "left")

        # For home counties in aux files the counties are all out of state
        # merge with state geography crosswalk will produce errors
        df['cty'] = df['cty'].fillna(0)

        # To do FIPS code should be 5 digit zero padded
        df[geocode_var+'cntyfips'] = df['cty'].apply(lambda x : str(int(x)).zfill(5))
        # drop crosswalk variables
        df = df.drop(columns = ['tabblk2020','cty'])
        
        # select county in study area
        if countylist != "" :
            if od in ['wac','rac']:
                countyarea = df.loc[df[geocode_var+'cntyfips'].isin(countylist)].copy()
                 # Append selected jobs data
                append_studyarea.append(countyarea)
            if od in ['od'] and geocode_var == 'w_geocode':
                countyarea = df.loc[df[geocode_var+'cntyfips'].isin(countylist)].copy()
                # Append selected jobs data
                append_studyarea.append(countyarea)
            if od in ['od'] and geocode_var == 'h_geocode':
                # Select observations that have home geocode in county but 
                # work geocode outside of county
                # without this step the append creates duplicates
                countyarea = df.loc[(df['h_geocodecntyfips'].isin(countylist)) &
                                (~df['w_geocodecntyfips'].isin(countylist))].copy()
                # Append selected jobs data
                append_studyarea.append(countyarea)
            
        if blocklist != "" and od in ['rac']:
            # RAC files include blocks inside and outside of main county 
            # Block list should be based on the OD File
            # select blocks in the blocklist but outside the county
            # without the county check process creates duplicates
            blockarea = df.loc[(df[geocode_var].isin(blocklist)) &
                                (~df[geocode_var+'cntyfips'].isin(countylist))].copy()
             # Append selected jobs data
            append_studyarea.append(blockarea)
        
    # Create dataframe from appended county data
    studyarea_df = pd.concat(append_studyarea)
    # check for duplicates and drop - only want to add blocks outside of county
    studyarea_df.drop_duplicates()
    
    if od in ['wac','rac']:
        # Fliter data with job counts (Starts with C)
        filter_col = [col for col in df if col.startswith(columnfilter)]
        
        # Remove firm age and size variables from filter list
        # only available for data years 2011-2017, for All Private Jobs (JT02)
        firm_vars = [col for col in df if col.startswith("CF")]
        filter_col = [col for col in filter_col if col not in firm_vars]
    
        # Add total job count if the column is missing
        if "C000" in filter_col:
                filter_col = [geocode_var] + filter_col
        else:
                filter_col = [geocode_var,'C000'] + filter_col
    
    if od == 'od':
        filter_col = [col for col in df if col.startswith("S")]
        filter_col = ['w_geocode','h_geocode'] + filter_col
    

    savefile = sys.path[0]+"/"+csv_filepath
    studyarea_df.to_csv(savefile, columns= filter_col, index=False)

    # Return data with job count
    return studyarea_df[filter_col]

def subtract_df(df1: pd.DataFrame, df2: pd.DataFrame, index_col: str):
    """ Subtract a "child" dataframe from a "parent" dataframe 
    
    Args:
        :param df1: dataframe that includes the child dataframe- "Parent" 
        :param df2: dataframe that is part the parent dataframe - "Child" Jobtype
        :param index_col: Column to use as the index to match observations
        :help index_col: Column will be census geography such as block id 
    
    Returns:
        pandas dataframe: subtracted df
    
    @author: Nathanael Rosenheim - nrosenheim@arch.tamu.edu
    @version: 2021-06-19T1714
    """
    
    # subtract the child dataframe from the parent dataframe
    df =  df1.set_index(index_col).subtract(df2.set_index(index_col), fill_value=0)
    
    # reset index - this moves the index column from index to a column
    df.reset_index(inplace=True)
    
    # Return dataframe that subtracts two dataframes
    return df

def add_df(df1: pd.DataFrame, df2: pd.DataFrame, index_col: str):
    """ Add two "child" dataframes 
    
    Args:
        :param df1: dataframe that is mutually exculive from sibling dataframe 
        :param df2: dataframe that is mutually exculive from sibling dataframe 
        :param index_col: Column to use as the index to match observations
        :help index_col: Column will be census geography such as block id 
    
    Returns:
        pandas dataframe: subtracted df
    
    @author: Nathanael Rosenheim - nrosenheim@arch.tamu.edu
    @version: 2021-06-19T1714
    """
    
    # add the child dataframe from the parent dataframe
    df =  df1.set_index(index_col).add(df2.set_index(index_col), fill_value=0)
    
    # reset index - this moves the index column from index to a column
    df.reset_index(inplace=True)
    
    # Return dataframe than adds two dataframes
    return df

# draft function
def import_home_to_aux_jobs():
    """
    draft function
    need to make a program that will loop through all of the jobs that
    will start in the selected area (home origin) but have a out of state destination
    """
    return

# create multually exclusive job types
def new_jobtypes(jobtype_df):
    """The job types initial job types are All, Private and Federal with counts 
    of All Jobs and Primary Jobs.
    The initial job types are not mutually exclusive - 
    for example JT02 includes JT03. 
    To make mutually exclusive jobs types two new types of jobs can be created
    - Public Sector jobs (jobs that are not private or federal - 
    might be public school jobs, local and state government jobs). 
    Additionally each job type can have non-primary jobs - 
    one person could have multiple jobs. 
    In total there would be 12 job type categories. 
    JT03 + JT05 + JT07 would be one set of mutually exclusive job counts -
    adding up to all primary jobs (JT01). 
    JT09+JT10+JT11 would be a second group - 
    adding up to all secondary jobs (JT08).
    
    New Job Types
    - “JT06” for All Public Sector Jobs
    - “JT07” for Public Sector Primary Jobs
    - “JT08” for All Non-primary Jobs
    - “JT09” for Private Non-primary Jobs
    - “JT10” for Federal Non-primary Jobs
    - “JT11” for Public Non-primary Jobs
    
    Arg:
        :param jobtype_df - dictionary of dataframes that includes all job types
        
    Returns:
        dictionary of pandas dataframe
    
    @author: Nathanael Rosenheim - nrosenheim@arch.tamu.edu
    @version: 2021-06-19T1843
    @version: 2024-01-18Amin
    """
    # Start an empty dictionary to save LODES data
    df = {}
    
    # Identify index variable(s) - for wac = ['w_geocode'],
    #                                  rac = ['h_geocode'],
    #                                   od = ['w_geocode','h_geocode']
    indexvar = [col for col in jobtype_df['JT03'] if col.endswith("geocode")]

    # Set exclusive job types
    if 'JT03' in jobtype_df:
        df['JT03'] = jobtype_df['JT03'].copy()
    if 'JT04' in jobtype_df:
        df['JT04'] = jobtype_df['JT04'].copy()
    if 'JT05' in jobtype_df:
        df['JT05'] = jobtype_df['JT05'].copy()
    
    # “JT06” for All Public Sector Jobs
    if 'JT00' in jobtype_df and 'JT02' in jobtype_df and 'JT04' in jobtype_df:
        df['JT06'] = subtract_df(df1 = jobtype_df['JT00'], # Parent Jobtype = All jobs
                                 df2 = add_df(jobtype_df['JT02'],
                                              jobtype_df['JT04'],indexvar), # Child Jobtype  = All Private and Federal Jobs
                                 index_col = indexvar)
    
    # “JT07” for Primary Public Sector Jobs
    if 'JT01' in jobtype_df and 'JT03' in jobtype_df and 'JT05' in jobtype_df:
        df['JT07'] = subtract_df(df1 = jobtype_df['JT01'], # Parent Jobtype = All Primary jobs
                                 df2 = add_df(jobtype_df['JT03'],
                                              # jobtype_df['JT04'],indexvar), # Child Jobtype  = Primary Private and Federal Jobs
                                              # @Amin: replace JT04 with JT05 to have the correct Child Jobtype
                                              jobtype_df['JT05'],indexvar), # Child Jobtype  = Primary Private and private Federal Jobs
                                 index_col = indexvar)

    # “JT08” for All Non-primary Jobs
    if 'JT00' in jobtype_df and 'JT01' in jobtype_df:
        df['JT08'] = subtract_df(df1 = jobtype_df['JT00'], # Parent = All jobs
                                 df2 = jobtype_df['JT01'], # Child = All Primary Jobs
                                 index_col = indexvar)     
    
    # “JT09” for Private Non-primary Jobs
    if 'JT02' in jobtype_df and 'JT03' in jobtype_df:
        df['JT09'] = subtract_df(df1 = jobtype_df['JT02'], # Parent = All Private jobs
                                 df2 = jobtype_df['JT03'], # Child = Private Primary Jobs
                                 index_col = indexvar)   
    
    # “JT10” for Federal Non-primary Jobs
    if 'JT04' in jobtype_df and 'JT05' in jobtype_df:
        df['JT10'] = subtract_df(df1 = jobtype_df['JT04'], # Parent = All Federal jobs
                                 df2 = jobtype_df['JT05'], # Child = Federal Primary Jobs
                                 index_col = indexvar) 
   
    # “JT11” for Public Non-primary Jobs
    # if 'JT06' in jobtype_df and 'JT07' in jobtype_df:

    # @Amin: "JT06' and 'JT07' are new job types, so they do not exist in jobtype_df
    # and consequently we will never have a JT11 in the output dataframe
    # new job types should be created using the job types that already exist in jobtype_df
    # for JT11 should use the JT00, JT01, JT02, JT03, JT04, JT05
    # for parent and child we still can use the df instead of jobtype_df
    # this change will probably address the several "JT11 : KeyError" errors when looping
    # through mutually exclusive job type to set fixed characteristic variables

    if 'JT00' in jobtype_df and 'JT01' in jobtype_df and 'JT02' in jobtype_df and 'JT03' in jobtype_df and 'JT04' in jobtype_df and 'JT05' in jobtype_df:
        df['JT11'] = subtract_df(df1 = df['JT06'], # Parent = All Public jobs
                                 df2 = df['JT07'], # Child = Primary Public Jobs
                                 index_col = indexvar)
        
    return df
    
    
    
def fix_char_vars(df,
                  mxjobtype,
                  jobcount,
                  segpart,
                  seg_stem,
                  newvar,
                  ):
    """
    Once the LODES data has set the mututally exclusive jobtype and 
    is based on the unique set of segements new variables can be made to prepare 
    for the stacking of the data

    Arg:
    :param 
        
    Returns:
        dictionary of pandas dataframe
    
    @author: Nathanael Rosenheim - nrosenheim@arch.tamu.edu
    @version: 2021-07-09
    """    
    # Add job type if not already defined
    if mxjobtype != '':
        df['jobtype'] = mxjobtype
            
    # identify job count
    df['jobcount'] =  df[jobcount]
    
    # Add by segment characteristic
    df['segpart'] = segpart
    if seg_stem != 'na': # For od files skip this step
        df[newvar] = df['segpart'].str.replace(seg_stem,"").astype(int)
        #filter_col = filter_col + [newvar]


    return df

def reshapelodes(wide_df: pd.DataFrame, 
                 newvar,
                 stem,
                 by = ''):
    """Using the mutually exculive job types and segement 
    transpose data and stack observations. This will result in a 
    long data file that has job counts by segement and 
    two-digit naics codes within each block.

        
    Args:
        :param wide_df: dataframe to reshape
        :param seg: Segment of workforce
            [segpart] = Segment of the workforce
        :type segpart: String
        :param mxjobtype: Mututally Exclusive Job Type
        :type jobtype: String 
        :param long_var: dictionary with variable stem and newvar name 
            - stem = first letters that identify collection of variables to reshape
        :example long_var = 'CNS' jobs by 2 digit NAICS industry
        :param seg_char: dictionary wtih Segement Stem Characteristic
        

    
    Returns:
        pandas dataframe: Long version of LODES data
        
    @author: Nathanael Rosenheim - nrosenheim@arch.tamu.edu
    @version: 2021-07-09T0809
    """
    
    if by == '':
        # Identify by variable(s) - for wac = ['w_geocode'],
        #                                  rac = ['h_geocode'],
        #                                   od = ['w_geocode','h_geocode']
        by = [col for col in wide_df if col.endswith("geocode")]

    # Check if new characteristic is already a column in the dataframe
    # For WAC and RAC group files Earnings, Age, and SuperSector will be fixed
    if newvar in [col for col in wide_df]:
        # If new variable already exists then do not reshape data
        df_reshaped = wide_df.copy()
        return df_reshaped
        

    df_reshaped = pd.wide_to_long(wide_df,[stem], i=by, j=newvar)
    # shift dataframe multiindex to columns
    df_reshaped.reset_index(inplace=True)
    
    # reset job count
    df_reshaped['jobcount'] =  df_reshaped[stem] 
    df_reshaped = df_reshaped.sort_values(by=by + [newvar])

    # Drop observations with no job count
    df_reshaped = df_reshaped.loc[df_reshaped['jobcount'] != 0]
    
    # Return data with job count
    return df_reshaped
    #return df_reshaped[filter_col]


def stack_jobset(unstacked_df):
    """Stack a complete set of jobs by multiple characteristics

        
    Args:
        :param unstacked_df: a dictionary of dataframes to stack  
    Returns:
        pandas dataframe: Stacked version of LODES data
        
    @author: Nathanael Rosenheim - nrosenheim@arch.tamu.edu
    @version: 2021-07-08
    """

    # Stack 
    df = pd.concat(unstacked_df.values(), ignore_index=True)
    
    # sort data
    geovars = [col for col in df if col.endswith("geocode")]
    df = df.sort_values(by=geovars)

    return df

def keep_nonzeros(df, zerovar):
    # Drop observations with zero values
    df = df.loc[df[zerovar] != 0]
    
    return df

def explorebyblock(df,select_var,selectlist):
    df = df.loc[df[select_var].isin(selectlist)].copy()

    return df
    
def add_missingeducation(df):
    """
    Education characteristic does not apply to Age Group 1
    Need to add CD00 = Age Group 1 Job Count
    """
    
    df.loc[:,'CD00']  = df['CA01']
    
    return df


def add_supersector(df):
    """
    Industry codes fall into three supersectors
    Supersector 1 is for Goods producing industries
    Supersector 2 is Trade, transportation, and utilities
    Supersector 3 is All other services 
    """
    
    # Create list of numbers for each range of super sector values
    SI01 = list(range(1,6))
    SI02 = list(range(6,9))
    SI03 = list(range(9,21))

    # make a dictionary for each supersector
    d1 = dict.fromkeys(SI01, 1)
    d2 = dict.fromkeys(SI02, 2)
    d3 = dict.fromkeys(SI03, 3)

    # Combine dictionaries
    supersector_map = {**d1, **d2, **d3}
    
    df.loc[:,'SuperSector'] = df['IndustryCode'].map(supersector_map)
    
    return df



def add_total_count_byvar(df, values_to_count, by_vars, values_to_count_col_rename):
    """
    Function adds a new column with counts of values by variables
    Used to determine denominator and numerator of probability a job is selected
    Also used to determine in reshaped dataset has correct number of jobs to expand in joblist
    """
    total_count_df = pd.pivot_table(df, values=values_to_count, index=by_vars,
                            aggfunc='count')
    total_count_df.reset_index(inplace = True)
    total_count_df = total_count_df.rename(columns = \
         {values_to_count : values_to_count_col_rename})

    # add probabilty denomninator to the original data frame 
    df = pd.merge(left = df,
                    right = total_count_df,
                    left_on = by_vars,
                    right_on = by_vars)

    return df

    
def reshapecascade(df,reshape_vars):
    """
    Function loops through characteristic variables and reshapes dataframe 
    in a loop creating a casacade of possible job combinations.
    
    """
    
    # Reshape requires each obseravtion to be uniquely identified 
    # The initial by variables include the geocode and the jobtype
    # Each loop will add a new variable to the by list for the next reshape
    geo_var_by = [col for col in df if col.endswith("geocode")]
    fixed_var_by = [col for col in df if col in ['Earnings','Age','SuperSector']]
    original_col_list = geo_var_by + fixed_var_by + ['jobtype','seg_stem','year']
    
    # start loop by setting the by vars to the original column list
    by = original_col_list
    for reshape_var in reshape_vars:
        # Make a dictionary for long reshape 
        newcharacteristic =  reshape_vars[reshape_var]
        chacteristic_stem =  reshape_var
        #print("   Reshaping data on",reshape_var,newcharacteristic)
        #long_var = {chacteristic_stem : newcharacteristic}
        #print(long_var)
        
        # Add missing education category before Age Reshape
        if chacteristic_stem == 'CA':
            #print("adding missing education")
            df = add_missingeducation(df)
        
        #reshape dataframe
        df = reshapelodes(
                   wide_df = df,
                   newvar = newcharacteristic,
                   stem = chacteristic_stem,
                   by = by)
        
        if newcharacteristic in original_col_list:
            by = by
        else:
            by = by + [newcharacteristic]
        #print(by)
    
    # Add Super Sector
    col_list = [col for col in df]
    if ('SuperSector' not in col_list) & ('IndustryCode' in col_list):
        df = add_supersector(df)
        by = by + ['SuperSector']

    return df[by + ['jobcount']]

def expand_df(df, expected_count):
    """
    expand dataset based on the number of jobs expected in block or block pairs
    and the number of jobs alrady in list
    """
    geo_var_by = [col for col in df if col.endswith("geocode")]

    # Expand requires all positive values - check to see if any observations have negative values
    negative_jobcount_observations = df.loc[df[expected_count] < 0].copy()
    negative_jobcount_len = len(negative_jobcount_observations)
    if negative_jobcount_len > 0:
        print(negative_jobcount_len,' Observation(s) have a negative job count. This is a known issue.')
        min_negative = negative_jobcount_observations[expected_count].min()
        max_negative = negative_jobcount_observations[expected_count].max()
        print('Negative jobcounts range from ', min_negative," to ", max_negative)
        # df.loc[df[expected_count] < 0, expected_count] = 1      @Amin: The value should be set to zero
        df.loc[df[expected_count] < 0, expected_count] = 0
        print('Observations with negative jobcount replaced with 0 values.')

    # Check if reshaped data already has enough job observations
    # Add available segments to by_vars

    possible_segements = ['Age','Earnings','SuperSector']
    segements_available = [col for col in df if col in possible_segements]
    #print('Available segements to check expand var',segements_available)
    df = add_total_count_byvar(df, 
                             values_to_count = expected_count,
                             by_vars = geo_var_by+['jobtype'], # test this concept+segements_available,  
                             values_to_count_col_rename = 'reshape_count')

    # Check how expand var is defined - important step
    df['expand_var'] = int(0)
    df['expand_var_flag'] = int(0)
    # if jobcount is equal to the total number of observations then expand should be 1
    # this means there are already enough observations - no need to expand to get more
    df.loc[(df[expected_count] == df['reshape_count']), 'expand_var'] = 1
    df.loc[(df[expected_count] == df['reshape_count']), 'expand_var_flag'] = 1

    # If jobcount is larger than the number of observations then the data needs to be expanded
    df.loc[(df[expected_count] > df['reshape_count']), 'expand_var'] = \
        df[expected_count] - df['reshape_count']
    df.loc[(df[expected_count] > df['reshape_count']), 'expand_var_flag'] = 2

    # if reshape count equals 1 then the expand var should be equal to the jobcount
    df.loc[(df['reshape_count'] == 1), 'expand_var'] = df[expected_count] 
    df.loc[(df['reshape_count'] == 1), 'expand_var_flag'] = 3

    # If job count is smaller than the number of observations then the data does not need to be expanded
    df.loc[(df[expected_count] < df['reshape_count']), 'expand_var'] = 1
    df.loc[(df[expected_count] < df['reshape_count']), 'expand_var_flag'] = 4

    # Expand data by jobcount - with respect to the number of jobs in the by list
    df = df.reindex(df.index.repeat(df['expand_var']))

    # reset index - without this step the merge will not include blocks with multiple jobids
    df.reset_index(inplace=True, drop = True)
    
    # drop variables for expanding
    df = df.drop(columns = ['expand_var','expand_var_flag','reshape_count']+[expected_count])
    return df

def add_jobids(df):
    """
    Function adds Job IDs based on the Earnings, Age, Supersector, and job characteristics
    Job IDs are used to match WAC jobs to RAC Jobs based on OD pair job id.
    The function creates 2 ids 
    1. jobidod matching WAC and RAC (area charactersitics) to OD
    2. jobidac matching area characteristics WAC and RAC jobs

    """

    # Reorder columns and make unique job id
    geo_var_by = [col for col in df if col.endswith("geocode")]
    fixedvars = geo_var_by +['jobtype','seg_stem','year'] + ['Age','Earnings','SuperSector']
    col_list = [col for col in df if col not in fixedvars]
    sorted_col_list = sorted(col_list)
      
    # Add Jobid for OD files # OD file matchs on Earnings, Age, and Sector
    df['jobidod'] = "jidod" + df['jobtype']
    for col in ['Age','Earnings','SuperSector']:
       df['jobidod'] = df['jobidod'] + df[col].astype(str)
    
    df['jobidod_counter'] = df.groupby(['jobidod']+geo_var_by).cumcount() + 1
    
    # Add Jobid for WAC and RAC files
    df['jobidac'] = df['jobidod'] + "jobidac"
    #print('Sorted Column list = ',sorted_col_list)
    for col in sorted_col_list:
        df['jobidac'] = df['jobidac'] + df[col].astype(str)
    
    df['jobidac_counter'] = df.groupby(['jobidac']+geo_var_by).cumcount() + 1
            
    #return columns
    return_col = ['jobidod','jobidod_counter','jobidac','jobidac_counter'] + fixedvars + sorted_col_list

    return df[return_col]


def add_probability_job_selected(df, 
                                prob_value = 'jobidac', 
                                by_vars = ['w_geocode','h_geocode','jobidod']):
    """
    The probability a job with set of characteristics is selected is based on the 
    number of jobs with the same jobidod in the home - work block pair

    prob_value = 'jobidac' : default is set to the unique id for the job id based on
        set of area charactersitcs

    ISSUE - probabilty incorrectly set to 1 when total jobs > 1
    
    """

    df = add_total_count_byvar(df, 
                             values_to_count = prob_value,
                             by_vars = by_vars,  
                             values_to_count_col_rename = 'denominator')
    df = add_total_count_byvar(df, 
                             values_to_count = 'denominator',
                             by_vars = by_vars+[prob_value],  
                             values_to_count_col_rename = "numerator")
       
    df['prob_selected'] = df['numerator'] / df['denominator'] 

    df = df.drop(columns = ['numerator','denominator'])

    return df

def wac_rac_joblist(df):
    """
    Function takes list of jobs in either WAC or RAC files and 
    reduces the joblist based on observations that appear in all three
    segment files. 

    df is a dictionary of dataframes created by create_job_wac_od_rac_joblist
    """
    # Create dictionary with reshaped data
    df_append = {} # start empty dictionary to collect dataframes
    df_append_wide = {} # start empty dictionary to collect dataframes
    df_append_wide_keep = {} # start empty dictionary to collect dataframes


    # Using Area Characteristics Data Reduce number of possible job types
    for od in ['wac','rac']:
        print("Checking jobs that appear in all three segement files for",od,"Files.")
        df_append[od] = df[od,'SA'].copy(deep = True)
        print("   For ",od," Age file includes: ",len(df_append[od]),"Possible Jobs")
        # df_append[od] = df_append[od].append(df[od,'SE'])
        # As of pandas 2.0, append (previously deprecated) was removed, so using concat to do the same command.
        df_append[od] = pd.concat([df_append[od], df[od, 'SE']], ignore_index=True)
        print("   For ",od," Earnings+Age file includes: ",len(df_append[od]),"Possible Jobs")
        # df_append[od] = df_append[od].append(df[od,'SI'])
        # As of pandas 2.0, append (previously deprecated) was removed, so using concat to do the same command.
        df_append[od] = pd.concat([df_append[od], df[od, 'SI']], ignore_index=True)
        print("   For ",od," SuperSector+Earnings+Age file includes: ",len(df_append[od]),"Possible Jobs")
        
        # Sort appended list
        df_append[od]  = df_append[od].sort_values(by=['jobidac','jobidac_counter'])
        
        # Reshape Data Long to Wide BY Segment Stem 
        df_append[od]['value'] = 1
        #widevars = ['seg_stem','jobidac_counter']
        geo_var = {'wac':'w_geocode', 'rac':'h_geocode'}
        #index_columns = [col for col in df_append if col not in widevars]
        possible_char_vars = ['Education', 'Ethnicity', 'IndustryCode', 'Race', 'Sex']
        char_vars = [col for col in df_append[od] if col in possible_char_vars]
        index_columns = ['jobidod','jobidac','jobidac_counter',geo_var[od],'jobtype','year','Age','Earnings','SuperSector'] + char_vars
        df_append_wide[od] = df_append[od].pivot_table(index=index_columns, 
                            columns='seg_stem', 
                            values='value',
                            aggfunc = 'count')

        df_append_wide[od].reset_index(inplace=True)
        
        ## Keep if Observation has all three Segements
        df_append_wide_keep[od] = df_append_wide[od].loc[(df_append_wide[od]['SA'] == 1) &
                    (df_append_wide[od]['SI'] == 1) &
                    (df_append_wide[od]['SE'] == 1) ]
        
        df_append_wide_keep[od].reset_index(inplace=True, drop = True)
        df_append_wide_keep[od].index.rename('index')
        print("Combining SuperSecore+Earnings+Age reduces possible joblists for",\
            od,"files to",len(df_append_wide_keep[od]),"Jobs")

    return df_append_wide_keep

def wac_rac_od_joblist(od_df, wac_df,rac_df, char_vars):
    """
    Function uses od, wac, and rac data frames to select jobs that match across
    the OD pairs.

    Function loops through each OD Pair to further reduce possible job type combinations

    od_df = reshapecascade_df['od','na'].copy()
    wac_df = df_append_wide_keep['wac'].copy()
    rac_df = df_append_wide_keep['rac'].copy()
    char_vars = ['Education', 'Ethnicity', 'IndustryCode', 'Race', 'Sex']
    """

    print("Checking jobs that appear in across OD pairs for WAC and RAC.")
    wac_rac = {} # start empty dictionary to collect dataframes


    # Itterate through each row of the origin-destination file
    for index, odpair in od_df.iterrows():
        #print(odpair['jobidod'])
        
        # what is the jobidod counter 
        # This will help determine how many job pairs need to be selected 
        jobidod_counter = odpair['jobidod_counter']
        # Select Observations from WAC and RAC to merge
        wac = wac_df.loc[(wac_df['jobidod'] == odpair['jobidod']) &
                        (wac_df['w_geocode'] == odpair['w_geocode'])].copy()
        rac = rac_df.loc[(rac_df['jobidod'] == odpair['jobidod']) &
                        (rac_df['h_geocode'] == odpair['h_geocode'])].copy()
        
        # Find char vars
        possible_char_vars = char_vars
        merge_char_vars = [col for col in wac if col in possible_char_vars]
        # merge WAC with RAC to find index
        wac_rac[index] = pd.merge(left = wac,
                        right = rac[['jobidod','jobidac','jobidac_counter',\
                            'h_geocode','year']+merge_char_vars],
                        left_on = ['jobidod','jobidac','jobidac_counter',\
                            'year']+merge_char_vars,
                        right_on = ['jobidod','jobidac','jobidac_counter',\
                            'year']+merge_char_vars,
                        how = "inner",
                        validate= "one_to_one")
        # Check to see if the merge worked - for RAC that are out of state the 
        # WAC_RAC file may not have any observations
        h_geocode_str = str(int(odpair['h_geocode'])).zfill(15)
        rac_stfips = h_geocode_str[0:2]
        w_geocode_str = str(int(odpair['w_geocode'])).zfill(15)
        wac_stfips = w_geocode_str[0:2]
        out_of_state_rac = wac_stfips != rac_stfips
        if (len(wac_rac[index]) == 0) & (out_of_state_rac):
            # if no data for wac_rac merge set wac_rac equal to the odpair
            # In these cases the wac_rac file will only have age, earnings and supersector data
            wac_rac[index] = od_df.loc[(od_df['jobidod'] == odpair['jobidod']) &
                        (od_df['h_geocode'] == odpair['h_geocode'])].copy()
        #print("For ",index, odpair['jobidod']," \
        # the list of wac-od-rac jobtypes inlcudes: ",len(wac_rac[index]))
        # Add job od id counter
        #print('setting jobidod counter to',jobidod_counter,'for pair',w_geocode_str,h_geocode_str,)
        wac_rac[index]['jobidod_counter'] = jobidod_counter

        
    # Stack wac + rac observations
    wac_rac_od = pd.concat(wac_rac.values(), ignore_index=True)

    # Keep only observations where the Area Characteristics counter matches the OD counter
    #wac_rac_od = wac_rac_od.loc[wac_rac_od['jobidac_counter'] == wac_rac_od['jobidod_counter']]

    # move geocodes to front of col list
    geo_vars = [col for col in wac_rac_od if col.endswith("geocode")]

    # Add total number of possible matches - within the jobid and jobid counter set
    wac_rac_od['jobidwacracod_counter'] = wac_rac_od.groupby(\
        ['jobidod','jobidod_counter']+geo_vars).cumcount()

    col_list = [col for col in wac_rac_od if col not in geo_vars + \
                               ['jobidwacracod_counter']]
    reorder_cols = geo_vars +  ['jobidwacracod_counter'] + col_list
    wac_rac_od = wac_rac_od[reorder_cols]

    print("Combining WAC and RAC files through OD Pairs \n", \
       "   reduces the total list of wac-od-rac jobtypes to: ",len(wac_rac_od))
    return wac_rac_od



# Function no longer needed
def save_stacked_file(df,
                programname,
                year: str, 
                od : str,
                state: str,
                county: str,
                seg_stem):
    """Stack a complete set of jobs by multiple characteristics

        
    Args:
        :param long_df: a dictionary of sataframes to stack
        :param long_var: dictionary with variable stem and newvar name 
            - stem = first letters that identify collection of variables to reshape
        :example long_var = 'CNS' jobs by 2 digit NAICS industry
                    - CA = Age
                    - CE = Earnings
                    - CNS = 2-digit NAICS industry
                    - CR = Race
                    - CT = Ethnicity
                    - CD = Educational Attainment
                    - CS = Sex
        :param seg_stem: dictionary wtih Segement Stem Characteristic  
    Returns:
        pandas dataframe: Stacked version of LODES data
        
    @author: Nathanael Rosenheim - nrosenheim@arch.tamu.edu
    @version: 2021-07-08
    """


    # add year variable
    df['year'] = year
    # Save Work at this point as CSV
    filename = f'{state}_{county}_{od}_{year}_{seg_stem}'
    savefile = sys.path[0]+"/"+programname+"/"+filename+".csv"
    print('\n The above files a have been combined into one file with fixed \n' + \
          '      mutually exclusive job types and segments by block. \n'+ \
          '      In the file named: '+ filename + '\n')
    df.to_csv(savefile)

    return df    

def add_latlon(df):
    """
    function adds lat and lon of block id to dataframe

    """

    # What geocode vars are in the dataframe
    geocode_vars = [col for col in df if col.endswith("geocode")]

    # loop with work through all states
    state_df = {}
    state_list = {}
    append_states = []

    # Add state list to work states
    df['w_geocode_str'] = df['w_geocode'].apply(lambda x : str(int(x)).zfill(15))
    df['w_stfips'] = df['w_geocode_str'].str[0:2]
    # Create lower case state abbrevation based on fips code
    df['w_stabbr'] = df['w_stfips'].apply(lambda x :  str.lower(us.states.lookup(x).abbr))
    work_state_list = df['w_stabbr'].unique().tolist()

    # Add state list to home states - home states can be outside of the community state
    df['h_geocode_str'] = df['h_geocode'].apply(lambda x : str(int(x)).zfill(15))
    df['h_stfips'] = df['h_geocode_str'].str[0:2]
    # Create lower case state abbrevation based on fips code
    df['h_stabbr'] = df['h_stfips'].apply(lambda x :  str.lower(us.states.lookup(x).abbr))

    home_state_list = df['h_stabbr'].unique().tolist()

    for work_state in work_state_list:
        xwalk_filename_workstate = f'{work_state}_xwalk'
        # Set file path where file will be downloaded
        datapath = "data_LODES"
        state_datapath = datapath+'/'+work_state
        xwalk_filepath_work = state_datapath+"/"+xwalk_filename_workstate+'.csv.gz'
        # Check if file exists - if not then download
        if not os.path.exists(xwalk_filepath_work):
            print("Geography crosswalk not downloaded - run Import LODES first")
        # Read in Crosswalk between blocks and County FIPS code
        xwalk_df_work = pd.read_csv(xwalk_filepath_work, usecols = ['tabblk2020','blklatdd','blklondd'])

        for home_state in home_state_list:
            print(home_state)
            # split up 
            state_df[home_state] = df.loc[df['h_stabbr'] == home_state]
            for geocode_var in geocode_vars:
                # Check if Geography Cross Walk has been downloaded
                # The geography cross walk cleans up minor issues with geocodes
                # download geography crosswalk
                xwalk_filename_homestate = f'{home_state}_xwalk'
                # Set file path where file will be downloaded
                datapath = "data_LODES"
                state_datapath = datapath+'/'+home_state
                xwalk_filepath_home = state_datapath+"/"+xwalk_filename_homestate+'.csv.gz'
                # Check if file exists - if not then download
                if not os.path.exists(xwalk_filepath_home):
                    print("Geography crosswalk not downloaded - run Import LODES first")

                # Read in Crosswalk between blocks and County FIPS code
                xwalk_df_home = pd.read_csv(xwalk_filepath_home, usecols = ['tabblk2020','blklatdd','blklondd'])

                # rename block lat lon to match geocode var
                if geocode_var == 'w_geocode':
                    suffix = "_w"
                    # select correct xwalk for work data
                    xwalk_df = xwalk_df_work
                elif geocode_var == 'h_geocode':
                    suffix = "_h"
                    # select correct xwalk for home data
                    xwalk_df = xwalk_df_home
                else:
                    print("error geocode var not in dataset")
                # Rename lat lon vars
                rename_dict = {}
                for var in ['tabblk2020','blklatdd','blklondd']:
                    rename_dict[var] = var+suffix
                
                print(rename_dict)
                xwalk_df = xwalk_df.rename(columns = rename_dict)
                
                state_df[home_state] = pd.merge(left = state_df[home_state],
                            right = xwalk_df,
                            left_on = geocode_var,
                            right_on = 'tabblk2020'+suffix,
                            how = "left")

        # Append states together
        #append_states = append_states.append(state_df[state])

    # Rebuild data frame with lat lon for all states
    latlon_df =  pd.concat(state_df.values(), ignore_index=True)
    
    return latlon_df

def distance(lat1, lat2, lon1, lon2):
    """
    LODES data uses great-circle distance to determine if geocodes
    are coaresend by Census Tract, PUMA, or Super-PUMA.

    Python 3 program to calculate Distance Between Two Points on Earth

    https://www.geeksforgeeks.org/program-distance-two-points-earth/

    example:
    lat1 = 53.32055555555556
    lat2 = 53.31861111111111
    lon1 = -1.7297222222222221
    lon2 =  -1.6997222222222223

    returns: 2.0043678382716137 K.M
    """
    from math import radians, cos, sin, asin, sqrt

    # The math module contains a function named
    # radians which converts from degrees to radians.
    lon1 = radians(lon1)
    lon2 = radians(lon2)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
      
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
 
    c = 2 * asin(sqrt(a))
    
    # Radius of earth in kilometers. Use 3956 for miles
    r = 6371
      
    # calculate the result
    return(c * r)

def add_distance(df, lat1, lat2, lon1, lon2):
    """
    Function applies distance function to OD pairs to add od distance in KM
    for output of add_latlon function use the following list of lat1 lat2  lon1 lon2
    'blklatdd_w', 'blklatdd_h', 'blklondd_w', 'blklondd_h'

    units = km
    """
    df['od_distance'] = df.apply(lambda x: distance(x[lat1], x[lat2], x[lon1], x[lon2]), axis = 1)

    return df


# create wkt line bewteen two points
def wkt_line(df,x1,y1,x2,y2):
    """
    Geopandas uses Well-known text (WKT) to represent vector geometry
    Function returns geopandas based on WKT line geometry
    This helps to visualize the origin-destination LODES data

    Note: Longitude is X axis and Latitude is Y axis
    
    """
    
    df['linestring'] = df.apply(lambda x: 'LINESTRING ( ' +
                                str(x[x1]) + ' ' +
                                str(x[y1]) + ', ' +
                                str(x[x2]) + ' ' +
                                str(x[y2]) + ')', axis = 1)
    
    # Convert WKT column to geoseries
    #from shapely import wkt
    #df['geometry'] = gpd.GeoSeries.from_wkt(df['linestring'])
    #gdf = gpd.GeoDataFrame(df, geometry='geometry')
    
    return df

def add_stfips(df,geocodevar):
    
    df[geocodevar+'_str'] = df[geocodevar].apply(lambda x : str(int(x)).zfill(15))
    df[geocodevar+'_stfips'] = df[geocodevar+'_str'].str[0:2]
    
    return df

def add_tractid(df,geocodevar):
    
    df[geocodevar+'_str'] = df[geocodevar].apply(lambda x : str(int(x)).zfill(15))
    df[geocodevar+'_tractid'] = df[geocodevar+'_str'].str[0:11]
    
    return df

def add_countyid(df,geocodevar):
    
    df[geocodevar+'_str'] = df[geocodevar].apply(lambda x : str(int(x)).zfill(15))
    df[geocodevar+'_countyid'] = df[geocodevar+'_str'].str[0:5]
    
    return df
    
def add_coarse_geovar(df):
    """
    LODES data coarsens the geocoding of the home block based on distance between origin and desination pairs
    if distance is greater than the average use PUMA (County)
    if distnace is greater than 500 miles use 
    """
    
    col_list = [col for col in df]
    # Check if dataframe has tract and county variables for home geocodes
    if 'w_geocode_tractid' not in col_list:
        df = add_tractid(df, 'w_geocode')
    if 'w_geocode_countyid' not in col_list:
        df = add_countyid(df, 'w_geocode')
    if 'w_geocode_stfips' not in col_list:
        df = add_stfips(df, 'w_geocode')

    if 'h_geocode_tractid' not in col_list:
        df = add_tractid(df, 'h_geocode')
    if 'h_geocode_countyid' not in col_list:
        df = add_countyid(df, 'h_geocode')
    if 'h_geocode_stfips' not in col_list:
        df = add_stfips(df, 'h_geocode')
    
    # Check if dataframe has od_distance
    if 'od_distance' not in col_list:
        if 'blklatdd_w' not in col_list:
            df = add_latlon(df)
        
        df = add_distance(df, 'blklatdd_w', 'blklatdd_h', 'blklondd_w', 'blklondd_h' )
    
    mean_dist = df['od_distance'].mean()
    print("The average commute distance for data is ",mean_dist)
    # Add coarse_geovar
    # All jobs are coarsened to tractid for home origin
    df.loc[:,'h_geocode_coarse'] = df['h_geocode_tractid']
    
    # locate observations with longer than average commutes
    # Coarse Geovar should be the PUMA with population of 100,000
    df.loc[(df['od_distance'] > mean_dist), 'h_geocode_coarse'] = df['h_geocode_countyid']
    
    # Locate observations with commutes longer than 500 km
    df.loc[(df['od_distance'] > 500), 'h_geocode_coarse'] = df['h_geocode_stfips']
    
    return df  

def add_jobidod_pair(expected_counts_df,
                    possible_pairs,
                    block_check,
                    all_segstems = all_segstems):
    """
    The Oringin Destination possible job list will include impossible
    pairs. Impossible means that if the pair is selected the 
    fitness will never reduce due to double counting workers
    with specific characteristics.
    This routine should work for pairs that have either 1 or 2 total jobs.
    Not sure if it works for pairs with 3 or more jobs.
    """

    condition_df = possible_pairs.copy()
    condition = (condition_df['h_geocode'] == block_check)
    reduce_od_pairs = condition_df.loc[condition].copy()

    # Get expected counts for od block pair
    expected_job_count_odpair_df = \
        explorebyblock(expected_counts_df,'h_geocode',[block_check])

    # Need to run loop by jobtype
    jobtype_list = reduce_od_pairs['jobtype'].unique().tolist() 

    # This loop will add a jobodid pair 
    # The pair is a string that has 2 jobodids - this helps to identify 
    # which observations go together - choose 1 and the other needs to be chosen
    # Start with empty variable
    reduce_od_pairs['jobidod_pair'] = ''

    reduce_od_pairs_jobtype_df = {}
    for jobtype in jobtype_list:
        #print(jobtype)
        expected_job_count_odpair_jobtype_df = \
            expected_job_count_odpair_df.loc[\
                expected_job_count_odpair_df['jobtype']==jobtype].copy()

        # Look at one jobtype at a time
        reduce_od_pairs_jobtype_df[jobtype] = \
            reduce_od_pairs.loc[\
                reduce_od_pairs['jobtype']==jobtype].copy()
        
        # The length should be 1
        length_of_od_pair = len(expected_job_count_odpair_jobtype_df)
            # store empty options list
        # Final options list will look something like
        # [[1, 3], [2, 3], [2, 3]] age options 1 & 3, earnings optinos 2&3 etc
        # Process only works for S000 = 2
        expected_S000 = expected_job_count_odpair_jobtype_df['S000'].squeeze()
        df_S000 = reduce_od_pairs_jobtype_df[jobtype]['S000'].mean()
        if expected_S000 != df_S000:
            print("Error check",length_of_od_pair,block_check,expected_S000,df_S000)
        if expected_S000 == 2:
            option_list = {}
            for segment in ['Age','Earnings','SuperSector']:
                seg_stem = all_segstems[segment]
                value_list = expected_job_count_odpair_jobtype_df[\
                    [seg_stem+'01',seg_stem+'02',seg_stem+'03']].values.tolist()
                option_list[segment] = []
                for index_value in range(len(value_list[0])):
                    #print(index_value,value_list[0][index_value])
                    if value_list[0][index_value] != 0:
                        option_list[segment].append(index_value +1)

            # Combine options to make a list of all possible options
            all_options = [option_list['Age'],\
                        option_list['Earnings'],\
                        option_list['SuperSector']]
            #print("List of all options all_options",option_list)
            #print("List of all options all_options",all_options)
            all_options_list = list(itertools.product(*all_options))
            
            # How many options are there?
            length_options = len(all_options_list)

            # Loop over options ticheck if pair exists
            reduce_od_pairs_jobtype_df[jobtype].loc[:,'no pair'] = 0
            for option1 in range(0,length_options):
                # match options - first goes with last and so on
                option1_list = all_options_list[option1]
                option2_list = all_options_list[length_options-option1-1]
                option1str = ''.join(str(option) for option in option1_list)
                option2str = ''.join(str(option) for option in option2_list)
                jobidod1 = 'jidod'+jobtype+option1str
                jobidod2 = 'jidod'+jobtype+option2str
                #print(jobidod1,"pairs with",jobidod2)

                # Check if pair exists in job list
                check_jobidod1 = (\
                    reduce_od_pairs_jobtype_df[jobtype]['jobidod']==jobidod1)
                check_jobidod2 = (\
                    reduce_od_pairs_jobtype_df[jobtype]['jobidod']==jobidod2)
                conditions= check_jobidod1 | check_jobidod2
                check_pair = reduce_od_pairs_jobtype_df[jobtype].loc[conditions]
                check_pair_jobids = check_pair['jobidod'].unique().tolist() 
                #print("Jobidod list",check_pair_jobids)
                if jobidod2 not in check_pair_jobids:
                    reduce_od_pairs_jobtype_df[jobtype].loc[conditions,'no pair'] = 1
                    reduce_od_pairs_jobtype_df[jobtype].loc[conditions,'jobidod_pair'] = 'no pair'
                elif jobidod2 in check_pair_jobids:
                    # set jobidod_pair - this will be used for the job select sort
                    pair_not_set = (reduce_od_pairs_jobtype_df[jobtype]['jobidod_pair'] == '')
                    reduce_od_pairs_jobtype_df[jobtype].loc[conditions & 
                        pair_not_set,'jobidod_pair'] = jobidod1+jobidod2
                    # Update the jobidod_counter
                    # Need to update the counter to select 1 part of pair 
                    # first option = 1 and second option = 2
                    print("Update Job ID OD Counter")
                    reduce_od_pairs_jobtype_df[jobtype].loc[check_jobidod1,\
                        'jobidod_counter'] = 1
                    reduce_od_pairs_jobtype_df[jobtype].loc[check_jobidod2,\
                        'jobidod_counter'] = 2

            # How much did the process reduce the pair?
            length_possible_pairs = len(reduce_od_pairs_jobtype_df[jobtype])

            # Return reduce pairs that have a possible pair.
            reduce_od_pairs_jobtype_df[jobtype] = reduce_od_pairs_jobtype_df[jobtype].\
                loc[reduce_od_pairs_jobtype_df[jobtype]['jobidod_pair'] != 'no pair']
            length_reduce_od_pairs = len(reduce_od_pairs_jobtype_df[jobtype])
            print("Possible job list reduced from",\
            length_possible_pairs,"to",length_reduce_od_pairs)
        elif expected_S000 == 1:
            reduce_od_pairs_jobtype_df[jobtype].loc[:,'no pair'] = 0
            reduce_od_pairs_jobtype_df[jobtype].loc[:,'jobidod_pair'] = 'S000=1'
        elif expected_S000 > 2:
            reduce_od_pairs_jobtype_df[jobtype].loc[:,'no pair'] = 0
            reduce_od_pairs_jobtype_df[jobtype].loc[:,'jobidod_pair'] = 'S000>2'

    # Recombine the data for jobtypes
    reduce_od_pairs_output = \
        pd.concat(reduce_od_pairs_jobtype_df.values(), ignore_index=True)
    
    return reduce_od_pairs_output

def add_primarykey(input_df, primary_key_name, uniqueid_part1):
    """
    Primary key needs to be unique and non-missing
    """
    
    print("Checking primary key name",primary_key_name)
    
    # Create copy of input dataframe to prevent chaining conflict
    add_uniqueid_df = input_df.copy()
    
    # Create column list to move primarykey to first column
    columnlist = [col for col in add_uniqueid_df if col != primary_key_name]
    new_columnlist = [primary_key_name]+ columnlist

    # Check if primary key it already exists
    # initially set primary_key_error to 0
    primary_key_error = 0
    if primary_key_name in list(add_uniqueid_df.columns):
        # Check if values are unique
        primary_key_error = primary_key_error_check(add_uniqueid_df,primary_key_name)
        if primary_key_error == 0:
            return add_uniqueid_df[new_columnlist]

    if (primary_key_error >= 1) or (primary_key_name not in list(add_uniqueid_df.columns)):
        # Part2 of unique id is a counter based on the part1
        # The counter ensures that values are unique within part1
        # Add unique ID based on group vars
        add_uniqueid_df['unique_part2'] = \
            add_uniqueid_df.groupby([uniqueid_part1]).cumcount() + 1
        # Need to zero pad part2 find the max number of characters
        part2_max = add_uniqueid_df['unique_part2'].max()
        part2_maxdigits = len(str(part2_max))
        add_uniqueid_df[primary_key_name] = add_uniqueid_df[uniqueid_part1] + \
            add_uniqueid_df['unique_part2'].apply(lambda x : str(int(x)).zfill(part2_maxdigits))
        # Check if values are unique
        error = primary_key_error_check(add_uniqueid_df,primary_key_name)
    
        if error == 0:
            return add_uniqueid_df[new_columnlist]
        if error > 0:
            print("Warning: Primary key is not set correctly")
            return add_uniqueid_df[new_columnlist]


def primary_key_error_check(add_uniqueid_df,primary_key_name):
    if add_uniqueid_df[primary_key_name].is_unique:
        print("Primary key variable",primary_key_name,"is unique.")
        error = 0
    else:
        print("Warning: Primary key variable",primary_key_name,"is not unique.")
        error = 1
    # Check if uniqueid is not missing
    missing_uniqueid = add_uniqueid_df.\
        loc[add_uniqueid_df[primary_key_name].isnull()].copy()
    length_missing_uniqueid = missing_uniqueid.shape[0]
    if length_missing_uniqueid == 0:
        print("Primary key",primary_key_name,"has no missing values")
    if length_missing_uniqueid > 0:
        print("Warning: Primary key",primary_key_name,"has missing values")
        error = error + 1

    return error

## functiona that have been added or significantly revised by @Amin

def special_merge(df_left, df_right, stem, by, key="merge_key"):
    """ 
    This function provides a way to merge two DataFrames with the option to flag the first occurrences
    of merge keys in the merged DataFrame and set the values of unflagged rows in the right DataFrame to zero. 
    """
    if key not in df_right.columns:
        df_right['merge_key'] = df_right[by].apply(lambda row: ''.join(row.astype(str)), axis=1)
    right_columns_to_keep = [col for col in df_right.columns if col not in df_left.columns]
    # identify the first ocuurance of each key
    first_occurrences = df_left.duplicated(key, keep='first')
    df_left['is_first_occurrence'] = ~first_occurrences
    df_merged = pd.merge(df_left, df_right[right_columns_to_keep + [key]], on=key, how='left')
    #sets the values of columns in df_merged with the same lables of df_right to 0 for rows where is_first_occurrence is False
    df_merged.loc[df_merged['is_first_occurrence'] == False, right_columns_to_keep] = 0
    df_merged.drop('is_first_occurrence', axis=1, inplace=True)
    df_merged.drop([col for col in df_merged.columns if col.startswith(stem)], axis=1, inplace= True)
    return df_merged


def create_specific_jobcount_dict(df, reshape_vars):
    """ 
    create a nested dictionary based on the wide df.
    The letters and digits will be the keys and the summation of the columns will be the values.
    The first set of keys in the nested dictionary are exactly the keys of the reshape_vars dictionary 
    and the second keys are two-digit numbers.
    We exclude columns that do not start with the keys of the reshape_vars dictionary.
    We'll also skip columns with None values as they are non-numeric.
    """
    # Start an empty dictionary to save the data
    nested_dict = {}

    # Calculate the sum of each column of the input dataframe if it's numeric and create a dictionary
    column_sums = {col: df[col].sum() if pd.api.types.is_numeric_dtype(df[col]) else None for col in df.columns}

    # Filter out the columns that start with the keys of reshape_vars
    filtered_column_sums = {col: sum_value for col, sum_value in column_sums.items() 
                        if any(col.startswith(key) for key in reshape_vars.keys()) and sum_value is not None}
    
    for col, sum_value in filtered_column_sums.items():
        for key, new_key in reshape_vars.items():
            if col.startswith(key):
                if new_key not in nested_dict:
                    nested_dict[new_key] = {}
                # Extracting two-digit number following the key in the column name
                two_digit_number = col[len(key):len(key)+2]
                nested_dict[new_key][two_digit_number] = sum_value
                
    return nested_dict


def format_dict_keys(input_dict):
    """
    This function takes a dictionary as input and ensures its keys are two-character strings.
    If a key is a single-digit integer, it adds a leading zero and converts it to a string.
    This function is used to change the format of the keys of the temp dictionary before comparison
    """
    
    formatted_dict = {}
    for key, value in input_dict.items():
        if isinstance(key, int) and key < 10:
            formatted_key = f"0{key}"
        else:
            formatted_key = str(key)
        formatted_dict[formatted_key] = value
    return formatted_dict


def compare_dictionaries(temp_dict, comparison_dict):
    """
    This function takes two dictionaries as input and compares their contents to find differences.
    """
    
    differences = []

    for key in temp_dict:
        if key in comparison_dict:
            for sub_key in temp_dict[key]:
                str_sub_key = str(sub_key).zfill(2)  # Convert to string with leading zeros
                if str_sub_key in comparison_dict[key]:
                    if temp_dict[key][sub_key] != comparison_dict[key][str_sub_key]:
                        diff = (key, str_sub_key, temp_dict[key][sub_key], comparison_dict[key][str_sub_key])
                        differences.append(diff)
                else:
                    diff = (key, str_sub_key, temp_dict[key][sub_key], f"{str_sub_key} not in the refrence dictionary")
                    differences.append(diff)

            for sub_keys in comparison_dict[key]:
                if sub_keys not in temp_dict[key]:
                    if comparison_dict[key][sub_keys] != 0:
                        diff = (key, sub_keys, f"{sub_keys} not in the temporary dictionary", comparison_dict[key][sub_keys])
                        differences.append(diff)

    if differences:
        print("Errors in job counts...... values of mutual keys do not match")
        return differences
    else:
        return "All mutual keys match"


def expand_rows(df, expand_var_col_name):
    """
    This function expands a dataframe based on the values of the specified column to ensure that the value of each row
    in that column is exactly one.
    """
    # Create an empty DataFrame to hold the expanded rows
    expanded_df = pd.DataFrame(columns=df.columns)

    for _, row in df.iterrows():
        expand_var = int(row[expand_var_col_name])
        if expand_var > 1:
            # Create additional rows with expand_var set to 1
            additional_rows = pd.DataFrame([row] * (expand_var))
            additional_rows[expand_var_col_name] = 1
            expanded_df = pd.concat([expanded_df, additional_rows], ignore_index=True)
        else:
            # Append the row as it is if expand_var is 1
            expanded_df = pd.concat([expanded_df, pd.DataFrame([row])], ignore_index=True)

    return expanded_df


def check_jobcount(df, df_original, reshape_vars):
    """
    check the job counts in the data rame resulting from reshaping the LODES file and compare them 
    with those in the original LODES file
    """
    df = expand_rows(df, 'jobcount')
    temp_dict = {col: {} for col in list(reshape_vars.values())}

    for col in list(reshape_vars.values()):
        temp_dict[col] = df.groupby(col)['jobcount'].sum().astype(int).to_dict()
    temp_dict = {col: format_dict_keys(col_dict) for col, col_dict in temp_dict.items()}
    comparison_dict = create_specific_jobcount_dict(df_original, reshape_vars)
    comparision_results = compare_dictionaries(temp_dict, comparison_dict)
    return comparision_results


def process_extra_required(df1, df2, key,seed_value=None):

    if seed_value is not None:
        np.random.seed(seed_value)
    
    for _, row in df1[df1[key] == 1].iterrows():
        factor = row['factor']
        group = row['group']
        
        if key == "extra":
            try:
                # Convert group to float for comparison, assuming df2's corresponding values are numeric
                group_value = int(float(group))
                matching_rows = df2[df2[factor] == group_value].index.tolist()
                if matching_rows:
                    # Randomly select one and set to NaN
                    random_row = np.random.choice(matching_rows, 1)[0]
                    df2.at[random_row, factor] = np.nan
            except ValueError as e:
                print(f"Error converting group value to float: {group} - {str(e)}")
        
        elif key == 'required':
            try:
                group_value = int(float(group))
                nan_rows = df2[df2[factor].isna()].index.tolist()
                if nan_rows:
                    # Randomly select one and set to the group value
                    random_row = np.random.choice(nan_rows, 1)[0]
                    df2.at[random_row, factor] = group_value
                else:
                    raise ValueError("The fix error function should be revised")
            except ValueError as e:
                print(f"Error converting group value to int: {group} - {str(e)}")
    
    return df2


def fix_jobcount_errors(df, comparision_results, df_original, reshape_vars):
    df = expand_rows(df, 'jobcount')
    comparision_results_df = pd.DataFrame(comparision_results, columns=['factor','group','output','original'])
    # print(f"Following differences found: \n{comparision_results_df}")
    comparision_results_df['output'] = pd.to_numeric(comparision_results_df['output'], errors='coerce').fillna(0).astype(int)
    comparision_results_df['original'] = pd.to_numeric(comparision_results_df['original'], errors='coerce').fillna(0).astype(int)
    comparision_results_df['extra'] = 0
    comparision_results_df['required'] = 0

    condition1 = comparision_results_df['output'] > comparision_results_df['original']
    condition2 = comparision_results_df['output'] < comparision_results_df['original']

    comparision_results_df.loc[condition1, 'extra'] = comparision_results_df['output'] - comparision_results_df['original']
    comparision_results_df.loc[condition2, 'required'] = comparision_results_df['original'] - comparision_results_df['output']

    comparision_results_df = expand_rows(comparision_results_df, 'extra')
    comparision_results_df = expand_rows(comparision_results_df, 'required')
    comparision_results_df.drop(columns=['output','original'], inplace=True)

    df_out_modified = process_extra_required(comparision_results_df, df, key='extra')
    df_out_modified = process_extra_required(comparision_results_df, df_out_modified, key='required')


    return df_out_modified
        

def reshapelodes_revised(wide_df: pd.DataFrame,
                         df_original,
                         newvar,
                         stem,
                         seed_value,
                         by = ''):
    """Using the mutually exculive job types and segement 
    transpose data and stack observations. This will result in a 
    long data file that has job counts by segement and 
    two-digit naics codes within each block.

        
    Args:
        :param wide_df: dataframe to reshape
        :param df_original: a copy of the unshaped dataframe
        :param seg: Segment of workforce
            [segpart] = Segment of the workforce
        :type segpart: String
        :param mxjobtype: Mututally Exclusive Job Type
        :type jobtype: String 
        :param long_var: dictionary with variable stem and newvar name 
            - stem = first letters that identify collection of variables to reshape
        :example long_var = 'CNS' jobs by 2 digit NAICS industry
        :param seg_char: dictionary wtih Segement Stem Characteristic
        

    
    Returns:
        pandas dataframe: Long version of LODES data
        
    @author: Nathanael Rosenheim - nrosenheim@arch.tamu.edu
    @author: Amin Enderami - a.enderami@ku.edu
    @version: 2024-03-01
    """
    
    if by == '':
        # Identify by variable(s) - for wac = ['w_geocode'],
        #                                  rac = ['h_geocode'],
        #                                   od = ['w_geocode','h_geocode']
        by = [col for col in wide_df if col.endswith("geocode")]
        
    if "merge_key" not in wide_df.columns:
        wide_df['merge_key'] = wide_df[by].apply(lambda row: ''.join(row.astype(str)), axis=1)

    # Check if new characteristic is already a column in the dataframe
    # For WAC and RAC group files Earnings, Age, and SuperSector will be fixed
    if newvar in [col for col in wide_df]:
        # If new variable already exists then do not reshape data
        df_reshaped = wide_df.copy()
        df_reshaped.drop([col for col in df_reshaped.columns if col.startswith(stem)], axis=1, inplace= True)
        return df_reshaped 

    columns_to_keep = [col for col in df_original.columns if col.startswith(stem)]
    df_for_reshaping = wide_df[by+ columns_to_keep + ['merge_key' , 'jobcount']]

    # removing any possible duplicate columns from the df_for_reshaping
    df_for_reshaping = df_for_reshaping.loc[:, ~df_for_reshaping.columns.duplicated()]

    # flag the rows where jobcount is one
    df_for_reshaping.loc[df_for_reshaping ['jobcount'] == 1, 'flag'] = True

    df_reshaped = pd.wide_to_long(df_for_reshaping, stem, i=by, j=newvar)
    df_reshaped.reset_index(inplace=True) # shift dataframe multiindex to columns
    df_reshaped['jobcount'] =  df_reshaped[stem].copy() # reset job count
    df_reshaped = df_reshaped.sort_values(by=by + [newvar])

    # Drop observations with no job count if not falagged
    condition1 = (df_reshaped['jobcount'] != 0)
    condition2 = (df_reshaped['flag'] == True)
    df_reshaped = df_reshaped.loc[(condition1) | (condition2)]

    df_reshaped_merged = special_merge(df_reshaped, df_original, stem, by=by, key='merge_key')

    df_temp = pd.DataFrame()
    df_temp = df_reshaped_merged.loc[df_reshaped_merged['flag'] == True].copy()

    if not df_temp.empty:
        
        for index, row in df_temp.iterrows():
            np.random.seed(seed_value + index)
            
            if row['jobcount'] > 1:
                job_count = row['jobcount']
                stem_filter_value = row[newvar]
                same_stem_filter_value_rows = df_temp[df_temp[newvar] == stem_filter_value]

                # Select only rows with jobcount equal to zero for sampling
                zero_jobcount_rows_with_same_stem_filter = same_stem_filter_value_rows[same_stem_filter_value_rows['jobcount'] == 0]
                if not zero_jobcount_rows_with_same_stem_filter.empty:  # Ensure there are rows to sample
                    sampled_job_count = min(int(job_count-1), len(zero_jobcount_rows_with_same_stem_filter))
                    sampled_rows = zero_jobcount_rows_with_same_stem_filter.sample(n=sampled_job_count, replace=False) # Randomly select counter number of rows from those with jobcount zero
                    # Update jobcount for the orginal row and randomly sampled rows
                    if int(job_count-1) > len(zero_jobcount_rows_with_same_stem_filter):
                        df_temp.at[index, 'jobcount'] = int(job_count) - len(zero_jobcount_rows_with_same_stem_filter)
                    else:
                        df_temp.at[index, 'jobcount'] = 1
                    df_temp.loc[sampled_rows.index, 'jobcount'] = 1
    
        
        df_reshaped_merged.loc[df_temp.index] = df_temp

    # Drop observations with no job count
    condition3 = (df_reshaped_merged['C000'] != 0)
    condition4 = (df_reshaped_merged['jobcount'] != 0)
    df_reshaped_merged = df_reshaped_merged.loc[(condition3) | (condition4)]
    df_reshaped_merged = df_reshaped_merged.drop(columns=['flag'])

    
    # Return data with job count
    return df_reshaped_merged


def reshapecascade_revised(df,reshape_vars, seed_value=None):
    """
    Function loops through characteristic variables and reshapes dataframe 
    in a loop creating a casacade of possible job combinations.
    
    """
    
    # Reshape requires each obseravtion to be uniquely identified 
    # The initial by variables include the geocode and the jobtype
    # Each loop will add a new variable to the by list for the next reshape
    geo_var_by = [col for col in df if col.endswith("geocode")]
    fixed_var_by = [col for col in df if col in ['Earnings','Age','SuperSector']]
    original_col_list = geo_var_by + fixed_var_by + ['jobtype','seg_stem','year']
    
    # start loop by setting the by vars to the original column list
    by = original_col_list
    # set seed for reproducibility
    if seed_value is not None:
        np.random.seed(seed_value)

    # Check if the column is of float type and convert it
    for column in df.columns:
        if df[column].dtype == 'float64':
            df[column] = df[column].astype('Int64')

    # creat an unshaped copy of the input df as a reference
    df_original = df.copy()
    # Add missing education category before Age Reshape
    df_original = add_missingeducation(df_original)
    
    for reshape_var in reshape_vars:
        # Make a dictionary for long reshape 
        newcharacteristic =  reshape_vars[reshape_var]
        chacteristic_stem =  reshape_var
        
        #reshape dataframe
        df = reshapelodes_revised(wide_df = df,
                                  df_original = df_original,
                                  newvar = newcharacteristic,
                                  stem = chacteristic_stem,
                                  seed_value = seed_value,
                                  by = by)
        
        if newcharacteristic in original_col_list:
            by = by
        else:
            by = by + [newcharacteristic]
        seed_value += 1

    df = df[by + ['jobcount']]
    
    comparision_results = check_jobcount(df, df_original, reshape_vars)

    max_iterations = 11
    iteration = 1
    while comparision_results != "All mutual keys match" and iteration < max_iterations:
        print(f"fixing the erros ..... iteration {iteration}")
        try:
            df = fix_jobcount_errors(df, comparision_results, df_original, reshape_vars)
        except Exception as e:
            print("An error occurred while fixing errors:", e)
            break

        comparision_results = check_jobcount(df, df_original, reshape_vars)
        iteration += 1

    if comparision_results == "All mutual keys match":
        print("\n"
              "All errors are fixed \n", \
              f"all mutual keys match after {iteration-1} iterations.")
    else:
        print("Reached maximum iterations or an error occurred during fixing errors \n", \
              f"Couldn't fix the errors after {iteration-1} time iteration \n", \
               "job counts still does not match")
    
    # drop the observations with zero job counts that may result after fixing the errors         
    df = df.loc[df['jobcount'] != 0]
    
    # Add Super Sector
    col_list = [col for col in df]
    if ('SuperSector' not in col_list) & ('IndustryCode' in col_list):
        df = add_supersector(df)
        by = by + ['SuperSector']

    return df[by + ['jobcount']]


def get_county_home_block_list(stacked_df , county_fips_code, year, od='od', seg='na'):
    stfips = county_fips_code[0:2]
    stabbr = str.lower(us.states.lookup(stfips).abbr)
    df = stacked_df[stabbr,county_fips_code,od,year,seg].copy(deep = True)
    df['h_geocode'] = df['h_geocode'].astype(str)
    df['countyfips'] = df['h_geocode'].str[0:5]
    
    
    home_block_list_df = df[df['countyfips'] == county_fips_code]
    home_block_list = list(home_block_list_df['h_geocode'].unique().astype('int64'))

    return home_block_list

def get_county_work_block_list(stacked_df , county_fips_code,year, od='od', seg='na'):
    stfips = county_fips_code[0:2]
    stabbr = str.lower(us.states.lookup(stfips).abbr)
    df = stacked_df[stabbr,county_fips_code,od,year,seg].copy(deep = True)
    df['w_geocode'] = df['w_geocode'].astype(str)
    df['countyfips'] = df['w_geocode'].str[0:5]
      
    work_block_list_df = df[df['countyfips'] == county_fips_code]
    work_block_list = list(work_block_list_df['w_geocode'].unique().astype('int64'))

    return work_block_list

def combine_wac_rac_joblist(df, od, seed_value=None):
    """
    Function takes list of jobs in either WAC or RAC files and 
    reduces the joblist based on observations that appear in all three
    segment files. 

    df is a dictionary of dataframes created by reshapecascade_revised with jobidac
    """
    # Create dictionary with reshaped data
    df_append = {} # start empty dictionary to collect dataframes
    df_append_wide = {} # start empty dictionary to collect dataframes
    df_append_wide_keep = {} # start empty dictionary to collect dataframes

    if seed_value is not None:
        np.random.seed(seed_value)

    # Using Area Characteristics Data Reduce number of possible job types
    print(f"Checking jobs that appear in all three segment files for {od} Files.")
    if (od, 'SE') in df:
        df_append[od] = df[od, 'SA'].copy(deep=True)
        print(f"   For {od} Age file includes: ", len(df_append[od]), "Possible Jobs")
    else:
        print(f"   Warning: {od}, 'SA' data is not available.")
    if (od, 'SE') in df:
        df_append[od] = pd.concat([df_append[od], df[od, 'SE']], ignore_index=True)
        print(f"   For {od} Earnings+Age file includes: ", len(df_append[od]), "Possible Jobs")
    else:
        print(f"   Warning: {od}, 'SE' data is not available.")
    if (od, 'SI') in df:
        df_append[od] = pd.concat([df_append[od], df[od, 'SI']], ignore_index=True)
        print(f"   For {od} SuperSector+Earnings+Age file includes: ", len(df_append[od]), "Possible Jobs")
    else:
        print(f"   Warning: {od}, 'SI' data is not available.")

    
    # Sort appended list
    df_append[od]  = df_append[od].sort_values(by=['jobidac','jobidac_counter'])
    
    # Reshape Data Long to Wide BY Segment Stem 
    df_append[od]['value'] = 1
    geo_var = {'wac':'w_geocode', 'rac':'h_geocode'}
    possible_char_vars = ['Education', 'Ethnicity', 'IndustryCode', 'Race', 'Sex']
    char_vars = [col for col in df_append[od] if col in possible_char_vars]
    index_columns = ['jobidod','jobidac','jobidac_counter',geo_var[od],'jobtype','year','Age','Earnings','SuperSector'] + char_vars
    df_append_wide[od] = df_append[od].pivot_table(index=index_columns, 
                        columns='seg_stem', 
                        values='value',
                        aggfunc = 'count')

    df_append_wide[od].reset_index(inplace=True)
    for seg in ['SA', 'SE', 'SI']:
        if seg in df_append[od].keys():
            df_append_wide[od][seg] = pd.to_numeric(df_append_wide[od][seg], errors='coerce').fillna(0).astype(int)
    # df_append_wide[od]['SE'] = pd.to_numeric(df_append_wide[od]['SE'], errors='coerce').fillna(0).astype(int)
    # df_append_wide[od]['SI'] = pd.to_numeric(df_append_wide[od]['SI'], errors='coerce').fillna(0).astype(int)
    values_list = []
    for seg in ['SA', 'SE', 'SI']:
        if seg in df_append_wide[od]:
            values_list.append(df_append_wide[od][seg])
    if values_list:
        prob_value = pd.concat(values_list, axis=1).mean(axis=1)
        df_append_wide[od]['prob'] = prob_value
    # df_append_wide[od]['prob'] = df_append_wide[od].apply(lambda row: (row['SA']+row['SE']+ row['SA'])/ 3, axis = 1)
    
    ## Keep if the observation exists in all three segements
    df_append_wide[od] = df_append_wide[od].sort_values(by=['prob'])
    df_append_wide_keep[od] = df_append_wide[od].loc[(df_append_wide[od]['prob'] == 1)]
    print(f"{len(df_append_wide_keep[od])} of possible jobs exist in all three segments")

    number_of_required_samples = int(len(df[od,'SA']) - len(df_append_wide_keep[od]))

    ## If more jobs is needed, keep observations based on their freuency in different segments  
    if number_of_required_samples > 0:
        print(f"processing to randomly select {number_of_required_samples} jobs from \n"
                "the list of possible jobs while prioritizing the jobs existing in multiple segments")
        additional_row = df_append_wide[od].loc[(df_append_wide[od]['prob'] != 1)].sample(n=number_of_required_samples,
                                                                                            weights = 'prob',
                                                                                            replace = False
                                                                                            )
        df_append_wide_keep[od] = pd.concat([df_append_wide_keep[od], additional_row], ignore_index=True)
    
    df_append_wide_keep[od].reset_index(inplace=True, drop = True)
    df_append_wide_keep[od].index.rename('index')
    df_append_wide_keep[od].drop(columns=['jobidac_counter', 'prob'], inplace=True)
    print("Combining SuperSecore+Earnings+Age reduces possible joblists for",\
        od,"files to",len(df_append_wide_keep[od]),"Jobs")

    return df_append_wide_keep