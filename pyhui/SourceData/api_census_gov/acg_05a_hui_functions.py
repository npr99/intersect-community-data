# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import numpy as np
import pandas as pd
import os # For saving output to path
import sys

# For printing conda list in log file
from IPython import get_ipython
ipython = get_ipython()
# looks like log overlaps in the output of conda list
# try pausing for a second
import time

# Save output as a log file function
from pyhui \
     import ICD_00c_save_output_log as logfile
     
# Load in data structure dictionaries
from pyhui.SourceData.api_census_gov.acg_00a_general_datastructures import *
from pyhui.SourceData.api_census_gov.acg_00b_hui_block2010 import *
from pyhui.SourceData.api_census_gov.acg_00c_hispan_block2010 import *
from pyhui.SourceData.api_census_gov.acg_00d_hhinc_ACS5yr2012 import *
from pyhui.SourceData.api_census_gov.acg_00e_incore_huiv2 \
    import incore_v2_DataStructure

# open, read, and execute python program with reusable commands
from pyhui.SourceData.api_census_gov.acg_01a_BaseInventory import BaseInventory
from pyhui.SourceData.api_census_gov.acg_02a_add_categorical_char \
     import add_new_char_by_random_merge_2dfs


class hui_workflow_functions():
    """
    Function runs full process for generating the housing unit inventories
    Process runs for 1 county.
    """

    def __init__(self,
            state_county: str,
            state_county_name: str,
            seed: int = 9876,
            version: str = '2.0.0',
            version_text: str = 'v2-0-0',
            basevintage: str = 2010,
            outputfolder: str ="",
            outputfolders = {},
            savefiles: bool = True):

        self.state_county = state_county
        self.state_county_name = state_county_name
        self.seed = seed
        self.version = version
        self.version_text = version_text
        self.basevintage = basevintage
        self.outputfolder = outputfolder
        self.outputfolders = outputfolders
        self.savefiles = savefiles


    def save_environment_version_details(self):
        print("\n***************************************")
        print("    Version control - list of installed packages")
        print("***************************************\n")       

        try:
            # print a list of all installed packages and version information
            ipython.magic("conda list")
            # Give ipython a second to output results
            # this step fixes issue with conda list being split by next command
            time.sleep(1)
        except:
            print("Unable to print version information")


    def run_hui_workflow(self, savelog=True):
        """
        Workflow to produce Housing Unit Inventory
        """
        # Start empty containers to store block level and tract level data
        tract_df = {}
        block_df = {}

        # Save output description as text
        output_filename = f'hui_{self.version_text}_{self.state_county}_{self.basevintage}_rs{self.seed}'
        self.output_filename = output_filename
        if savelog == True:
            log_filepath = self.outputfolders['logfiles']+"/"+output_filename+'.log'
            # start log file
            logfile.start(log_filepath)
            self.save_environment_version_details()

        print("\n***************************************")
        print("    Obtain and clean core housing unit characteristics for",self.state_county_name)
        print("***************************************\n")

        print(self.outputfolders)
        block_df['core'] = BaseInventory.get_apidata(state_county = self.state_county,
                                            outputfolders = self.outputfolders,
                                            outputfile = "CoreHUI")
        
        block_df['family'] = BaseInventory.graft_on_new_char(base_inventory= block_df['core'],
                                state_county = self.state_county,
                                new_char = 'family',
                                new_char_dictionaries = [family_byrace_P18_varstem_roots],
                                outputfile = "hui",
                                outputfolders = self.outputfolders)

        block_df['hispan'] = BaseInventory.graft_on_new_char(base_inventory= block_df['family'],
                                        state_county = self.state_county,
                                        new_char = 'hispan',
                                        new_char_dictionaries = 
                                        [tenure_size_H16HAI_varstem_roots,
                                            hispan_byrace_H7_varstem_roots,
                                            tenure_byhispan_H15_varstem_roots
                                            ],
                                        basevintage = "2010", 
                                        basegeolevel = 'Block',
                                        outputfile = "hui",
                                        outputfolders = self.outputfolders)

        # Generate Household Income Inventory - By Race
        tract_df["B19001"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'tract',
                                        vintage = "2012", 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [hhinc_varstem_roots],
                                        outputfolders = self.outputfolders,
                                        outputfile = "B19001")

        # Generate Family Income Inventory - By Race 
        tract_df["B19101"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'tract',
                                        vintage = "2012", 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [family_varstem_roots],
                                        outputfolders = self.outputfolders,
                                        outputfile = "B19101")
                    
        print("\n***************************************")
        print("    Random income data with core characteristics.")
        print("***************************************\n")
        
        income_by_family = add_new_char_by_random_merge_2dfs(
        dfs = {'primary'  : {'data': tract_df['B19001'], 
                        'primarykey' : 'uniqueidB19001',
                        'geolevel' : 'Tract',
                        'geovintage' :'2010',
                        'notes' : 'Household level data with income.'},
            'secondary' : {'data': tract_df['B19101'], 
                        'primarykey' : 'uniqueidB19101',
                        'geolevel' : 'Tract',
                        'geovintage' :'2010',
                        'notes' : 'Family level data with income.'}},
        seed = self.seed,
        common_group_vars = ['incomegroup'],
        new_char = 'family',
        geolevel = "Tract",
        geovintage = "2010",
        by_groups = {'All' : {'by_variables' : ['race','hispan']}},
        fillna_value = 0,
        state_county = self.state_county,
        outputfile = "B19001rmB19101",
        outputfolder = self.outputfolders['RandomMerge'],
        savefiles = self.savefiles)

        # Set up round options
        rounds = income_by_family.make_round_options_dict()

        tract_income_match = income_by_family.run_random_merge_2dfs(rounds)

        print("\n***************************************")
        print("    Merge income data with block data.")
        print("***************************************\n")

        block_income = add_new_char_by_random_merge_2dfs(
        dfs = {'primary'  : {'data': block_df['hispan'], 
                        'primarykey' : 'huid',
                        'geolevel' : 'Block',
                        'geovintage' :'2010',
                        'notes' : 'Household level data without income.'},
            'secondary' : {'data': tract_income_match['primary'], 
                        'primarykey' : 'uniqueidB19001',
                        'geolevel' : 'Tract',
                        'geovintage' :'2010',
                        'notes' : 'Household and Family level data with income.'}},
        seed = self.seed,
        common_group_vars = ['family'],
        new_char = 'incomegroup',
        geolevel = "Tract",
        geovintage = "2010",
        by_groups = {'Hispanic'     : {'by_variables' : ['hispan']},
                    'not Hispanic' : {'by_variables' : ['race']}},
        fillna_value= -999,
        state_county = self.state_county,
        outputfile = "hui_B19001rmB19101",
        outputfolder = self.outputfolders['RandomMerge'],
        reuse_secondary = True,
        savefiles = self.savefiles)

        # Set up round options
        rounds = block_income.make_round_options_dict()

        block_income_df = block_income.run_random_merge_2dfs(rounds)

        # Stop log file
        if savelog == True:
            logfile.stop()

        return block_income_df

    def final_polish_hui(self, input_df):

        print("\n***************************************")
        print("    Try to polish final hui data.")
        print("***************************************\n")

        hui_df = self.final_hui_polish(input_df)

        print("\n***************************************")
        print("    Save cleaned data file.")
        print("***************************************\n")

        if self.savefiles == True:
            csv_filepath = self.outputfolders['top']+"/"+self.output_filename+'.csv'
            savefile = sys.path[0]+"/"+csv_filepath
            hui_df.to_csv(savefile, index=False)
            print("File saved:",savefile)
        
        return hui_df
    # Code for adding random income

    # Dictionary with round options
    @staticmethod
    def make_incomegroup_dict():
        ## Add min and max income values
        incomegroup_dict = {1: {'minincome': 0, 'maxincome': 9999},
        2: {'minincome': 10000, 'maxincome': 14999},
        3: {'minincome': 15000, 'maxincome': 19999},
        4: {'minincome': 20000, 'maxincome': 24999},
        5: {'minincome': 25000, 'maxincome': 29999},
        6: {'minincome': 30000, 'maxincome': 34999},
        7: {'minincome': 35000, 'maxincome': 39999},
        8: {'minincome': 40000, 'maxincome': 44999},
        9: {'minincome': 45000, 'maxincome': 49999},
        10: {'minincome': 50000, 'maxincome': 59999},
        11: {'minincome': 60000, 'maxincome': 74999},
        12: {'minincome': 75000, 'maxincome': 99999},
        13: {'minincome': 100000, 'maxincome': 124999},
        14: {'minincome': 125000, 'maxincome': 149999},
        15: {'minincome': 150000, 'maxincome': 199999},
        16: {'minincome': 200000, 'maxincome': 250000},
        -999 : {'minincome': 0, 'maxincome': 250000, 'note' : 
        'Category -999 = income group not set. Assume income could be anything from 0 to 250000.'},
        0 : {'minincome': 0, 'maxincome': 1, 'note' : 
        'Category 0 = observation has no income, but the random income process requires'+\
            'a value to work for all observations. Remove ranincome after set.'+\
                'This category applies to vacant housing units and group quarters.'} }

        return incomegroup_dict

    @staticmethod
    def add_minmaxincome(input_df,incomegroup_dict):
        #condition = (df['incomegroup'].notnull())
        output_df = input_df.copy()
        output_df['minincome'] = output_df.\
            apply(lambda x: incomegroup_dict[x['incomegroup']]['minincome'], axis=1)
        output_df['maxincome'] = output_df.\
            apply(lambda x: incomegroup_dict[x['incomegroup']]['maxincome'], axis=1)

        return output_df

    @staticmethod
    def remove_cat0_randincome(input_df):
        """
        For category 0 need to replace values with missing.
        Without this step the descriptive statistics (median, mean)
        will be skewed. 
        Category 0 represents observations where the 
        characteristic is not applicable.
        For example - vacant housing units do not have a household income.
        """

        output_df = input_df.copy()
        condition = (output_df['incomegroup'] == 0)
        output_df.loc[condition, 'minincome'] = np.nan
        output_df.loc[condition, 'maxincome'] = np.nan
        output_df.loc[condition, 'randincomeB19101'] = np.nan

        return output_df

    def add_randincome(self, df, seed):
        random_generator = np.random.RandomState(seed)

        output_df = df.copy()
        #Make sure income group has 0 category
        output_df['incomegroup'] = \
        output_df['incomegroup'].fillna(value=0)
        output_df['incomegroup'].describe()

        # read in incomegroup dictionary
        incomegroup_dict = self.make_incomegroup_dict()
        # Add min max values
        output_df = self.add_minmaxincome(output_df,incomegroup_dict)

        # Add random income value
        output_df['randincomeB19101'] = output_df.apply(lambda x: \
            random_generator.randint(x['minincome'],x['maxincome']), axis=1)
        
        # remove income from income category 0
        output_df = self.remove_cat0_randincome(output_df)

        # add 5 household income groups
        output_df = self.add_hhinc_groups(output_df)

        return output_df

    def add_poverty(self,input_df):
        """
        Add poverty based on US Census Poverty Thresholds
        https://www.census.gov/topics/income-poverty/poverty/guidance/poverty-measures.html
        https://www2.census.gov/programs-surveys/cps/tables/time-series/historical-poverty-thresholds/thresh12.xls

        Use weighted average threshold by household size
        """

        output_df = input_df.copy()

        poverty_by_numprec_dict = {1: 11720,
                    2:  14937,
                    3: 18284,
                    4: 23492,
                    5: 27827,
                    6: 31471,
                    7: 35743}

        for numprec in poverty_by_numprec_dict:
            randincome_less_than    = (output_df['randincomeB19101'] < \
                poverty_by_numprec_dict[numprec])
            numprec_equals = (output_df['numprec'] == numprec)
            conditions = randincome_less_than & numprec_equals
            output_df.loc[conditions,'poverty'] = 1

            randincome_greater_than    = (output_df['randincomeB19101'] >= \
                poverty_by_numprec_dict[numprec])
            conditions = randincome_greater_than & numprec_equals
            output_df.loc[conditions,'poverty'] = 0

        # Add 0 hhinc - for no income group
        randincome_missing =  (output_df['randincomeB19101'].isnull())
        is_gqtype  = (output_df['gqtype'] > 0)
        is_vacant  = (output_df['vacancy'] > 0)
        conditions = randincome_missing | is_gqtype | is_vacant
        output_df.loc[conditions,'poverty'] = np.nan

        return output_df

    def fill_missingvalues(self,input_df):
        """
        vacancy type and gqytpe should be 0 for 
        occupied housing units

        numprec should be 0 for vacant housing units

        Vacancy should be 0 for Group Quarters
        gqtype should be 0 for vacant housing units

        consider making all categorical variables have 0 values
        for observations where category is not applicable
        vancant housing units race = 0 and ownershp = 0 for example
        """

        output_df = input_df.copy()

        # Conditions for occupied housing units
        occupied_hu = (output_df['numprec'] > 0)
        not_gqtype  = (output_df['gqtype'].isnull())
        not_vacant  = (output_df['vacancy'].isnull())
        conditions = occupied_hu & not_gqtype & not_vacant
        # Fill in missing values
        output_df.loc[conditions, 'gqtype'] = 0
        output_df.loc[conditions, 'vacancy'] = 0
    
        # Conditions for vacant housing units
        vacant_hu = (output_df['vacancy'] > 0)
        not_occupied  = (output_df['numprec'].isnull())
        conditions = vacant_hu & not_occupied
        output_df.loc[conditions, 'numprec'] = 0
        output_df.loc[conditions, 'gqtype'] = 0

        # Conditions for group quarters housing units
        gq_hu = (output_df['gqtype'] > 0)
        occupied  = (output_df['numprec'].notnull())
        conditions = gq_hu & occupied
        output_df.loc[conditions, 'vacancy'] = 0


        return output_df

    def add_hhinc_groups(self,input_df):
        """
        Add 5 income groups
        """

        output_df = input_df.copy()

        hhinc_dict = {1: {'minincome': 0, 'maxincome': 14999},
        2: {'minincome': 15000, 'maxincome': 24999},
        3: {'minincome': 25000, 'maxincome': 74999},
        4: {'minincome': 75000, 'maxincome': 99999},
        5: {'minincome': 100000, 'maxincome': 10000000}}

        for hhinc in hhinc_dict:
            randincome_greater_than = (output_df['randincomeB19101'] >= hhinc_dict[hhinc]['minincome'])
            randincome_less_than    = (output_df['randincomeB19101'] <= hhinc_dict[hhinc]['maxincome'])
            conditions = randincome_greater_than & randincome_less_than
            output_df.loc[conditions,'hhinc'] = hhinc

        # Add 0 hhinc - for no income group
        randincome_missing =  (output_df['randincomeB19101'].isnull())
        is_gqtype  = (output_df['gqtype'] > 0)
        is_vacant  = (output_df['vacancy'] > 0)
        conditions = randincome_missing | is_gqtype | is_vacant
        output_df.loc[conditions,'hhinc'] = 0

        return output_df


    @staticmethod
    def drop_extra_columns(input_df):
        """
        Need to drop columns used to predict variables
        """

        output_df = input_df.copy()

        drop_if_starts_with = ['prob_','hucount_','preccount_','sumby_','min','max','totalprob_']
        for substring in drop_if_starts_with:
            drop_vars = [col for col in output_df if col.startswith(substring)]
            #print(drop_vars)
            # Drop columns
            output_df = output_df.drop(drop_vars, axis=1)

        drop_if_ends_with = ['_counter','_flagset','_flag','_flagsetrm']
        for substring in drop_if_ends_with:
            drop_vars = [col for col in output_df if col.endswith(substring)]
            #print(drop_vars)
            # Drop columns
            output_df = output_df.drop(drop_vars, axis=1)

        drop_if_contains = ['byP','byH']
        for substring in drop_if_contains:
            drop_vars = [col for col in output_df if substring in col]
            #print(drop_vars)
            # Drop columns
            output_df = output_df.drop(drop_vars, axis=1)

        return output_df

    def save_incore_version2(self, input_df):
        """
        IN-CORE expects specific columns
        Alpha release of housing unit inventories had 
        columns in specific order
        """

        output_df = input_df.copy()
        # create list of all required ergo:buildingInventoryVer6 columns
        incore_columns = incore_v2_DataStructure
        
        current_column_names = list(output_df.columns)
        output_col_order = []
        # Loop through required column names
        for incore_col in incore_columns.keys():
            print('Checking output for',incore_col)
            output_col_order.append(incore_col)
            # Check if required column is in current column names
            if incore_col not in current_column_names:
                print('   Attempt to add',incore_col)
                # If not in list check if current version has in-core required
                hui_col_name = incore_columns[incore_col]['huiv3-0-0']
                print('   Check current version for',hui_col_name)
                if hui_col_name in current_column_names:
                    # Attempt to rename current version column name
                    try:
                        print('    Renaming',hui_col_name,incore_col)
                        output_df = output_df.\
                            rename(columns={hui_col_name: incore_col})
                        continue
                    except:
                        print("Current version is missing",incore_col)
                if hui_col_name not in current_column_names:
                    # Check if there is a formula for making the IN-CORE col
                    try:
                        print('    Attempt to make',incore_col)
                        formula = incore_columns[incore_col]['formula']
                        output_df.loc[:,incore_col] = eval(formula)
                    except:
                        print("Error - not able to make",incore_col)
                        output_col_order.remove(incore_col)
        # Loop through columns to set type
        for incore_col in incore_columns.keys():    
            # Set variable type
            var_type = incore_columns[incore_col]['pyType']
            print('Checking',incore_col,'Data Type')
            current_type = type(output_df[incore_col])
            print('   Current type:',current_type,'Expected type',var_type)
            output_df[incore_col] = output_df[incore_col].astype(var_type)

            # Check expected length
            # Issue for geography codes that need to be zero padded
            if 'length' in incore_columns[incore_col]:
                expected_length = incore_columns[incore_col]['length']
                # find minimum length and maximum length of string
                string_list = output_df[incore_col].astype(str)
                varid_list = list(string_list.fillna(value="0"))
                varid_min = min(varid_list)       
                min_var_len = len(varid_min)
                varid_max = max(varid_list)       
                max_var_len = len(varid_max)
                if min_var_len != expected_length or max_var_len != expected_length:
                    print('    Attempt to fix length of',incore_col)
                    formula = incore_columns[incore_col]['formula']
                    output_df.loc[:,incore_col] = eval(formula)
                else:
                    print('    Length of',incore_col,'is correct')

        output_df = output_df[output_col_order]
        # Save output description as text
        output_filename = f'hui_{self.version_text}_{self.state_county}_{self.basevintage}_rs{self.seed}'
        print("\n***************************************")
        print("    Save IN-CORE v2 data file.")
        print("***************************************\n")

        csv_filepath = self.outputfolders['top']+"/"+output_filename+'.csv'
        savefile = sys.path[0]+"/"+csv_filepath
        output_df.to_csv(savefile, index=False)
        print("File saved:",savefile)       
        return output_df    


    def final_hui_polish(self, input_df):
        
        output_df = input_df.copy()
        try:
            print("Add random income.")
            output_df = self.add_randincome(output_df,self.seed)
            print("Add poverty.")
            output_df = self.add_poverty(output_df)
            print("Make category 0 for numprec, vacancy and gqtype")  
            output_df = self.fill_missingvalues(output_df)
            print("Drop extra columns.")                                                                  
            output_df = self.drop_extra_columns(output_df)
        except:
            print('Data not ready for final polish.')

        return output_df
    