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
from pyncoda \
     import ncoda_00c_save_output_log as logfile
     
# Load in data structure dictionaries
from pyncoda.CommunitySourceData.api_census_gov.acg_00a_general_datastructures import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00e_incore_huiv2 \
    import incore_v2_DataStructure

# open, read, and execute python program with reusable commands
from pyncoda.CommunitySourceData.api_census_gov.acg_02a_add_categorical_char \
     import add_new_char_by_random_merge_2dfs


class hua_workflow_functions():
    """
    Function runs full process for generating the housing unit inventories
    Process runs for 1 county.
    """

    def __init__(self,
            hui_df,
            addpt_df,
            bldg_df,
            state_county: str,
            state_county_name: str,
            seed: int = 9876,
            version: str = '2.0.0',
            version_text: str = 'v2-0-0',
            basevintage: str = 2010,
            outputfolder: str ="",
            outputfolders = {},
            savefiles: bool = True):

        self.hui_df = hui_df
        self.addpt_df = addpt_df
        self.bldg_df = bldg_df
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

    def check_addpt_huicounter(self, addpt_df):
        """
        Check the that the Address Point Inventory includes
        variables for the housing unit counter by structure
        """
        # Check that the Address Point Inventory includes
        # variables for the housing unit counter by structure
        # if not, add them
        if 'huicounter' not in addpt_df.columns:
            addpt_df['huicounter_addpt'] = 0

            # By structure id create a counter for huid
            addpt_df['huicounter_addpt'] = addpt_df.groupby('strctid').cumcount() + 1

            # Copy huicounter three times to create a new column for each
            # Additional columns needed for the random merge
            addpt_df['huicounter1'] = addpt_df['huicounter_addpt']
            addpt_df['huicounter2'] = addpt_df['huicounter_addpt']
            addpt_df['huicounter3'] = addpt_df['huicounter_addpt']

        return addpt_df

    def check_addpt_predictownershp(self, addpt_df):
        """
        Predict structure ownership (tenure) based on the 
        number of housing units in the structure
        """

        if 'ownershp1' not in addpt_df.columns:
            addpt_df['ownershp1'] = -777

            # Change ownershp if huestimate is greater than 0
            # Assume that structure is owner occupied if 
            # the number of estimate housing units is 1 or 2
            # If husestimate is greater than 2 assume renter occupied
            addpt_df.loc[addpt_df['huestimate'] ==1, 'ownershp1'] = 1
            addpt_df.loc[addpt_df['huestimate'] ==2, 'ownershp1'] = 1

            # the ownership variable is temporary in the addpt_df

        # For second round ownership assume all structures with more than 2
        # housing units are renter occupied
        if 'ownershp2' not in addpt_df.columns:
            addpt_df['ownershp2'] = -777
            addpt_df.loc[addpt_df['huestimate'] >2, 'ownershp2'] = 2

        # For third round ownership assume all structure are renter occupied
        if 'ownershp3' not in addpt_df.columns:
            addpt_df['ownershp3'] = -777
            addpt_df.loc[addpt_df['huestimate'] >0, 'ownershp3'] = 2

        return addpt_df

    def update_addpt_predictownershp(self, hua_df, addpt_df):
        """
        Update predicted structure ownership (tenure) based on the 
        the tenure of the first round housing units allocated
        to the structure.
        """


        # Check the average value of tenure status by structure
        updated_ownership_df = hua_df[['ownershp','strctid']].\
            groupby('strctid').mean()
        updated_ownership_df.reset_index(inplace=True)

        # rename ownership column to predictownershp
        updated_ownership_df.rename(columns={'ownershp':'predictownershp'},
            inplace=True)
        # Update the ownership variable in the addpt_df
        # merge based on structure id
        addptv2_df = pd.merge(left = addpt_df, 
                      right = updated_ownership_df,
                      on='strctid', 
                      how='left')

        # Update the ownership variable in the addpt_df based on predicted ownership
        condition1 = (addptv2_df['predictownershp'] != addptv2_df['ownershp1'])
        condition2 = (~addptv2_df['predictownershp'].isna())
        condition3 = (addptv2_df['ownershp1']==1)

        addptv2_df.loc[condition1 & condition2 & condition3,'ownershp1'] \
            = addptv2_df['predictownershp']

        # Check ownership is 1 or 2
        addptv2_df.loc[addptv2_df['ownershp1'] >2, 'ownershp1'] = 2
        addptv2_df.loc[(addptv2_df['ownershp1'] > 1) & 
                       (addptv2_df['ownershp1'] < 2), 'ownershp1'] = 1
        addptv2_df.loc[(addptv2_df['ownershp1'] < 1), 'ownershp1'] = -777

        # drop predictownershp column
        addptv2_df.drop(columns=['predictownershp'], inplace=True)

        return addptv2_df

    def check_hui_ownershp(self, hui_df):
        """
        Check the that the Housing Unit Inventory includes
        variables for the ownership prediction
        """
        # Check that the Housing Unit Inventory includes
        # variables for the ownership prediction
        # if not, add them

        for ownershpvar in ['ownershp1','ownershp2','ownershp3']:
            if ownershpvar not in hui_df.columns:

                # Copy ownership variable to new column
                hui_df[ownershpvar] = hui_df['ownershp'] 

                # Fill in missing values with 0
                # this should apply to vacant structures
                hui_df.loc[hui_df[ownershpvar].isna(), ownershpvar] = 0

        return hui_df

    def check_hui_huicounter(self, hui_df):
        """
        Check the that the Housing Unit Inventory includes
        variables for the housing unit counter
        """
        # Check that the Housing Unit Inventory includes
        # variables for the housing unit counter
        # if not, add them

        i = 1
        for huicounter in ['huicounter1','huicounter2','huicounter3']:
            if huicounter not in hui_df.columns:

                # Copy huicounter three times to create a new column for each
                # Additional columns needed for the random merge
                hui_df[huicounter] = i

            # increment i by 1
            i += 1

        return hui_df

    def run_hua_workflow(self, savelog=True):
        """
        Workflow to produce Housing Unit Allocation
        """
        # Start empty containers to store hua data
        hua_df = {}

        # Save output description as text
        output_filename = f'hua_{self.version_text}_{self.state_county}_{self.basevintage}_rs{self.seed}'
        self.output_filename = output_filename
        if savelog == True:
            log_filepath = self.outputfolders['logfiles']+"/"+output_filename+'.log'
            # start log file
            logfile.start(log_filepath)
            self.save_environment_version_details()

        print("\n***************************************")
        print("    Run Housing Unit Allocation for",self.state_county_name)
        print("***************************************\n")

        print("\n***************************************")
        print("    Merge housing unit and address point data with first 3 counters.")
        print("***************************************\n")
        # intersect by each housing unit counter over geolevel options
        # Copy dataframes and check that they have the required columns
        hui_intersect_addpt_df = self.check_hui_huicounter(self.hui_df.copy())
        addpt_intersect_hui_df = self.check_addpt_huicounter(self.addpt_df.copy())

        # add ownership predicted to address point data
        addpt_intersect_hui_df = self.check_addpt_predictownershp(addpt_intersect_hui_df)
        hui_intersect_addpt_df = self.check_hui_ownershp(hui_intersect_addpt_df)

        for huicounter in ['huicounter1','huicounter2','huicounter3']:
            for ownershp in ['ownershp1','ownershp2','ownershp3']:

                # Setup random Merge hui and addpt dataframes
                hui_addptr1 = add_new_char_by_random_merge_2dfs(
                        dfs = {'primary'  : {'data': hui_intersect_addpt_df, 
                                        'primarykey' : 'huid',
                                        'geolevel' : 'Housing Unit Inventory',
                                        'geovintage' :'2010',
                                        'notes' : 'Housing Unit records.'},
                        'secondary' : {'data': addpt_intersect_hui_df, 
                                        'primarykey' : 'addptid', # primary key needs to be different from new char
                                        'geolevel' : 'Address Point Inventory',
                                        'geovintage' :'2010',
                                        'notes' : 'Address Points for Possible Housing Units.'}},
                        seed = self.seed,
                        common_group_vars = [huicounter, ownershp],
                        new_char = 'strctid',
                        extra_vars = ['addrptid','guid','huestimate','huicounter_addpt','plcname10','x','y'],
                        geolevel = "Block",
                        geovintage = "2010",
                        by_groups = {'NA' : {'by_variables' : []}},
                        fillna_value= '-999',
                        state_county = self.state_county,
                        outputfile = "hui_addpt_guidr1",
                        outputfolder = self.outputfolders['RandomMerge'])

                # Set up round options
                rounds = {'options': {
                        'huiall' : {'notes' : 'Attempt to merge hui on all common group vars.',
                                        'common_group_vars' : 
                                                hui_addptr1.common_group_vars,
                                        'by_groups' :
                                                hui_addptr1.by_groups}
                        },
                        'geo_levels' : ['Block'] # ['Block','BlockGroup']  
                        }

                # Update hui addpt file for next merge
                hua_addptr1_df = hui_addptr1.run_random_merge_2dfs(rounds)
                hui_intersect_addpt_df = hua_addptr1_df['primary']
                addpt_intersect_hui_df = hua_addptr1_df['secondary']

            # After first round of assigning housing units to the first 
            # housing unit counter update tenure for the structure
            # this will allow a duplex to be assigned only renters or owners
            print("\n***************************************")
            print("   Update Predicted Tenure.")
            print("***************************************\n")
            addpt_intersect_hui_df = \
                self.update_addpt_predictownershp(
                    hua_df = hui_intersect_addpt_df,
                    addpt_df = addpt_intersect_hui_df)


        print("\n***************************************")
        print("    Merge housing unit and address point data with first no counters.")
        print("***************************************\n")
        hui_addptr2 = add_new_char_by_random_merge_2dfs(
                dfs = {'primary'  : {'data': hui_intersect_addpt_df, 
                                'primarykey' : 'huid',
                                'geolevel' : 'Housing Unit Inventory',
                                'geovintage' :'2010',
                                'notes' : 'Housing Unit records.'},
                'secondary' : {'data': addpt_intersect_hui_df, 
                                'primarykey' : 'addptid', # primary key needs to be different from new char
                                'geolevel' : 'Address Point Inventory',
                                'geovintage' :'2010',
                                'notes' : 'Address Points for Possible Housing Units.'}},
                seed = self.seed,
                common_group_vars = ['ownershp1'],
                new_char = 'strctid',
                extra_vars = ['addrptid','guid','huestimate','huicounter_addpt','plcname10','x','y'],
                geolevel = "Block",
                geovintage = "2010",
                by_groups = {'NA' : {'by_variables' : []}},
                fillna_value= '-999',
                state_county = self.state_county,
                outputfile = "hui_addpt_guidr2",
                outputfolder = self.outputfolders['RandomMerge'])
        # Set up round options
        rounds = {'options': {
                'huinocounter' : {'notes' : 'Attempt to merge hui without counter.',
                                'common_group_vars' : 
                                        hui_addptr2.common_group_vars,
                                'by_groups' :
                                        hui_addptr2.by_groups},
                'huinocounternotenure' : {'notes' : 'Attempt to merge hui by geolevel only.',
                                'common_group_vars' : 
                                        [],
                                'by_groups' :
                                        hui_addptr2.by_groups},
                },
                'geo_levels' : ['Block','BlockGroup']  
                }
        # Run random merge
        hua_addptr2_df = hui_addptr2.run_random_merge_2dfs(rounds)

        # Stop log file
        if savelog == True:
            logfile.stop()

        return hua_addptr2_df