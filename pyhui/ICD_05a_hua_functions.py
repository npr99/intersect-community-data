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
from pyhui.SourceData.api_census_gov.acg_00e_incore_huiv2 \
    import incore_v2_DataStructure

# open, read, and execute python program with reusable commands
from pyhui.SourceData.api_census_gov.acg_02a_add_categorical_char \
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


    def run_hua_workflow(self, savelog=True):
        """
        Workflow to produce Housing Unit Allocation
        """
        # Start empty containers to store hua data
        hua_df = {}

        # Save output description as text
        output_filename = f'hui_{self.version_text}_{self.state_county}_{self.basevintage}_rs{self.seed}'
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
        hui_intersect_addpt_df = self.hui_df.copy()
        addpt_intersect_hui_df = self.addpt_df.copy()
        for huicounter in ['huicounter1','huicounter2','huicounter3']:
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
                    common_group_vars = [huicounter, 'tenure'],
                    new_char = 'guid',
                    extra_vars = ['strctid','plcname10','x','y'],
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
                                            hui_addptr1.by_groups},
                    },
                    'geo_levels' : ['Block'] # ['Block','BlockGroup','Tract','County']  
                    }

            # Update hui addpt file for next merge
            hua_addptr1_df = hui_addptr1.run_random_merge_2dfs(rounds)
            hui_intersect_addpt_df = hua_addptr1_df['primary']
            addpt_intersect_hui_df = hua_addptr1_df['secondary']

        print("\n***************************************")
        print("   Update Predicted Tenure.")
        print("***************************************\n")

        # look at the predicted tenure for each structure

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
                common_group_vars = ['tenure'],
                new_char = 'guid',
                extra_vars = ['strctid','plcname10','x','y'],
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
                                        ['tenure'],
                                'by_groups' :
                                        hui_addptr2.by_groups},
                'huinocounternotenure' : {'notes' : 'Attempt to merge hui by geolevel only.',
                                'common_group_vars' : 
                                        [],
                                'by_groups' :
                                        hui_addptr2.by_groups},
                },
                'geo_levels' : ['Block','BlockGroup','Tract','County']  
                }
        # Run random merge
        hua_addptr2_df = hui_addptr2.run_random_merge_2dfs(rounds)

        # Stop log file
        if savelog == True:
            logfile.stop()

        return hua_addptr2_df
    