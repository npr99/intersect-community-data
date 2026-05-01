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
from pyncoda.CommunitySourceData.api_census_gov.acg_00a_createAPI_datastructure import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00a_general_datastructures import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00f_preci_block2010 import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00f_preci_block2020 import (
    sexbyage_P12_2020_varstem_roots,
    sexbyage_P12HAI_2020_varstem_roots,
    hispan_byrace_P5_2020_varstem_roots
)

# open, read, and execute python program with reusable commands
from pyncoda.CommunitySourceData.api_census_gov.acg_01a_BaseInventory import BaseInventory
from pyncoda.CommunitySourceData.api_census_gov.acg_02a_add_categorical_char \
     import add_new_char_by_random_merge_2dfs
from pyncoda.CommunitySourceData.api_census_gov.acg_02c_agefunctions \
     import *
from pyncoda.CommunitySourceData.api_census_gov.acg_02d_polishdf \
     import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00h_disability_ACS5yr2012 import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00h_disability_ACS5yr2022 import *

class prec_workflow_functions():
    """
    Function runs full process for generating the person record files
    Process runs for 1 county.
    """

    def __init__(self,
            state_county: str,
            state_county_name: str,
            seed: int = 9876,
            version: str = '3.0.0',
            version_text: str = 'v3-0-0',
            basevintage: str = '2010',
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



    """
    Code from 
    github.com\npr99\Population_Inventory\pyincore_data_addons\SourceData\api_census_gov
    """
    
    def run_prec_workflow(self, savelog=True):
        """
        Workflow to produce Person Record Inventory
        """
        # Start empty containers to store block level and tract level data
        tract_df = {}
        block_df = {}

        # Save output description as text
        output_filename = f'prec_{self.version_text}_{self.state_county}_{self.basevintage}_rs{self.seed}'
        self.output_filename = output_filename
        if savelog == True:
            log_filepath = self.outputfolders['logfiles']+"/"+output_filename+'.log'
            # start log file
            logfile.start(log_filepath)
            self.save_environment_version_details()

        print("\n***************************************")
        print("    Obtain and clean core person record characteristics for",self.state_county_name)
        print("***************************************\n")

        print(self.outputfolders)
        # Generate Person by Age, Sex, Race, and Hispanic
        block_df["preci"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'block',
                                        vintage = str(self.basevintage), 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [sexbyage_P12_varstem_roots if str(self.basevintage) == '2010' else
                                                            sexbyage_P12_2020_varstem_roots],
                                        outputfolders = self.outputfolders,
                                        outputfile = f"CorePREC_{self.basevintage}")

        block_df["precihispan"] = BaseInventory.graft_on_new_char(base_inventory= block_df['preci'],
                                        state_county = self.state_county,
                                        new_char = 'hispan',
                                        new_char_dictionaries = [
                                            sexbyage_P12HAI_varstem_roots if str(self.basevintage) == '2010' else sexbyage_P12HAI_2020_varstem_roots,
                                            hispan_byrace_P5_varstem_roots if str(self.basevintage) == '2010' else hispan_byrace_P5_2020_varstem_roots
                                            ],
                                        outputfile = f"preci_{self.basevintage}",
                                        outputfolders = self.outputfolders)

        # Generate sex by age with individual years
        vintage = str(self.basevintage)
        dataset_name = 'dec/sf1' if str(self.basevintage) == '2010' else 'dec/dhc' 
        group = 'PCT12'
        sexbyage_PCT12 = createAPI_datastructure.obtain_api_metadata(
                            vintage = vintage,
                            dataset_name = dataset_name,
                            group = group,
                            outputfolder = self.outputfolder)

        tract_df["PCT12"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'tract',
                                        vintage = str(self.basevintage), 
                                        mutually_exclusive_varstems_roots_dictionaries = [sexbyage_PCT12_varstem_roots],
                                        outputfolders = self.outputfolders,
                                        outputfile = f"{group}_{self.basevintage}")

        # Add random age to block_df["precihispan"]
        block_df["precihispan"] = add_randage(
                                    block_df["precihispan"],
                                    seed = self.seed,
                                    varname = 'randageP12')
        # Add agegroups to block_df["precihispan"]
        block_df["precihispan"] = add_P12age_groups(
                                    block_df["precihispan"],
                                    varname = 'randageP12')
        # Add random age to tract_df["PCT12"]
        tract_df["PCT12"] = add_randage(
                                    tract_df["PCT12"],
                                    seed = self.seed,
                                    varname = 'randagePCT12')
        # Add agegroups to tract_df["PCT12"]
        tract_df["PCT12"] = add_P12age_groups(
                                    tract_df["PCT12"],
                                    varname = 'randagePCT12')
                  
        print("\n***************************************")
        print("    Person Block by Age with Tract by Age data.")
        print("***************************************\n")

        add_age = add_new_char_by_random_merge_2dfs(
            dfs = {'primary'  : {'data': block_df["precihispan"], 
                            'primarykey' : 'precid',
                            'geolevel' : 'Block',
                            'geovintage' : str(self.basevintage),
                            'notes' : 'Block agegroup, sex, race, ethnicity data.'},
                'secondary' : {'data': tract_df["PCT12"], 
                            'primarykey' : 'uniqueidPCT12',
                            'geolevel' : 'Tract',
                            'geovintage' : str(self.basevintage),
                            'notes' : 'Tract single age years, sex data.'}},
            seed = self.seed,
            common_group_vars = ['agegroupP12'],
            new_char = 'randagePCT12',
            geolevel = "Tract",
            geovintage = str(self.basevintage),
            by_groups = {'All' : {'by_variables' : ['sex']}},
            fillna_value= -999,
            state_county = self.state_county,
            outputfile = f"preci_randomage_{self.basevintage}",
            outputfolder = self.outputfolders['RandomMerge'])

        # Set up round options
        rounds = {'options': {
                'option1' : {'notes' : 'By original common group vars and by groups variables.',
                            'common_group_vars' : 
                                    add_age.common_group_vars,
                            'by_groups' :
                                    add_age.by_groups}
                                },
                'geo_levels' : ['Tract']                         
                }

        prec_age_df = add_age.run_random_merge_2dfs(rounds)

        print("\n***************************************")
        print("    Add Disability Characteristics from ACS Tract Data")
        print("***************************************\n")

        # Retrieve tract-level disability data
        tract_df["B18101"] = BaseInventory.get_apidata(
                                        state_county = self.state_county,
                                        geo_level = 'tract',
                                        vintage = str(int(self.basevintage)+2),
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [disability_B18101_varstem_roots if str(self.basevintage) == '2010' else
                                                            disability_B18101_2022_varstem_roots],
                                        outputfolders = self.outputfolders,
                                        outputfile = f"B18101_disability_{self.basevintage}")

        # Add B18101 age groups to person records
        print("Adding B18101 age groups for disability matching...")
        prec_age_df['primary'] = add_B18101age_groups(
                                    prec_age_df['primary'],
                                    varname = 'randagePCT12')

        # Also add B18101 age groups to tract disability data
        tract_df["B18101"] = add_B18101age_groups(
                                    tract_df["B18101"],
                                    varname = 'randagePCT12')

        print("Random merging disability data...")
        add_disability = add_new_char_by_random_merge_2dfs(
            dfs = {'primary'  : {'data': prec_age_df['primary'],
                            'primarykey' : 'precid',
                            'geolevel' : 'Block',
                            'geovintage' : str(self.basevintage),
                            'notes' : 'Person-level data without disability'},
                'secondary' : {'data': tract_df["B18101"],
                            'primarykey' : 'uniqueidB18101',
                            'geolevel' : 'Tract',
                            'geovintage' : str(self.basevintage),
                            'notes' : 'Tract-level disability counts by sex and age'}},
            seed = self.seed,
            common_group_vars = ['agegroupB18101'],
            new_char = 'disability',
            geolevel = "Tract",
            geovintage = str(self.basevintage),
            by_groups = {'All' : {'by_variables' : ['sex']}},
            fillna_value = -999,
            state_county = self.state_county,
            outputfile = f"prec_disability_{self.basevintage}",
            outputfolder = self.outputfolders['RandomMerge'],
            savefiles = self.savefiles)

        # Set up round options
        rounds = add_disability.make_round_options_dict()

        # Run multi-round random merge
        # Flags automatically created:
        # - disability_flagsetrm: Overall assignment flag (0 = not assigned, 1 = assigned)
        # - disability_Tract2010_flagsetrm: Round-specific flag (0, 1, 2, 3, etc.)
        # These track which persons received disability assignments and in which round
        prec_disability_df = add_disability.run_random_merge_2dfs(rounds)

        # Update return variable to include disability
        prec_age_df = prec_disability_df

        print("\n***************************************")
        print("    Try to polish final hui data.")
        print("***************************************\n")

        prec_df = self.final_polish_prec(prec_age_df['primary'])

        print("\n***************************************")
        print("    Save cleaned data file.")
        print("***************************************\n")

        csv_filepath = self.outputfolders['top']+"/"+output_filename+'.csv'

        savefile = os.path.join(os.getcwd(), csv_filepath)
        prec_df.to_csv(savefile, index=False)
        print("File saved:",savefile)

        # Stop log file
        logfile.stop()

        return prec_df

    # Function not currently used - might be in PRECHUI Workflow
    def hui_tidy_P43(self):
        """
        Obtain, Tidy, and transfer data with population
        in group quarters by age, sex, by gqtype

        """

        print("\n***************************************")
        print("    Set up Data structures for obtaining data.")
        print("***************************************\n")
        vintage = str(self.basevintage)
        dataset_name = 'dec/sf1' if str(self.basevintage) == '2010' else 'dec/dhc' 
        group = 'P43'
        prec_P43_dict = createAPI_datastructure.obtain_api_metadata(
                vintage = vintage,
                dataset_name = dataset_name,
                group = group,
                outputfolder = self.outputfolder,
                version_text = self.version_text)
        
        # Need to add graft chars to metadata
        # Graft chars are used to check the merge by variables in grafting function
        prec_P43_dict['metadata']['graft_chars'] = ['gqytpe']

        
        print("\n***************************************")
        print("   Obtain and Clean P43 Data.")
        print("***************************************\n")
        block_df = {}
        block_df["P43"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'block',
                                        vintage = str(self.basevintage), 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [prec_P43_dict],
                                        outputfolders = self.outputfolders,
                                        outputfile = f"P43_{self.basevintage}")


        # Add random age
        print("Add random age and P43 age groups.")
        block_df["P43"] = self.add_randage(
                                    block_df["P43"],
                                    seed = self.seed,
                                    varname = 'randageP43')
        # Add agegroups to block_df["hhage_hispan"]
        block_df["P43"] = self.add_P43age_groups(
                                    block_df["P43"],
                                    varname = 'randageP43')

        return block_df["P43"]

    def final_polish_prec(self, input_df):

        print("\n***************************************")
        print("    Try to polish final prec data.")
        print("***************************************\n")

        print("Drop extra columns.")                                                                  
        prec_df = drop_extra_columns(input_df)

        print("\n***************************************")
        print("    Save cleaned data file.")
        print("***************************************\n")

        if self.savefiles == True:
            csv_filepath = self.outputfolders['top']+"/"+self.output_filename+'.csv'

            savefile = os.path.join(os.getcwd(), csv_filepath)
            prec_df.to_csv(savefile, index=False)
            print("File saved:",savefile)
        
        return prec_df
