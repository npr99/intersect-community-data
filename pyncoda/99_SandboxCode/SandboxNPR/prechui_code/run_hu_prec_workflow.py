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

# open, read, and execute python program with reusable commands
from pyincore_data_addons.SourceData.api_census_gov.BaseInventoryv3 import BaseInventory
from pyincore_data_addons.SourceData.api_census_gov.hui_add_categorical_char \
     import add_new_char_by_random_merge_2dfs
from pyincore_data_addons.SourceData.api_census_gov.CreateAPI_DataStructure \
    import createAPI_datastructure
# Save output as a log file function
from pyincore_data_addons \
     import save_output_log as logfile

# Load in data structure dictionaries
from pyincore_data_addons.SourceData.api_census_gov.acg_00a_general_datastructures import *
from pyincore_data_addons.SourceData.api_census_gov.acg_00f_preci_block2010 import *
from pyincore_data_addons.SourceData.api_census_gov.acg_00b_hui_block2010 import *
from pyincore_data_addons.SourceData.api_census_gov.acg_00c_hispan_block2010 import *
from pyincore_data_addons.SourceData.api_census_gov.acg_00d_hhinc_ACS5yr2012 import *
from pyincore_data_addons.SourceData.api_census_gov.acg_00e_incore_huiv010 \
    import incore_v010_DataStructure

class run_hui_prec_workflow():
    """
    Function runs full process for generating the housing unit and person record inventories
    Process runs for 1 county.
    """

    def __init__(self,
            state_county: str,
            state_county_name: str,
            seed: int = 9876,
            version: str = '0.2.0',
            version_text: str = 'v0-2-0',
            basevintage: str = 2010,
            outputfolder: str ="",
            outputfolders = {}):

        self.state_county = state_county
        self.state_county_name = state_county_name
        self.seed = seed
        self.version = version
        self.version_text = version_text
        self.basevintage = basevintage
        self.outputfolder = outputfolder
        self.outputfolders = outputfolders


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


    def run_hui_workflow(self):
        """
        Workflow to produce Housing Unit Inventory
        """
        # Start empty containers to store block level and tract level data
        tract_df = {}
        block_df = {}

        # Save output description as text
        output_filename = f'hui_{self.version_text}_{self.state_county}_{self.basevintage}_rs{self.seed}'
        self.output_filename = output_filename
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
                    
        # Generate Household Income Inventory - By Tenure 
        tract_df["B25118"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'tract',
                                        vintage = "2012", 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [hhinc_B25118_varstem_roots],
                                        outputfolders = self.outputfolders,
                                        outputfile = "B25118")

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
        outputfolder = self.outputfolders['RandomMerge'])

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
        outputfolder = self.outputfolders['RandomMerge'])

        # Set up round options
        rounds = block_income.make_round_options_dict()

        block_income_df = block_income.run_random_merge_2dfs(rounds)

        # Stop log file
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

        csv_filepath = self.outputfolders['top']+"/"+self.output_filename+'.csv'
        savefile = sys.path[0]+"/"+csv_filepath
        hui_df.to_csv(savefile, index=False)
        print("File saved:",savefile)
        
        return hui_df
    
    def hui_tidy_H17(self):
        """
        Obtain, Tidy, and transfor data for base inventory with householder age.
        """

        print("\n***************************************")
        print("    Set up Data structures for obtaining data.")
        print("***************************************\n")
        vintage = '2010'
        dataset_name = 'dec/sf1' 
        group = 'H17'
        hhagetenure_H17 = createAPI_datastructure.obtain_api_metadata(
                vintage = vintage,
                dataset_name = dataset_name,
                group = group,
                outputfolder = self.outputfolder,
                version_text = self.version_text)
        
        # Need to add graft chars to metadata
        # Graft chars are used to check the merge by variables in grafting function
        hhagetenure_H17['metadata']['graft_chars'] = ['ownershp','minageyrs','maxageyrs','race','hispan']

        hhagetenure_H17IAG = createAPI_datastructure.add_byracehispan(hhagetenure_H17,
                dec10byracehispan_All,
                dec10byracehispan_IAG_mx,
                newgroup = "IAG",
                newcharbyvar = '')

        hhagetenure_H17HAI = createAPI_datastructure.add_byracehispan(hhagetenure_H17,
                byracehispan_groups = dec10hispannotwhite_HAI,
                byracehispan_groups_mx = dec10hispannotwhite_HAI_mx,
                newgroup = "HAI",
                newcharbyvar = 'hispanbyH17HAI')
        
        # Need to update metadata for H17HAI
        hhagetenure_H17HAI['metadata']['char_vars'].append('hispanbyH17HAI')
        hhagetenure_H17HAI['metadata']['new_char'] = ['hispanbyH17HAI']
        # Update graft chars should not include race and Hispanic
        hhagetenure_H17HAI['metadata']['graft_chars'] = ['ownershp','minageyrs','maxageyrs']


        print("\n***************************************")
        print("   Obtain and Clean H17 Data.")
        print("***************************************\n")
        block_df = {}
        block_df["hhage"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'block',
                                        vintage = "2010", 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [hhagetenure_H17IAG],
                                        outputfolders = self.outputfolders,
                                        outputfile = "CoreHUI_H17IAG")


        block_df['hhage_hispan'] = BaseInventory.graft_on_new_char(base_inventory= block_df['hhage'],
                                        state_county = self.state_county,
                                        new_char = 'hispan',
                                        new_char_dictionaries = 
                                            [hhagetenure_H17HAI, 
                                            hispan_byrace_H7_varstem_roots,
                                            tenure_byhispan_H15_varstem_roots ],
                                        basevintage = "2010", 
                                        basegeolevel = 'Block',
                                        outputfile = "hui_hhage",
                                        outputfolders = self.outputfolders)

        
        # Add random age to block_df["hhage_hispan"]
        print("Add random age and H17 age groups.")
        block_df["hhage_hispan"] = self.add_randage(
                                    block_df["hhage_hispan"],
                                    seed = self.seed,
                                    varname = 'randageH17')
        # Add agegroups to block_df["hhage_hispan"]
        block_df["hhage_hispan"] = self.add_H17age_groups(
                                    block_df["hhage_hispan"],
                                    varname = 'randageH17')

        # Need to rename primary key huid - will mess up random merge
        block_df["hhage_hispan"] = block_df["hhage_hispan"].\
            rename(columns={"huid": "uniqueidH17"})

        return block_df["hhage_hispan"]

    def hui_randommerge_H17(self, primary_df, secondary_df):
        """
        Randome Core base inventory with householder age.
        """
        print("\n***************************************")
        print("    Merge householder age with block data.")
        print("***************************************\n")

        add_hhage = add_new_char_by_random_merge_2dfs(
        dfs = {'primary'  : {'data': primary_df, 
                        'primarykey' : 'huid',
                        'geolevel' : 'Block',
                        'geovintage' :'2010',
                        'notes' : 'Household level data without householder age.'},
            'secondary' : {'data': secondary_df, 
                        'primarykey' : 'uniqueidH17',
                        'geolevel' : 'Block',
                        'geovintage' :'2010',
                        'notes' : 'Householder age group data.'}},
        seed = self.seed,
        common_group_vars = ['ownershp'],
        new_char = 'agegroupH17',
        geolevel = "Block",
        geovintage = "2010",
        by_groups = {'All'     : {'by_variables' : ['hispan','race']}},
        fillna_value= -999,
        state_county = self.state_county,
        outputfile = "hui_H17",
        outputfolder = self.outputfolders['RandomMerge'])

        # Set up round options
        rounds = {'options': {
                'option1' : {'notes' : 'By original common group vars and by groups variables.',
                            'common_group_vars' : 
                                    add_hhage.common_group_vars,
                            'by_groups' :
                                    add_hhage.by_groups},
                'option2' : {'notes' : 'Drop common group variables.',
                            'common_group_vars' : 
                                    [],
                            'by_groups' :
                                    add_hhage.by_groups},
                'option3' : {'notes' : 'Run merge by race only.',
                            'common_group_vars' : 
                                    [],
                            'by_groups' :
                                    {'Race only'     : {'by_variables' : ['race']}}},
                'option4' : {'notes' : 'No by variables.',
                            'common_group_vars' : 
                                    [],
                            'by_groups' :
                                    {'No vars'     : {'by_variables' : []}}}
                                },
                'geo_levels' : ['Block','Tract','County']                         
                }

        block_hhage_df = add_hhage.run_random_merge_2dfs(rounds)

        return block_hhage_df

    def hui_randommerge_base_H17_H18(self, primary_df, secondary_df):
        """
        Randome Core base inventory with householder age.
        """
        print("\n***************************************")
        print("    Merge householder age with block data.")
        print("***************************************\n")

        add_hhage = add_new_char_by_random_merge_2dfs(
        dfs = {'primary'  : {'data': primary_df, 
                        'primarykey' : 'huid',
                        'geolevel' : 'Block',
                        'geovintage' :'2010',
                        'notes' : 'Household level data without householder age.'},
            'secondary' : {'data': secondary_df, 
                        'primarykey' : 'uniqueidH17',
                        'geolevel' : 'Block',
                        'geovintage' :'2010',
                        'notes' : 'Householder age group data.'}},
        seed = self.seed,
        common_group_vars = ['ownershp','family','numprec'],
        new_char = 'agegroupH17',
        extra_vars= ['sex','agegroupH18'],
        geolevel = "Block",
        geovintage = "2010",
        by_groups = {'All'     : {'by_variables' : ['hispan','race']}},
        fillna_value= -999,
        state_county = self.state_county,
        outputfile = "hui_base_H17_H18",
        outputfolder = self.outputfolders['RandomMerge'])

        # Set up round options
        rounds = {'options': {
                'option1' : {'notes' : 'By original common group vars and by groups variables.',
                            'common_group_vars' : 
                                    ['ownershp','family','numprec'],
                            'by_groups' :
                                    add_hhage.by_groups},
                'option2' : {'notes' : 'Try race only second to get single person households.',
                            'common_group_vars' : 
                                    ['ownershp','family','numprec'],
                            'by_groups' :
                                    {'Race only'     : {'by_variables' : ['race']}}},
                'option3' : {'notes' : 'Third try to get single person households.',
                            'common_group_vars' : 
                                    ['ownershp','family','numprec'],
                            'by_groups' :
                                    {'No vars'     : {'by_variables' : []}}},
                'option4' : {'notes' : 'Remove numprec from options.',
                            'common_group_vars' : 
                                    ['ownershp','family'],
                            'by_groups' :
                                    add_hhage.by_groups},
                'option5' : {'notes' : 'Try race only without numprec.',
                            'common_group_vars' : 
                                    ['ownershp','family'],
                            'by_groups' :
                                    {'Race only'     : {'by_variables' : ['race']}}},
                'option6' : {'notes' : 'Third try to match on family and ownershp.',
                            'common_group_vars' : 
                                    ['ownershp','family'],
                            'by_groups' :
                                    {'No vars'     : {'by_variables' : []}}},
                'option7' : {'notes' : 'Remove family from options.',
                            'common_group_vars' : 
                                    ['ownershp'],
                            'by_groups' :
                                    add_hhage.by_groups},
                'option8' : {'notes' : 'Try race only without family.',
                            'common_group_vars' : 
                                    ['ownershp'],
                            'by_groups' :
                                    {'Race only'     : {'by_variables' : ['race']}}},
                'option9' : {'notes' : 'Third try to match on ownershp.',
                            'common_group_vars' : 
                                    ['ownershp'],
                            'by_groups' :
                                    {'No vars'     : {'by_variables' : []}}},
                                },
                'geo_levels' : ['Block','Tract','County']                         
                }

        block_hui_df = add_hhage.run_random_merge_2dfs(rounds)

        return block_hui_df

    def hui_tidy_H18(self):
        """
        Obtain, Tidy, and transfor data with sex, family type, and numprec by
        householder age.

        """

        print("\n***************************************")
        print("    Set up Data structures for obtaining data.")
        print("***************************************\n")
        vintage = '2010'
        dataset_name = 'dec/sf1' 
        group = 'H18'
        hhagetenurefamilytype_H18 = createAPI_datastructure.obtain_api_metadata(
                vintage = vintage,
                dataset_name = dataset_name,
                group = group,
                outputfolder = self.outputfolder,
                version_text = self.version_text)
        
        # Need to add graft chars to metadata
        # Graft chars are used to check the merge by variables in grafting function
        hhagetenurefamilytype_H18['metadata']['graft_chars'] = \
            ['ownershp','agegroupH18']

        print("\n***************************************")
        print("   Obtain and Clean H18 Data.")
        print("***************************************\n")
        block_df = {}
        block_df["H18"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'block',
                                        vintage = "2010", 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [hhagetenurefamilytype_H18],
                                        outputfolders = self.outputfolders,
                                        outputfile = "hui_H18")

        
        # Add random age
        print("Add random age and H18 age groups.")
        block_df["H18"] = self.add_randage(
                                    block_df["H18"],
                                    seed = self.seed,
                                    varname = 'randageH18')
        # Add agegroups
        block_df["H18"] = self.add_H18age_groups(
                                    block_df["H18"],
                                    varname = 'randageH18')

        return block_df["H18"]


    def hui_randommerge_H17_H18(self, primary_df, secondary_df):
        """
        Random merge H17 with H18 - one new char at a time.
        """
        print("\n***************************************")
        print("    Set up primary data to have common age group.")
        print("***************************************\n")

        # Add random age
        print("Add random age and H18 age groups.")
        primary_df = self.add_randage(
                                    primary_df,
                                    seed = self.seed,
                                    varname = 'randageH18')
        # Add agegroups
        primary_df = self.add_H18age_groups(
                                    primary_df,
                                    varname = 'randageH18')

        print("\n***************************************")
        print("    Merge householder age with block data.")
        print("***************************************\n")

        add_H18 = add_new_char_by_random_merge_2dfs(
        dfs = {'primary'  : {'data': primary_df, 
                        'primarykey' : 'uniqueidH17',
                        'geolevel' : 'Block',
                        'geovintage' :'2010',
                        'notes' : 'Household level data with householder age.'},
            'secondary' : {'data': secondary_df, 
                        'primarykey' : 'uniqueidH18',
                        'geolevel' : 'Block',
                        'geovintage' :'2010',
                        'notes' : 'Sex and family type data.'}},
        seed = self.seed,
        common_group_vars = ['ownershp','agegroupH18'],
        new_char = 'numprec',
        extra_vars= ['sex','family'],
        geolevel = "Block",
        geovintage = "2010",
        by_groups = {'none'     : {'by_variables' : []}},
        fillna_value= -888,
        state_county = self.state_county,
        outputfile = "hui_H17_18",
        outputfolder = self.outputfolders['RandomMerge'])

        # Set up round options
        rounds = {'options': {
                'option1' : {'notes' : 'By original common group vars and by groups variables.',
                            'common_group_vars' : 
                                    add_H18.common_group_vars,
                            'by_groups' :
                                    add_H18.by_groups}
                                },
                'geo_levels' : ['Block']                         
                }

        block_H17H18_df = add_H18.run_random_merge_2dfs(rounds)

        return block_H17H18_df

    def run_prec_workflow(self):
        """
        Workflow to produce Person Record Inventory
        """
        # Starty empty containers to store block level and tract level data
        tract_df = {}
        block_df = {}

        # Save output description as text
        output_filename = f'prec_{self.version_text}_{self.state_county}_{self.basevintage}_rs{self.seed}'
        log_filepath = self.outputfolders['logfiles']+"/"+output_filename+'.log'
        # start log file
        logfile.start(log_filepath)
        self.save_environment_version_details()

        print("\n***************************************")
        print("    Obtain and clean core person record characteristics for",self.state_county_name)
        print("***************************************\n")

                # Generate Person by Age, Sex, Race, and Hispanic
        block_df["preci"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'block',
                                        vintage = "2010", 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [sexbyage_P12_varstem_roots],
                                        outputfolders = self.outputfolders,
                                        outputfile = "CorePRECI")

        block_df["precihispan"] = BaseInventory.graft_on_new_char(base_inventory= block_df['preci'],
                                        state_county = self.state_county,
                                        new_char = 'hispan',
                                        new_char_dictionaries = [sexbyage_P12HAI_varstem_roots,
                                            hispan_byrace_P5_varstem_roots
                                            ],
                                        outputfile = "preci",
                                        outputfolders = self.outputfolders)

        # Generate sex by age with individual years
        vintage = '2010'
        dataset_name = 'dec/sf1' 
        group = 'PCT12'
        sexbyage_PCT12 = createAPI_datastructure.obtain_api_metadata(
                            vintage = vintage,
                            dataset_name = dataset_name,
                            group = group,
                            outputfolder = self.outputfolder)

        tract_df["PCT12"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'tract',
                                        vintage = "2010", 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [sexbyage_PCT12],
                                        outputfolders = self.outputfolders,
                                        outputfile = group)

        # Add random age to block_df["precihispan"]
        block_df["precihispan"] = self.add_randage(
                                    block_df["precihispan"],
                                    seed = self.seed,
                                    varname = 'randageP12')
        # Add agegroups to block_df["precihispan"]
        block_df["precihispan"] = self.add_P12age_groups(
                                    block_df["precihispan"],
                                    varname = 'randageP12')
        # Add random age to tract_df["PCT12"]
        tract_df["PCT12"] = self.add_randage(
                                    tract_df["PCT12"],
                                    seed = self.seed,
                                    varname = 'randagePCT12')
        # Add agegroups to tract_df["PCT12"]
        tract_df["PCT12"] = self.add_P12age_groups(
                                    tract_df["PCT12"],
                                    varname = 'randagePCT12')

                        
        print("\n***************************************")
        print("    Person Block by Age with Tract by Age data.")
        print("***************************************\n")

        add_age = add_new_char_by_random_merge_2dfs(
        dfs = {'primary'  : {'data': block_df["precihispan"], 
                        'primarykey' : 'precid',
                        'geolevel' : 'Block',
                        'geovintage' :'2010',
                        'notes' : 'Block agegroup, sex, race, ethnicity data.'},
            'secondary' : {'data': tract_df["PCT12"], 
                        'primarykey' : 'uniqueidPCT12',
                        'geolevel' : 'Tract',
                        'geovintage' :'2010',
                        'notes' : 'Tract single age years, sex data.'}},
        seed = self.seed,
        common_group_vars = ['agegroupP12'],
        new_char = 'randagePCT12',
        geolevel = "Tract",
        geovintage = "2010",
        by_groups = {'All' : {'by_variables' : ['sex']}},
        fillna_value= -999,
        state_county = self.state_county,
        outputfile = "preci_randomage",
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
        print("    Try to polish final hui data.")
        print("***************************************\n")

        prec_df = self.final_prec_polish(prec_age_df['primary'])

        print("\n***************************************")
        print("    Save cleaned data file.")
        print("***************************************\n")

        csv_filepath = self.outputfolders['top']+"/"+output_filename+'.csv'
        savefile = sys.path[0]+"/"+csv_filepath
        prec_df.to_csv(savefile, index=False)
        print("File saved:",savefile)

        # Stop log file
        logfile.stop()

        return prec_df

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
            'a value to work for all obersvations. Remove ranincome after set.'+\
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
        Without this step the descriptive statisitcs (median, mean)
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

    def add_randage(self, df, seed, varname):
        random_generator = np.random.RandomState(seed)

        output_df = df.copy()

        # Add random age value
        # Check if minage years ==  maxage years
        minage_equals_maxage = (output_df['minageyrs'] == output_df['maxageyrs'])
        minage_notmissing = (output_df['minageyrs'].notnull())
        conditions = minage_equals_maxage & minage_notmissing
        output_df.loc[conditions, varname] = output_df['minageyrs']

        # If min age is less than max age then use random number generator
        minage_less_maxage = (output_df['minageyrs'] < output_df['maxageyrs'])
        conditions = minage_less_maxage & minage_notmissing
        output_df.loc[conditions, varname] = output_df[conditions].apply(lambda x: \
            random_generator.randint(x['minageyrs'],x['maxageyrs']), axis=1)

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

    def add_P12age_groups(self,input_df,varname):
        """
        Add age groups for PC12
        """

        output_df = input_df.copy()

        agegroupP12_dict = {1: {'minageyrs': 0, 'maxageyrs': 4},
        2: {'minageyrs': 5, 'maxageyrs': 9},
        3: {'minageyrs': 10, 'maxageyrs': 14},
        4: {'minageyrs': 15, 'maxageyrs': 17},
        5: {'minageyrs': 18, 'maxageyrs': 19},
        6: {'minageyrs': 20, 'maxageyrs': 20},
        7: {'minageyrs': 21, 'maxageyrs': 21},
        8: {'minageyrs': 22, 'maxageyrs': 24},
        9: {'minageyrs': 25, 'maxageyrs': 29},
        10: {'minageyrs': 30, 'maxageyrs': 34},
        11: {'minageyrs': 35, 'maxageyrs': 39},
        12: {'minageyrs': 40, 'maxageyrs': 44},
        13: {'minageyrs': 45, 'maxageyrs': 49},
        14: {'minageyrs': 50, 'maxageyrs': 54},
        15: {'minageyrs': 55, 'maxageyrs': 59},
        16: {'minageyrs': 60, 'maxageyrs': 61},
        17: {'minageyrs': 62, 'maxageyrs': 64},
        18: {'minageyrs': 65, 'maxageyrs': 66},
        19: {'minageyrs': 67, 'maxageyrs': 69},
        20: {'minageyrs': 70, 'maxageyrs': 74},
        21: {'minageyrs': 75, 'maxageyrs': 79},
        22: {'minageyrs': 80, 'maxageyrs': 84},
        22: {'minageyrs': 85, 'maxageyrs': 110}}

        for agegroup in agegroupP12_dict:
            randincome_greater_than = \
                (output_df[varname] >= agegroupP12_dict[agegroup]['minageyrs'])
            randincome_less_than    = \
                (output_df[varname] <= agegroupP12_dict[agegroup]['maxageyrs'])
            conditions = randincome_greater_than & randincome_less_than
            output_df.loc[conditions,'agegroupP12'] = agegroup

        # Add 0 agegroup - for no age data
        randage_missing =  (output_df[varname].isnull())
        conditions = randage_missing
        output_df.loc[conditions,'agegroupP12'] = 0

        return output_df
    
    def add_H17age_groups(self,input_df,varname):
        """
        Add age groups for H17
        """

        output_df = input_df.copy()

        agegroupH17_dict = {1:{'minageyrs': 15, 'maxageyrs': 24},
        2: {'minageyrs': 25, 'maxageyrs': 34},
        3: {'minageyrs': 35, 'maxageyrs': 44},
        4: {'minageyrs': 45, 'maxageyrs': 54},
        5: {'minageyrs': 55, 'maxageyrs': 59},
        6: {'minageyrs': 60, 'maxageyrs': 64},
        7: {'minageyrs': 65, 'maxageyrs': 74},
        8: {'minageyrs': 75, 'maxageyrs': 84},
        9: {'minageyrs': 85, 'maxageyrs': 110}}

        for agegroup in agegroupH17_dict:
            randvar_greater_than = \
                (output_df[varname] >= agegroupH17_dict[agegroup]['minageyrs'])
            randvar_less_than    = \
                (output_df[varname] <= agegroupH17_dict[agegroup]['maxageyrs'])
            conditions = randvar_greater_than & randvar_less_than
            output_df.loc[conditions,'agegroupH17'] = agegroup

        # Add 0 agegroup - for no age data
        randage_missing =  (output_df[varname].isnull())
        conditions = randage_missing
        output_df.loc[conditions,'agegroupH17'] = 0

        return output_df

    @staticmethod
    def add_H18age_groups(input_df,varname):
        """
        Add age groups for H18
        """

        output_df = input_df.copy()

        agegroupH18_dict = {1: {'minageH18': 15, 'maxageH18': 34},
            2: {'minageH18': 35, 'maxageH18': 64},
            3: {'minageH18': 65, 'maxageH18': 110}}

        for agegroup in agegroupH18_dict:
            randvar_greater_than = \
                (output_df[varname] >= agegroupH18_dict[agegroup]['minageH18'])
            randvar_less_than    = \
                (output_df[varname] <= agegroupH18_dict[agegroup]['maxageH18'])
            conditions = randvar_greater_than & randvar_less_than
            output_df.loc[conditions,'agegroupH18'] = agegroup

        # Add 0 agegroup - for no age data
        randage_missing =  (output_df[varname].isnull())
        conditions = randage_missing
        output_df.loc[conditions,'agegroupH18'] = 0

        return output_df

    @staticmethod
    def add_B19037age_groups(input_df,varname):
        """
        Add age groups for B19037
        """

        output_df = input_df.copy()

        agegroupB19037_dict = {1: {'minageyrs': 15, 'maxageyrs': 25},
            2: {'minageyrs': 25, 'maxageyrs': 44},
            3: {'minageyrs': 45, 'maxageyrs': 64},
            4: {'minageyrs': 65, 'maxageyrs': 110}}

        for agegroup in agegroupB19037_dict:
            randvar_greater_than = \
                (output_df[varname] >= agegroupB19037_dict[agegroup]['minageyrs'])
            randvar_less_than    = \
                (output_df[varname] <= agegroupB19037_dict[agegroup]['maxageyrs'])
            conditions = randvar_greater_than & randvar_less_than
            output_df.loc[conditions,'agegroupB19037'] = agegroup

        # Add 0 agegroup - for no age data
        randage_missing =  (output_df[varname].isnull())
        conditions = randage_missing
        output_df.loc[conditions,'agegroupB19037'] = 0

        return output_df

    @staticmethod
    def add_P43age_groups(input_df,varname):
        """
        Add age groups for P43
        """

        output_df = input_df.copy()

        agegroupP43_dict = {1: {'minageyrs': 0, 'maxageyrs': 17},
            2: {'minageyrs': 18, 'maxageyrs': 64},
            3: {'minageyrs': 65, 'maxageyrs': 110}}

        for agegroup in agegroupP43_dict:
            randvar_greater_than = \
                (output_df[varname] >= agegroupP43_dict[agegroup]['minageyrs'])
            randvar_less_than    = \
                (output_df[varname] <= agegroupP43_dict[agegroup]['maxageyrs'])
            conditions = randvar_greater_than & randvar_less_than
            output_df.loc[conditions,'agegroupP43'] = agegroup

        # Add 0 agegroup - for no age data
        randage_missing =  (output_df[varname].isnull())
        conditions = randage_missing
        output_df.loc[conditions,'agegroupP43'] = 0

        return output_df

    def hui_tidy_P43(self):
        """
        Obtain, Tidy, and transfor data with population
        in group quarters by age, sex, by gqtype

        """

        print("\n***************************************")
        print("    Set up Data structures for obtaining data.")
        print("***************************************\n")
        vintage = '2010'
        dataset_name = 'dec/sf1' 
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
                                        vintage = "2010", 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [prec_P43_dict],
                                        outputfolders = self.outputfolders,
                                        outputfile = "P43")


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

    @staticmethod
    def drop_extra_prechui_columns(input_df):
        """
        Need to drop columns used to predict varaibles
        """

        output_df = input_df.copy()

        drop_if_starts_with = ['agegroup']
        for substring in drop_if_starts_with:
            drop_vars = [col for col in output_df if col.startswith(substring)]
            #print(drop_vars)
            # Drop columns
            output_df = output_df.drop(drop_vars, axis=1)

        return output_df

    def save_incore_version010(self, input_df):
        """
        IN-CORE expects specific columns
        Alpha release of housing unit inventories had 
        columns in specific order
        """

        output_df = input_df.copy()
        # create list of all required ergo:buildingInventoryVer6 columns
        incore_columns = incore_v010_DataStructure
        
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
                hui_col_name = incore_columns[incore_col]['huiv0-2-0']
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

        output_df = output_df[output_col_order]
        # Save output description as text
        output_filename = f'hui_v0-1-0_{self.state_county}_{self.basevintage}_rs{self.seed}'
        print("\n***************************************")
        print("    Save IN-CORE v0.1.0 data file.")
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
            print("Make category 0 for numprec, vacancy and gqtype")  
            output_df = self.fill_missingvalues(output_df)
            print("Drop extra columns.")                                                                  
            output_df = self.drop_extra_columns(output_df)
        except:
            print('Data not ready for final polish.')

        return output_df
    
    def final_prec_polish(self, input_df):
        
        output_df = input_df.copy()
        try:
            print("Drop extra columns.")                                                                  
            output_df = self.drop_extra_columns(output_df)
        except:
            print('Data not ready for final polish.')

        return output_df


    def hui_tidy_B19037(self):
        """
        Obtain, Tidy, and transfor data with income by householder
        age, race and ethnicity

        """

        print("\n***************************************")
        print("    Set up Data structures for obtaining data.")
        print("***************************************\n")
        vintage = '2012'
        dataset_name = 'acs/acs5' 
        group = 'B19037'
        hhincomebyage_B19037 = createAPI_datastructure.obtain_api_metadata(
                vintage = vintage,
                dataset_name = dataset_name,
                group = group,
                outputfolder = self.outputfolder,
                version_text = self.version_text)
        
        # Need to add graft chars to metadata
        # Graft chars are used to check the merge by variables in grafting function
        hhincomebyage_B19037['metadata']['graft_chars'] = ['minageyrs','maxageyrs','mindollars','maxdollars','race']

        hhincomebyage_B19037HAG = createAPI_datastructure.add_byracehispan(hhincomebyage_B19037,
                byracehispan_groups = acsbyracehispan_All,
                byracehispan_groups_mx = acsbyracehispan_HAG,
                newgroup = "HAG",
                newcharbyvar = '')
        
        print("\n***************************************")
        print("   Obtain and Clean B19037 Data.")
        print("***************************************\n")
        tract_df = {}
        tract_df["B19037"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'tract',
                                        vintage = "2012", 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [hhincomebyage_B19037HAG],
                                        outputfolders = self.outputfolders,
                                        outputfile = "B19037")


        # Add random age
        print("Add random age and B19037 age groups.")
        tract_df['B19037'] = self.add_randage(
                                    tract_df['B19037'],
                                    seed = self.seed,
                                    varname = 'randageB19037')
        # Add agegroups to block_df["hhage_hispan"]
        tract_df['B19037'] = self.add_B19037age_groups(
                                    tract_df["B19037"],
                                    varname = 'randageB19037')

        return tract_df['B19037']