# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import numpy as np
import pandas as pd
import os # For saving output to path
import sys



from pyincore_data_addons.SourceData.api_census_gov.BaseInventoryv3 \
    import BaseInventory

# Load in data structure dictionaries
from pyincore_data_addons.SourceData.api_census_gov.CreateAPI_DataStructure \
    import createAPI_datastructure
from pyincore_data_addons.SourceData.api_census_gov.acg_00a_general_datastructures \
    import *

class tidy_censusapi():
    """
    To obtain and tidy source data.
    Tidy refers to converting json files into long dataframes
    Where rows represent housing units or persons
    and columns represent distinct categorical values

    """

    def __init__(self,
            state_county: str,
            state_county_name: str,
            seed: int = 9876,
            version: str = '0.2.0',
            version_text: str = 'v0-2-0',
            basevintage: str = 2010,
            basegeolevel: str = 'Block',
            outputfolder: str = "",
            outputfolders = {}):

        self.state_county = state_county
        self.state_county_name = state_county_name
        self.seed = seed
        self.version = version
        self.version_text = version_text
        self.basevintage = basevintage
        self.basegeolevel = basegeolevel
        self.outputfolder = outputfolder
        self.outputfolders = outputfolders

    def tidy_group(self,group: str = 'P43',
                        vintage: str = '2010',
                        dataset_name: str = 'dec/sf1',
                        geo_level: str = 'block',
                        graft_chars: str = ['gqtype']):
        """
        Obtain, Tidy, and transform data for a group

        group = str that represents Census data group
        """

        print("\n***************************************")
        print("    Set up Data structures for obtaining data.")
        print("***************************************\n")
        prec_dict = createAPI_datastructure.obtain_api_metadata(
                vintage = vintage,
                dataset_name = dataset_name,
                group = group,
                outputfolder = self.outputfolder,
                version_text = self.version_text)

        # Need to add graft chars to metadata
        # Graft chars are used to check the merge by variables in grafting function
        prec_dict['metadata']['graft_chars'] = graft_chars
        
        print("\n***************************************")
        print("   Obtain and Clean group Data.")
        print("***************************************\n")
        group_df = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = geo_level,
                                        vintage = vintage, 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [prec_dict],
                                        outputfolders = self.outputfolders,
                                        outputfile = group)

        return group_df

    def add_category_group(self,input_df, group: str = 'P43'):
        """
        Different census data groups use different age categories.
        Using the minimum and maximum age years values
        this function generates a random age and then 
        looks to see what age category the observation falls within.
        This helps to link different groups of data.
        """
        output_df = input_df.copy()
        output_df = self.add_randage(
                                    output_df,
                                    seed = self.seed,
                                    randage_var = 'randage'+group)
        
        output_df = self.add_age_groups(
                                    output_df,
                                    group = group,
                                    randage_var = 'randage'+group)

        return output_df
    
    @staticmethod
    def add_randage(df, seed, randage_var):
        """
        code to add a random age
        """
        random_generator = np.random.RandomState(seed)

        output_df = df.copy()

        # Add random age value
        # Check if minage years ==  maxage years
        minage_equals_maxage = (output_df['minageyrs'] == output_df['maxageyrs'])
        minage_notmissing = (output_df['minageyrs'].notnull())
        conditions = minage_equals_maxage & minage_notmissing
        output_df.loc[conditions, randage_var] = output_df['minageyrs']

        # If min age is less than max age then use random number generator
        minage_less_maxage = (output_df['minageyrs'] < output_df['maxageyrs'])
        conditions = minage_less_maxage & minage_notmissing
        output_df.loc[conditions, randage_var] = output_df[conditions].apply(lambda x: \
            random_generator.randint(x['minageyrs'],x['maxageyrs']), axis=1)

        return output_df

    @staticmethod
    def add_age_groups(input_df,
                        agegroup_dict = {},
                        group: str = 'P43', 
                        randage_var: str = 'randageP43'):
        """
        Add age groups based on dictionary
        """

        output_df = input_df.copy()

        # Program has the following preset age group dictionaries
        # But user can provide their own
        if agegroup_dict == {}:
            agegroup_dicts = {
                'P43' :{1: {'minageyrs': 0, 'maxageyrs': 17},
                        2: {'minageyrs': 18, 'maxageyrs': 64},
                        3: {'minageyrs': 65, 'maxageyrs': 110}},
                'H17' :{0: {'minageyrs': 0, 'maxageyrs': 14},
                        1: {'minageyrs': 15, 'maxageyrs': 24},
                        2: {'minageyrs': 25, 'maxageyrs': 34},
                        3: {'minageyrs': 35, 'maxageyrs': 44},
                        4: {'minageyrs': 45, 'maxageyrs': 54},
                        5: {'minageyrs': 55, 'maxageyrs': 59},
                        6: {'minageyrs': 60, 'maxageyrs': 64},
                        7: {'minageyrs': 65, 'maxageyrs': 74},
                        8: {'minageyrs': 75, 'maxageyrs': 84},
                        9: {'minageyrs': 85, 'maxageyrs': 110}},
                'H18' :{0: {'minageyrs': 0, 'maxageyrs': 14},
                        1: {'minageyrs': 15, 'maxageyrs': 34},
                        2: {'minageyrs': 35, 'maxageyrs': 64},
                        3: {'minageyrs': 65, 'maxageyrs': 110}}}
            agegroup_dict = agegroup_dicts[group]

        for agegroup in agegroup_dict:
            randvar_greater_than = \
                (output_df[randage_var] >= agegroup_dict[agegroup]['minageyrs'])
            randvar_less_than    = \
                (output_df[randage_var] <= agegroup_dict[agegroup]['maxageyrs'])
            conditions = randvar_greater_than & randvar_less_than
            output_df.loc[conditions,'agegroup'+group] = agegroup

        # Add 0 agegroup - for no age data
        randage_missing =  (output_df[randage_var].isnull())
        conditions = randage_missing
        output_df.loc[conditions,'agegroup'+group] = 0

        return output_df

    @staticmethod
    def create_conditionset(df,primary_key,conditionset):
        """
        Frequent task to locate obseravtions based on a set of conditions
        Condition set needs to be a dictionary.
        Condtions need to be in double quotes and will be evaluated in funciton.

        example conditionset:
            conditionset = {
                    'not_householder'       : "(df['person_record_counter'] != 1)",
                    'Husband-wife family'   : "(df['sex'] == -999)",
                    'Assume child'          : "(df['person_record_counter'] > 2)",
                    'Not Group Quarters'    : "(df['gqtype'] == 0)"}

        """

        # Start conditions with all observations
        conditions = (df[primary_key].notnull())
        for condition in conditionset.keys():
            conditions = conditions & eval(conditionset[condition])

        return conditions