# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import numpy as np
import pandas as pd
import os # For saving output to path
import sys

# Save output as a log file function
from pyincore_data_addons \
     import save_output_log as logfile

from pyincore_data_addons.SourceData.api_census_gov.BaseInventoryv3 import BaseInventory
from pyincore_data_addons.SourceData.api_census_gov.run_hu_prec_workflow import run_hui_prec_workflow
from pyincore_data_addons.SourceData.api_census_gov.hui_add_categorical_char \
     import add_new_char_by_random_merge_2dfs
from pyincore_data_addons.SourceData.api_census_gov.tidy_censusapi \
    import tidy_censusapi

# Load in data structure dictionaries
from pyincore_data_addons.SourceData.api_census_gov.CreateAPI_DataStructure \
    import createAPI_datastructure
from pyincore_data_addons.SourceData.api_census_gov.acg_00a_general_datastructures import *

class merge_hu_prec_worklow():
    """
    Functions to merge housing unit and person record inventories.

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

    def adjust_numprec7_hui(self,hui_df,prec_df,verify_results = False):
        """
        In the housing unit inventory the max housing unit size is 7
        But housing units can have more than 7 persons.
        The difference between the person record inventory and the 
        housing unit inventory provides a guess for how large 
        housing units could be within a block.
        """

        geo_id = self.basegeolevel+str(self.basevintage)+'str'

        # Find total population by housing unit inventory numprec
        total_pop_by_numprec_df = pd.pivot_table(hui_df, 
                        values='numprec', 
                        index=[geo_id],
                        aggfunc=np.sum)
        total_pop_by_numprec_df.reset_index(inplace = True)
        total_pop_by_numprec_df = total_pop_by_numprec_df.rename(columns = \
            {'numprec' : 'total_population_numprec'})

        # How many housing units have 7 persons (and not group quarters)
        condition1 = (hui_df['numprec'] == 7)
        condition2 = (hui_df['gqtype'] == 0)
        conditions = condition1 & condition2
        total_count_7numprec_df = pd.pivot_table(hui_df[conditions], 
                                values='huid', 
                                index=[geo_id],
                                aggfunc='count')
        total_count_7numprec_df.reset_index(inplace = True)
        total_count_7numprec_df = total_count_7numprec_df.rename(columns = \
            {'huid' : 'count_7numprec'})

        # Merge total population and count of numprec
        fix_7numprec_df = pd.merge(left = total_pop_by_numprec_df,
                        right = total_count_7numprec_df,
                        left_on = geo_id,
                        right_on = geo_id,
                        how = 'left')
        fix_7numprec_df['count_7numprec'] = fix_7numprec_df['count_7numprec'].fillna(value=0)

        # Find Total Population by count of Person Records
        total_pop_by_preci_df = pd.pivot_table(prec_df, 
                        values='precid', 
                        index=[geo_id],
                        aggfunc='count')
        total_pop_by_preci_df.reset_index(inplace = True)
        total_pop_by_preci_df = total_pop_by_preci_df.rename(columns = \
            {'precid' : 'total_population_prec'})

        # Merge total population and count of numprec
        fix_7numprec_df = pd.merge(left = fix_7numprec_df,
                        right = total_pop_by_preci_df,
                        left_on = geo_id,
                        right_on = geo_id,
                        how = 'left')

        # Calculate population difference
        fix_7numprec_df.loc[:,'pop_difference'] = \
            fix_7numprec_df['total_population_prec'] - \
            fix_7numprec_df['total_population_numprec']
        fix_7numprec_df.loc[:,'difference_per7numprec'] = \
            fix_7numprec_df['pop_difference'] / fix_7numprec_df['count_7numprec']
        fix_7numprec_df['difference_per7numprec'] = \
            fix_7numprec_df['difference_per7numprec'].fillna(value=0)
        
        # Create a new numprec value based on difference
        fix_7numprec_df.loc[:,'new_numprec'] =  7 + fix_7numprec_df['pop_difference']
        # Add numprec value for merge
        fix_7numprec_df.loc[:,'numprec']  = 7

        # Merge new numprec by block with 7 person households
        hui_adjusted_numprec_df = pd.merge(left = hui_df,
                        right = fix_7numprec_df[[geo_id,'numprec','new_numprec']],
                        left_on = [geo_id,'numprec'],
                        right_on = [geo_id,'numprec'],
                        how = 'left')

        # update numprec
        hui_adjusted_numprec_df['numprec'] = \
            hui_adjusted_numprec_df['new_numprec'].fillna(hui_adjusted_numprec_df['numprec'])
        # Drop duplicate columns
        hui_adjusted_numprec_df = hui_adjusted_numprec_df.drop(['new_numprec'], axis=1)

        # Options to verify results
        if verify_results == True:
            verify_tables = {}
            table_df = hui_adjusted_numprec_df
            table = pd.pivot_table(table_df, values='huid', index=['numprec'],
                                    margins = True, margins_name = 'Total',
                                    columns=['gqtype'], aggfunc='count')
            verify_tables['Numprec by GQ Type'] = table
            verify_tables['Descriptive Stats'] = fix_7numprec_df.describe().T
            table_df = fix_7numprec_df
            table = pd.pivot_table(table_df, 
                                values=['total_population_numprec','total_population_prec','pop_difference'],
                                margins = True, margins_name = 'Total',
                                index=['count_7numprec'], aggfunc=np.sum)
            verify_tables['Total Population differences'] = table
            return hui_adjusted_numprec_df, verify_tables

        return hui_adjusted_numprec_df

    def prepare_hui_df_for_merge(self,hui_df,prec_df):
        """
        Adjust numprec to account for households large than 7 persons.
        Add person record counter

        The characteristics of person 1 are based on characteristics of the householder
        Identify possible characteristics of person 2 - 7+
        
        """
        # Save output description as text
        output_filename = f'prechui_{self.version_text}_{self.state_county}_{self.basevintage}_rs{self.seed}'
        self.output_filename = output_filename
        log_filepath = self.outputfolders['logfiles']+"/"+output_filename+'.log'
        # start log file
        logfile.start(log_filepath)

        hui_adjusted_numprec_df = self.adjust_numprec7_hui(
            hui_df = hui_df,
            prec_df = prec_df)

        # Only expand observations with numprec greater than 0
        condition1 = (hui_adjusted_numprec_df['numprec'] > 0)
        expand_df = hui_adjusted_numprec_df.loc[condition1]
        hui_numprec = BaseInventory.expand_df(df = expand_df, expand_var= 'numprec')

        # Add Person Record Number
        hui_numprec['pernum'] = hui_numprec.groupby(['huid']).cumcount() + 1
        # Create new uniqueid based on huid and counter
        # Need to zero pad part2 find the max number of characters
        part2_max = hui_numprec['pernum'].max()
        part2_maxdigits = len(str(part2_max))
        hui_numprec.loc[:,'uniquehuid_numprec'] = hui_numprec['huid'] + \
            hui_numprec['pernum'].apply(lambda x : str(int(x)).zfill(part2_maxdigits))

        # Add numprec back to expanded data
        hui_numprec = pd.merge(left = hui_numprec,
                               right = expand_df[['huid','numprec']],
                               on = 'huid',
                               how = 'left')

        print("\n***************************************")
        print("    Work On Family Structure.")
        print("***************************************\n")
        # Expanded Housing unit inventory expands householder
        # race, hispan, sex, and age to all householders
        # Need to adjust these to predict household structure
        # Potentially use P20 and P29 to refine household structure

        assume_child_husbandwifefamily = {
                'not_householder'       : "(df['pernum'] != 1)",
                'Family'                : "(df['family'] == 1)",
                'Husband-wife'          : "(df['sex'] == -999)",
                'Assume child obs'      : "(df['pernum'] > 2)",
                'Not Group Quarters'    : "(df['gqtype'] == 0)"}
        assume_child_singleparent = {
                'not_householder'       : "(df['pernum'] != 1)",
                'Family'                : "(df['family'] == 1)",
                'Single Parent'         : "(df['sex'] == 1) | (df['sex'] == 2)",
                'Assume child obs'      : "(df['pernum'] > 1)",
                'Not Group Quarters'    : "(df['gqtype'] == 0)"}

        # Set age to not set for assumed children in households
        for conditionset in [assume_child_husbandwifefamily,assume_child_singleparent]:
            assume_child_obs = tidy_censusapi.create_conditionset(
                df = hui_numprec,
                primary_key = 'huid',
                conditionset = conditionset)
            # Reset agegroup and sex to -999
            hui_numprec.loc[assume_child_obs,'agegroupH17'] = -999
            hui_numprec.loc[assume_child_obs,'agegroupH18'] = -999
            hui_numprec.loc[assume_child_obs,'sex'] = -999
            hui_numprec.loc[assume_child_obs,'child'] = 1

        # Fill in missing child values
        hui_numprec['child'] = hui_numprec['child'].fillna(value=-999)

        # Need to reset age to -999 for other household members
        notchild_or_spouse = {
                'not_householder'       : "(df['pernum'] != 1)",
                'Not Spouse'            : "(df['pernum'] != 2)",
                'Not assumed child'     : "(df['child'] != 1)",
                'Not Group Quarters'    : "(df['gqtype'] == 0)"}
        reset_age = tidy_censusapi.create_conditionset(
                df = hui_numprec,
                primary_key = 'huid',
                conditionset = notchild_or_spouse)
        # Reset agegroup and sex to -999
        hui_numprec.loc[reset_age,'agegroupH18'] = -999
        hui_numprec.loc[reset_age,'agegroupH17'] = -999

        print("\n***************************************")
        print("    Work with Group Quarters Data.")
        print("***************************************\n")
        # Need to fill in missing values for group quarters
        # Either with -999 and then P43 - age and sex by group quarters type
        is_gqtype = (hui_numprec['gqtype'] > 0)
        for char_var in ['sex','race','hispan','agegroupH17','agegroupH18']:
            charvar_missing = (hui_numprec[char_var].isnull())
            conditions = is_gqtype & charvar_missing
            # Fill missing values with -999 - not set
            hui_numprec.loc[conditions,char_var] = -999
        # Tidy P43 data
        tidy_groups = tidy_censusapi(
            state_county = self.state_county,
            state_county_name= self.state_county_name,
            seed = self.seed,
            version = self.version,
            version_text = self.version_text,
            basevintage = self.basevintage,
            outputfolder = self.outputfolder,
            outputfolders = self.outputfolders)

        p43_df = tidy_groups.tidy_group(group = 'P43',
                            vintage = '2010',
                            dataset_name = 'dec/sf1',
                            geo_level = 'block',
                            graft_chars = ['gqtype'])

        # Prepare Person Record for merge
        print("Use random age to add P43 age groups.")
        # Add agegroups
        p43_df = tidy_groups.add_category_group(
                                    p43_df,
                                    group = 'P43')
        print("\n***************************************")
        print("    Random merge between Housing Inventory and Group Quarters Records.")
        print("***************************************\n")
        hui_p43 = add_new_char_by_random_merge_2dfs(
            dfs = {'primary'  : {'data': hui_numprec, 
                            'primarykey' : 'uniquehuid_numprec',
                            'geolevel' : 'Block',
                            'geovintage' :'2010',
                            'notes' : 'Housing unit inventory expanded by numprec.'},
                'secondary' : {'data': p43_df, 
                            'primarykey' : 'uniquehuidP43',
                            'geolevel' : 'Block',
                            'geovintage' :'2010',
                            'notes' : 'Group quarters Data.'}},
            seed = self.seed,
            common_group_vars = ['gqtype'],
            new_char = 'agegroupP43',
            extra_vars = ['sex'],
            geolevel = "Block",
            geovintage = "2010",
            by_groups = {'NA' : {'by_variables' : []}},
            fillna_value= -999,
            state_county = self.state_county,
            outputfile = "hui_p43",
            outputfolder = self.outputfolders['RandomMerge'])

        # Set up round options
        rounds = {'options': {
                'option1' : {'notes' : 'Attempt to merge householder on all common group vars.',
                            'common_group_vars' : 
                                    hui_p43.common_group_vars,
                            'by_groups' :
                                    hui_p43.by_groups}
                                },
                'geo_levels' : ['Block']                         
                }
        hui_p43_df = hui_p43.run_random_merge_2dfs(rounds)

        print("\n***************************************")
        print("    Random merge between Housing Inventory and Person Records.")
        print("***************************************\n")
        # Prepare Person Record for merge
        print("Use random age to add P43 age groups.")
        # Add agegroups for merges
        prec_df = tidy_groups.add_age_groups(
                                    prec_df,
                                    group = 'P43',
                                    randage_var = 'randagePCT12')

        prec_df = tidy_groups.add_age_groups(
                                    prec_df,
                                    group = 'H18',
                                    randage_var = 'randagePCT12')
        # H17 has 9 categories vs H18 has 3 
        prec_df = tidy_groups.add_age_groups(
                                    prec_df,
                                    group = 'H17',
                                    randage_var = 'randagePCT12')
                            
        # Add child variable
        prec_df.loc[prec_df['randagePCT12'] >= 18, 'child'] = 0
        prec_df.loc[prec_df['randagePCT12'] < 18, 'child'] = 1

        prec_hui = add_new_char_by_random_merge_2dfs(
            dfs = {'primary'  : {'data': prec_df, 
                            'primarykey' : 'precid',
                            'geolevel' : 'Block',
                            'geovintage' :'2010',
                            'notes' : 'Person records with race, hispan, age, sex.'},
                'secondary' : {'data': hui_p43_df['primary'], 
                            'primarykey' : 'uniquehuid_numprec',
                            'geolevel' : 'Block',
                            'geovintage' :'2010',
                            'notes' : 'Housing Unit Data.'}},
            seed = self.seed,
            common_group_vars = ['agegroupH17','sex','race','hispan'],
            new_char = 'huid',
            extra_vars = ['gqtype','numprec','pernum','family'],
            geolevel = "Block",
            geovintage = "2010",
            by_groups = {'NA' : {'by_variables' : []}},
            fillna_value= -999,
            state_county = self.state_county,
            outputfile = "prec_hui_randomhuid",
            outputfolder = self.outputfolders['RandomMerge'])

        # Set up round options
        rounds = {'options': {
                'householderH17' : {'notes' : 'Attempt to merge householder on all common group vars.',
                            'common_group_vars' : 
                                    ['agegroupH17','sex','race','hispan'],
                            'by_groups' :
                                    prec_hui.by_groups},
                'groupquarters' : {'notes' : 'Attempt to merge for group quarters by sex and agegroup',
                            'common_group_vars' : 
                                    ['sex','agegroupP43'],
                            'by_groups' :
                                    prec_hui.by_groups},  
                'child1' : {'notes' : 'Attempt to add children based on race and hispan.',
                            'common_group_vars' : 
                                    ['race','hispan','child'],
                            'by_groups' :
                                    prec_hui.by_groups},
                'child2' : {'notes' : 'Attempt to add children without race and hispan.',
                            'common_group_vars' : 
                                    ['child'],
                            'by_groups' :
                                    prec_hui.by_groups},
                'spouse' : {'notes' : 'Attempt to merge other persons based on race and hispan.\
                                        Assumes that non child houshold members have same \
                                        race and hispan variables as householder',
                            'common_group_vars' : 
                                    ['race','hispan','agegroupH17'],
                            'by_groups' :
                                    prec_hui.by_groups},
                'householderH18' : {'notes' : 'Attempt to merge householder on all common group vars.',
                            'common_group_vars' : 
                                    ['agegroupH18','sex','race','hispan'],
                            'by_groups' :
                                    prec_hui.by_groups},  
                'others' : {'notes' : 'Attempt to merge add other persons to housing units.\
                                        No race, hispan or sex assumptions.',
                            'common_group_vars' : 
                                    [],
                            'by_groups' :
                                    prec_hui.by_groups}
                                },
                'geo_levels' : ['Block']                         
                }

        prec_hui_df = prec_hui.run_random_merge_2dfs(rounds)

        print("\n***************************************")
        print("    Try to polish final prechui data.")
        print("***************************************\n")
        
        # Sort data by huid and person counter
        prec_hui_df['primary'] = prec_hui_df['primary'].sort_values(by = ['huid','pernum'])
        # move huid to second column
        # Create column list to move primarykey to first column
        primary_key_names = ['precid','huid','pernum','Block2010str']
        columnlist = [col for col in prec_hui_df['primary'] if col not in primary_key_names]
        new_columnlist = primary_key_names + columnlist
        prec_hui_df['primary']  = prec_hui_df['primary'][new_columnlist]

        # drop extra columns
        prec_df = run_hui_prec_workflow.drop_extra_columns(prec_hui_df['primary'])


        print("\n***************************************")
        print("    Save cleaned data file.")
        print("***************************************\n")
        csv_filepath = self.outputfolders['top']+"/"+output_filename+'.csv'
        savefile = sys.path[0]+"/"+csv_filepath
        prec_df.to_csv(savefile, index=False)
        print("File saved:",savefile)
        
        # Stop log file
        logfile.stop()
        
        return prec_hui_df

