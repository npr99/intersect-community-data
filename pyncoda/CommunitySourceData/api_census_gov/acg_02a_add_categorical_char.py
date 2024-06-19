# Copyright (c) 2021 Nathanael Rosenheim, University of Illinois and others. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import requests # Census API Calls
import os  # Operating System (os) For folders and finding working directory
import pandas as pd
import sys  # saving CSV files

from pyncoda.CommunitySourceData.api_census_gov.acg_00b_hui_block2010 import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00c_hispan_block2010 import *

class add_new_char_by_random_merge_2dfs():
    """"
    Function intersects 2 data frames with common variables.

    Function allows for split data frames by subgroups. 
    For example, Hispanic and not Hispanic - 
    Need to have split group data into Hispanic, Any Race and Not Hispanic. 
    These dataframes will not be mutually exclusive. 
    Need to split Hispanic and not Hispanic before merging the data. 
    For family and non-family. This because there are issues with having missing 
    values in the group by and the merge.
    Need to create family and non-family dataframe out of household and family counts. 

    Function returns combined group data that fills in missing values based on match.
    For example, the group B19001 has all households (family and non-family.)
    The group B19101 has family households. Merging the 2 groups creates a new group
    that identifies families and non-families.
    """

    def __init__(self,
        dfs,
        state_county: str,
        seed: int = 2564,
        common_group_vars: list = ['incomegroup'],
        new_char: str = 'family',
        extra_vars: list = [],
        geolevel: str = "Tract",
        geovintage: str = "2010",
        by_groups = {'Hispanic' : ['hispan'], "not Hispanic" : ['race']},
        fillna_value: int = 0,
        outputfile: str = "incomebyfamily",
        outputfolder: str = "",
        check_merge: str = "check_merge",
        reuse_secondary: bool = False,
        savefiles: bool = True):

        self.seed = seed
        self.dfs = dfs
        self.new_char = new_char
        self.extra_vars = extra_vars
        self.fillna_value = fillna_value
        self.geovintage = geovintage
        self.state_county = state_county
        self.outputfile = outputfile
        self.outputfolder = outputfolder
        self.check_merge = check_merge
        self.reuse_secondary = reuse_secondary
        self.savefiles = savefiles

        # Variables that might be updated by rounds
        self.geolevel = geolevel
        self.geovar = self.geolevel+self.geovintage
        self.common_group_vars = common_group_vars
        self.by_groups = by_groups
        # create by groups based on by_groups
        self.groupby_vars = {}
        self.groupby_vars_ascending = {}
        for by_group in self.by_groups:
            self.groupby_vars[by_group] = [self.geovar]+\
                self.by_groups[by_group]['by_variables']+common_group_vars
            # Merge can be shaped by the group by vars
            # Initially assume that all vars used in sort are ascending
            self.groupby_vars_ascending[by_group] = []
            for var in self.groupby_vars[by_group]:
                # Create a list with all true values for group by vars
                self.groupby_vars_ascending[by_group].append(True)

        # The primary keys are critical the random merge process
        # Set primary key
        self.primary_key_name = {}
        for key in self.dfs.keys():
            self.primary_key_name[key] = self.dfs[key]['primarykey']
        # Store primary keys to keep in merges and set flags
        groups = list(self.dfs.keys())
        group1 = groups[0]
        group2 = groups[1]
        self.primary_key_group1 = self.dfs[group1]['primarykey']
        self.primary_key_group2 = self.dfs[group2]['primarykey']
        # Random merge can be conducted in multiple rounds
        self.round = 1

    @staticmethod
    def prepare_randommerge(randmerge_df, seed, 
                       unique_sort_vars = ["huid"],
                       groupby_vars = ["blockid"],
                       groupby_vars_ascending = [True],
                       sort_vars = ["ownershp", "vacancy"],
                       sort_vars_ascending = [True, True]
                       ):
        """
        Set random merge order for Housing Unit Inventory, Address Point Inventory, and Joblists
        Args:
            housing_unit_inventory (pd.DataFrame): Housing unit inventory.
            seed (int): Random number generator seed for reproducibility.
            groupby_vars: List of variables to group data by - default ["blockid"]
            sort_vars: List of variables to sort data by. This will prioritize some observations to be merged first
                default = ["ownershp", "vacancy"]
                The default matches to the prepare_infrastructure_inventory which places residential structures
                with fewer housing units at the top of the list. This sorting helps ensure that
                owner occupied houses are more likely to be linked to single dwelling unit houses.
            unique_sort_vars: List of variables that uniquely identify unit of analysis
                default = ["HUID"]
                to replicate the random sort the dataframe must first be sorted by a unique id
        Returns:
            pd.DataFrame: Randomly Sorted Dataframe with Random Merge ID
        """
        size_row = randmerge_df.shape[0]

        print("Sorting by ",unique_sort_vars)
        random_generator = np.random.RandomState(seed)
        sorted_0 = randmerge_df.sort_values(by=unique_sort_vars).copy()

        # Create Random number list to use for merge 
        random_order = random_generator.uniform(0, 1, size_row)

        sorted_0.loc[:,"random_order"] = random_order

        #  Sort by groupby vars and sort vars and Random Order
        print("Sorting before random merge order by",groupby_vars+sort_vars+["random_order"])
        sorted_1 = sorted_0.sort_values(by=groupby_vars+sort_vars+["random_order"],
                                        ascending=groupby_vars_ascending +
                                        sort_vars_ascending+[True])

        # Add random merge order based on sorting
        # Issue - Treat missing values as part of sort
        print("Generating random merge order by",groupby_vars)
        sorted_1.loc[:,"random_mergeorder"] = sorted_1.groupby(groupby_vars,
                                    dropna=False).cumcount() + 1

        sorted_2 = sorted_1.sort_values(by=groupby_vars+["random_mergeorder"],
                                        ascending=groupby_vars_ascending + [True])
        
        # Remove random_order from column list
        columnlist = [col for col in sorted_2 if col != "random_order"]

        return sorted_2[columnlist]

    def add_geovarid(self, input_df):
        """
        Function generates a geovarid for various geolevels
        Examples:
            County2010 = '48167' - is the geovarid for Galveston County
            Tract2010  = '48167720100' - is a geovarid for a census tract in Galveston County
            Block2010  = '481677201001000' is a geovarid for a census block in Galveston County
        The Geovarids are nested and can be used for merging data at different geolevels.

        geovintage - year for the census geography. Geography for 
        tracts and blocks change with decennial census (2000, 2010, 2020)
        """
        # Create copy of input df - try to prevent chaining conflict
        add_geovarid_df = input_df.copy()

        # Save a list of the current columns - without geovarid
        column_list = list(add_geovarid_df.columns)

        # Check if blockid in column list
        if ('blockid' in column_list) & \
            ('block' not in column_list):
            print("Adding Block2010 to column list")
            # create  column in input df
            # Version 2.0 of HUI renames Block2010 to blockid
            add_geovarid_df['Block2010'] = add_geovarid_df['blockid']

        # Save a list of the current columns - without geovarid
        column_list = list(add_geovarid_df.columns)

        # Set geoid FIPS code by concatenating state, county, census geography ids
        # Check if geocodes are strings
        geo_levels = {'State':  {'length' : 2, 'total_len' : 2,  'required' : ['state'] },
                      'County': {'length' : 3, 'total_len' : 5,  'required' : ['state','county']},
                      'Tract':  {'length' : 6, 'total_len' : 11, 'required' : ['state','county','tract']},
                      'BlockGroup' : {'length' : 1, 'total_len' : 12, 'required' : ['state','county','tract','blockgroup'],
                                      'notes' :'Block Group code is first digit of block id'},
                      'Block':  {'length' : 4, 'total_len' : 15, 'required' : ['state','county','tract','block']}
                      }

        # Name of Geovar to add
        geovarid = self.geolevel+self.geovintage

        # Check to see what geolevels are available 
        geolevels_available = []
        # Check to see what geovarids are available
        geovarids_available = []
        # Make sure all input variable are correctly zero padded and saved as strings
        for geo_level in geo_levels:
            # Geo level needs to be all lower case to match api variables
            geo_level_lower = geo_level.lower()
            if geo_level_lower in column_list:
                # Each geolevel is a zero padded string
                length = geo_levels[geo_level]['length']
                print("Check length of",geo_level_lower,"expected length",length)
                check_length = self.check_var_length(
                    input_df = add_geovarid_df,
                    var = geo_level_lower,
                    expected_length = length)
                if (check_length == "Match") or \
                    (check_length == "Possible match with zero pad"):
                    # Check variable type
                    # Issue with typ converting to int or float
                    geo_level_type = add_geovarid_df[geo_level_lower].dtypes
                    print(geo_level_lower,"is type",geo_level_type)
                    add_geovarid_df.loc[:,geo_level_lower] =  \
                        add_geovarid_df[geo_level_lower].\
                            apply(lambda x: str(x).zfill(length))
                    geolevels_available.append(geo_level_lower)
                    geo_level_type = add_geovarid_df[geo_level_lower].dtypes
                    print("after update",geo_level_lower,"is type",geo_level_type)
            # Check to see what geovarids are available
            geovarid_test = geo_level+self.geovintage
            if geovarid_test in column_list:
                total_length_of_geovar = geo_levels[geo_level]['total_len']
                check_length = self.check_var_length(
                    add_geovarid_df,geovarid_test,total_length_of_geovar)
                if (check_length == "Match") or \
                   (check_length == "Possible match with zero pad"):
                    add_geovarid_df.loc[:,geovarid_test] =  \
                        add_geovarid_df[geovarid_test].apply(lambda x: str(x).\
                            zfill(total_length_of_geovar))
                    geovarids_available.append(geovarid_test)
                elif (check_length == "Possible convert to float"):
                    print("Possible convert to float")
                    add_geovarid_df.loc[:,geovarid_test] =  \
                        add_geovarid_df[geovarid_test].apply(lambda x: str(x)[:-2].\
                            zfill(total_length_of_geovar))
                    geovarids_available.append(geovarid_test)
        print('Geolevels available',geolevels_available)
        print('Geolvarids available',geovarids_available)
        # Generate Geovarid based on available columns
        # What is the total length expected for the geolevel 
        total_length_of_geovar = geo_levels[self.geolevel]['total_len']
        # What are the required input variables
        required_vars = geo_levels[self.geolevel]['required']

        print('Adding',geovarid,'expected length',total_length_of_geovar)
        # Check to make sure that all columns needed are in list
        if all(cols in column_list for cols in geolevels_available) and \
            all(cols in column_list for cols in required_vars) and \
            (geolevels_available == required_vars) and \
            (geolevels_available != []):
            print('Dataframe has required geo levels',geolevels_available)
            # Set geovarid to empty
            add_geovarid_df.loc[:,geovarid] = ''
            for geo_level in required_vars:
                geo_level_type = add_geovarid_df[geo_level.lower()].dtypes
                print(geo_level.lower(),"is type",geo_level_type)
                # Add geo level to geovarid
                add_geovarid_df.loc[:,geovarid] = add_geovarid_df[geovarid] + \
                    add_geovarid_df[geo_level.lower()]
        # If geolevel columns are not in list check if block id is in list
        elif 'Block'+self.geovintage in geovarids_available:
            print('Dataframe has Block',self.geovintage,'for new geovar',geovarid)
            # Check that the block id is a zero padded 15 digit string
            # The geovarid is the first x characters
            add_geovarid_df.loc[:,geovarid] = add_geovarid_df['Block'+self.geovintage].\
                apply(lambda x : str(int(x)).zfill(15)[0:total_length_of_geovar])
        elif 'Tract'+self.geovintage in geovarids_available:
            print('Dataframe has Tract',self.geovintage,'for new geovar',geovarid)
            # Check that the tract id is a zero padded 11 digit string
            # The geovarid is the first x characters
            #print('Before update confirm',geovarid,'has expected length.')
            #self.check_var_length(add_geovarid_df,geovarid,total_length_of_geovar)
            add_geovarid_df.loc[:,geovarid] = add_geovarid_df['Tract'+self.geovintage].\
                apply(lambda x : str(int(x)).zfill(11)[0:total_length_of_geovar])
            #print('After update confirm',geovarid,'has expected length.')
            #self.check_var_length(add_geovarid_df,geovarid,total_length_of_geovar)
        elif 'GEO_ID' in column_list:
            # GEO_ID has the FIPS code data using the substring
            print('Dataframe has GEO_ID for new geovar',geovarid)
            add_geovarid_df.loc[:,geovarid] = add_geovarid_df['GEO_ID'].\
                apply(lambda x : str(x).zfill(11)[x.find("US")+2:\
                    total_length_of_geovar+x.find("US")+2])        
        else:
            print('Warning: Column list does not have required columns to make',geovarid)

        # Update column list to move geovarid to front
        columnlist = [col for col in add_geovarid_df if col != geovarid]
        new_columnlist = [geovarid]+ columnlist
        # Confirm geovarid is set correctly
        print('Confirming',geovarid,'has expected length.')
        check_length = self.check_var_length(
            add_geovarid_df,geovarid,total_length_of_geovar)
        return add_geovarid_df[new_columnlist]
    

    @staticmethod
    def check_var_length(input_df,var,expected_length):
        """
        Need to check length of geography variables 
        Returns true or false value
        """
        # Make sure variable is a string
        string_list = input_df[var].astype(str)
        varid_list = list(string_list.fillna(value="0"))
        varid_max = max(varid_list)       
        var_len = len(varid_max)

        # Check if max variable is just 0
        if varid_max == "0" or varid_max == 'nan':
            # update variable length
            var_len = 0

        print("Longest",var,":",varid_max)
        print(var,"Expected Length",expected_length,"Available Length",var_len)
        if var_len == expected_length:
            return "Match"
        elif var_len == expected_length-1:
            return "Possible match with zero pad"
        elif (var_len == expected_length+2) & ('.' in varid_max):
            return "Possible convert to float"
        else:
            return "No match"
    
    def add_primarykey(self, input_df, key):
        """
        Primary key needs to be unique and non-missing
        """
        
        primary_key_name =self.primary_key_name[key]
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
            primary_key_error = self.primary_key_error_check(add_uniqueid_df,primary_key_name)
            if primary_key_error == 0:
                return add_uniqueid_df[new_columnlist]

        if (primary_key_error >= 1) or (primary_key_name not in list(add_uniqueid_df.columns)):
            uniqueid_part1 = self.geolevel+self.geovintage
            # Part2 of unique id is a counter based on the part1
            # The counter ensures that values are unique within part1
            # Add unique ID based on group vars
            add_uniqueid_df.loc[:,'unique_part2'] = \
                add_uniqueid_df.groupby([uniqueid_part1]).cumcount() + 1
            # Need to zero pad part2 find the max number of characters
            part2_max = add_uniqueid_df['unique_part2'].max()
            part2_maxdigits = len(str(part2_max))
            add_uniqueid_df.loc[:,primary_key_name] = add_uniqueid_df[uniqueid_part1] + \
                add_uniqueid_df['unique_part2'].apply(lambda x : str(int(x)).zfill(part2_maxdigits))
            # Check if values are unique
            error = self.primary_key_error_check(add_uniqueid_df,primary_key_name)
        
            if error == 0:
                return add_uniqueid_df[new_columnlist]
            if error > 0:
                print("Warning: Primary key is not set correctly")
                return add_uniqueid_df[new_columnlist]


    @staticmethod
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

            
    @staticmethod
    def split_byHispanic(df):
        """
        function used to split characteristic data by Hispanic and not Hispanic 
        This is to create separate distributions of characterstics before 
        merging the data with the housing unit inventory.
        """

        # start a new dictionary that will have Hispanic and Not Hispanic Dataframes
        df_split = {}

        # Observations where Hispanic is 1
        df_split["Hispanic"] = df.loc[df['hispan'] == 1].copy()
        # Observations where Hispanic is 0 or Missing but Race is not Missing
        df_split["not Hispanic"] = df.loc[(df['hispan'] == 0) | 
                                        (df['hispan'] == -999) & 
                                        ~(df['race'].isnull())].copy()
        # Observations where Hispanic and Race is Missing
        df_split["no Hispanic data"] = df.loc[(df['hispan'].isnull()) &
                                        (df['race'].isnull())].copy()

        # Check that split lengths equal original dataframe length
        length_df1 = df.shape[0]
        length_df2 = df_split["Hispanic"].shape[0]
        length_df3 = df_split["not Hispanic"].shape[0]
        length_df4 = df_split["no Hispanic data"].shape[0]
        print("Length of input dataframe (df) :",length_df1)
        print("Length of Hispanic df          :",length_df2)
        print("Length of not Hispanic df      :",length_df3)
        print("Length of no Hispanic data df  :",length_df4)
        print("Length of split dfs = input df :",length_df1 == length_df2 + length_df3 + length_df4)

        return df_split

    @staticmethod
    def drop_randommerge_adduniqueid(df, 
                        remove_cols = ['random_mergeorder','hhincfamincmerge'],
                        uniqueid_part1 = 'Tract2010'):
    
        # Add unique ID based on group vars
        df.loc[:,'unique_part2'] = df.groupby([uniqueid_part1]).cumcount() + 1
        df.loc[:,'uniqueid'] = df[uniqueid_part1] + \
            df['unique_part2'].apply(lambda x : str(int(x)).zfill(5)) 
        if df['uniqueid'].is_unique:
            print("Unique variable is unique.")
        else:
            print("Warning: Unique variable is not unique.")

        # Store Column List
        columnlist = [col for col in df]
        remove_cols = remove_cols + ['unique_part2']
        # Remove columns not needed for random merge
        for remove_col in remove_cols:
            if remove_col in columnlist:
                new_columnlist = [col for col in df if col != remove_col]
                df = df[new_columnlist]

        # Move unique id to first column
        columnlist = [col for col in df if col != "uniqueid"]
        new_columnlist = ["uniqueid"]+ columnlist

        return df[new_columnlist]


    def merge_groups(self,group1_df, group2_df, groupby_vars):

        """
        To add new characteristic based on random merge of 2 groups.
        This function takes the prepared dataframes and merges then based on the 
        group by variables.
        For missing values after the merge fill missing values with 0 as a default.

        """
        # Start dictionary to store merged data
        merged_groups = {}
        merge_vars = groupby_vars + ['random_mergeorder']
        print("Check merge vars includes geovarid:",merge_vars)
        # Keep flag vars
        group1_flag_vars = [col for col in group1_df if '_flagsetrm' in col]
        group2_flag_vars = [col for col in group2_df if '_flagsetrm' in col]
        # Keep primary key and merge group vars
        keep_vars_group1 = [self.primary_key_group1] + merge_vars + group1_flag_vars
        keep_vars_group2 = [self.primary_key_group2] + merge_vars \
            + [self.new_char] + self.extra_vars + group2_flag_vars

        # Merge Group 1 with Group 2 Data
        merged_groups = pd.merge(left = group1_df[keep_vars_group1], 
                                right = group2_df[keep_vars_group2],
                                how='outer', 
                                left_on= merge_vars,
                                right_on=merge_vars,
                                sort=True, suffixes=("_x", "_y"),
                                copy=True, indicator=True, validate="1:1")
        merged_groups = merged_groups.\
            rename(columns={"_merge": self.check_merge})
        
        # Fill in missing variables
        merged_groups[self.new_char] = merged_groups[self.new_char].\
            fillna(value=self.fillna_value)

        # Check non-matching observations
        primarydf_nomatch = merged_groups.loc[
                (merged_groups[self.check_merge] == 'left_only')].copy()
        length_nonmatch1 = primarydf_nomatch.shape[0]
        print("Primary data frame has extra",self.new_char,\
            " observations with no match:",length_nonmatch1)
        print("Observations with no match filled with",self.fillna_value)
        newchar_nomatch = merged_groups.loc[
                (merged_groups[self.check_merge] == 'right_only')].copy()
        length_nonmatch2 = newchar_nomatch.shape[0]
        print("Merge found extra",self.new_char," observations:",length_nonmatch2)

        return merged_groups

    def setup_run_random_merge_2dfs(self):
        """
        Intersect 2 data frames based on groups, variable set and random merge
        """
        # Prepare data for merge
        preped_for_merge_data = {}
        for key in self.dfs.keys():
            print("\n***************************************")
            print("    Setting up ",key,"data with primary key and flags")
            print("***************************************\n")
            # Check that primary and secondary data frames have 
            # primary key set correctly
            # Add geovar
            lower_case_geovars = ['block','blockgroup','tract','county']
            upper_case_geovars = ['Block','BlockGroup','Tract','County']
            if self.geolevel in lower_case_geovars + upper_case_geovars:
                self.dfs[key]['data'] = self.add_geovarid(self.dfs[key]['data'])
            # Add primary key
            self.dfs[key]['data'] = self.add_primarykey(self.dfs[key]['data'],key)

            # Add set flag variable by groups with geovar
            self.dfs[key]['data']  = self.set_flags_for_merge(
                self.dfs[key]['data'] )
            # Copy data with flag not set
            flag_not_set_condition = \
                (self.dfs[key]['data'][self.flaggeo_var] == 0)
            key_df = self.dfs[key]['data'].loc[flag_not_set_condition].copy()
            print("Attempting random merge for",key_df.shape[0],key,"observations.")

            # Check if sort vars options have been set
            if 'sort_vars' in self.dfs[key]:
                self.sort_vars = self.dfs[key]['sort_vars']
                self.sort_vars_ascending = self.dfs[key]['sort_vars_ascending']
                print("  Random merge will be sorted by",self.sort_vars)
            else:
                self.sort_vars = []
                self.sort_vars_ascending = []

            # Split up data frame by characteristic 
            # Check if By Groups is by Hispanic - if so split groups
            if self.by_groups.keys() == {"Hispanic","not Hispanic"}:
                print("\n***************************************")
                print("    Splitting",key,"data by Hispanic")
                print("***************************************\n")
                preped_for_merge_data[key] = self.split_byHispanic(key_df)
            else:
                # If Hispanic and not Hispanic not the by groups
                # Then assume no by groups - future versions my have more options
                preped_for_merge_data[key] = {}
                for by_group in self.by_groups:
                    preped_for_merge_data[key][by_group] = key_df.copy()
            # Check to make sure all by groups have data
            for by_group in self.by_groups:
                length_df =  preped_for_merge_data[key][by_group].shape[0]
                if length_df == 0:
                    print("Warning:",by_group,"dataframe has length",length_df)
            # add random merge order to dataframes
            # add seed increment to ensure random merge
            seed_increment = 1
            for by_group in self.by_groups:
                print("\n***************************************")
                print("    Preparing",key,"by",by_group,"data for random merge.")
                print("***************************************\n")
                preped_for_merge_data[key][by_group] = \
                        self.prepare_randommerge(
                                    randmerge_df = preped_for_merge_data[key][by_group],
                                    seed = self.seed+self.round+seed_increment,
                                    unique_sort_vars = [self.primary_key_name[key]],
                                    groupby_vars = self.groupby_vars[by_group],
                                    groupby_vars_ascending = \
                                        self.groupby_vars_ascending[by_group],
                                    sort_vars = self.sort_vars,
                                    sort_vars_ascending = self.sort_vars_ascending)
                # increase seed increment
                seed_increment += 1

        # merge groups based on group vars and random merge order
        merged_dfs = {}
        for by_group in self.by_groups:
            print(by_group)
            groups = list(self.dfs.keys())
            group1 = groups[0]
            group2 = groups[1]
            group1_df = preped_for_merge_data[group1][by_group].copy()
            group2_df = preped_for_merge_data[group2][by_group].copy()

            print("\n***************************************")
            print("    Random Merge",group1,"with",group2,"by",by_group)
            print("***************************************\n")
            merged_dfs[by_group] = self.merge_groups(group1_df = group1_df,
                             group2_df = group2_df,
                             groupby_vars = self.groupby_vars[by_group]) 
                    
            print("\n***************************************")
            print("    Set Flags after Merge")
            print("***************************************\n")
            
            # Update variables after merge
            flag_vars = [col for col in group1_df if '_flagsetrm' in col]
            for var in flag_vars:
                merged_dfs[by_group] = \
                    self.update_cols_after_merge(merged_dfs[by_group],var)
            merged_dfs[by_group] = self.set_flags_for_merge(
                    merged_dfs[by_group])
            # Check flags are set
            flag_set_round = (merged_dfs[by_group][self.flaggeo_var] == self.round)
            print("Observations geovar flag set",self.round,"=",flag_set_round.sum())     
            # Update round setting
            self.round += 1

        # Combine merged_dfs
        merged_dfs_recombine = pd.concat(merged_dfs.values(), 
                            ignore_index=True, axis=0)
        # Check flags are set
        flag_set_round = (merged_dfs[by_group][self.flag_var] == self.round-1)
        print("After Recombine flag set",self.round-1,"=",flag_set_round.sum())  

        print("\n***************************************")
        print("   Generate output data")
        print("***************************************\n")

        # Save output data for keys
        output_df = {}
        percent_left_to_predict = {}
        for key in self.dfs.keys():
            length_input_data = self.dfs[key]['data'].shape[0]
            output_df[key] = self.dfs[key]['data'].copy()
            primary_key = self.dfs[key]['primarykey']

            # make sure primary key is in data
            result_data_to_merge = merged_dfs_recombine.\
                loc[(merged_dfs_recombine[primary_key].notnull())].copy()
            # Variables to keep in secondary data 
            flag_vars = [col for col in result_data_to_merge \
                 if '_flagsetrm' in col]
            print('Flag vars available',flag_vars)
            keep_vars = [primary_key]+\
                [self.new_char]+flag_vars + self.extra_vars

            # Check flags are set
            flag_set_round = (output_df[key][self.flag_var] == self.round-1)
            print("Before update output_df observations flag set",self.round-1,"=",flag_set_round.sum())   
            flag_set_round = (result_data_to_merge[keep_vars][self.flag_var] == self.round-1)
            print("Before update results observations flag set",self.round-1,"=",flag_set_round.sum())   

            output_df[key] = pd.merge(
                left = output_df[key],
                right = result_data_to_merge[keep_vars],
                left_on = primary_key,
                right_on = primary_key,
                validate = "1:1",
                how = 'left',
                copy = True
            )
            # Update variables after merge
            for var in keep_vars:
                output_df[key] = \
                    self.update_cols_after_merge(output_df[key],var)
            # Check flags are set
            flag_set_round = (output_df[key][self.flaggeo_var] == self.round-1)
            print("After update observations geovar flag set",\
                self.round-1,"=",flag_set_round.sum())   

            print("\n***************************************")
            print("    Check random merge results for",key,"data.")
            print("***************************************\n")

            length_output_data = output_df[key].shape[0]
            # Check number of observations with new char set
            print("Check by geovar flag",self.flaggeo_var)
            condition1 = (output_df[key][self.flaggeo_var] != 0)
            print("Observations flag not equal to 0",condition1.sum())   

            conditions = condition1
            observations_with_newchar = output_df[key].\
                loc[conditions].shape[0]
            obs_left_to_predict = (length_input_data-observations_with_newchar)
            percent_left_to_predict[key] = (obs_left_to_predict/length_output_data)*100

            if length_input_data == length_output_data:
                print("Input and output data have the same length",length_input_data)
                print("Outputdata has",observations_with_newchar,\
                    "Observations with predicted",self.new_char)
                print("Percent left to predict: %5.2f" % (percent_left_to_predict[key]))

                print("\n***************************************")
                print("    Overwrite input data with update output data.")
                print("***************************************\n")

                self.dfs[key]['data'] = output_df[key].copy()

            else:
                print('Warning: Output data does not match input data.')
                percent_left_to_predict[key] = -999
                return output_df, percent_left_to_predict[key]

            
        
        return output_df, percent_left_to_predict

    def update_cols_after_merge(self, input_df,column_name):
        """
        After merge need to update the columns that will be repeated
        """
        if column_name+'_y' in list(input_df.columns):
            input_df[column_name] = \
                input_df[column_name+'_y'].fillna(input_df[column_name+'_x'])
            # Drop duplicate columns
            input_df = input_df.drop([column_name+'_x', column_name+'_y'], axis=1)

        return input_df

    def set_flags_for_merge(self,input_df):
        """
        Function to set flags before or after merge
        """

        # Check if flag set is in column list
        column_list = list(input_df.columns)
        # Create flag var based on new char - overall flag 
        # Need an overall flag for looping geovars
        self.flag_var = self.new_char+'_flagsetrm'
        if self.flag_var not in column_list:
            print("Initializing primary flag set variable for",self.new_char)
            print("New flag:",self.flag_var)
            input_df.loc[:, self.flag_var] = 0
        # Create flag var based on new char and geovar
        self.flaggeo_var = self.new_char+'_'+self.geovar+'_flagsetrm'
        print(column_list)
        if self.flaggeo_var not in column_list:
            print("Initializing geovar flag set variable for",self.new_char,\
                "at geolevel",self.geovar)
            print("New flag:",self.flaggeo_var)
            input_df.loc[:, self.flaggeo_var] = 0

        set_flag_df = input_df.copy()

        # Condition list for new characteristic set
        # Condition to check if flagset is not set
        flag_not_set = (set_flag_df[self.flag_var] == 0)
        print("Observations without primary flag set",flag_not_set.sum()) 

        # Set geovar flag if primary flag is already set
        # Without this step observations in secondary may be used multiple times
        flag_set = (set_flag_df[self.flag_var] != 0)
        geoflag_not_set = (set_flag_df[self.flaggeo_var] == 0)

        print("Observations without geovar flag set",geoflag_not_set.sum()) 
        conditions = flag_set & geoflag_not_set
        set_flag_df.loc[conditions, self.flaggeo_var] = -777
        geoflag_not_set = (set_flag_df[self.flaggeo_var] == 0)
        print("After updated observations without geovar flag set",geoflag_not_set.sum())        


        if self.new_char in column_list:
            # Set conditions to check new characteristic
            # New characteristic is swt or not set
            not_set_value = self.fillna_value
            new_char_has_a_set_value = (set_flag_df[self.new_char] != not_set_value)
            new_char_has_not_set_value = (set_flag_df[self.new_char] == not_set_value)
            new_char_is_not_missing = (set_flag_df[self.new_char].notnull())
            new_char_is_missing  = (set_flag_df[self.new_char].isnull())
            new_char_set = new_char_has_a_set_value & new_char_is_not_missing
            new_char_notset = new_char_has_not_set_value | new_char_is_missing

            # If primary key for primary data is in column list
            if self.primary_key_group1 in column_list:
                withprimary = (set_flag_df[self.primary_key_group1].notnull())
                # For primary data before merge - no secondary primary key
                if self.primary_key_group2 not in column_list:
                    conditions = new_char_set & withprimary & geoflag_not_set
                    print("Setting",conditions.sum(),"flags for",\
                        self.new_char,"set before random merge.")
                    set_flag_df.loc[conditions, self.flaggeo_var] = .5
            # If primary key for secondary data is in column list
            if self.primary_key_group2 in column_list:
                withsecondary = (set_flag_df[self.primary_key_group2].notnull())
                # For secondary data before merge - no primary primary key
                if self.primary_key_group1 not in column_list:
                    conditions = new_char_set & withsecondary & geoflag_not_set
                    print("Setting",conditions.sum(),"flags for",\
                        self.new_char,"secondary data not used.")
                    set_flag_df.loc[conditions, self.flaggeo_var] = 0
            # Check if dataframe has both primary and secondary primary key
            if all(cols in column_list for cols in \
                [self.primary_key_group1,self.primary_key_group2]):
                print("Round =",self.round)
                # New characteristic is set using both primary and secondary data
                conditions = new_char_set & withprimary & withsecondary & geoflag_not_set
                print("Setting",conditions.sum(),"flags for",\
                    "observations set by random merge using both primary and secondary data.")
                set_flag_df.loc[conditions, self.flaggeo_var] = self.round
                # New characteristic is set using primary
                conditions = new_char_set & withprimary & ~withsecondary & geoflag_not_set
                print("Setting",conditions.sum(),"flags for",\
                    "observations set by random merge using only primary data.")
                set_flag_df.loc[conditions, self.flaggeo_var] = self.round+.5

        elif self.new_char not in column_list:
            # If new char not in column list then set flag set to 0
            print("Setting",len(set_flag_df),"flags for",\
                "data without",self.new_char,"before merge.")
            set_flag_df.loc[:, self.flaggeo_var] = 0

        # For observations that can not have new characteristic set
        # Cases that do not have the common group var, and by group vars
        # Example is vacant or group quarters that do not have required variables
        #conditions = geoflag_not_set
        no_groupby_vars_conditions = {}
        for by_group in self.by_groups:
            for groupby_var in self.groupby_vars[by_group]:
                if groupby_var in column_list:
                    no_groupby_vars_conditions[groupby_var] = \
                        (set_flag_df[groupby_var].isnull())
                    print(no_groupby_vars_conditions[groupby_var].sum(),\
                        "observations do not have required variable",groupby_var)
            for by_group in self.by_groups:
                for by_group_var in self.by_groups[by_group]['by_variables']:
                    if by_group_var in column_list:
                        no_groupby_vars_conditions[by_group_var] = \
                            (set_flag_df[by_group_var].isnull())
                        print(no_groupby_vars_conditions[by_group_var].sum(),\
                        "observations do not have required variable",by_group_var)
            # if the above set of loops added any new conditions
            if len(no_groupby_vars_conditions.keys()) > 0:
                for key in no_groupby_vars_conditions.keys():
                    # If any of the conditions are true then set flag to -888
                    conditions = geoflag_not_set & no_groupby_vars_conditions[key]
                    print("Setting",conditions.sum(),"flags for",\
                    "observations without required variable",key)
                    set_flag_df.loc[conditions, self.flaggeo_var] = -888

        # Update primary flag
        flag_not_set = (set_flag_df[self.flag_var] == 0)
        geovarflag_set = (set_flag_df[self.flaggeo_var] != 0)
        # It is possible that the geovar is missing 
        geovarflag_notset_missing = (set_flag_df[self.flaggeo_var] != -888)
        # If flag is -888 the primary flag should be left as 0
        conditions = flag_not_set & geovarflag_set & geovarflag_notset_missing
        set_flag_df.loc[conditions, self.flag_var] = set_flag_df[self.flaggeo_var]

        return set_flag_df

    # Dictionary with round options
    def make_round_options_dict(self):
        rounds = {'options': {
                'option1' : {'notes' : 'By original common group vars and by groups variables.',
                            'common_group_vars' : 
                                    self.common_group_vars,
                            'by_groups' :
                                    self.by_groups},
                'option2' : {'notes' : 'Drop common group variables.',
                            'common_group_vars' : 
                                    [],
                            'by_groups' :
                                    self.by_groups},
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
                'geo_levels' : ['Tract','County']                         
                }

        return rounds
    

    def run_random_merge_2dfs(self, rounds):
        """"
        Function runs full process and checks merge by rounds
        """
        # Check if final CSV file has already been selected
        # Process creates 2 files a primary file and a secondary file
        # The primary file will have the new characteristic from the secondary
        # The secondary file will help identify observations not used in the merge
        csv_filename = f'{self.outputfile}_{self.state_county}_{self.geovintage}_rs{self.seed}'
        csv_filename_primary = csv_filename+'_primary'
        csv_filename_secondary = csv_filename+'_secondary'
        csv_filepath_primary = self.outputfolder+"/"+csv_filename_primary+'.csv'
        csv_filepath_secondary = self.outputfolder+"/"+csv_filename_secondary+'.csv'

        # Check if selected data already exists - if yes read in saved file
        if os.path.exists(csv_filepath_primary) & os.path.exists(csv_filepath_secondary):
            output_df = {}
            output_df['primary'] = pd.read_csv(csv_filepath_primary, low_memory=False)
            output_df['secondary'] = pd.read_csv(csv_filepath_secondary, low_memory=False)
            # If file already exists return csv as dataframe
            print("File",csv_filepath_primary,"Already exists - Skipping Random Merge.")
            print("File",csv_filename_secondary,"Already exists - Skipping Random Merge.")
            return output_df


        # Assume percent_left_to_predict starts off at 100
        round_percent_left_to_predict = 100

        # Need an outer loop that will restart process 
        # Restart needs to happen if secondary data is used up
        should_restart = True
        while should_restart:
            print("Round",self.round)
            should_restart = False
            # Run options until all values are predicted
            for geo_level in rounds['geo_levels']:
                print("\n***************************************")
                print("***************************************\n")
                print('Performing random merge at geography level:',geo_level)
                print("\n***************************************")
                print("***************************************\n")
                for option in rounds['options']:
                    print("\n***************************************")
                    print("***************************************\n")
                    print(rounds['options'][option]['notes'])
                    print("\n***************************************")
                    print("***************************************\n")

                    # Update variables before merge
                    self.geolevel = geo_level
                    lower_case_geovars = ['block','blockgroup','tract','county']
                    upper_case_geovars = ['Block','BlockGroup','Tract','County']
                    if self.geolevel in lower_case_geovars+upper_case_geovars:
                        self.geovar = geo_level+self.geovintage
                    else:
                        self.geovar = geo_level
                    self.common_group_vars = rounds['options'][option]['common_group_vars']
                    self.by_groups = rounds['options'][option]['by_groups']
                    # create by groups based on by_groups
                    self.groupby_vars = {}
                    self.groupby_vars_ascending = {}
                    for by_group in self.by_groups:
                        by_variables = rounds['options'][option]['by_groups'][by_group]['by_variables']
                        self.groupby_vars[by_group] = [self.geovar]+\
                            by_variables+self.common_group_vars
                        # Merge can be shaped by the group by vars
                        # Initially assume that all vars used in sort are ascending
                        self.groupby_vars_ascending[by_group] = []
                        for var in self.groupby_vars[by_group]:
                            # Create a list with all true values for group by vars
                            self.groupby_vars_ascending[by_group].append(True)
                        
                    print('Running random merge by',self.groupby_vars[by_group])

                    output_df, round_percent_left_to_predict = \
                        self.setup_run_random_merge_2dfs()
                    
                    print("\n+++++++++++++++++++++++++++++++++++++++")
                    print("Percent left to predict: %10.6f" % (round_percent_left_to_predict['primary']))
                    print("\n+++++++++++++++++++++++++++++++++++++++")

                    if round_percent_left_to_predict['primary'] == -999:
                        print('Error in process.')
                        return 'Error'
                    elif round_percent_left_to_predict['primary']  == 0:
                        print("\n***************************************")
                        print("    Random merge complete.")
                        print("***************************************\n")

                        if self.savefiles == True:
                            print("Save primary and secondary files with all columns")
                            savefile = os.path.join(os.getcwd(), csv_filepath_primary)
                            output_df['primary'].to_csv(savefile, index=False)
                            savefile = os.path.join(os.getcwd(), csv_filepath_secondary)
                            output_df['secondary'].to_csv(savefile, index=False)

                        return output_df
                    # Create break if rounds exceeds 100
                    if self.round > 100:
                        print("Possible loop error.")
                        should_restart = False
                        return output_df
                    if round_percent_left_to_predict['secondary']  == 0:
                        if self.reuse_secondary == True:
                            print("\n***************************************")
                            print("    All secondary data has been used.")
                            print("    All secondary data will reused.")
                            print("***************************************\n")
                            print("    Need to reset flags and options.")
                            # Reset all flags
                            self.dfs['secondary']['data'].loc[:, self.flag_var] = 0
                            self.dfs['secondary']['data'].loc[:, self.flaggeo_var] = 0
                            # reset option to 1
                            should_restart = True
                            break
                        else:
                            print("\n***************************************")
                            print("    All secondary data has been used.")
                            print("    All secondary will not be reused.")
                            print("***************************************\n")
                            break



        print("\n***************************************")
        print("    Random merge almost complete.")
        print("***************************************\n")
        print("Check primary and secondary files to understand why merge is not complete")
        if self.savefiles == True:
            csv_filepath_primary_almost = self.outputfolder+"/"+csv_filename_primary+'_almost.csv'
            savefile = os.path.join(os.getcwd(), csv_filepath_primary_almost)
            output_df['primary'].to_csv(savefile, index=False)
            csv_filepath_secondary_almost = self.outputfolder+"/"+csv_filename_secondary+'_almost.csv'
            savefile = os.path.join(os.getcwd(), csv_filepath_secondary_almost)
            output_df['secondary'].to_csv(savefile, index=False)
        return output_df



        



