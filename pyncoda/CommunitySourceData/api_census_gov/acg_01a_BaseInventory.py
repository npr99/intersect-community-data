# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import requests # Census API Calls
import os  # Operating System (os) For folders and finding working directory
import pandas as pd
import sys  # saving CSV files
import json  # used to read in Census variables

from pyncoda.CommunitySourceData.api_census_gov.acg_00b_hui_block2010 import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00c_hispan_block2010 import *

class BaseInventory():
    """Utility methods for generating Housing Unit Inventory or Person Record Inventory"""


    @staticmethod
    def get_data_based_on_varstems_and_roots(state_county: str, 
                                            varstems_roots_dictionary: dict = {},
                                            outputfolder: str = "popinv_workflow",
                                            outputfolders = {'TidyCommunitySourceData' : 'countydata/popinv_workflow'}
                                            ):
        """
        General function that takes a dictionary of varstems and roots 
        and loops through list to get variables

        After the data is obtained the reshaped data will have a new variable that
        captures the variable root. Which maps to new characteristic variables

        Args:
            state_county (str): A 5 character county FIPS code of concatenated State and County FIPS Codes.
                see full list https://www.nrcs.usda.gov/wps/portal/nrcs/detail/national/home/?cid=nrcs143_013697
            varstems_roots_dictionary (dict): Dictionary with details on variable inputs to inventory

        Returns:
            obj, dict: A dataframe for with housing unit or person record count data
        """

        # Collect Metadata for process
        concept         = varstems_roots_dictionary['metadata']['concept']
        vintage         = varstems_roots_dictionary['metadata']['vintage']
        dataset_name    = varstems_roots_dictionary['metadata']['dataset_name']
        for_geography   = varstems_roots_dictionary['metadata']['for_geography']
        char_vars       = varstems_roots_dictionary['metadata']['char_vars']
        mutually_exclusive = varstems_roots_dictionary['metadata']['mutually_exclusive']
        countvar       = varstems_roots_dictionary['metadata']['countvar']
        group       = varstems_roots_dictionary['metadata']['group']
        unit_of_analysis       = varstems_roots_dictionary['metadata']['unit_of_analysis']

        # Check if API call has already by completed
        csv_filename = f'{group}_{state_county}_{vintage}'
        csv_filepath = outputfolders['TidyCommunitySourceData']+"/"+csv_filename+'.csv'
        
        # Check if selected data already exists - if yes read in saved file
        if os.path.exists(csv_filepath):
            df = pd.read_csv(csv_filepath, low_memory=False) 
            # If file already exists return csv as dataframe
            print("File",csv_filepath,"Already exists - Skipping API Call.")
            return df

        df_varstems = [] # Create empty list to append each group of variables
        for varstem in varstems_roots_dictionary:
            #print(varstem)
            # Check if metadata exists
            if varstem == 'metadata':
                # print('Skip Variable Stem and Root Metadata.')
                # Continue exits current for loop and moves to next varstem
                continue    # skip metadata description

            varoortlist = varstems_roots_dictionary[varstem]

            # Check if characteristic variables includes race
            print(char_vars)
            if 'byracehispan' in char_vars:
                racehispn_groups_dictionary = varstems_roots_dictionary['metadata']['byracehispan']
            else:
                racehispn_groups_dictionary = {'' : {'Label' : 'Race and Hispanic Not Applicable'}}

            # Check if varstem has multiple parts - possible if more than 50 variables
            if '_part' in varstem:
                # save varstem for api save
                varstem_api = varstem
                # remove _part substring and part number from varstem
                part_number = varstem[-2:]
                varstem = varstem.replace('_part'+part_number,'')
            else:
                varstem_api = varstem
            
            # Start an empty dictionary to store each racehispangroup
            # This is important for creating mutually exclusive dataframes
            df = {}
            print("\n**********************************")
            print("Obtain data from Census API",concept)
            for racehispangroup in racehispn_groups_dictionary:
                # loop through variable roots in the dictionary
                # this limits the total number of variables in the API call 
                # this also helps to identify household characteristics by steps

                # Start list of variables to get
                get_vars = 'GEO_ID'
                # Census API has a limit of 50 variables
                get_var_count = 1 

                # add race letter to variable stem
                varstem_race = varstem + racehispangroup

                # Label for data about to obtain
                label = racehispn_groups_dictionary[racehispangroup]['Label']
                print("    Obtaining data for",varstem_race,concept,"by",label)

                #print(varstem_race)
                # Loop through each var root to make variable name to call in api
                for varroot_str in varoortlist:
                    #print(varroot)
                    # Variable parameters
                    get_vars = get_vars + ','+varstem_race+varroot_str
                    get_var_count = get_var_count + 1
                    if get_var_count > 50:
                        print("too many variables")
                        return 
                
                df[racehispangroup] = BaseInventory.obtain_census_api(state_county = state_county,
                                                        vintage = vintage, 
                                                        dataset_name = dataset_name,
                                                        var_stem = varstem_api+racehispangroup,
                                                        get_vars = get_vars,
                                                        for_geography = for_geography,
                                                        outputfolders = outputfolders)

                # Rename columns and change to integer type
                # Loop through each var root to change type and name
                for varroot_str in varoortlist:
                    int_var = varstem_race+varroot_str
                    df[racehispangroup][int_var] = df[racehispangroup][int_var].astype('int')
                    # also need to rename column for subtraction
                    # rename does not include the race category 
                    rename_var = varstem+varroot_str
                    df[racehispangroup] = df[racehispangroup].rename(columns={int_var : rename_var})
            
            # Create mutually exclusive dataframes
            if mutually_exclusive == False:
                mx_df = {}
                mutually_exclusive_dict = varstems_roots_dictionary['metadata']['mutually_exclusive_dict']
                indexvar = varstems_roots_dictionary['metadata']['indexvar']
                
                # Update racehispn_groups_dictionary with mutually exclusive dictionary
                # This assumes that the mutually exclusive dictionary relates to race hispan
                # Might need a if statement to check this assumption
                racehispn_groups_dictionary = varstems_roots_dictionary['metadata']['mutually_exclusive_dict']

                print("\n**********************************")
                print("Create mutually exclusive dataframe for:")
                for mxdf_key in mutually_exclusive_dict:
                    label = mutually_exclusive_dict[mxdf_key]['Label']
                    function = mutually_exclusive_dict[mxdf_key]['equation']
                    mx_df[mxdf_key] = eval(function)
                    mx_df[mxdf_key].label = label
                    print("     ",label)
            # if mutually exclusive is True or Missing
            # copy dataframes into new mx_df
            else:
                mx_df = {}
                for key in df.keys():
                    mx_df[key] = df[key].copy(deep=True)
                    mx_df[key].label = concept
            
            # loop through mutually exclusive dataframes
            print("\n**********************************")
            print("Reshape dataframe to convert unit of analysis")
            for key in mx_df.keys():
                # reshape geoid level data to housing unit level data
                geoid_count = mx_df[key].shape[0]
                print("   For mutually exclusive dataframe for:",mx_df[key].label)
                df_reshape = BaseInventory.reshape_geoid_to_countvar(
                                                        wide_df = mx_df[key],
                                                        newvar = 'precode',
                                                        stem = varstem,
                                                        countvar = countvar
                                                        )
                new_count = df_reshape.shape[0]
                geo_level = for_geography.replace(':*','')
                print("       Unit of analysis converted from",geoid_count,geo_level,"s, to",new_count,unit_of_analysis)

                # Recode the new variable precoded by the variable root
                # Note the variable root may have multiple characteristics 
                for char_var in char_vars:
                    if char_var not in  ['byracehispan']:
                        df_reshape[char_var] = df_reshape['precode'].apply(lambda x : varoortlist[x][char_var])
                    if char_var == 'byracehispan':
                        df_reshape['race'] = racehispn_groups_dictionary[key]['race']
                        df_reshape['hispan'] = racehispn_groups_dictionary[key]['hispan']

                # append dataframes
                df_varstems.append(df_reshape)

        # Concatenate append dataframes
        df_return = pd.concat(df_varstems)

        # Drop precode variable
        df_return = df_return.drop(columns=['precode'])

        # Save File as CSV
        savefile = sys.path[0]+"/"+csv_filepath
        df_return.to_csv(savefile, index=False)
        
        return df_return


    @staticmethod
    def get_apidata(state_county: str, 
                   geo_level: str = 'Block',
                   vintage: str = '2010',
                   mutually_exclusive_varstems_roots_dictionaries: list =
                                      [tenure_size_H16_varstem_roots,
                                       vacancy_status_H5_varstem_roots,
                                       group_quarters_P42_varstem_roots],
                    outputfolder = "popinv_workflow",
                    outputfolders = {'countydata' : 'countydata/popinv_workflow'},
                    outputfile = "CoreHUI"):

        """Create housing unit or person count Level dataframe from block level data

        Args:
            state_county (str): A 5 character county FIPS code of concatenated State and County FIPS Codes.
                see full list https://www.nrcs.usda.gov/wps/portal/nrcs/detail/national/home/?cid=nrcs143_013697
            vintage (str): Census Year.
            dataset_name (str): Census dataset name
            mutually_exclusive_varstems_roots_dictionaries (list): List of mutually exclusive
                variables stems and root dictionaries. The default tenure by size, 
                vacancy status, and group quarters provides the basic list for all 
                housing units in a community.

        Returns:
            df: A dataframe for with housing unit or person count data with tenure, household size,
            race, ethnicity, vacancy status, and group quarters types

        """
        # Check if final CSV file has already been selected
        csv_filename = f'{outputfile}_{state_county}_{vintage}'
        csv_filepath = outputfolders['BaseInventory']+"/"+csv_filename+'.csv'

        # Check if selected data already exists - if yes read in saved file
        if os.path.exists(csv_filepath):
            df = pd.read_csv(csv_filepath, 
                    dtype={
                            geo_level+vintage: str
                        }, low_memory=False)
            # If file already exists return csv as dataframe
            print("File",csv_filepath,"Already exists - Skipping API Call.")
            return df

        df_apidata = [] # Create empty list to append each group of variables

        # loop over tenure by size, vacancy status, and group quarters variables
        for varstems_roots_dictionary in mutually_exclusive_varstems_roots_dictionaries:
            # Collect unit inventory based on varstem roots dictionary
            print(outputfolders)
            df_apidata.append(BaseInventory.get_data_based_on_varstems_and_roots(
                                                state_county = state_county,
                                                varstems_roots_dictionary = varstems_roots_dictionary,
                                                outputfolders = outputfolders)
                            )

        # Create dataframe from appended census data
        df = pd.concat(df_apidata)

        # Fill NaN does not work for missing Hispanic Values
        # Fill NaN variables with 0 - for example vacancy does not have race characteristics
        # df = df.fillna(value = 0)

        # Reset the index - without this step the index is not unique
        df.reset_index(inplace=True, drop = True)

        # Expand unit data
        # Id expand variable
        column_list = [col for col in df]
        if 'hucount' in column_list:
            df = BaseInventory.expand_df(df = df, expand_var= 'hucount')

            # Add Counter
            df['hu_counter'] = df.groupby(['GEO_ID']).cumcount() + 1
        elif 'preccount' in column_list:
            df = BaseInventory.expand_df(df = df, expand_var= 'preccount')

            # Add Counter
            df['prec_counter'] = df.groupby(['GEO_ID']).cumcount() + 1

        # Core files need block ids
        if "Core" in outputfile:
            # Add Unique ID
            geolevel='Block'
            df = BaseInventory.add_block_geoidstr(df, geolevel=geolevel, year = vintage)

            if 'hucount' in column_list:
                primary_key = "huid"
                counter_var = 'hu_counter'
                # Include ID type to differentiate between huid from precid
                id_type = "H"
            elif 'preccount' in column_list:
                primary_key = "precid"
                counter_var = 'prec_counter'
                id_type = "P"

            # Generate unique ID
            counter_var_max = df[counter_var].max()
            counter_var_maxdigits = len(str(counter_var_max))
            df.loc[:,primary_key] = df.apply(lambda x: x[geolevel+vintage+'str'] + id_type +
                                            str(x[counter_var]).zfill(counter_var_maxdigits), axis=1)
                                                
            # Reorder columns
            primary_key_list = [primary_key]
            foreign_keys = [geolevel+vintage, geolevel+vintage+'str']
            geo_vars_to_drop = ['GEO_ID','state','county','tract','block','index']
            char_vars = [col for col in df if col not in primary_key_list+foreign_keys+geo_vars_to_drop]
            col_list = primary_key_list + foreign_keys + char_vars
            df = df[col_list]

        savefile = sys.path[0]+"/"+csv_filepath
        df.to_csv(savefile, index=False)

        return df


    @staticmethod
    def obtain_census_api(state_county: str, 
                        vintage: str = "2010", 
                        dataset_name: str = 'dec/sf1',
                        get_vars: str = 'GEO_ID',
                        var_stem: str = '',
                        for_geography: str = 'block:*',
                        outputfolders = {'CommunitySourceData' : 'countydata/popinv_workflow'}):

        """General utility for obtaining census data in a county or group of counties.

        Args:
            state_county (str): A 5 character FIPS code of concatenated State and County FIPS Codes.
                see full list https://www.nrcs.usda.gov/wps/portal/nrcs/detail/national/home/?cid=nrcs143_013697
                Can be a list example ['41007'] or a dictionary {'41007' : 'Clatsop County, OR'}
            vintage (str): Census Year.
            dataset_name (str): Census dataset name.
            get_vars (str): list of variables to get from the API.
            for_geography (str): census geography to get data for default = all blocks (block:*)
                other options include block_group, tract

        Returns:
            obj, dict: A dataframe for with block level housing unit data

        """

        # split concatenated state and county values
        state = state_county[0:2]
        county = state_county[2:5]
        #logger.debug('State:  '+state)
        #logger.debug('County: '+county)

        # Check if final CSV file has already been selected
        json_filename = f'{var_stem}_{state_county}_{vintage}'

        # Add Source Data folder for Census API
        censusapi_folder = outputfolders['CommunitySourceData']+'/api_census_gov'
        # Make directory to save output
        if not os.path.exists(censusapi_folder):
            os.mkdir(censusapi_folder)

        # Check if data has already been downloaded
        json_filepath = censusapi_folder+"/"+json_filename+'.json'

        # Check if selected data already exists - if yes read in saved file
        if os.path.exists(json_filepath):
             # reading the data from the file
            with open(json_filepath) as f:
                data = json.load(f)
            print("Dictionary file",json_filepath,"Already exists - Skipping API Call.")
            # Convert the requested json into pandas dataframe
            df = pd.DataFrame(columns=data[0], data=data[1:])
            return df

        # Set up hyperlink for Census API
        api_hyperlink = ('https://api.census.gov/data/' + vintage + '/'+dataset_name + '?get=' + get_vars +
                        '&in=state:' + state + '&in=county:' + county + '&for='+for_geography)

        print("       Census API data from: " + api_hyperlink)

        # Obtain Census API JSON Data
        apijson = requests.get(api_hyperlink)
        if apijson.status_code != 200:
            print("API status code:",apijson.status_code)
            error_msg = "Failed to download the data from Census API."
            #logger.error(error_msg)
            raise Exception(error_msg)

        # Convert the requested json into pandas dataframe
        df = pd.DataFrame(columns=apijson.json()[0], data=apijson.json()[1:])
        
        # save json as text file
        with open(json_filepath, 'w') as convert_file:
            json.dump(apijson.json(),convert_file)

        return df

    @staticmethod
    def add_block_geoidstr(df,
                     geolevel: str = 'Block',
                     year: str = '2010'):

        # Set geoid FIPS code by concatenating state, county, census geography ids
        # Check if geocodes are strings
        geo_levels = {'state':  {'len' : 2},
                      'county': {'len' : 3},
                      'tract':  {'len' : 6},
                      'block':  {'len' : 4}}
        for geo_level in geo_levels:
            # Each geolevel is a zero padded string
            len = geo_levels[geo_level]['len']
            df.loc[:,geo_level] =  df[geo_level].apply(lambda x: str(x).zfill(len))

        df.loc[:,geolevel+year] = (df['state']+df['county'] +
                                df['tract']+df['block'])

        # To avoid problems with how the block group id is read saving it
        # as a string will reduce possibility for future errors
        df.loc[:,geolevel+year+'str'] = df[geolevel+year].apply(
            lambda x: "B"+str(x).zfill(15))
        
        return df

    @staticmethod
    def reshape_geoid_to_countvar(wide_df: pd.DataFrame, 
                 newvar,
                 stem,
                 countvar):
            """Using the mutually exclusive unit of analysis types  
            transpose data. This will result in a 
            long data file that has new unit of analysis unit counts by characteristic.
                
            Args:
                :param wide_df: dataframe to reshape
                :param newvar: name of new variable
                :param stem: The variable stem to be used for the reshape
                :param countvar: what does the variable count - persons, housing units
                
            
            Returns:
                pandas dataframe: Long version of data
                
            """                

            # Create list of columns to melt
            value_vars = [col for col in wide_df if col.startswith(stem)]

            # Create list of by columns - variables not in value var list
            by_vars = [col for col in wide_df if col not in value_vars]

            # Add suffix option to keep suffix as non-integer
            #df_reshaped = pd.wide_to_long(wide_df,[stem], i=by, j=newvar, suffix=r'\w+')
            df_reshaped = pd.melt(wide_df, 
                                    id_vars = by_vars,
                                    value_vars = value_vars,
                                    value_name = countvar)
            # shift dataframe multiindex to columns
            df_reshaped.reset_index(inplace=True)

            # Create new variable based on stem 
            #print(stem)
            df_reshaped.loc[:,newvar] = df_reshaped['variable'].apply(lambda x : x.replace(stem,''))
            
            # sort by by variables and new variable variable
            df_reshaped = df_reshaped.sort_values(by=by_vars + [newvar])

            # Convert data type to int
            df_reshaped[countvar] = df_reshaped[countvar].astype(int)

            # Drop observations with no housing unit count
            df_reshaped = df_reshaped.loc[df_reshaped[countvar] != 0]

            # drop temporary 'variable' variable created by melt command
            df_reshaped = df_reshaped.drop(columns = ['variable'])
            
            # Return data with housing unit  count
            return df_reshaped

    @staticmethod
    def expand_df(df, expand_var):
        """
        expand dataset based on the expand variable
        """

        # Expand data by the expand variable
        df = df.reindex(df.index.repeat(df[expand_var]))

        # reset index
        df.reset_index(inplace=True, drop = True)
        
        # drop variables for expanding
        df = df.drop(columns = [expand_var])

        return df

    @staticmethod
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

    """
    Section Break - Code used to add new characteristics 
    """

    @staticmethod
    def graft_on_new_char(base_inventory: pd.DataFrame,
                state_county: str, 
                new_char: str = 'hispan',
                new_char_dictionaries: list = [tenure_size_H16HAI_varstem_roots,
                    hispan_byrace_H7_varstem_roots,
                    tenure_byhispan_H15_varstem_roots
                    ],
                basevintage: str = "2010", 
                basegeolevel: str = 'Block',
                outputfile: str = "",
                outputfolders = {'BaseInventory' : 'state_county/popinv_workflow'},
                outputfolder: str = "popinv_workflow"):
        """
        The characteristics in the base inventory can be expanded
        Given additional tables in the Census Data it is possible 
        to add on new characteristics by merging multiple inventories
        together. The merge depends on shared characteristics.

        Example - The census provides multiple tables with counts of  units
        that are Hispanic. This includes tenure and size by Hispanic,
        Race by Hispanic, and Tenure by Hispanic.
        
        new_char_dictionaries (list): order list by tables with the most number of characteristics
        
        """
        # Check if final CSV file has aleady been selected
        csv_filename = f'{outputfile}_{new_char}_{state_county}_{basevintage}'
        csv_filepath = outputfolders['BaseInventory']+"/"+csv_filename+'.csv'

        # Check if selected data already exists - if yes read in saved file
        if os.path.exists(csv_filepath):
            expanded_hui = pd.read_csv(csv_filepath, 
                    dtype={
                            basegeolevel+basevintage: str
                        })
            # If file already exists return csv as dataframe
            print("File",csv_filepath,"Already exists with new variable grafted - Skipping API Call.")
            return expanded_hui

        # start loop by setting new housing unit inventory based on base HUI
        expanded_hui = base_inventory.copy(deep = True)

        # Check if new characteristic in in the dataframe
        if new_char in expanded_hui.columns:
            print('Base Housing Unit Inventory has new characteristic',new_char)
            print('Graft process will predict missing values of ',new_char)
        else:
            expanded_hui.loc[:,new_char] = np.nan
            print(new_char,'initially set to missing for all observations.')

        # Start off by checking new characteristic is not already set
        conditions = (expanded_hui[new_char] == 0)
        expanded_hui.loc[conditions, new_char+'_flag'] = new_char+" set to 0 by core hui"
        expanded_hui.loc[conditions, new_char+'_flagset'] = 1
        expanded_hui.loc[conditions, 'totalprob_'+new_char] = 0

        conditions = (expanded_hui[new_char] == 1)
        expanded_hui.loc[conditions, new_char+'_flag'] = new_char+" set to 1 by core hui"
        expanded_hui.loc[conditions, new_char+'_flagset'] = 1
        expanded_hui.loc[conditions, 'totalprob_'+new_char] = 1
        #  For observations with -999 new characteristic assume values = 1 and set flag to 0
        conditions =(expanded_hui[new_char] == -999)
        expanded_hui.loc[conditions, new_char+'_flagset'] = 0
        expanded_hui.loc[conditions, new_char+'_flag'] = new_char + " Not Set"
        expanded_hui.loc[conditions, new_char] = 1

        if 'vacancy' in expanded_hui.columns:
            # Replace new var with missing for observations that are vacant
            conditions = (expanded_hui['vacancy'] >= 1) & (expanded_hui['vacancy'] <=7)
            expanded_hui.loc[conditions, new_char] = np.nan
            expanded_hui.loc[conditions, new_char+'_flag'] = new_char+" set to missing by vacant"
            expanded_hui.loc[conditions, new_char+'_flagset'] = 1
            expanded_hui.loc[conditions, 'totalprob_'+new_char] = np.nan
        if 'gqtype' in expanded_hui.columns:
            # Replace new var with missing for observations are group quarters
            conditions = (expanded_hui['gqtype'] >= 1) & (expanded_hui['gqtype'] <=7)
            expanded_hui.loc[conditions, new_char] = np.nan
            expanded_hui.loc[conditions, new_char+'_flag'] = new_char+" set to missing by group quarters"
            expanded_hui.loc[conditions, new_char+'_flagset'] = 1
            expanded_hui.loc[conditions, 'totalprob_'+new_char] = np.nan

        # check shape of dataframe
        #print("\n\nShape of dataframe:",expanded_hui.shape)

        # Focus on observations that do not have the new var set
        expanded_hui_split = {}
        expanded_hui_split['Set1'] = expanded_hui.loc[expanded_hui[new_char+'_flagset'] == 1]
        expanded_hui_split['Not Set'] = expanded_hui.loc[expanded_hui[new_char+'_flagset'] == 0]
        base_inventory_notset_length = expanded_hui_split['Not Set'].shape[0]
        print("\n***************************************")
        print("    Base Inventory has",base_inventory_notset_length,"observations not set")
        print("***************************************\n")
        # Need to store counter vars, merge vars, and groups to update after loop
        base_counter_var_list = []
        merge_vars_dict = {}
        group_dict = {}
        # Need to store new char dataframes from each loop
        newchar_df = {}
        for newchar_varstem_roots in new_char_dictionaries:
            # Identify Character Vars for merge between base inventory and expanded hui
            char_vars =newchar_varstem_roots['metadata']['char_vars']
            graft_chars =newchar_varstem_roots['metadata']['graft_chars']
            group =newchar_varstem_roots['metadata']['group']
            print("\n***************************************")
            print("    Predicting",new_char,"based on",char_vars,graft_chars,group)
            print("***************************************\n")

            base_inventory_notset_length = expanded_hui_split['Not Set'].shape[0]
            print("\n***************************************")
            print("    Base Inventory has",base_inventory_notset_length,"observations not set")
            print("***************************************\n")
            if base_inventory_notset_length == 0:
                print("All observations have predicted",new_char)
                # break out of loop
                break

            newchar_df[group] = BaseInventory.get_data_based_on_varstems_and_roots(
                                                        state_county = state_county,
                                                        varstems_roots_dictionary = newchar_varstem_roots,
                                                        outputfolders = outputfolders)
            # rename count variable before merge - count variable is for the by 
            if 'hucount' in newchar_df[group].columns:
                countvar = 'hucount'
            elif 'preccount' in newchar_df[group].columns:
                countvar = 'preccount'
       
            newchar_var = [col for col in char_vars if col.startswith(new_char)]
            print(newchar_var[0])
            new_countvar = countvar+'_'+newchar_var[0]
            newchar_df[group] = newchar_df[group].rename(columns= {countvar : new_countvar})

            # Add GeoLevel ID
            newchar_df[group] = BaseInventory.add_block_geoidstr(newchar_df[group], 
                                                        geolevel=basegeolevel, 
                                                        year = basevintage)

            # Reorder columns
            primary_key = [basegeolevel+basevintage+'str']

            # remove by race hispanic - variable - this is replaced by race and hispan
            if 'byracehispan' in char_vars:
                #print(char_vars)
                # Remove byrace hispan from variable list
                char_vars_v2 = [var for var in char_vars if var != 'byracehispan']
                # Add missing graft characteristics
                missing_graft_chars = [var for var in graft_chars if var not in char_vars_v2]
                # Add race and hispan
                char_vars_v2 = char_vars_v2 + missing_graft_chars
                #print(char_vars_v2)
                col_list = primary_key + char_vars_v2 + [new_countvar]

                # find merge vars - merge vars are char vars that do not include new char column
                merge_char_vars = [col for col in char_vars_v2 if col not in newchar_var]
            else:
                col_list = primary_key + char_vars + [new_countvar]
                # find merge vars - merge vars are char vars that do not include new char column
                merge_char_vars = [col for col in char_vars if col not in newchar_var]
            newchar_df[group] = newchar_df[group][col_list]

            # find merge vars - merge vars are char vars that do not include new char column
            merge_vars = primary_key+merge_char_vars
            #print(merge_vars)

            # create base cumulative count variable
            # Add Counter the dataframe to compare to expected units for the new characteristic
            base_counter_var = newchar_var[0]+'_counter'
            # Add base counter to list of counters to update
            base_counter_var_list.append(base_counter_var)
            # Need to store merge vars for later update of counter var
            merge_vars_dict[base_counter_var] = merge_vars
            # Need to store groups for later update of counter var
            group_dict[base_counter_var] = group
            # the base inventory includes the new characteristic Variable
            # Try only setting base counter var after total probability is set
            # expanded_hui_split['Not Set'].loc[:,base_counter_var] = expanded_hui_split['Not Set'].groupby(merge_vars+[new_char]).cumcount() + 1

            # update the count var variable by reducing the count by the already set values
            # Update works for tables not based on race and hispanic values
            if 'byracehispan' not in char_vars:
                newchar_df_update_count = BaseInventory.\
                    update_newchar_countvar(dict_df = expanded_hui_split,
                        newchar_df = newchar_df[group],
                        merge_vars = merge_vars,
                        new_char = new_char,
                        new_countvar = new_countvar)

            else:
                newchar_df_update_count = newchar_df[group].copy()

            # add expected count of units by merge vars
            #print("Check before adding expected counts",group,newchar_varstem_roots['metadata']['concept'],"Split data for errors.")
            #print(expanded_hui_split['Not Set'].head())
            expanded_hui_split['Not Set'] = pd.merge(left = expanded_hui_split['Not Set'],
                                right = newchar_df_update_count,
                                left_on = merge_vars,
                                right_on = merge_vars,
                                how = "outer")
            #print(expanded_hui_split['Not Set'].head(1))
            #print("\n\nShape of dataframe after merge:",expanded_hui_split['Not Set'].shape)
            # fill missing values for hucount
            expanded_hui_split['Not Set'][new_countvar] = expanded_hui_split['Not Set'][new_countvar].fillna(value = 0)
            
            # fill missing values with the opposite of the new characteristic Variable
            # In the dictionary the new characteristic should have a value of 0 or 1
            fillna_test = expanded_hui_split['Not Set'][newchar_var[0]].mean()
            if fillna_test == 1:
                print("Fill missing values with 0")
                expanded_hui_split['Not Set'][newchar_var[0]] = \
                    expanded_hui_split['Not Set'][newchar_var[0]].fillna(value = 0)
            if fillna_test == 0:
                print("Fill missing values with 1")
                expanded_hui_split['Not Set'][newchar_var[0]] = \
                    expanded_hui_split['Not Set'][newchar_var[0]].fillna(value = 1)

            #print("\n\nShape of dataframe before total sum:",expanded_hui.shape)
            # Add sum of new characteristic variable by merge vars
            #print(expanded_hui_split['Not Set'].head(1))
            #print(merge_vars+[new_char+'_flagset'])
            #print("Check",newchar_varstem_roots['metadata']['concept'],"Split data for errors.")
            #print(expanded_hui_split['Not Set'].head())
            expanded_hui_split['Not Set'] = BaseInventory.add_total_sum_byvar(df = expanded_hui_split['Not Set'],
                                                        values_to_sum = new_char,
                                                        by_vars = merge_vars+[new_char+'_flagset'],
                                                        values_to_sum_col_rename = 'sumby_'+newchar_var[0])
            #print("\n\nShape of dataframe after total sum:",expanded_hui_split['Not Set'].shape)
            #print("\n\nColumns after total sum:",expanded_hui_split['Not Set'].columns)
            # Add probability of new charactersistic
            numerator =  expanded_hui_split['Not Set'][new_countvar]

            try:
                denominator =  expanded_hui_split['Not Set']['sumby_'+newchar_var[0]]
            except:
                print("Error Check",newchar_varstem_roots['metadata']['concept'],"in graft on new char.")
                #print(newchar_df_update_count.head())
                return expanded_hui_split['Not Set'] 
            
            expanded_hui_split['Not Set'].loc[:,'prob_'+newchar_var[0]] = numerator / denominator
            # Replace probability with 0 if denominator = 0
            expanded_hui_split['Not Set'].loc[denominator == 0,'prob_'+newchar_var[0]] = 0


            # If probability == 1 & new char flag is not set then reset new characteristic to newchar value
            condition1 = (expanded_hui_split['Not Set']['prob_'+newchar_var[0]]==1) 
            condition2 = (expanded_hui_split['Not Set'][new_char+'_flag'] == new_char + " Not Set")
            conditions = condition1 & condition2
            expanded_hui_split['Not Set'].loc[conditions, new_char] = expanded_hui_split['Not Set'][newchar_var[0]]
            # reset flag variable to capture update to new characteristic
            expanded_hui_split['Not Set'].loc[conditions, new_char+'_flag'] = \
                new_char+" set by prob = 1 "+newchar_var[0]
            expanded_hui_split['Not Set'].loc[conditions, new_char+'_flagset'] = 1
            expanded_hui_split['Not Set'].loc[conditions, 'totalprob_'+new_char] = 1

            # If probability == 0 & new char flag is not set &
            #  then reset new characteristic to 0
            condition1 = (expanded_hui_split['Not Set']['prob_'+newchar_var[0]]==0) 
            condition2 = (expanded_hui_split['Not Set'][new_char+'_flag'] == new_char + " Not Set")
            conditions = condition1 & condition2
            expanded_hui_split['Not Set'].loc[conditions, new_char] = 0
            # reset flag variable to capture update to new characteristic
            expanded_hui_split['Not Set'].loc[conditions, new_char+'_flag'] = \
                new_char+" set by prob = 0 "+newchar_var[0]        
            expanded_hui_split['Not Set'].loc[conditions, new_char+'_flagset'] = 1
            expanded_hui_split['Not Set'].loc[conditions, 'totalprob_'+new_char] = 0

            # update split
            expanded_hui_split[newchar_var[0]] = expanded_hui_split['Not Set'].loc[expanded_hui_split['Not Set'][new_char+'_flagset'] == 1]
            expanded_hui_split['Not Set'] = expanded_hui_split['Not Set'].loc[expanded_hui_split['Not Set'][new_char+'_flagset'] == 0]
            
            # Old probability is the total probability from the previous round - fill missing values with 1
            old_probability = expanded_hui_split['Not Set']['totalprob_'+new_char].fillna(value = 1)
            updated_probability = expanded_hui_split['Not Set']['prob_'+newchar_var[0]].fillna(value = 1)
            expanded_hui_split['Not Set'].loc[:,'totalprob_'+new_char] = old_probability * updated_probability

        # After total probability is set
        # Update Base Counter by Total Probability - this will avoid issue with 
        # Over assigning new characteristic to race category 2
        for base_counter_var in base_counter_var_list:
            # pull merge vars for counter var
            merge_vars = merge_vars_dict[base_counter_var]
            print(merge_vars)
            print("Updating",base_counter_var," based on total probability and", merge_vars)
            # Sort so that largest total probabilities are at the top
            expanded_hui_split['Not Set'] =  expanded_hui_split['Not Set'].\
                sort_values(by='totalprob_'+new_char, ascending = False)
            expanded_hui_split['Not Set'].loc[:,base_counter_var] = \
                expanded_hui_split['Not Set'].groupby(merge_vars+[new_char]).cumcount() + 1


        # Update Counts and check new char by counter
        for base_counter_var in base_counter_var_list:
            newchar_var = base_counter_var.replace('_counter','')
            new_countvar = countvar+'_'+newchar_var
            # pull merge vars for counter var
            merge_vars = merge_vars_dict[base_counter_var]
            print("Merge vars for",base_counter_var,"=",merge_vars)
            group = group_dict[base_counter_var]
            # If by race hispan is in the character list need to skip
            # the first set of flags. Without this White Hispanic are overcounted
            if 'byracehispan' not in char_vars:
                skip_sets = ['Not Set','Set1']
            else:
                skip_sets = ['Not Set']
            newchar_df_update_count = BaseInventory.update_newchar_countvar(
                            dict_df = expanded_hui_split,
                            newchar_df = newchar_df[group],
                            merge_vars = merge_vars,
                            new_char = new_char,
                            new_countvar = new_countvar,
                            skip_sets = skip_sets)

            # Update new count var name
            updated_countvar = new_countvar+'updated'
            newchar_df_update_count = newchar_df_update_count.\
            rename(columns= {new_countvar : updated_countvar})
            # add expected count of units by merge vars
            expanded_hui_split['Not Set'] = pd.merge(left = expanded_hui_split['Not Set'],
                                right = newchar_df_update_count[merge_vars+[updated_countvar]],
                                left_on = merge_vars,
                                right_on = merge_vars,
                                how = "outer")

            # fill missing values for hucount
            expanded_hui_split['Not Set'].loc[:,updated_countvar] = \
                expanded_hui_split['Not Set'][updated_countvar].fillna(value = 0)
            
            # Update Flag based on updated counter

            # After first round of setting new var use less than counters to update newvar
            # Running less than counter check first leads to over estimation of new char
            # More conservative approach than under estimation 
            # If flag is not set and counter is greater than expected unit count
            print("Updating flags based on",updated_countvar)
            condition1 = (expanded_hui_split['Not Set'][base_counter_var]<=\
                          expanded_hui_split['Not Set'][updated_countvar]) 
            condition2 = (expanded_hui_split['Not Set'][new_char+'_flag'] == \
                             new_char + " Not Set")
            # New characteristic is 1 but since counter is less than expected set to 1
            contition3 = (expanded_hui_split['Not Set'][newchar_var]== 1)
            conditions = condition1 & condition2 & contition3
            expanded_hui_split['Not Set'].loc[conditions, new_char] = 1
            # # reset flag variable to capture update to new characteristic
            expanded_hui_split['Not Set'].loc[conditions, new_char+'_flag'] = \
                new_char+" set 1 by less than counter "+newchar_var
            expanded_hui_split['Not Set'].loc[conditions, new_char+'_flagset'] = 1

            # New characteristic is 0 but since counter is less than expected set to 0
            contition4 = (expanded_hui_split['Not Set'][newchar_var]== 0)
            conditions = condition1 & condition2 & contition4
            expanded_hui_split['Not Set'].loc[conditions, new_char] = 0
            # # reset flag variable to capture update to new characteristic
            expanded_hui_split['Not Set'].loc[conditions, new_char+'_flag'] = \
                new_char+" set 0 by less than counter "+newchar_var
            expanded_hui_split['Not Set'].loc[conditions, new_char+'_flagset'] = 1

            # update split
            expanded_hui_split[base_counter_var] = expanded_hui_split['Not Set'].\
                loc[expanded_hui_split['Not Set'][new_char+'_flagset'] == 1]
            expanded_hui_split['Not Set'] = expanded_hui_split['Not Set'].\
                loc[expanded_hui_split['Not Set'][new_char+'_flagset'] == 0]
            
            

        #Recombine splits 
        # Remove dataframes with no length form dictionary before combining
        # Without this step error is generated
        for key in expanded_hui_split.keys():
            length_of_dataframe = expanded_hui_split[key].shape[0]
            print('Length of',key,'split dataframe',length_of_dataframe)
            if length_of_dataframe == 0:
                # remove key from dictionary
                #expanded_hui_split.pop(key, None)
                print(key, 'Has no observations')
        expanded_hui_recombine = pd.concat(expanded_hui_split.values(), 
                                ignore_index=True, axis=0)
        print("\n\nShape of dataframe after total sum:",expanded_hui_recombine.shape)

        # After first round of setting new var use less than counters to update newvar
        # Running less than counter check first leads to over estimation of new char
        # More conservative approach than under estimation        
        
        # After second round of setting new var use greater than counters to update newvar
        prob_cols = [col for col in expanded_hui_recombine if col.startswith("prob_"+new_char)]
        print(prob_cols)
        for prob_col in prob_cols:
            newchar_var = prob_col.replace('prob_','')
            print(newchar_var)

            counter_var = newchar_var+'_counter'
            new_countvar = countvar+'_'+newchar_var+'updated'
            # If flag is not set and counter is greater than expected unit count
            condition1 = (expanded_hui_recombine[counter_var]>\
                expanded_hui_recombine[new_countvar]) 
            condition2 = (expanded_hui_recombine[new_char+'_flag'] == new_char + " Not Set")
            # New characteristic is 1 but since counter is greater than expected set to 0
            contition3 = (expanded_hui_recombine[newchar_var]== 1)
            conditions = condition1 & condition2 & contition3
            expanded_hui_recombine.loc[conditions, new_char] = 0
            # # reset flag variable to capture update to new characteristic
            expanded_hui_recombine.loc[conditions, new_char+'_flag'] = \
                new_char+" set 0 by greater than counter "+newchar_var
            expanded_hui_recombine.loc[conditions, new_char+'_flagset'] = 1

            # New characteristic is 0 but since counter is greater than expected set to 1
            contition4 = (expanded_hui_recombine[newchar_var]== 0)
            conditions = condition1 & condition2 & contition4
            expanded_hui_recombine.loc[conditions, new_char] = 1
            # # reset flag variable to capture update to new characteristic
            expanded_hui_recombine.loc[conditions, new_char+'_flag'] = \
                new_char+" set 1 by greater than counter "+newchar_var
            expanded_hui_recombine.loc[conditions, new_char+'_flagset'] = 1
        
        savefile = sys.path[0]+"/"+csv_filepath
        expanded_hui_recombine.to_csv(savefile, index=False)

        return expanded_hui_recombine

    @staticmethod                
    def update_newchar_countvar(dict_df,
                        newchar_df,
                        merge_vars: list = ['Block2010str', 'race'],
                        new_char: str = 'hispan',
                        new_countvar: str = 'precount_hispanbyP5',
                        skip_sets: list = ['Not Set']):
        """
        To predict new characteristic need to update the 
        count of the characteristic as it is predicted based
        on various census tables.
        """

        new_char_set_dict_df = {}
        # Loop through keys of split dataframes to sum totals
        for key in dict_df.keys():
            # Only sum split dataframes that are set - skip Not Set and Set1
            # Set1 
            if key not in skip_sets:
                #print("Updating count for",key,"by",merge_vars)
                new_char_set_dict_df[key] = dict_df[key][merge_vars+[new_char]].\
                    groupby(merge_vars).sum()
                new_char_set_dict_df[key].reset_index(inplace=True)
                #print("update count shape = ",new_char_set_dict_df[key].shape)
        # After loop sum each set of set values
        new_char_set_df = pd.concat(new_char_set_dict_df)
        new_char_set_df_count = new_char_set_df[merge_vars+[new_char]].\
            groupby(merge_vars).sum()
        new_char_set_df_count.reset_index(inplace=True)
        new_char_set_df_count = new_char_set_df_count.\
            rename(columns= {new_char : new_countvar})

        # Subtract New Char Count of Set observations from New Char Data Frame
        newchar_df_update_count = BaseInventory.subtract_df(newchar_df,\
            new_char_set_df_count,index_col=merge_vars)

        return newchar_df_update_count

    def probability_graft_on_new_char(df, new_char: str = 'hispan'):
        """
        Use output of graft_on_new_char to check probability of new char
        """

        # what are the probability columns
        prob_cols = [col for col in df if col.startswith("prob_"+new_char)]

        # Calculate total probability


        return df

    def add_total_sum_byvar(df, values_to_sum, by_vars, values_to_sum_col_rename):
        """
        Function adds a new column with sum of values by variables
        Used to determine numerator of probability a binary new characteristic 
        """

        # Check to make sure values to sum is not in by vars
        #print("Total sum based on :",by_vars)
        #print("   new sum to add will be called: ",values_to_sum_col_rename)
        if values_to_sum in by_vars:
            # By vars can not include the value to sum
            by_vars = [var for var in by_vars if var not in [values_to_sum]]
            print("   Fix by vars :",by_vars)

        total_sum_df = pd.pivot_table(df, values=values_to_sum, index=by_vars,
                                aggfunc=np.sum)
        total_sum_df.reset_index(inplace = True)
        total_sum_df = total_sum_df.rename(columns = {values_to_sum : values_to_sum_col_rename})

        # add probability denomninator to the original data frame 
        df = pd.merge(left = df,
                        right = total_sum_df,
                        left_on = by_vars,
                        right_on = by_vars,
                        how = 'outer')

        return df

    def add_probability_job_selected(df, 
                            unit_of_analysis_id: str = 'huid', 
                            by_vars: list = ['blockid']):
        """
        The probability a unit of analysis has new characteristic
        based on the number of units with the same set of by_vars
        unit_of_analysis_id (str) default 'huid' : default is set to the unique id
            for the housing units or precid for person records
        by_vars (list): Default blockid
        """

        df = BaseInventory.add_total_count_byvar(df, 
                                values_to_count = unit_of_analysis_id,
                                by_vars = by_vars,  
                                values_to_count_col_rename = 'denominator')
        df = BaseInventory.add_total_count_byvar(df, 
                                values_to_count = 'denominator',
                                by_vars = by_vars+[unit_of_analysis_id],  
                                values_to_count_col_rename = "numerator")
        
        df['prob_selected'] = df['numerator'] / df['denominator'] 

        df = df.drop(columns = ['numerator','denominator'])

        return df