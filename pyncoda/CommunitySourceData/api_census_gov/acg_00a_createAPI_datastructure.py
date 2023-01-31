# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import requests ## Required for the Census API
import pandas as pd # For reading, writing and wrangling data
import json # used to read in metadata for Census variables
import numpy as np # For setting missing values
import urllib.request, json  # reading in json files
import os # for reading in files
import copy # For coping dictionaries

from pyncoda.CommunitySourceData.api_census_gov.acg_00a_general_datastructures import *

class createAPI_datastructure():
    """"
    Using API metadata create a data dictionary that has 
    require information for obtaining and cleaning Census API data.
    Output of this function is an input to the BaseInventory Functions,
    and the add categorical char functions.
    """

    def obtain_api_metadata(vintage: str = "2010" ,
                        dataset_name: str = "dec/sf1",
                        group: str = "P12",
                        outputfolder: str = "",
                        version_text: str  = 'v0-2-0'):
        """
        Function obtains API variable metadata.
        Metadata used to create data structure dictionaries.
        """

        # Add Source Data folder for Census API
        # Data structures apply to multiple counties
        datastructure_folder = outputfolder+'/00_datastructures'
        # Make directory to save output
        if not os.path.exists(datastructure_folder):
            os.mkdir(datastructure_folder)
        # Add API Sub folder
        api_datastructure_folder = outputfolder+'/00_datastructures/api_census_gov'
        # Make directory to save output
        if not os.path.exists(api_datastructure_folder):
            os.mkdir(api_datastructure_folder)

        # Check if API call has already by completed
        # save dictionary as text file
        dict_filename = f'{group}_{vintage}_datastructure'
        dict_filepath = api_datastructure_folder+"/"+dict_filename+version_text+'.txt'
        
        # Check if selected data already exists - if yes read in saved file
        if os.path.exists(dict_filepath):
            # reading the data from the file
            with open(dict_filepath) as f:
                data = f.read()
            #print("Data type before reconstruction : ", type(data))
            # reconstructing the data as a dictionary
            dict = json.loads(data)
            #print("Data type after reconstruction : ", type(dict))
            # If file already exists return csv as dataframe
            print("Dictionary file",dict_filepath,"Already exists - Skipping API Call.")
            return dict

        # Start empty dictionary to store variable metadata
        datastructure_dict = {}

        datastructure_dict['metadata'] = {}
        datastructure_dict['metadata']['concept'] = ''
        datastructure_dict['metadata']['vintage'] = vintage
        datastructure_dict['metadata']['dataset_name'] = dataset_name
        datastructure_dict['metadata']['group'] = group
        datastructure_dict['metadata']['notes'] = []
        # Assume mutually exclusive is True
        # mutually exculusive is false for groups that have race and Hispanic tables
        datastructure_dict['metadata']['mutually_exclusive'] = True

        # Figure out geography
        if dataset_name == "dec/sf1":
            # For decennial census assume block level unless
            if "CT" in group:
                for_geography = 'tract:*'
                indexvar = ['GEO_ID','state','county','tract']
            else:
                for_geography = 'block:*'
                indexvar = ['GEO_ID','state','county','tract','block']
        # If not decennial census assume acs and tract level data
        else:
            for_geography = 'tract:*'
            indexvar = ['GEO_ID','state','county','tract']
        datastructure_dict['metadata']['for_geography'] = for_geography
        datastructure_dict['metadata']['indexvar'] = indexvar
        
        # Figure out unit of analysis
        # First letter of group can help determine unit of analyis
        # This works for decennial census
        first_letter_of_group = group[0:1]
        if dataset_name == "dec/sf1":
            if first_letter_of_group == 'P':
                unit_of_analysis = 'person'
                countvar = 'preccount'
            if first_letter_of_group == 'H':
                unit_of_analysis = 'household'
                countvar = 'hucount'       
        # If not decennial census assume acs and household level data
        else:
            unit_of_analysis = 'household'
            countvar = 'hucount'  
        datastructure_dict['metadata']['unit_of_analysis'] = unit_of_analysis
        datastructure_dict['metadata']['countvar'] = countvar                  
        
        # Add notes to metadata
        group_api_page = f'https://api.census.gov/data/{vintage}/{dataset_name}/groups/{group}.html.'
        datastructure_dict['metadata']['notes'].append(group_api_page)
        print("Obtaining data stucture for",group)
        print("Check weblink for variable list:",group_api_page)

        # Create varstem from group for decennial census
        # The variable stem is the group letters plus the number zero padded
        if dataset_name == "dec/sf1":
            group_head = group.rstrip('0123456789')
            group_tail = group[len(group_head):]
            varstem = group_head+str(group_tail).zfill(3)
        # If not decennial census assume that varstem = group
        else:
            varstem = group

        # Start empty container for variable stem
        datastructure_dict[varstem] = {}
        # Run loop until 404 error is hit
        request_error = 0
        i = 1
        while request_error == 0:
            variable = varstem + str(i).zfill(3)
            # ACS variables have the letter E for 'estimate' on the end
            # ACS variables also have an underscore between the varstem and hte number
            if dataset_name == 'acs/acs5':
                variable = varstem + '_'+str(i).zfill(3) + 'E'
            variable_metadata_hyperlink = (f'https://api.census.gov/data/{vintage}/{dataset_name}/variables/{variable}.json')
            #print(variable_metadata_hyperlink)
            # Obtain Census API JSON Data
            if requests.get(variable_metadata_hyperlink).status_code == 404:
                request_error = 1
                print("Assume reached the end of variable list with",variable)
                break
            # if no error code lode metadata
            else:
                with urllib.request.urlopen(variable_metadata_hyperlink) as url:
                    variable_metadata = json.load(url)
                # Advance i for next variable
                i = i+1

            # Store concept
            census_concept_string = str(variable_metadata["concept"])
            if datastructure_dict['metadata']['concept'] != census_concept_string:
                #print("Obtaining metadata for concept",census_concept_string)
                datastructure_dict['metadata']['concept'] = census_concept_string

            # Find the variable label 
            census_label_string = str(variable_metadata["label"])
            #print(census_label_string)
            # Find Variable Line number - variable name minus group varstem
            variable_linenumber = variable.replace(varstem,"")
            # For ACS remove the underscore
            if dataset_name == 'acs/acs5':
                variable_linenumber = variable_linenumber.replace("_","")
            # start empty container for variable linenumber
            datastructure_dict[varstem][variable_linenumber] = {}
            datastructure_dict[varstem][variable_linenumber]['label'] = census_label_string
            #print(census_label_string)
            # Split Label in to new variables
            for substring in census_label_string.split("!!"):
                # Skip Total
                if substring != "Total":
                    if 'Male' in substring:
                        datastructure_dict[varstem][variable_linenumber]['sex'] = 1
                    if 'Female' in substring:
                        datastructure_dict[varstem][variable_linenumber]['sex'] = 2
                    if substring == 'Owner occupied':
                        datastructure_dict[varstem][variable_linenumber]['ownershp'] = 1
                    if substring == 'Renter occupied':
                        datastructure_dict[varstem][variable_linenumber]['ownershp'] = 2
                    if substring == 'Family households':
                        datastructure_dict[varstem][variable_linenumber]['family'] = 1
                    if substring == 'Nonfamily households':
                        datastructure_dict[varstem][variable_linenumber]['family'] = 0
                    if substring == 'Living alone':
                        datastructure_dict[varstem][variable_linenumber]['numprec'] = 1
                    if substring == 'Not living alone':
                        datastructure_dict[varstem][variable_linenumber]['numprec'] = -999
                    if substring == 'Husband-wife family':
                        datastructure_dict[varstem][variable_linenumber]['sex'] = -999
                        datastructure_dict[varstem][variable_linenumber]['numprec'] = -999
                    if substring == 'Other family':
                        datastructure_dict[varstem][variable_linenumber]['numprec'] = -999
                    # Add group quarters type
                    for value_labels in [group_quarters_valueLabels]:
                        for variable in value_labels.keys():
                            var_label = value_labels['metadata']['label']
                            if variable != 'metadata':
                                for value in value_labels[variable].keys():
                                    checkstring = value_labels[variable][value]['label']
                                    if checkstring in substring:
                                        #print('Setting values for',var_label,checkstring)
                                        datastructure_dict[varstem][variable_linenumber][variable] = value


                    if "year" in substring:
                        # Find min and max years based on string
                        # using List comprehension + isdigit() +split()
                        # getting numbers from string 
                        ages = [int(i) for i in substring.split() if i.isdigit()]
                        # Check length of age list - should be 2 for min and max ages
                        if len(ages) == 2:
                            datastructure_dict[varstem][variable_linenumber]['minageyrs'] = ages[0]
                            datastructure_dict[varstem][variable_linenumber]['maxageyrs'] = ages[1]
                        # There are 2 cases where min and max age are assumed
                        if len(ages) == 1:
                            #print(ages,substring)
                            if "Under" in substring:
                                datastructure_dict[varstem][variable_linenumber]['minageyrs'] = 0
                                datastructure_dict[varstem][variable_linenumber]['maxageyrs'] = ages[0]-1
                            elif "under" in substring:
                                datastructure_dict[varstem][variable_linenumber]['minageyrs'] = 0
                                datastructure_dict[varstem][variable_linenumber]['maxageyrs'] = ages[0]-1
                                # Check if for householder
                                if "Householder" in substring:
                                    datastructure_dict[varstem][variable_linenumber]['minageyrs'] = 15
                                    datastructure_dict['metadata']['notes'].append('Assume min age of houseohld is 15 years')
                            elif "and over" in substring:
                                datastructure_dict[varstem][variable_linenumber]['minageyrs'] = ages[0]
                                # Assume max age of 110 years
                                datastructure_dict[varstem][variable_linenumber]['maxageyrs'] = 110
                                datastructure_dict['metadata']['notes'].append('Assume max age of 110 years')
                            else:
                                # Min and Max years will be the same
                                datastructure_dict[varstem][variable_linenumber]['minageyrs'] = ages[0]
                                datastructure_dict[varstem][variable_linenumber]['maxageyrs'] = ages[0]
                    if "$" in substring:
                        # Find min and max dollar values based on string
                        # using List comprehension + isdigit() +split()
                        # getting numbers from string 
                        # remove commas and dollar signs from substring
                        substring = substring.replace(',','')
                        substring = substring.replace('$','')
                        #print(substring)
                        dollars = [int(i) for i in substring.split() if i.isdigit()]
                        #print(dollars)
                        # Check length of age list - should be 2 for min and max ages
                        if len(dollars) == 2:
                            datastructure_dict[varstem][variable_linenumber]['mindollars'] = dollars[0]
                            datastructure_dict[varstem][variable_linenumber]['maxdollars'] = dollars[1]
                        # There are 2 cases where min and max age are assumed
                        if len(dollars) == 1:
                            if "Less than" in substring:
                                datastructure_dict[varstem][variable_linenumber]['mindollars'] = 0
                                datastructure_dict[varstem][variable_linenumber]['maxdollars'] = dollars[0]-1
                            elif "or more" in substring:
                                datastructure_dict[varstem][variable_linenumber]['mindollars'] = dollars[0]
                                # Assume max dollars is $50,000 more than max
                                datastructure_dict[varstem][variable_linenumber]['maxdollars'] = dollars[0]+50000
                                datastructure_dict['metadata']['notes'].append('Assume max dollars is $50,000 more than max')
                            else:
                                # Min and Max years will be the same
                                datastructure_dict[varstem][variable_linenumber]['mindollars'] = dollars[0]
                                datastructure_dict[varstem][variable_linenumber]['maxdollars'] = dollars[0]                

        # All dictionary items should have the same number of characteristics
        # Items like "Total" will lead to double counting observations
        # Remove items that have fewer than the maximum number of characteristics
        # Step 1 - find max character count
        max_char_count = 1
        print(datastructure_dict.keys())
        for dict_key in datastructure_dict.keys():
            if dict_key != 'metadata':
                for variable in datastructure_dict[dict_key].keys():
                    char_count = len(datastructure_dict[dict_key][variable].keys())
                    #print(char_count)
                    if char_count > max_char_count:
                        max_char_count = char_count
        # Step 2 - remove (pop) items from dictionary
        remove_vars = []
        for dict_key in datastructure_dict.keys():
            if dict_key != 'metadata':
                for var_stem in datastructure_dict[dict_key].keys():
                    char_count = len(datastructure_dict[dict_key][var_stem].keys())
                    if char_count < max_char_count:
                        print("Remove",var_stem,"from dictionary.")
                        remove_vars.append(var_stem)
                for remove_var in remove_vars:
                    datastructure_dict[dict_key].pop(remove_var)
                
        # Add char vars to metadata
        for dict_key in datastructure_dict.keys():
            if dict_key != 'metadata':
                # Get variables for first key - assume all keys the same
                first_key = list(datastructure_dict[dict_key].keys())[0]
                vars = datastructure_dict[dict_key][first_key].keys()
                vars = datastructure_dict[dict_key][var_stem].keys()
                # char vars are vars in dictionary not equal to label
                char_vars = [var for var in vars if var != 'label']
                datastructure_dict['metadata']['char_vars'] = char_vars

        # Check if variable groups has more than max for API call
        for dict_key in datastructure_dict.keys():
            if dict_key != 'metadata':
                number_of_vars_in_group = len(datastructure_dict[dict_key].keys())
                print("Data structure has",number_of_vars_in_group,"variables.")
                if number_of_vars_in_group > 45:
                    print("Data structure for API call will have too many variables.")
                    print("Census API limits data to 50 variables - including GEOID and geovars.")
                    print("Need to split up varstem into parts.")
                    partcount = 1
                    varcount = 1
                    new_data_structure_dict = {}
                    # Copy metadata into new dictionary
                    new_data_structure_dict['metadata'] = datastructure_dict['metadata']

                    varstem = dict_key
                    # Start empty dictionary to store new varstem parts
                    varstem_part = varstem+'_part'+str(partcount).zfill(2)
                    new_data_structure_dict[varstem_part] = {}
                    for var in datastructure_dict[dict_key].keys():
                        new_data_structure_dict[varstem_part][var] = datastructure_dict[varstem][var]
                        varcount += 1
                        if varcount == 45:
                            partcount += 1
                            print("Max variables for API call reached. Starting new varstem part",partcount)
                            varcount = 1
                            varstem_part = varstem+'_part'+str(partcount).zfill(2)
                            new_data_structure_dict[varstem_part] = {}

                    # replace data structure
                    datastructure_dict = {}
                    datastructure_dict = new_data_structure_dict

                else:
                    print("Data structure has fewer than max variables for API call.")

        # save dictionary as text file
        with open(dict_filepath, 'w') as convert_file:
            convert_file.write(json.dumps(datastructure_dict))

        return datastructure_dict

    def convert_datastructure_dict_to_groups(data_structure_dict, 
                                                minvar: str = 'mindollars', 
                                                maxvar: str = 'maxdollars',
                                                newgroup: str = 'incomeB19'):
        """
        Example of income need to convert to income groups based on min and max income
        """
        i = 1
        group_dict = {}
        for group in data_structure_dict.keys():
            if group != 'metadata':
                for var in data_structure_dict[group].keys():
                    group_dict[i] = {}
                    group_dict[i]['min'+newgroup] = data_structure_dict[group][var][minvar]
                    group_dict[i]['max'+newgroup] = data_structure_dict[group][var][maxvar]
                    i += 1

        return group_dict

    
    def add_byracehispan(data_structure_dict, 
                            byracehispan_groups,
                            byracehispan_groups_mx,
                            newgroup: str = "HAI",
                            newcharbyvar: str = ""):
        """
        Function adds by race hispanic to the dictionary.
        Some data structures can be run by race and Hispanic
        To produce mutually exclusive values by race and Hispanic.
        """

        output_dict = copy.deepcopy(data_structure_dict)

        if 'byracehispan' not in output_dict['metadata']['char_vars']:
            output_dict['metadata']['char_vars'].append('byracehispan')
        output_dict['metadata']['mutually_exclusive'] = False
        output_dict['metadata']['byracehispan'] = byracehispan_groups
        output_dict['metadata']['mutually_exclusive_dict'] = byracehispan_groups_mx

        # Update group name to reflect race Hispanic
        group = output_dict['metadata']['group']
        if newgroup not in group:
            output_dict['metadata']['group'] = group + newgroup

        # Add new char by var - this is needed for grafting new variables
        if newcharbyvar != "":
            for group in output_dict.keys():
                if group != 'metadata':
                    for var_stem in output_dict[group].keys():
                        output_dict[group][var_stem][newcharbyvar] = 1
        
        return output_dict