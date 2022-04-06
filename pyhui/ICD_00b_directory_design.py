# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import os # For saving output to path
import sys

    
def directory_design(state_county_name,
                     outputfolder):
    """
    Setup output directories 
    Process generates many files 
    need to organize files in a way the makes the final file
    easy to find and easy to understand inputs

    Process centers around individual counties.
    """
    # Create primary folder to save output
    folder_name = state_county_name.replace(' ','')
    folder_name = folder_name.replace(',','_')
    # Create output folder for county
    outputfolder_county = outputfolder+'/'+folder_name
        # Make directory to save output
    if not os.path.exists(outputfolder_county):
        os.mkdir(outputfolder_county)
        print('Creating folder',outputfolder_county,'to store output.')

    directory_names = {'logfiles' : 'Store text files with log of workflow.',
                        'SourceData' : 'Source source data files. Helps with software \
                            development and replication.',
                        'TidySourceData' : 'Cleaned source data and \
                            inputs for base inventory.', 
                        'BaseInventory' : 'Initial inventories before random merge. \
                            Use as inputs to uncertainty propagation.',
                        'RandomMerge' : 'Results of random merge with flag columns.',
                        'Verify' : 'Results to compare inventories with source data.',
                        'Explore' : 'Explore output results.',
                        'Uncertainty_propagation' : 'Results of MCS to determine \
                            which random seed to use for validation.',
                        'Validation' : 'Application using inventory e.g. IN-CORE.'}
    
    # create self dictionary to store paths
    outputfolders = {}
    outputfolders['top'] = outputfolder_county
    # Add counter to file names so that they sort in logical order
    counter = 0
    for directory_name in directory_names:
        directory_name_with_counter = str(counter).zfill(2) + '_' + directory_name
        check_folder_exists = outputfolder_county + '/' + directory_name_with_counter
        # Make directory to save output
        if not os.path.exists(check_folder_exists):
            os.mkdir(check_folder_exists)
            print('Creating folder',check_folder_exists)
            print('    ',directory_name,'Folder purpose:',\
                directory_names[directory_name])
        # save directory information
        outputfolders[directory_name] = check_folder_exists
        counter += 1

    return outputfolders