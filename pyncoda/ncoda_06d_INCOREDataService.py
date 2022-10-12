
import numpy as np
import pandas as pd
import os # For saving output to path
import urllib
import sys

# Functions from IN-CORE
from pyincore import IncoreClient, DataService

def loginto_incore_dataservice():
    """
    code for logging into IN-CORE
    
    Set up pyincore and read in data
    IN-CORE is an open source python package that can be used to model the resilience of a community. To download IN-CORE, see:

    https://incore.ncsa.illinois.edu/

    Registration is free.

    """

    client_login = IncoreClient()
    # IN-CORE caches files on the local machine, it might be necessary to clear the memory
    #client_login.clear_cache() 

    # create data_service object for loading files
    data_service_login = DataService(client_login)

    return data_service_login

def check_file_on_incore(title):
    """
    Check if HUI data is on IN-CORE
    """

    data_service_checkfile = loginto_incore_dataservice()
    # Search Data Services for dataset

    url = urllib.parse.urljoin(data_service_checkfile.base_url, "search")
    search_title = {"text": title}
    matched_datasets = data_service_checkfile.client.get(url, params=search_title)

    return matched_datasets

def return_dataservice_id(title, output_filename):
    
    # Check if file exists on IN-CORE
    matched_datasets = check_file_on_incore(title)
    match_count = len(matched_datasets.json())
    print(f'Number of datasets matching {title}: {match_count}')

    if match_count == 1:
        for dataset in matched_datasets.json():
            incore_filename = dataset['fileDescriptors'][0]['filename']
            if (dataset['title'] == title) and (incore_filename == output_filename+'.csv'):
                print(f'Dataset {title} already exists in IN-CORE')
                print(f'Dataset already exists in IN-CORE with filename {incore_filename}')
                dataset_id = dataset['id']
                print("Use dataset_id:",dataset_id)
                
                # Exit function and return dataset_id
                return dataset_id
            else:
                print(f'Dataset {title} ')
                print(f'with matching filename {incore_filename} does not exist in IN-CORE')

                return None
    elif match_count == 0:
        print(f'Dataset {title} does not exist in IN-CORE')

        return None
    else:
        print("There are multiple datasets matching the title. Please select one.")
        for i, dataset in enumerate(matched_datasets):
            print(i,matched_datasets[i]['dataset']['id'])
        dataset_id = matched_datasets[int(input("Enter dataset number: "))]['dataset']["id"]
        print("Use dataset_id:",dataset_id)
        return dataset_id