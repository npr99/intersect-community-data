"""
# Generate Housing Unit and Person Record Inventory Files

This notebook obtains and cleans data required for the Housing Unit and 
Person Record Inventories. The workflow  replicates the original alpha version 
of Housing Unit Inventory workflow in Python using Census API. 

The workflow also expands the Housing Unit Inventory to include
household income based on family and non-family income distributions 
by race and ethnicity. The workflow further extends the process by generating a 
person record inventory with age, sex, race, and ethnicity. 

For the original Alpha Release of the housing unit inventory 
process and example applications see:

Rosenheim, Nathanael (2021) “Detailed Household and Housing Unit Characteristics: 
Alpha Release of Housing Unit Inventories.” DesignSafe-CI. https://doi.org/10.17603/ds2-jwf6-s535.

The 2010 Census Data provides detailed household, housing unit, 
and person level characteristics at the census block level. 

The 2021 5-year American Community Survey provides detailed 
household level characteristics at the census tract level.
"""
import numpy as np
import pandas as pd
import os # For saving output to path
import sys

# open, read, and execute python program with reusable commands
from pyincore_data_addons.SourceData.api_census_gov.run_hu_prec_workflow \
    import run_hui_prec_workflow
from pyincore_data_addons.SourceData.api_census_gov.merge_hu_prec_worklow \
    import merge_hu_prec_worklow
from pyincore_data_addons.ICD00b_directory_design import directory_design
from pyincore_data_addons.SourceData.api_census_gov.CodeBook import *
from pyincore_data_addons.SourceData.api_census_gov.Explore import *

from pyincore_data_addons.SourceData.api_census_gov.acg_00e_incore_huiv010 \
    import incore_v010_DataStructure

version = '0.2.0'
version_text = 'v0-2-0'

# Store Program Name for output files to have the same name
programname = "PopInv_1av1_GenerateHUI_PRECI"
# Save Outputfolder - due to long folder name paths output saved to folder with shorter name
# files from this program will be saved with the program name - this helps to follow the overall workflow
outputfolder = "popinv_workflow_2022-01-05"
# Make directory to save output
if not os.path.exists(outputfolder):
    os.mkdir(outputfolder)

communities = {'Joplin_MO' : {
                    'community_name' : 'Joplin, MO',
                    'counties' : { 
                        1 : {'FIPS Code' : '29097', 'Name' : 'Jasper County, MO'},
                        2 : {'FIPS Code' : '29145', 'Name' : 'Newton County, MO'}}}
                }

communities = {'Seaside_OR' : {
                    'community_name' : 'Seaside, OR',
                    'counties' : { 
                        1 : {'FIPS Code' : '41007', 'Name' : 'Clatsop County, OR'}}},                   
                }
                
communities = {'Lumberton_NC' : {
                    'community_name' : 'Lumberton, NC',
                    'counties' : { 
                        1 : {'FIPS Code' : '37155', 'Name' : 'Robeson County, NC'}}},                   
                }
        
communities = {'Galveston_TX' : {
                    'community_name' : 'Galveston, TX',
                    'counties' : { 
                        1 : {'FIPS Code' : '48167', 'Name' : 'Galveston County, TX'}}},                   
                }

# Set random seed for reproducibility
seed = 9876
basevintage = 2010

# Create empty container to store outputs for in-core
# Will use these to combine multiple counties
hui_incore_county_df = {}
prechui_df = {}

for community in communities.keys():
    print("Setting up Housing Unit and Person Record Inventories for",communities[community]['community_name'])
    for county in communities[community]['counties'].keys():
        state_county = communities[community]['counties'][county]['FIPS Code']
        state_county_name  = communities[community]['counties'][county]['Name']
        print(state_county_name,': county FIPS Code',state_county)
    
        outputfolders = directory_design(state_county_name = state_county_name,
                                            outputfolder = outputfolder)
                                            
        generate_df = run_hui_prec_workflow(
            state_county = state_county,
            state_county_name= state_county_name,
            seed = seed,
            version = version,
            version_text = version_text,
            basevintage = basevintage,
            outputfolder = outputfolder,
            outputfolders = outputfolders)

        # Generate base housing unit inventory
        base_hui_df = generate_df.run_hui_workflow()
        # Add householder by tenure and age
        h17_df = generate_df.hui_tidy_H17()
        # Add householder sex, numprec, and household type
        h18_df = generate_df.hui_tidy_H18()
        h17_h18_df = generate_df.hui_randommerge_H17_H18(h17_df,h18_df)
        # Add householder age, sex, numprec and household type to base hui
        hui_h17_h18_df  = generate_df.hui_randommerge_base_H17_H18(base_hui_df['primary'],h17_h18_df['primary'])
        # Need to add income by householder age
        hui_B19037_df  = generate_df.hui_tidy_B19037()
        hui_df = generate_df.final_polish_hui(hui_h17_h18_df['primary'])
        prec_df = generate_df.run_prec_workflow()

        # Merge Housing Unit and Person Record Inventories
        merge_hu_prec_df = merge_hu_prec_worklow(
            state_county = state_county,
            state_county_name= state_county_name,
            seed = seed,
            version = version,
            version_text = version_text,
            basevintage = basevintage,
            outputfolder = outputfolder,
            outputfolders = outputfolders)
        
        prechui_df[state_county] = merge_hu_prec_df.prepare_hui_df_for_merge(
                        hui_df = hui_df,
                        prec_df = prec_df)
        
        # Need to collapse prechui_df by huid to update numprec in hui_df

        # Save version for IN-CORE in v0.1.0 format
        hui_incore_county_df[state_county] = generate_df.save_incore_version010(hui_df)

    # combine multiple counties
    hui_incore_df = pd.concat(hui_incore_county_df.values(), 
                                    ignore_index=True, axis=0)

    #Save results for community name
    output_filename = f'hui_v0-1-0_{community}_{basevintage}_rs{seed}'
    csv_filepath = outputfolders['top']+"/"+output_filename+'.csv'
    savefile = sys.path[0]+"/"+csv_filepath
    hui_incore_df.to_csv(savefile, index=False)

    # Create PDF Codebook
    pdfcodebook = codebook(input_df = hui_incore_df,
            datastructure = incore_v010_DataStructure,
            communities = communities,
            community = community,
            year = basevintage,
            output_filename = output_filename,
            outputfolders = outputfolders)
    pdfcodebook.create_categorical_data_codebook()