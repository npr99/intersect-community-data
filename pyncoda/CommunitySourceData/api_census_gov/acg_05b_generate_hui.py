"""
# Generate Housing Unit File

Functions to obtain and clean data required for the version 2 Housing Unit Inventory. 
The workflow  replicates the original version 1 (alpha version)
of Housing Unit Inventory workflow in Python using Census API. 

The workflow also expands the Housing Unit Inventory to include
household income based on family and non-family income distributions 
by race and ethnicity.

For version 1 of the housing unit inventory 
process and example applications see:

Rosenheim, Nathanael (2021) “Detailed Household and Housing Unit Characteristics: 
Data and Replication Code.” DesignSafe-CI. 
https://doi.org/10.17603/ds2-jwf6-s535.

The 2010 Census Data provides detailed household and housing unit, 
level characteristics at the census block level. 

The 2012 5-year American Community Survey provides detailed 
household level characteristics at the census tract level.

## Description of Program
- program:    ncoda_1av1_run_HUI_v2_workflow
- task:       Obtain and clean data for Housing Unit Inventory.
- See github commits for description of program updates
- Current Version:    2022-03-31 - preparing for publication
- project:    Interdependent Networked Community Resilience Modeling Environment 
        (IN-CORE), Subtask 5.2 - Social Institutions
- funding:	  NIST Financial Assistance Award Numbers: 70NANB15H044 and 70NANB20H008 
- author:     Nathanael Rosenheim

- Suggested Citation:
Rosenheim, Nathanael. (2022). "Detailed Household and Housing Unit 
Characteristics: Data and Replication Code." [Version 2] 
DesignSafe-CI. https://doi.org/10.17603/ds2-jwf6-s535

"""
import numpy as np
import pandas as pd
import os # For saving output to path
import sys

# open, read, and execute python program with reusable commands
from pyncoda.CommunitySourceData.api_census_gov.acg_05a_hui_functions \
    import hui_workflow_functions
from pyncoda.ncoda_00b_directory_design import directory_design
from pyncoda.ncoda_06c_Codebook import *
from pyncoda.ncoda_04a_Figures import *

from pyncoda.CommunitySourceData.api_census_gov.acg_00e_incore_huiv2 \
    import incore_v2_DataStructure

version = '2.0.0'
version_text = 'v2-0-0'

# Save Outputfolder - due to long folder name paths output saved to folder with shorter name
# files from this program will be saved with the program name - 
# this helps to follow the overall workflow
outputfolder = "..//ncoda_workflow_2022-03-31"
# Make directory to save output
if not os.path.exists(outputfolder):
    os.mkdir(outputfolder)

# To run workflow for multiple communities
communities = {'Lumberton_NC' : {
                    'community_name' : 'Lumberton, NC',
                    'counties' : { 
                        1 : {'FIPS Code' : '37155', 'Name' : 'Robeson County, NC'}}},                   
                'Shelby_TN' : {
                    'community_name' : 'Memphis, TN',
                    'counties' : { 
                        1 : {'FIPS Code' : '47157', 'Name' : 'Shelby County, TN'}}},
                'Joplin_MO' : {
                    'community_name' : 'Joplin, MO',
                    'counties' : { 
                        1 : {'FIPS Code' : '29097', 'Name' : 'Jasper County, MO'},
                        2 : {'FIPS Code' : '29145', 'Name' : 'Newton County, MO'}}},
                'Seaside_OR' : {
                    'community_name' : 'Seaside, OR',
                    'counties' : { 
                        1 : {'FIPS Code' : '41007', 'Name' : 'Clatsop County, OR'}}},                   
                'Galveston_TX' : {
                    'community_name' : 'Galveston, TX',
                    'counties' : { 
                        1 : {'FIPS Code' : '48167', 'Name' : 'Galveston County, TX'}}},                   
                'Mobile_AL' : {
                    'community_name' : 'Mobile, AL',
                    'counties' : { 
                        1 : {'FIPS Code' : '01097', 'Name' : 'Mobile County, AL'}}}                    
                }

''' Example of running workflow for one community
communities = {'Lumberton_NC' : {
                    'community_name' : 'Lumberton, NC',
                    'counties' : { 
                        1 : {'FIPS Code' : '37155', 'Name' : 'Robeson County, NC'}}}}
'''

# Set random seed for reproducibility
seed = 1000
basevintage = 2010

for community in communities.keys():
    # Create empty container to store outputs for in-core
    # Will use these to combine multiple counties
    hui_incore_county_df = {}
    print("Setting up Housing Unit Inventory for",communities[community]['community_name'])
    for county in communities[community]['counties'].keys():
        state_county = communities[community]['counties'][county]['FIPS Code']
        state_county_name  = communities[community]['counties'][county]['Name']
        print(state_county_name,': county FIPS Code',state_county)
    
        outputfolders = directory_design(state_county_name = state_county_name,
                                            outputfolder = outputfolder)
                                            
        generate_df = hui_workflow_functions(
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
        hui_df = generate_df.final_polish_hui(base_hui_df['primary'])

        # Save version for IN-CORE in v2 format
        hui_incore_county_df[state_county] = \
            generate_df.save_incore_version2(hui_df)

    # combine multiple counties
    hui_incore_df = pd.concat(hui_incore_county_df.values(), 
                                    ignore_index=True, axis=0)

    # Remove .0 from data
    hui_incore_df_fixed = hui_incore_df.applymap(lambda cell: int(cell) if str(cell).endswith('.0') else cell)

    #Save results for community name
    output_filename = f'hui_{version_text}_{community}_{basevintage}_rs{seed}'
    csv_filepath = outputfolders['top']+"/"+output_filename+'.csv'
    savefile = sys.path[0]+"/"+csv_filepath
    hui_incore_df_fixed.to_csv(savefile, index=False)

    # Save second set of files in common directory
    common_directory = outputfolders['top']+"/../../"+output_filename
    hui_incore_df_fixed.to_csv(common_directory+'.csv', index=False)
    
    # Generate figures for explore data
    figures_list = []
    for by_var in ["race","hispan","family"]:
        income_by_var_figure = income_distribution(input_df = hui_incore_df,
                        variable = "randincome",
                        by_variable = by_var,
                        datastructure = incore_v2_DataStructure,
                        communities= communities,
                        community = community,
                        year = basevintage,
                        outputfolders = outputfolders)
        filename = income_by_var_figure+".png"
        figures_list.append(filename)

    # Paths for codebook text
    CommunitySourceData_filepath = "pyncoda\\CommunitySourceData\\api_census_gov"
    keyterms_filepath = CommunitySourceData_filepath+ \
            '\\'+"acg_00a_keyterms.md"

    projectoverview_filepath = 'pyncoda\\'+ "ncoda_00a_projectoverview.md"

    # Create PDF Codebook
    pdfcodebook = codebook(input_df = hui_incore_df_fixed,
            header_title = 'Housing Unit Inventory',
            datastructure = incore_v2_DataStructure,
            projectoverview = projectoverview_filepath,
            keyterms = keyterms_filepath,
            communities = communities,
            community = community,
            year = basevintage,
            output_filename = output_filename,
            outputfolders = outputfolders,
            figures = figures_list,
            image_path = 'IN-CORE_HRRC_CodebookBanner.png')
    pdf_codebook = pdfcodebook.create_codebook()



