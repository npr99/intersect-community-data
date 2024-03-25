import numpy as np
import pandas as pd
import os # For saving output to path

# open, read, and execute python program with reusable commands
from pyincore_data_addons.SourceData.api_census_gov.run_hu_prec_workflow import run_hui_prec_workflow
from pyincore_data_addons.SourceData.api_census_gov.merge_hu_prec_worklow import merge_hu_prec_worklow
from pyincore_data_addons.ICD00b_directory_design import directory_design

#state_counties = {'48167' : 'Galveston County, TX'}
#state_counties = {'37155' : 'Robeson County, NC'}
#state_counties = {'26127' : 'Oceanna County, MI'}
state_counties = {'48041' : 'Brazos County, TX'}

# Set random seed for reproducibility
seed = 9876
basevintage = 2010
version = '0.2.0'
version_text = 'v0-2-0'

# Store Program Name for output files to have the same name
programname = "PopInv_1av1_GenerateHUI_PRECI"
# Save Outputfolder - due to long folder name paths output saved to folder with shorter name
# files from this program will be saved with the program name - this helps to follow the overall workflow
outputfolder = "popinv_workflow_2021-12-30"
# Make directory to save output
if not os.path.exists(outputfolder):
    os.mkdir(outputfolder)


for state_county in state_counties:

    state_county_name = state_counties[state_county]
    outputfolders = directory_design(state_county_name = state_counties[state_county],
                                        outputfolder = outputfolder)
                                        
    generate_df = run_hui_prec_workflow(
        state_county = state_county,
        state_county_name= state_counties[state_county],
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

    merge_hu_prec_df = merge_hu_prec_worklow(
        state_county = state_county,
        state_county_name= state_counties[state_county],
        seed = seed,
        version = version,
        version_text = version_text,
        basevintage = basevintage,
        outputfolder = outputfolder,
        outputfolders = outputfolders)
    
    prechui_df = merge_hu_prec_df.prepare_hui_df_for_merge(
                    hui_df = hui_df,
                    prec_df = prec_df)
    
    # Need to collapse prechui_df by huid to update numprec in hui_df

    # Save version for IN-CORE in v0.1.0 format
    hui_incore_df = generate_df.save_incore_version010(hui_df)