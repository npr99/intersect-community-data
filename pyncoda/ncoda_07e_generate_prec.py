"""
# Generate Person Record File

This notebook obtains and cleans data required Person Record Files. 

The workflow extends the process by generating a 
person record files with age, sex, race, and ethnicity. 

The 2010 Census Data provides detailed household, housing unit, 
and person level characteristics at the census block level. 

The 2021 5-year American Community Survey provides detailed 
household level characteristics at the census tract level.

## Description of Program
- program:    ncoda_07e_generate_prec
- task:       Obtain and clean data for Person Record Files.
- See github commits for description of program updates
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
import urllib
import sys

# Functions from IN-CORE
from pyincore import IncoreClient, DataService

# open, read, and execute python program with reusable commands
from pyncoda.CommunitySourceData.api_census_gov.acg_05b_prec_functions \
    import prec_workflow_functions
from pyncoda.ncoda_00b_directory_design import directory_design
from pyncoda.ncoda_04a_Figures import *
from pyncoda.ncoda_06c_Codebook import *
from pyncoda.ncoda_06d_INCOREDataService import *

from pyncoda.CommunitySourceData.api_census_gov.acg_00g_prec_datastructure \
    import prec_v300_DataStructure

class generate_prec_functions():
    """
    Function runs full process for generating the person record files
    Process runs for multiple counties.

    Outputs CSV files and Codebooks
    """

    def __init__(self,
            communities,
            seed: int = 9876,
            version: str = '3.0.0',
            version_text: str = 'v3-0-0',
            basevintage: str = 2010,
            outputfolder: str ="",
            outputfolders = {},
            savefiles: bool = True):

        self.communities = communities
        self.seed = seed
        self.version = version
        self.version_text = version_text
        self.basevintage = basevintage
        self.outputfolder = outputfolder
        self.outputfolders = outputfolders
        self.savefiles = savefiles


        # Save Outputfolder - due to long folder name paths output saved to folder with shorter name
        # files from this program will be saved with the program name - 
        # this helps to follow the overall workflow
        # Make directory to save output
        if not os.path.exists(self.outputfolder):
            os.mkdir(self.outputfolder)

    def generate_prec_v300(self):
        """
        Generate Person Record File data 
        version 3.0.0
        """

        for community in self.communities.keys():
            # Create empty container to store outputs by county
            # Will use these to combine multiple counties
            prec_county_df = {}
            title = "Person Record File v3.0.0 data for "+self.communities[community]['community_name']
            print("Generating",title)
            output_filename = f'prec_{self.version_text}_{community}_{self.basevintage}_rs{self.seed}'
            county_list = ''
    
            # Workflow for generating prec data for IN-CORE
            for county in self.communities[community]['counties'].keys():
                state_county = self.communities[community]['counties'][county]['FIPS Code']
                state_county_name  = self.communities[community]['counties'][county]['Name']
                print(state_county_name,': county FIPS Code',state_county)
                county_list = county_list + state_county_name+': county FIPS Code '+state_county

                # create output folders for prec data generation
                outputfolders = directory_design(state_county_name = state_county_name,
                                                    outputfolder = self.outputfolder)
                                                    
                generate_df = prec_workflow_functions(
                    state_county = state_county,
                    state_county_name= state_county_name,
                    seed = self.seed,
                    version = self.version,
                    version_text = self.version_text,
                    basevintage = self.basevintage,
                    outputfolder = self.outputfolder,
                    outputfolders = outputfolders)

                # Generate base housing unit inventory
                prec_county_df[state_county] = generate_df.run_prec_workflow()


            # combine multiple counties
            prec_df = pd.concat(prec_county_df.values(), 
                                            ignore_index=True, axis=0)

            # Remove .0 from data - may not be an issue
            prec_df_fixed = prec_df.applymap(lambda cell: int(cell) if str(cell).endswith('.0') else cell)

            #Save results for community name
            csv_filepath = outputfolders['top']+"/"+output_filename+'.csv'
            savefile = sys.path[0]+"/"+csv_filepath
            prec_df_fixed.to_csv(savefile, index=False)

            # Save second set of files in common directory
            common_directory = outputfolders['top']+"/../"+output_filename
            prec_df_fixed.to_csv(common_directory+'.csv', index=False)

        return prec_df_fixed