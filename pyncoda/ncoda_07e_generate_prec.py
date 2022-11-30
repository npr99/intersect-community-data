"""
# Generate Person Record File

This notebook obtains and cleans data required Person Record Files. 

The workflow extends the process by generating a 
person record files with age, sex, race, and ethnicity. 

For the original Alpha Release of the housing unit inventory 
process and example applications see:

Rosenheim, Nathanael (2021) “Detailed Household and Housing Unit Characteristics: 
Alpha Release of Housing Unit Inventories.” DesignSafe-CI. https://doi.org/10.17603/ds2-jwf6-s535.

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
from pyncoda.CommunitySourceData.api_census_gov.acg_05a_hui_functions \
    import hui_workflow_functions
from pyncoda.ncoda_00b_directory_design import directory_design
from pyncoda.ncoda_04a_Figures import *
from pyncoda.ncoda_06c_Codebook import *
from pyncoda.ncoda_06d_INCOREDataService import *

from pyncoda.CommunitySourceData.api_census_gov.acg_00e_incore_huiv2 \
    import incore_v2_DataStructure

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

    