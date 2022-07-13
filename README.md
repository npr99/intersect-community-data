[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/npr99/intersect-community-data/HEAD?labpath=ncoda_06dv1_run_HUI_v2_workflow.ipynb)

[![DOI](https://zenodo.org/badge/478737101.svg)](https://zenodo.org/badge/latestdoi/478737101)

![IN-CORE HRRC Banner](https://github.com/npr99/intersect-community-data/blob/main/IN-CORE_HRRC_Banner.png?raw=true)

**Repository Title**: Intersect Community Data

**Point of Contact**: Nathanael Rosenheim: nrosenheim@arch.tamu.edu

**Citation**: Rosenheim, Nathanael. (2022). "Detailed Household and Housing Unit Characteristics: Data and Replication Code." [Version 2] __DesignSafe-CI__. https://doi.org/10.17603/ds2-jwf6-s535

**Project Description** : People are the most important part of community resilience planning. However, models for community resilience planning tend to focus on buildings and infrastructure. This repository provides a solution that connects people to buildings for community resilience models. The housing unit inventory method transforms aggregated population data into disaggregated housing unit data that includes occupied and vacant housing unit characteristics. Detailed household characteristics include size, race, ethnicity, income, group quarters type, vacancy type and census block. Applications use the housing unit allocation method to assign the housing unit inventory to structures within each census block through a reproducible and randomized process. The benefits of the housing unit inventory include community resilience statistics that intersect detailed population characteristics with hazard impacts on infrastructure; uncertainty propagation; and a means to identify gaps in infrastructure data such as limited building data. This repository includes all of the python code files. Python is an open source programming language and the code files provide future users with the tools to generate a 2010 housing unit inventory for any county in the United States. Applications of the method are reproducible in IN-CORE (Interdependent Networked Community Resilience Modeling Environment).

**Funding Source: Award name and number**: Center for Risk-Based Community Resilience Planning : NIST-70NANB20H008, NIST-70NANB15H044 (http://resilience.colostate.edu/)

**Related Works**:

Rosenheim, Nathanael, Roberto Guidotti, Paolo Gardoni & Walter Gillis Peacock. (2019). Integration of detailed household and housing unit characteristic data with critical infrastructure for post-hazard resilience modeling. __Sustainable and Resilient Infrastructure__. https://doi.org/10.1080/23789689.2019.1681821

Rosenheim, Nathanael. (2021). "Detailed Household and Housing Unit Characteristics: Data and Replication Code." [Version 1] __DesignSafe-CI__. https://doi.org/10.17603/ds2-jwf6-s535

Interdependent Networked Community Resilience Modeling Environment (IN-CORE) : https://incore.ncsa.illinois.edu/

Hazard Reduction and Recovery Center (HRRC): https://www.arch.tamu.edu/impact/centers-institutes-outreach/hazard-reduction-recovery-center/

**Keywords**: Social and Economic Population Data, Housing Unit, US Census, Synthetic Population

**Output Data License**: Open Data Commons Attribution License (https://opendatacommons.org/licenses/by/summary/)

**Program Code**: Mozilla Public License Version 2.0 (https://www.mozilla.org/en-US/MPL/2.0/)

**DesignSafe-CI project identification number**: Project-2961 - Available at: https://www.designsafe-ci.org/data/browser/public/designsafe.storage.published/PRJ-2961

## Important Acronyms
- acg = api.census.gov - primary data source
- HRRC = Hazard Reduction and Recovery Center
- hui = housing unit inventory
- ncoda = Intersect Community Data
- IN-CORE = Interdependent Networked Community Resilience Modeling Environment

## Census Data Terms of Use
This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau. https://www.census.gov/data/developers/about/terms-of-service.html

"Users will not use these data, alone or in combination with any other Census or non-Census data, to:
1. identify any individual person, household, business or other entity
2. not link or combine these data with information in any other Census or non-Census dataset in a manner that identifies an individual person, household, business or other entity
3. not publish information from these data files, particularly in combination with any other Census or non-Census data, in a manner that identifies any individual person, household, business or other entity; and
4. not use the identity of any person or establishment discovered inadvertently and advise the Director of the Census Bureau of any such discovery by calling the U.S. Census Bureau’s Policy Coordination Office at 301-763-6440."

## Setup Environment 
see `environment.yml` file for dependencies

## Data File Naming Convention
The housing unit inventory file names include the following convention:
- Acronym   = hui - for housing unit inventory
- version   = v2-0-0 - version of the file
- community = community name - name of the community ex. Lumberton, NC
- year      = year of the data - ex. 2010
- random seed = random seed used to generate the data - ex. rs1000

Example description of the file name `hui_v2-0-0_Lumberton_NC_2010_rs1000.csv`:
- Version 2 Housing Unit Inventory for Lumberton, NC, using 2010 Census data. Set the random seed to 1000 to replicate the data. 

Each data file is a comma separated value (CSV) file and each data file has a PDF file associated with it. The PDF file is the Codebook for the data file.
The Codebook includes an overview of the project, details and notes on each variable, 
links to verify the data, and figures for exploring the data. **NOTE** - To understand the data please refer to the Codebook.

## Program File Naming Convention
Program file names are designed to be unique and descriptive. The file names provide a way to identify the source of the data (based on the acronym), the order the programs should be run, and the version of the program.
All program files (python files) are named with the following convention:
* Acronym  (e.g ICD - for Intersect Community Data)
* Data science workflow step:
    - 00 - Administrative, Overview of Data, General Utility Functions
    - 01 - Obtain Data (read in ata from api, wget, data parsing, etc.)
    - 02 - Data Cleaning (merging files, adding new colum

## Acknowledgements
[Work on acknowledgments]