
## Data File Naming Convention

## Important Acronyms
- acg = api.census.gov - primary data source
- HRRC = Hazard Reduction and Recovery Center
- hui = housing unit inventory
- ncoda = Intersect Community Data
- IN-CORE = Interdependent Networked Community Resilience Modeling Environment


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
    - 02 - Data Cleaning (merging files, adding new columns, checking keys, etc.)
    - 03 - Data Analysis
    - 04 - Data Visualization
    - 05 - Data Output (combined workflow steps for producing csv files)
    - 06 - Data Production (codebook, data sharing, archiving, etc.)
    - 07 - Data workflows (produce files from other programs)
    - 08 - Validation workflows
    - 09 - Uncertainty propagation workflows
* Program run order step (e.g run a before b or b depends on code in a)
* version  (e.g v2 - version 2 of the file)
    - Github used for incremental version control, version number updated when program file has a major change.
