"""
Function to read in data for 2009-2010 CCD NCES

https://nces.ed.gov/ccd/files.asp#Fiscal:2,LevelId:7,SchoolYearId:24,Page:1
https://nces.ed.gov/ccd/data/zip/sc092a_sas.zip

Have the option of a flat file or a sas file
Tried the SAS file but pandas did not read the file well
https://pandas.pydata.org/docs/reference/api/pandas.read_sas.html

need to use WGET to download the file and then unzip it and then read it in.

"""
import sys
import pandas as pd
import os
from pyncoda.CommunitySourceData.nces_ed_gov.nces_00a_datastructure \
    import *
from pyncoda.CommunitySourceData.nces_ed_gov.nces_00c_cleanutils \
    import *

def nces_clean_student_ccd(
        outputfolder,
        input_directory = '\\Outputdata\\00_SourceData\\nces_ed_gov\\unzipped',
        ccd_file_name  = "sc092a.sas7bdat",
        selectvar = 'CONUM09',
        county_list = ['37155'], 
        communityname = 'RobesonCounty_NC',
        year = '09',
        integer_columns = ['NCESSCH','FIPST','LEAID','SCHNO',
                           'STID09','SEASCH09','LATCOD09','LONCOD09'],
        ):


    # Check if final CSV file has aleady been generated
    csv_filename = f'nces_ccd_{communityname}_{year}'
    csv_filepath = outputfolder+"/"+csv_filename+'.csv'

    # Check if selected data already exists - if yes read in saved file
    if os.path.exists(csv_filepath):
        output_df = pd.read_csv(csv_filepath, low_memory=False,
            dtype = {'NCESSCH' : str})
        # If file already exists return csv as dataframe
        print("File",csv_filepath,"Already exists - Skipping Clean CCD NCES.")
        return output_df

    # If file does not exist read in raw data
    ccd_folderpath = sys.path[0]+input_directory
    ccd_filepath = ccd_folderpath+'\\'+ccd_file_name

    ccd_df = pd.read_sas(ccd_filepath, format = 'sas7bdat')

    # Clean up all columns 
    # https://nces.ed.gov/ccd/data/txt/psu092alay.txt
    for col in list(ccd_df.columns):
        print(col)
        ccd_df[col] = ccd_df[col].astype(str)
        ccd_df[col] = ccd_df[col].str.replace("b", "")
        ccd_df[col] = ccd_df[col].str.replace("'", "")

        try: # To convert columns to integers
            if col not in integer_columns:
                ccd_df[col] = ccd_df[col].astype(float)
                ccd_df[col] = ccd_df[col].astype(int)

                missingvalue_codes = {
                -1: "when numeric data are missing; that is, " + \
                    "a value is expected but none was measured.",
                -2: "when numeric data are not applicable; "+ \
                    "that is, a value is neither expected nor measured.",
                -9: "when the submitted data item does not " + \
                    "meet NCES data quality standards; "+ \
                    "the value is suppressed."
                }
                for missingvalue_code in missingvalue_codes:
                    condition = (ccd_df[col] == missingvalue_code)
                    count_obs = len(ccd_df.loc[condition])
                    print("    ",count_obs,"Observations with missing value code",missingvalue_code)
                    print("    Code meaning:",missingvalue_codes[missingvalue_code])
                    ccd_df.loc[condition,col] = 0
                    print("    Missing values replaces with 0")
        except:
            print(col,"not converted to integer")

    # Select county data from ccd_df
    ccd_df = select_var(data = ccd_df,
                        selectvar = selectvar,
                        selectlist = county_list)
    ccd_v2a_datadict = ccd_v2a_datastructure()
    wide_vars = ccd_v2a_widevars(ccd_v2a_datadict)

    """
    ### Check if the sum of wide vars always equals total
    NCES seems to suggest that students belonging to an 
    unknown or non-CCD race category are not captured.  
    """
 
    ccd_df['TotalStudents'] = 0
    for var in wide_vars:
        ccd_df['TotalStudents'] = ccd_df['TotalStudents'] + ccd_df[var]

    # MEMBER09 is the total number of students by Grade Level
    ccd_df['CheckTotal1'] = ccd_df['MEMBER09'] - ccd_df['TotalStudents'] 
    # TOTETH09 count of students by total ethnic 
    ccd_df['CheckTotal2'] = ccd_df['TOTETH09'] - ccd_df['TotalStudents'] 

    # Flag if CheckTotal1 or CheckTotal2 is not 0
    ccd_df['CountFlag1'] = 0
    ccd_df.loc[ccd_df['CheckTotal1']!=0,'CountFlag1'] = 1
    ccd_df['CountFlag2'] = 0
    ccd_df.loc[ccd_df['CheckTotal2']!=0,'CountFlag2'] = 1

    """ Explore data
    ccd_df.head()

    # States use different race categories
    ccd_df.RACECAT09.describe()
    table_df = ccd_df
    table = table_df[['FIPST','RACECAT09']].groupby(['FIPST']).describe()
    table

    total_race_vars = ['MEMBER09','AM09','ASIAN09','HISP09','BLACK09','WHITE09','PACIFIC09',
                        'TR09','TOTETH09']
    id_vars = ['NCESSCH','SCHNAM09'] 
    check_totals = ccd_df[id_vars+total_race_vars].copy()
    check_totals.loc[check_totals['NCESSCH']=='370393002236'].T

    """
    # Save results for one county the outputfolder
    savefile = sys.path[0]+"/"+csv_filepath
    ccd_df.to_csv(savefile, index=False)


    return ccd_df