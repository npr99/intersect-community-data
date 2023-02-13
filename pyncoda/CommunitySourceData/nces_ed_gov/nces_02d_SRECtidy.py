"""


"""
import pandas as pd
import os
import sys

from pyncoda.CommunitySourceData.nces_ed_gov.nces_02c_SRECcleanCCD \
    import nces_clean_student_ccd

from pyncoda.CommunitySourceData.nces_ed_gov.nces_00a_datastructure \
    import *

from pyncoda.ncoda_00f_tidy \
    import icd_tidy as icdtidy 

def tidy_SREC_nces(outputfolder, CONUM = '37155', year = '09'):
    """
    Function that converts Student Record NCES data from wide to long
    Each observation is one student with variables 
    representing the student's school,
    gradelevel, race, ethnicity, and sex

    """
    # Check if final CSV file has aleady been generated
    csv_filename = f'nces_tidy_SCREC_ccd_{CONUM}_{year}'
    csv_filepath = outputfolder+"/"+csv_filename+'.csv'

    # Check if selected data already exists - if yes read in saved file
    if os.path.exists(csv_filepath):
        output_df = pd.read_csv(csv_filepath, low_memory=False,
            dtype = {'NCESSCH' : str,
                    'ncessch_1' : str,
                    'ncessch_2' : str,
                    'ncessch_3' : str,
                    'ncessch_4' : str,
                    'ncessch_5' : str,
                    'ncessch_6' : str,
                    'racecat5' : int,
                    'race' : int,
                    'hispan' : int,
                    'sex' : int
                    })
        # If file already exists return csv as dataframe
        print("File",csv_filepath,"Already exists - Skipping Tidy SCREC NCES.")
        return output_df

    # Select county data from ccd_df
    ccd_df = nces_obtain_ccd0910()
    ccd_df = ccd_df.loc[ccd_df['CONUM09'] == CONUM].copy()

    ccd_v2a_datadict = ccd_v2a_datastructure()
    wide_vars = ccd_v2a_widevars(ccd_v2a_datadict)

    #ccd_df = nces_obtain_ccd0910()
    """
    ### Check if the sum of wide vars always equals total
    NCES seems to suggest that students belonging to an 
    unknown or non-CCD race category are not captured.  
    """
    ccd_df['TotalWideVar'] = 0
    for var in wide_vars:
        ccd_df['TotalWideVar'] = ccd_df['TotalWideVar'] + ccd_df[var]


    ccd_df['Check_MEMBER09'] = ccd_df['MEMBER09'] - ccd_df['TotalWideVar'] 
    ccd_df['Check_TOTETH09'] = ccd_df['TOTETH09'] - ccd_df['TotalWideVar'] 

    """
    ## Prepare data for reshape
    Need to add a stem to each variable for the melt (wide to long to work)
    """

    countvar = 'StudentCount'
    id_vars = ['NCESSCH','SCHNAM09','LEVEL09','CHARTR09','LATCOD09','LONCOD09'] 

    print("******************************")
    print(" Reshaping data from wide to long")
    print("******************************")

    df_reshaped = pd.melt(ccd_df, 
                        id_vars = id_vars,
                        value_vars = wide_vars,
                        value_name = countvar)
    # shift dataframe multiindex to columns
    df_reshaped.reset_index(inplace=True)
    # Convert data type to int
    df_reshaped[countvar] = df_reshaped[countvar].astype(int)
    # Drop observations with no housig unit count
    df_reshaped = df_reshaped.loc[df_reshaped[countvar] != 0]

    print("******************************")
    print(" Add characteristic variables")
    print("******************************")
    # Create Variable List with mutually Exclusive values
    year = '09'
    for race in ccd_v2a_datadict['RACECAT_5']:
        print("      Add race = ", \
            ccd_v2a_datadict['RACECAT_5'][race]['label'])
        racecat_char = ccd_v2a_datadict['RACECAT_5'][race]['racecat5']
        race_char = ccd_v2a_datadict['RACECAT_5'][race]['race']
        hispan_char = ccd_v2a_datadict['RACECAT_5'][race]['hispan']
        for grade in ccd_v2a_datadict['gradelevels']:
            #print("      Add gradelevel = ", \
            #    ccd_v2a_datadict['gradelevels'][grade]['label'])
            gradelevel_char = ccd_v2a_datadict['gradelevels'][grade]['gradelevel']
            for sex in ccd_v2a_datadict['Sex']:
                #print("      Add sex = ", ccd_v2a_datadict['Sex'][sex]['label'])
                sex_char = ccd_v2a_datadict['Sex'][sex]['sex']
                char_var = race+grade+sex+year
                #print(char_var)
                condition = (df_reshaped['variable'] == char_var)
                df_reshaped.loc[condition, 'racecat5'] = racecat_char
                df_reshaped.loc[condition, 'race'] = race_char
                df_reshaped.loc[condition, 'hispan'] = hispan_char
                df_reshaped.loc[condition, 'sex'] = sex_char
                df_reshaped.loc[condition, 'gradelevel'] = gradelevel_char

    # drop temporary 'variable' variable created by melt command
    df_reshaped = df_reshaped.drop(columns = ['variable'])
                
    df_reshaped

    print("******************************")
    print(" Expand Dataset")
    print("******************************")
    df_expand = icdtidy.expand_df(df_reshaped,'StudentCount')

    print("     Add Counter")
    df_expand['srec_counter'] = df_expand.groupby(['NCESSCH']).cumcount() + 1
    print("     Generate unique ID")
    counter_var = 'srec_counter'
    primary_key = 'srecid'
    schoolid = 'NCESSCH'
    id_type = "S"
    schoolyear = '20092010'
    counter_var_max = df_expand[counter_var].max()
    counter_var_maxdigits = len(str(counter_var_max))
    print("     Counter max length",counter_var_maxdigits)
    df_expand.loc[:,primary_key] = df_expand.apply(lambda x: \
                                    id_type + x[schoolid] + 
                                    'syr' + schoolyear + 'c' +
                                    str(x[counter_var]).\
                                       zfill(counter_var_maxdigits), axis=1)

    srec_df = df_expand.copy()

    ### Create 7 levels of ncessch ids
    # ncessch_2 = Middle
    # ncessch_3 = High
    # ncessch_4 = Other
    # ncessch_5 = Overlaping SABS
    # ncessch_6 = Charter Schools - CIS Academy in Pembroke
    #               Southeast Academy - not open in 2009-2010
    for level in ['1','2','3','4']:
        srec_df.loc[srec_df['LEVEL09'] == level, 'ncessch_'+level] = srec_df['NCESSCH']

    # Manually fix issue with L Gilbert Carroll Middle SAB
    condition1 = (srec_df['NCESSCH'] == '370393002235')
    conditions = condition1
    srec_df.loc[conditions,'ncessch_5'] = '370393002235'

    # Manually fix issue with CIS ACADEMY
    # Located in Pembroke, North Carolina
    condition1 = (srec_df['NCESSCH'] == '370004002349')
    conditions = condition1
    srec_df.loc[conditions,'ncessch_6'] = '370004002349'

    # Create gradelevel1 and gradelevel2 columns for merge with person records
    # these new columns are the same but need to be named and repeated for merge
    srec_df['gradelevel1'] = srec_df['gradelevel']
    srec_df['gradelevel2'] = srec_df['gradelevel']
    srec_df['gradelevel3'] = srec_df['gradelevel']

    #The student record file has 3 gradelevel columns
    # 2 are used for the random merge. One is assigned to the person.

    """ Explore data
    srec_df.head()

    ccd_df[['NCESSCH','MEMBER09','TOTETH09','TotalWideVar']].\
        loc[ccd_df['NCESSCH']=='370393002236'].head()

    pd.set_option('display.max_columns', None)
    ccd_df[['FIPST','Check_MEMBER09']].\
        loc[ccd_df['Check_MEMBER09']>0].groupby(['FIPST']).describe()

    srec_df.srecid.astype(str).describe()
    
    srec_df[['CHARTR09','SCHNAM09','LEVEL09','gradelevel']].\
        groupby(['CHARTR09','LEVEL09','gradelevel']).describe()

    """

    # Save results for one county the outputfolder
    savefile = sys.path[0]+"/"+csv_filepath
    srec_df.to_csv(savefile, index=False)

    return srec_df

