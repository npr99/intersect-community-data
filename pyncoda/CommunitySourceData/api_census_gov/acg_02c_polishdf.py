import pandas as pd
import numpy as np

@staticmethod
def fill_missingvalues(input_df):
    """
    vacancy type and gqytpe should be 0 for 
    occupied housing units

    numprec should be 0 for vacant housing units

    Vacancy should be 0 for Group Quarters
    gqtype should be 0 for vacant housing units

    consider making all categorical variables have 0 values
    for observations where category is not applicable
    vacant housing units race = 0 and ownershp = 0 for example
    """

    output_df = input_df.copy()

    # Conditions for occupied housing units
    occupied_hu = (output_df['numprec'] > 0)
    not_gqtype  = (output_df['gqtype'].isnull())
    not_vacant  = (output_df['vacancy'].isnull())
    conditions = occupied_hu & not_gqtype & not_vacant
    # Fill in missing values
    output_df.loc[conditions, 'gqtype'] = 0
    output_df.loc[conditions, 'vacancy'] = 0

    # Conditions for vacant housing units
    vacant_hu = (output_df['vacancy'] > 0)
    not_occupied  = (output_df['numprec'].isnull())
    conditions = vacant_hu & not_occupied
    output_df.loc[conditions, 'numprec'] = 0
    output_df.loc[conditions, 'gqtype'] = 0

    # Conditions for group quarters housing units
    gq_hu = (output_df['gqtype'] > 0)
    occupied  = (output_df['numprec'].notnull())
    conditions = gq_hu & occupied
    output_df.loc[conditions, 'vacancy'] = 0


    return output_df



@staticmethod
def drop_extra_columns(input_df):
    """
    Need to drop columns used to predict variables
    """

    output_df = input_df.copy()

    drop_if_starts_with = ['prob_','hucount_','preccount_','sumby_','min','max','totalprob_']
    for substring in drop_if_starts_with:
        drop_vars = [col for col in output_df if col.startswith(substring)]
        #print(drop_vars)
        # Drop columns
        output_df = output_df.drop(drop_vars, axis=1)

    drop_if_ends_with = ['_counter','_flagset','_flag','_flagsetrm']
    for substring in drop_if_ends_with:
        drop_vars = [col for col in output_df if col.endswith(substring)]
        #print(drop_vars)
        # Drop columns
        output_df = output_df.drop(drop_vars, axis=1)

    drop_if_contains = ['byP','byH']
    for substring in drop_if_contains:
        drop_vars = [col for col in output_df if substring in col]
        #print(drop_vars)
        # Drop columns
        output_df = output_df.drop(drop_vars, axis=1)

    return output_df