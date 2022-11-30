import pandas as pd
import numpy as np

def add_randage(df, seed, varname):
    random_generator = np.random.RandomState(seed)

    output_df = df.copy()

    # Add random age value
    # Check if minage years ==  maxage years
    minage_equals_maxage = (output_df['minageyrs'] == output_df['maxageyrs'])
    minage_notmissing = (output_df['minageyrs'].notnull())
    conditions = minage_equals_maxage & minage_notmissing
    output_df.loc[conditions, varname] = output_df['minageyrs']

    # If min age is less than max age then use random number generator
    minage_less_maxage = (output_df['minageyrs'] < output_df['maxageyrs'])
    conditions = minage_less_maxage & minage_notmissing
    output_df.loc[conditions, varname] = output_df[conditions].apply(lambda x: \
        random_generator.randint(x['minageyrs'],x['maxageyrs']), axis=1)

    return output_df


def add_P12age_groups(input_df,varname):
    """
    Add age groups for PC12
    """

    output_df = input_df.copy()

    agegroupP12_dict = {1: {'minageyrs': 0, 'maxageyrs': 4},
    2: {'minageyrs': 5, 'maxageyrs': 9},
    3: {'minageyrs': 10, 'maxageyrs': 14},
    4: {'minageyrs': 15, 'maxageyrs': 17},
    5: {'minageyrs': 18, 'maxageyrs': 19},
    6: {'minageyrs': 20, 'maxageyrs': 20},
    7: {'minageyrs': 21, 'maxageyrs': 21},
    8: {'minageyrs': 22, 'maxageyrs': 24},
    9: {'minageyrs': 25, 'maxageyrs': 29},
    10: {'minageyrs': 30, 'maxageyrs': 34},
    11: {'minageyrs': 35, 'maxageyrs': 39},
    12: {'minageyrs': 40, 'maxageyrs': 44},
    13: {'minageyrs': 45, 'maxageyrs': 49},
    14: {'minageyrs': 50, 'maxageyrs': 54},
    15: {'minageyrs': 55, 'maxageyrs': 59},
    16: {'minageyrs': 60, 'maxageyrs': 61},
    17: {'minageyrs': 62, 'maxageyrs': 64},
    18: {'minageyrs': 65, 'maxageyrs': 66},
    19: {'minageyrs': 67, 'maxageyrs': 69},
    20: {'minageyrs': 70, 'maxageyrs': 74},
    21: {'minageyrs': 75, 'maxageyrs': 79},
    22: {'minageyrs': 80, 'maxageyrs': 84},
    22: {'minageyrs': 85, 'maxageyrs': 110}}

    for agegroup in agegroupP12_dict:
        randincome_greater_than = \
            (output_df[varname] >= agegroupP12_dict[agegroup]['minageyrs'])
        randincome_less_than    = \
            (output_df[varname] <= agegroupP12_dict[agegroup]['maxageyrs'])
        conditions = randincome_greater_than & randincome_less_than
        output_df.loc[conditions,'agegroupP12'] = agegroup

    # Add 0 agegroup - for no age data
    randage_missing =  (output_df[varname].isnull())
    conditions = randage_missing
    output_df.loc[conditions,'agegroupP12'] = 0

    return output_df

def add_H17age_groups(input_df,varname):
    """
    Add age groups for H17
    """

    output_df = input_df.copy()

    agegroupH17_dict = {1:{'minageyrs': 15, 'maxageyrs': 24},
    2: {'minageyrs': 25, 'maxageyrs': 34},
    3: {'minageyrs': 35, 'maxageyrs': 44},
    4: {'minageyrs': 45, 'maxageyrs': 54},
    5: {'minageyrs': 55, 'maxageyrs': 59},
    6: {'minageyrs': 60, 'maxageyrs': 64},
    7: {'minageyrs': 65, 'maxageyrs': 74},
    8: {'minageyrs': 75, 'maxageyrs': 84},
    9: {'minageyrs': 85, 'maxageyrs': 110}}

    for agegroup in agegroupH17_dict:
        randvar_greater_than = \
            (output_df[varname] >= agegroupH17_dict[agegroup]['minageyrs'])
        randvar_less_than    = \
            (output_df[varname] <= agegroupH17_dict[agegroup]['maxageyrs'])
        conditions = randvar_greater_than & randvar_less_than
        output_df.loc[conditions,'agegroupH17'] = agegroup

    # Add 0 agegroup - for no age data
    randage_missing =  (output_df[varname].isnull())
    conditions = randage_missing
    output_df.loc[conditions,'agegroupH17'] = 0

    return output_df


def add_H18age_groups(input_df,varname):
    """
    Add age groups for H18
    """

    output_df = input_df.copy()

    agegroupH18_dict = {1: {'minageH18': 15, 'maxageH18': 34},
        2: {'minageH18': 35, 'maxageH18': 64},
        3: {'minageH18': 65, 'maxageH18': 110}}

    for agegroup in agegroupH18_dict:
        randvar_greater_than = \
            (output_df[varname] >= agegroupH18_dict[agegroup]['minageH18'])
        randvar_less_than    = \
            (output_df[varname] <= agegroupH18_dict[agegroup]['maxageH18'])
        conditions = randvar_greater_than & randvar_less_than
        output_df.loc[conditions,'agegroupH18'] = agegroup

    # Add 0 agegroup - for no age data
    randage_missing =  (output_df[varname].isnull())
    conditions = randage_missing
    output_df.loc[conditions,'agegroupH18'] = 0

    return output_df

def add_B19037age_groups(input_df,varname):
    """
    Add age groups for B19037
    """

    output_df = input_df.copy()

    agegroupB19037_dict = {1: {'minageyrs': 15, 'maxageyrs': 25},
        2: {'minageyrs': 25, 'maxageyrs': 44},
        3: {'minageyrs': 45, 'maxageyrs': 64},
        4: {'minageyrs': 65, 'maxageyrs': 110}}

    for agegroup in agegroupB19037_dict:
        randvar_greater_than = \
            (output_df[varname] >= agegroupB19037_dict[agegroup]['minageyrs'])
        randvar_less_than    = \
            (output_df[varname] <= agegroupB19037_dict[agegroup]['maxageyrs'])
        conditions = randvar_greater_than & randvar_less_than
        output_df.loc[conditions,'agegroupB19037'] = agegroup

    # Add 0 agegroup - for no age data
    randage_missing =  (output_df[varname].isnull())
    conditions = randage_missing
    output_df.loc[conditions,'agegroupB19037'] = 0

    return output_df

def add_P43age_groups(input_df,varname):
    """
    Add age groups for P43
    """

    output_df = input_df.copy()

    agegroupP43_dict = {1: {'minageyrs': 0, 'maxageyrs': 17},
        2: {'minageyrs': 18, 'maxageyrs': 64},
        3: {'minageyrs': 65, 'maxageyrs': 110}}

    for agegroup in agegroupP43_dict:
        randvar_greater_than = \
            (output_df[varname] >= agegroupP43_dict[agegroup]['minageyrs'])
        randvar_less_than    = \
            (output_df[varname] <= agegroupP43_dict[agegroup]['maxageyrs'])
        conditions = randvar_greater_than & randvar_less_than
        output_df.loc[conditions,'agegroupP43'] = agegroup

    # Add 0 agegroup - for no age data
    randage_missing =  (output_df[varname].isnull())
    conditions = randage_missing
    output_df.loc[conditions,'agegroupP43'] = 0

    return output_df

