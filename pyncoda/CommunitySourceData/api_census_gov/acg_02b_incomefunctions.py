import numpy as np

# Code for adding random income
# Dictionary with round options
@staticmethod
def make_incomegroup_dict():
    ## Add min and max income values
    incomegroup_dict = {1: {'minincome': 0, 'maxincome': 9999},
    2: {'minincome': 10000, 'maxincome': 14999},
    3: {'minincome': 15000, 'maxincome': 19999},
    4: {'minincome': 20000, 'maxincome': 24999},
    5: {'minincome': 25000, 'maxincome': 29999},
    6: {'minincome': 30000, 'maxincome': 34999},
    7: {'minincome': 35000, 'maxincome': 39999},
    8: {'minincome': 40000, 'maxincome': 44999},
    9: {'minincome': 45000, 'maxincome': 49999},
    10: {'minincome': 50000, 'maxincome': 59999},
    11: {'minincome': 60000, 'maxincome': 74999},
    12: {'minincome': 75000, 'maxincome': 99999},
    13: {'minincome': 100000, 'maxincome': 124999},
    14: {'minincome': 125000, 'maxincome': 149999},
    15: {'minincome': 150000, 'maxincome': 199999},
    16: {'minincome': 200000, 'maxincome': 250000},
    -999 : {'minincome': 0, 'maxincome': 250000, 'note' : 
    'Category -999 = income group not set. Assume income could be anything from 0 to 250000.'},
    0 : {'minincome': 0, 'maxincome': 1, 'note' : 
    'Category 0 = observation has no income, but the random income process requires'+\
        'a value to work for all observations. Remove randincome after set.'+\
            'This category applies to vacant housing units and group quarters.'} }

    return incomegroup_dict

@staticmethod
def add_minmaxincome(input_df,incomegroup_dict):
    #condition = (df['incomegroup'].notnull())
    output_df = input_df.copy()
    output_df['minincome'] = output_df.\
        apply(lambda x: incomegroup_dict[x['incomegroup']]['minincome'], axis=1)
    output_df['maxincome'] = output_df.\
        apply(lambda x: incomegroup_dict[x['incomegroup']]['maxincome'], axis=1)

    return output_df

@staticmethod
def remove_cat0_randincome(input_df):
    """
    For category 0 need to replace values with missing.
    Without this step the descriptive statistics (median, mean)
    will be skewed. 
    Category 0 represents observations where the 
    characteristic is not applicable.
    For example - vacant housing units do not have a household income.
    """

    output_df = input_df.copy()
    condition = (output_df['incomegroup'] == 0)
    output_df.loc[condition, 'minincome'] = np.nan
    output_df.loc[condition, 'maxincome'] = np.nan
    output_df.loc[condition, 'randincomeB19101'] = np.nan

    return output_df

@staticmethod
def add_randincome(self, df, seed):
    random_generator = np.random.RandomState(seed)

    output_df = df.copy()
    #Make sure income group has 0 category
    output_df['incomegroup'] = \
    output_df['incomegroup'].fillna(value=0)
    output_df['incomegroup'].describe()

    # read in incomegroup dictionary
    incomegroup_dict = make_incomegroup_dict()
    # Add min max values
    output_df = add_minmaxincome(output_df,incomegroup_dict)

    # Add random income value
    output_df['randincomeB19101'] = output_df.apply(lambda x: \
        random_generator.randint(x['minincome'],x['maxincome']), axis=1)
    
    # remove income from income category 0
    output_df = remove_cat0_randincome(output_df)

    # add 5 household income groups
    output_df = add_hhinc_groups(output_df)

    return output_df

@staticmethod
def add_poverty(input_df):
    """
    Add poverty based on US Census Poverty Thresholds
    https://www.census.gov/topics/income-poverty/poverty/guidance/poverty-measures.html
    https://www2.census.gov/programs-surveys/cps/tables/time-series/historical-poverty-thresholds/thresh12.xls

    Use weighted average threshold by household size
    """

    output_df = input_df.copy()

    poverty_by_numprec_dict = {1: 11720,
                2:  14937,
                3: 18284,
                4: 23492,
                5: 27827,
                6: 31471,
                7: 35743}

    for numprec in poverty_by_numprec_dict:
        randincome_less_than    = (output_df['randincomeB19101'] < \
            poverty_by_numprec_dict[numprec])
        numprec_equals = (output_df['numprec'] == numprec)
        conditions = randincome_less_than & numprec_equals
        output_df.loc[conditions,'poverty'] = 1

        randincome_greater_than    = (output_df['randincomeB19101'] >= \
            poverty_by_numprec_dict[numprec])
        conditions = randincome_greater_than & numprec_equals
        output_df.loc[conditions,'poverty'] = 0

    # Add 0 hhinc - for no income group
    randincome_missing =  (output_df['randincomeB19101'].isnull())
    is_gqtype  = (output_df['gqtype'] > 0)
    is_vacant  = (output_df['vacancy'] > 0)
    conditions = randincome_missing | is_gqtype | is_vacant
    output_df.loc[conditions,'poverty'] = np.nan

    return output_df

@staticmethod    
def add_hhinc_groups(input_df):
    """
    Add 5 income groups
    """

    output_df = input_df.copy()

    hhinc_dict = {1: {'minincome': 0, 'maxincome': 14999},
    2: {'minincome': 15000, 'maxincome': 24999},
    3: {'minincome': 25000, 'maxincome': 74999},
    4: {'minincome': 75000, 'maxincome': 99999},
    5: {'minincome': 100000, 'maxincome': 10000000}}

    for hhinc in hhinc_dict:
        randincome_greater_than = (output_df['randincomeB19101'] >= hhinc_dict[hhinc]['minincome'])
        randincome_less_than    = (output_df['randincomeB19101'] <= hhinc_dict[hhinc]['maxincome'])
        conditions = randincome_greater_than & randincome_less_than
        output_df.loc[conditions,'hhinc'] = hhinc

    # Add 0 hhinc - for no income group
    randincome_missing =  (output_df['randincomeB19101'].isnull())
    is_gqtype  = (output_df['gqtype'] > 0)
    is_vacant  = (output_df['vacancy'] > 0)
    conditions = randincome_missing | is_gqtype | is_vacant
    output_df.loc[conditions,'hhinc'] = 0

    return output_df