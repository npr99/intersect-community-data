import numpy as np

# General functions for making categorical variables

def add_label_cat_conditions_df(df, conditions):
    """Label Categorical Variable Values and add to dataframe.
    Use this function with values are based on conditions.
    
    Args:
        df (obj): Pandas DataFrame object.
        conditions (dict): Dictionary of conditions for value labels.
    Returns:
        object: Pandas DataFrame object.
    """

    cat_var = conditions['cat_var']['variable_label']

    # Create 2 versions of categorical variable
    # one for the integer version
    # one for the string version with labels
    # Check if variable is already in dataframe
    if cat_var+'_int' not in df.columns:
        df[cat_var+'_int'] = np.nan
    if cat_var+'_str' not in df.columns:
        df[cat_var+'_str'] = "No Data"

    for item in conditions['condition_list'].keys():
        condition =  conditions['condition_list'][item]['condition']
        value_label = conditions['condition_list'][item]['value_label']
        #print("checking condition",value_label)
        df.loc[eval(condition), cat_var+'_str'] = value_label
        df.loc[eval(condition), cat_var+'_int'] = item
        # How many observations had this condition
        len_df = df.loc[df[cat_var+'_int'] == item].shape[0]
        print(f"{value_label} had {len_df} observations")

    # Set variable to missing if no data- makes tables look nicer
    df.loc[(df[cat_var+'_str'] == "No Data"), 
        cat_var+'_str'] = np.nan

    return df

def add_label_cat_values_df(df, valuelabels, variable = ''):
    """Label Categorical Variable Values and add to dataframe.
    Use this function with categorical values 
    are integer values.
    Args:
        df (obj): Pandas DataFrame object.
        valuelabels (dict): Dictionary of value labels.
        variable (str): Variable to label.
    Returns:
        object: Pandas DataFrame object with new column that has value labels.
    """

    if variable == '':
        variable  = valuelabels['categorical_variable']['variable']
    variable_label = valuelabels['categorical_variable']['variable_label']

    #print(df[variable].describe())

    df[variable_label] = "No Data"

    for item in valuelabels['value_list'].keys():
        #print(variable, item)
        value =  valuelabels['value_list'][item]['value']
        value_label = valuelabels['value_list'][item]['value_label']
        #print(value, value_label)
        df.loc[df[variable] == value, variable_label] = value_label
        # print count of new value
        len_df = df.loc[df[variable] == value].shape[0]
        #print(f"{value_label} had {len_df} observations")

    # Set variable to missing if no data- makes tables look nicer
    df.loc[(df[variable_label] == "No Data"), 
        variable_label] = np.nan
    
    #print(df[variable_label].describe())

    return df

""" example code
hhinc_valuelabels = {'categorical_variable': {'variable' : 'hhinc',
                                'variable_label' : 'Household Income Group',
                                'notes' : '5 Household Income Groups based on random income.'},
                    'value_list' : {
                        1 : {'value': 1, 'value_label': "1 Less than $15,000"},
                        2 : {'value': 2, 'value_label': "2 $15,000 to $24,999"},
                        3 : {'value': 3, 'value_label': "3 $25,000 to $74,999"},
                        4 : {'value': 4, 'value_label': "4 $75,000 to $99,999"},
                        5 : {'value': 5, 'value_label': "5 $100,000 or more"}}
                    }

pd_df = add_label_cat_values_df(pd_df, valuelabels = hhinc_valuelabels)

ds3_conditions = {'cat_var' : {'variable_label' : 'Probability Complete Failure',
                         'notes' : 'Probability of complete failure based on damage state 3'},
              'condition_list' : {
                1 : {'condition': "(df['DS_3'] == 0)", 'value_label': "0 0%"},
                2 : {'condition': "(df['DS_3'] > 0)", 'value_label': "1 Less than 20%"},
                3 : {'condition': "(df['DS_3'] > .2)", 'value_label': "2 20-40%"},
                4 : {'condition': "(df['DS_3'] > .4)", 'value_label': "3 40-60%"},
                5 : {'condition': "(df['DS_3'] > .6)", 'value_label': "4 60-80%"},
                6 : {'condition': "(df['DS_3'] > .8)", 'value_label': "5 80-100%"},
                7 : {'condition': "(df['DS_3'] == 1)", 'value_label': "6 100%"}}
            }

      
dsf_valuelabels = {'categorical_variable' : {'variable' : 'd_sf',
                   'variable_label' : 'Single Family Dwelling',
                   'notes' : 'Categories for single family dwellings'},
              'value_list' : {
                1 : {'value': 0, 'value_label': "0 Not Single Family"},
                2 : {'value': 1, 'value_label': "1 Single Family"}}
            }
    

pd_df = add_label_cat_conditions_df(pd_df, conditions = ds3_conditions)
pd_df = add_label_cat_values_df(pd_df, valuelabels = dsf_valuelabels)
""" 