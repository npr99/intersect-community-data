"""
General Data Structure for Inventory
Block Level Data for the 2010 Census
Tract level Data for ACS

Each Dictionary represents data tables in the 2010 Census

Base API URL parameters, found at https://api.census.gov/data.html
List of all variables available in SF1 
https://api.census.gov/data/2010/dec/sf1/variables.html
"""

import numpy as np

subtract_function = "BaseInventory.subtract_df"

"""
Observations with code -999 indcate that the value is not set, but can be set.
Setting -999 helps with merging. If set to np.nan the merge does not work.
This is an issue that was found with trying to predict family households.
"""
"""
By race and Hispanic groups - the Census Consistently labels variables for race with letters
Not Mutually Exclusive
"""
dec10byracehispan_All=  { 
        '' : {'race' : np.nan, 'hispan' : np.nan, 'Label' : 'Total'},
        'A' : {'race' : 1, 'hispan' : -999, 'Label' : 'White alone'},
        'B' : {'race' : 2, 'hispan' : -999, 'Label' : 'Black or African American alone'},
        'C' : {'race' : 3, 'hispan' : -999, 'Label' : 'American Indian and Alaska Native alone'},
        'D' : {'race' : 4, 'hispan' : -999, 'Label' : 'Asian alone'},
        'E' : {'race' : 5, 'hispan' : -999, 'Label' : 'Native Hawaiian and Other Pacific Islander alone'},
        'F' : {'race' : 6, 'hispan' : -999, 'Label' : 'Some Other Race alone'},
        'G' : {'race' : 7, 'hispan' : -999, 'Label' : 'Two or More Races'},
        'I' : {'race' : 1, 'hispan' :0, 'Label' : 'White alone, not Hispanic'},
        'H' : {'race' : -999, 'hispan' : 1, 'Label' : 'Hispanic or Latino'}
        }

"""
Mutually Exclusive Race Hispanic Categories
"""

dec10byracehispan_IAG_mx =  { 
        'I' : {'race' : 1, 'hispan' : 0, 'Label' : 'White alone, Not Hispanic', 
            'equation' : "df['I'].copy(deep=True)"},
        'A-I' : {'race' : 1, 'hispan' : 1, 'Label' : 'White alone, Hispanic',
            'equation' : subtract_function+"(df['A'],df['I'],index_col=indexvar)"},
        'B' : {'race' : 2, 'hispan' : -999, 'Label' : 'Black or African American alone',
            'equation' : "df['B'].copy(deep=True)"},
        'C' : {'race' : 3, 'hispan' : -999, 'Label' : 'American Indian and Alaska Native alone',
            'equation' : "df['C'].copy(deep=True)"},
        'D' : {'race' : 4, 'hispan' : -999, 'Label' : 'Asian alone',
            'equation' : "df['D'].copy(deep=True)"},
        'E' : {'race' : 5, 'hispan' : -999, 'Label' : 'Native Hawaiian and Other Pacific Islander alone',
            'equation' : "df['E'].copy(deep=True)"},
        'F' : {'race' : 6, 'hispan' : -999, 'Label' : 'Some Other Race alone',
            'equation' : "df['F'].copy(deep=True)"},
        'G' : {'race' : 7, 'hispan' : -999, 'Label' : 'Two or More Races',
            'equation' : "df['G'].copy(deep=True)"}
        }


"""
By race and Hispanic groups - the Census Consistently labels variables for race with letters
Not Mutually Exclusive
"""
dec10hispannotwhite_HAI =  {
        'A' : {'race' : 1, 'hispan' : -999, 'Label' : 'White alone'},
        'I' : {'race' : 1, 'hispan' :0, 'Label' : 'White alone, not Hispanic'},
        'H' : {'race' : -999, 'hispan' : 1, 'Label' : 'Hispanic or Latino'}
        }

"""
Mutually Exclusive Race Hispanic Categories
"""

dec10hispannotwhite_HAI_mx =  {
        'H-A-I' : {'race' : -999, 'hispan' : 1, 'Label' : 'Hispanic or Latino, any race Not white',
            'equation' : subtract_function+"(df['H'],"+subtract_function+"(df['A'],df['I'],"+\
                "index_col=indexvar),index_col=indexvar)"}
        }


"""
**************************************************************
ACS Dictionaries
**************************************************************
"""
"""
Data Structure for Housing Unit Inventory
ACS 5-year Census Tract and Block Group Level Data for the 2012 ACS

Base API URL parameters, found at https://api.census.gov/data.html
List of all variables available in 5-year ACS 
https://api.census.gov/data/2012/acs/acs5/variables.html
"""

"""
By race groups - the Census Consistently labels variables for race with letters
"""
acsbyrace_All =  { 
        'A_' : {'race' : 1, 'Label' : 'White alone'},
        'B_' : {'race' : 2, 'Label' : 'Black or African American alone'},
        'C_' : {'race' : 3, 'Label' : 'American Indian and Alaska Native alone'},
        'D_' : {'race' : 4, 'Label' : 'Asian alone'},
        'E_' : {'race' : 5, 'Label' : 'Native Hawaiian and Other Pacific Islander alone'},
        'F_' : {'race' : 6, 'Label' : 'Some Other Race alone'},
        'G_' : {'race' : 7, 'Label' : 'Two or More Races'}
        }

acsbyracehispan_All =  { 
        '_' : {'race' : np.nan, 'hispan' : np.nan, 'Label' : 'Total'},
        'A_' : {'race' : 1, 'hispan' : -999, 'Label' : 'White alone'},
        'B_' : {'race' : 2, 'hispan' : -999, 'Label' : 'Black or African American alone'},
        'C_' : {'race' : 3, 'hispan' : -999, 'Label' : 'American Indian and Alaska Native alone'},
        'D_' : {'race' : 4, 'hispan' : -999, 'Label' : 'Asian alone'},
        'E_' : {'race' : 5, 'hispan' : -999, 'Label' : 'Native Hawaiian and Other Pacific Islander alone'},
        'F_' : {'race' : 6, 'hispan' : -999, 'Label' : 'Some Other Race alone'},
        'G_' : {'race' : 7, 'hispan' : -999, 'Label' : 'Two or More Races'},
        'H_' : {'race' : 1, 'hispan' :0, 'Label' : 'White alone, not Hispanic'},
        'I_' : {'race' : -999, 'hispan' : 1, 'Label' : 'Hispanic or Latino'}
        }

"""
Almost Mutually Exclusive Race Hispanic Categories
This arrangement of income will double count Hispanic Households in race 2-7.
However, the merge with block level data will be based on Hispanic first then race.
"""
acsbyracehispan_HAG =  { 
        'H_' : {'race' : 1, 'hispan' : 0, 'Label' : 'White alone, Not Hispanic', 
            'equation' : "df['H_'].copy(deep=True)"},
        'A-H' : {'race' : 1, 'hispan' : 1, 'Label' : 'White alone, Hispanic',
            'equation' : subtract_function+"(df['A_'],df['H_'],index_col=indexvar)"},
        'A-H-I' : {'race' : -999, 'hispan' : 1, 'Label' : 'any race not White alone, Hispanic',
            'equation' : subtract_function+"(df['I_'],mx_df['A-H'],index_col=indexvar)"},
        'B_' : {'race' : 2, 'hispan' : -999, 'Label' : 'Black or African American alone',
            'equation' : "df['B_'].copy(deep=True)"},
        'C_' : {'race' : 3, 'hispan' : -999, 'Label' : 'American Indian and Alaska Native alone',
            'equation' : "df['C_'].copy(deep=True)"},
        'D_' : {'race' : 4, 'hispan' : -999, 'Label' : 'Asian alone',
            'equation' : "df['D_'].copy(deep=True)"},
        'E_' : {'race' : 5, 'hispan' : -999, 'Label' : 'Native Hawaiian and Other Pacific Islander alone',
            'equation' : "df['E_'].copy(deep=True)"},
        'F_' : {'race' : 6, 'hispan' : -999, 'Label' : 'Some Other Race alone',
            'equation' : "df['F_'].copy(deep=True)"},
        'G_' : {'race' : 7, 'hispan' : -999, 'Label' : 'Two or More Races',
            'equation' : "df['G_'].copy(deep=True)"}
        }


"""
Primary variable Categories
"""
group_quarters_valueLabels = {'gqtype' : 
                                {1: {'label' : 'Correctional facilities for adults'},
                                 2: {'label' : 'Juvenile facilities'},
                                 3: {'label' : 'Nursing facilities/Skilled-nursing facilities'},
                                 4: {'label' : 'Other institutional facilities'},
                                 5: {'label' : 'College/University student housing'},
                                 6: {'label' : 'Military quarters '},
                                 7: {'label' : 'Other noninstitutional facilities'}
                                 },
'metadata' : 
{'label' : 'Group Quarters Type',
'notes' : ['Group quarters are places where people live or stay \
in a group living arrangement, which are owned or \
managed by an entity or organization providing \
housing and/or services for the residents.']}
                          }