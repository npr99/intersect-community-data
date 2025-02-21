"""
Data Structure for Housing Unit Inventory
ACS 5-year Census Tract and Block Group Level Data for the 2022 ACS

Base API URL parameters, found at https://api.census.gov/data.html
List of all variables available in 5-year ACS 
https://api.census.gov/data/2022/acs/acs5/variables.html
"""

import numpy as np

"""
By race groups - the Census Consistently labels variables for race with letters
"""
acsbyrace_groups_varstems =  { 
        'A_' : {'race' : 1, 'Label' : 'White alone'},
        'B_' : {'race' : 2, 'Label' : 'Black or African American alone'},
        'C_' : {'race' : 3, 'Label' : 'American Indian and Alaska Native alone'},
        'D_' : {'race' : 4, 'Label' : 'Asian alone'},
        'E_' : {'race' : 5, 'Label' : 'Native Hawaiian and Other Pacific Islander alone'},
        'F_' : {'race' : 6, 'Label' : 'Some Other Race alone'},
        'G_' : {'race' : 7, 'Label' : 'Two or More Races'}
        }

acsbyracehispan_groups_varstems =  { 
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
subtract_function = "BaseInventory.subtract_df"
acsbyracehispan_groups_varstems_mxpt1 =  { 
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
Groups of Data that provide counts of households by race, family, and income group

Variable roots that when added to household groups stems provides variable name 
for family by household income.
The roots need to be 3 character zero padded to make the correct variable name.

For example  B19101A_002E is the variable for the count of families that have
income less than $10,000 and a householder who is White Alone
The dictionary includes the concept, and characteristic categorical codes for numprec and ownershp variables
"""

hhinc_B19001_varstem_roots_2022 = {'metadata' : {
                        'concept' : 'HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2022 INFLATION-ADJUSTED DOLLARS)',
                        'byracehispan' : acsbyracehispan_groups_varstems,
                        'graft_chars' : ['race'],
                        'new_char': 'incomegroup',
                        'char_vars' : ['byracehispan','incomegroup'],
                        'group' : 'B19001',
                        'vintage' : '2022', 
                        'dataset_name' : 'acs/acs5',
                        'for_geography' : 'tract:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : False,
                        'mutually_exclusive_dict' : acsbyracehispan_groups_varstems_mxpt1,
                        'indexvar' : ['GEO_ID','state','county','tract'],
                        'countvar' : 'hucount'
                        },
                        'B19001' : {
                        '002E' : {'incomegroup' : 1, 'label' : 'Less than $10,000'},
                        '003E' : {'incomegroup' : 2, 'label' : '$10,000 to $14,999'},
                        '004E' : {'incomegroup' : 3, 'label' : '$15,000 to $19,999'},
                        '005E' : {'incomegroup' : 4, 'label' : '$20,000 to $24,999'},
                        '006E' : {'incomegroup' : 5, 'label' : '$25,000 to $29,999'},
                        '007E' : {'incomegroup' : 6, 'label' : '$30,000 to $34,999'},
                        '008E' : {'incomegroup' : 7, 'label' : '$35,000 to $39,999'},
                        '009E' : {'incomegroup' : 8, 'label' : '$40,000 to $44,999'},
                        '010E' : {'incomegroup' : 9, 'label' : '$45,000 to $49,999'},
                        '011E' : {'incomegroup' : 10, 'label' : '$50,000 to $59,999'},
                        '012E' : {'incomegroup' : 11, 'label' : '$60,000 to $74,999'},
                        '013E' : {'incomegroup' : 12, 'label' : '$75,000 to $99,999'},
                        '014E' : {'incomegroup' : 13, 'label' : '$100,000 to $124,999'},
                        '015E' : {'incomegroup' : 14, 'label' : '$125,000 to $149,999'},
                        '016E' : {'incomegroup' : 15, 'label' : '$150,000 to $199,999'},
                        '017E' : {'incomegroup' : 16, 'label' : '$200,000 or more'},
                    }
                }

hhincfamily_B19101_varstem_roots_2022 = {'metadata' : {
                        'concept' : 'FAMILY INCOME IN THE PAST 12 MONTHS (IN 2022 INFLATION-ADJUSTED DOLLARS)',
                        'byracehispan' : acsbyracehispan_groups_varstems,
                        'graft_chars' : ['race'],
                        'graft_chars' : ['race','family'],
                        'char_vars' : ['byracehispan','family','incomegroup'],
                        'group' : 'B19101',
                        'vintage' : '2022', 
                        'dataset_name' : 'acs/acs5',
                        'for_geography' : 'tract:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : False,
                        'mutually_exclusive_dict' : acsbyracehispan_groups_varstems_mxpt1,
                        'indexvar' : ['GEO_ID','state','county','tract'],
                        'countvar' : 'hucount',
                        'notes' : 'Assume family is for numprec > 1'
                        },
                        'B19101' : {
                        '002E' : {'family' : 1, 'incomegroup' : 1, 'label' : 'Less than $10,000'},
                        '003E' : {'family' : 1, 'incomegroup' : 2, 'label' : '$10,000 to $14,999'},
                        '004E' : {'family' : 1, 'incomegroup' : 3, 'label' : '$15,000 to $19,999'},
                        '005E' : {'family' : 1, 'incomegroup' : 4, 'label' : '$20,000 to $24,999'},
                        '006E' : {'family' : 1, 'incomegroup' : 5, 'label' : '$25,000 to $29,999'},
                        '007E' : {'family' : 1, 'incomegroup' : 6, 'label' : '$30,000 to $34,999'},
                        '008E' : {'family' : 1, 'incomegroup' : 7, 'label' : '$35,000 to $39,999'},
                        '009E' : {'family' : 1, 'incomegroup' : 8, 'label' : '$40,000 to $44,999'},
                        '010E' : {'family' : 1, 'incomegroup' : 9, 'label' : '$45,000 to $49,999'},
                        '011E' : {'family' : 1, 'incomegroup' : 10, 'label' : '$50,000 to $59,999'},
                        '012E' : {'family' : 1, 'incomegroup' : 11, 'label' : '$60,000 to $74,999'},
                        '013E' : {'family' : 1, 'incomegroup' : 12, 'label' : '$75,000 to $99,999'},
                        '014E' : {'family' : 1, 'incomegroup' : 13, 'label' : '$100,000 to $124,999'},
                        '015E' : {'family' : 1, 'incomegroup' : 14, 'label' : '$125,000 to $149,999'},
                        '016E' : {'family' : 1, 'incomegroup' : 15, 'label' : '$150,000 to $199,999'},
                        '017E' : {'family' : 1, 'incomegroup' : 16, 'label' : '$200,000 or more'},
                    }
                }

"""
Household income by tenure could be used as a check on setting household income by race and ethnicity
if household income by tenure does not match the values could be "shuffled"
to better match table B25118
"""
hhinc_B25118_varstem_roots_2022 = {'metadata' : {
                        'concept' : 'TENURE BY HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2022 INFLATION-ADJUSTED DOLLARS)',
                        'graft_chars' : ['ownershp'],
                        'new_char': ['minincome','maxincome'],
                        'char_vars' : ['ownershp','minincome','maxincome'],
                        'group' : 'B25118',
                        'vintage' : '2022', 
                        'dataset_name' : 'acs/acs5',
                        'for_geography' : 'tract:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : True,
                        'indexvar' : ['GEO_ID','state','county','tract'],
                        'countvar' : 'hucount',
                        'notes' : {'Using minimum income and maximum income because 11 income groups are not consistent.'}
                        },
                        'B25118' : {
                        '_003E' : {'ownershp' : 1, 'minincome' :     0, 'maxincome' :  4999, 'label' : 'Less than $5,000'},
                        '_004E' : {'ownershp' : 1, 'minincome' :  5000, 'maxincome' :  9999, 'label' : '$5,000 to $9,999'},
                        '_005E' : {'ownershp' : 1, 'minincome' : 10000, 'maxincome' : 14999, 'label' : '$10,000 to $14,999'},
                        '_006E' : {'ownershp' : 1, 'minincome' : 15000, 'maxincome' : 19999, 'label' : '$15,000 to $19,999'},
                        '_007E' : {'ownershp' : 1, 'minincome' : 20000, 'maxincome' : 24999, 'label' : '$20,000 to $24,999'},
                        '_008E' : {'ownershp' : 1, 'minincome' : 25000, 'maxincome' : 34999, 'label' : '$25,000 to $34,999'},
                        '_009E' : {'ownershp' : 1, 'minincome' : 35000, 'maxincome' : 49999, 'label' : '35,000 to $49,999'},
                        '_010E' : {'ownershp' : 1, 'minincome' : 50000, 'maxincome' : 74999, 'label' : '$50,000 to $74,999'},
                        '_011E' : {'ownershp' : 1, 'minincome' : 75000, 'maxincome' : 99999, 'label' : '$75,000 to $99,999'},
                        '_012E' : {'ownershp' : 1, 'minincome' :100000, 'maxincome' :149999, 'label' : '$100,000 to $149,999'},
                        '_013E' : {'ownershp' : 1, 'minincome' :150000, 'maxincome' :np.nan, 'label' : '$150,000 or more'},
                        '_015E' : {'ownershp' : 2, 'minincome' :     0, 'maxincome' :  4999, 'label' : 'Less than $5,000'},
                        '_016E' : {'ownershp' : 2, 'minincome' :  5000, 'maxincome' :  9999, 'label' : '$5,000 to $9,999'},
                        '_017E' : {'ownershp' : 2, 'minincome' : 10000, 'maxincome' : 14999, 'label' : '$10,000 to $14,999'},
                        '_018E' : {'ownershp' : 2, 'minincome' : 15000, 'maxincome' : 19999, 'label' : '$15,000 to $19,999'},
                        '_019E' : {'ownershp' : 2, 'minincome' : 20000, 'maxincome' : 24999, 'label' : '$20,000 to $24,999'},
                        '_020E' : {'ownershp' : 2, 'minincome' : 25000, 'maxincome' : 34999, 'label' : '$25,000 to $34,999'},
                        '_021E' : {'ownershp' : 2, 'minincome' : 35000, 'maxincome' : 49999, 'label' : '35,000 to $49,999'},
                        '_022E' : {'ownershp' : 2, 'minincome' : 50000, 'maxincome' : 74999, 'label' : '$50,000 to $74,999'},
                        '_023E' : {'ownershp' : 2, 'minincome' : 75000, 'maxincome' : 99999, 'label' : '$75,000 to $99,999'},
                        '_024E' : {'ownershp' : 2, 'minincome' :100000, 'maxincome' :149999, 'label' : '$100,000 to $149,999'},
                        '_025E' : {'ownershp' : 2, 'minincome' :150000, 'maxincome' :np.nan, 'label' : '$150,000 or more'},
                    }
                }

hhinc_B19013_varstem_roots_2022 = {'metadata' : {
                        'concept' : 'MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2022 INFLATION-ADJUSTED DOLLARS)',
                        'byrace' : acsbyrace_groups_varstems,
                        'graft_chars' : ['race','hispan'],
                        'char_vars' : ['byrace','hispan'],
                        'group' : 'B19013',
                        'vintage' : '2022', 
                        'dataset_name' : 'acs/acs5',
                        'for_geography' : 'tract:*',
                        'unit_of_analysis' : 'geoid',
                        'countvar' : 'medianincome',
                        'notes' : {'Use median income by race to check results.'}
                        },
                        'B19013' : {
                        'A_001E' : {'race' : 1, 'hispan' : -999},
                        'B_001E' : {'race' : 2, 'hispan' : -999},
                        'C_001E' : {'race' : 3, 'hispan' : -999},
                        'D_001E' : {'race' : 4, 'hispan' : -999},
                        'E_001E' : {'race' : 5, 'hispan' : -999},
                        'F_001E' : {'race' : 6, 'hispan' : -999},
                        'G_001E' : {'race' : 7, 'hispan' : -999},
                        'H_001E' : {'race' : 1, 'hispan' : 0},
                        'I_001E' : {'race' : -999, 'hispan' : 1},
                    }
                }

"""
https://github.com/npr99/PlanningMethods/blob/master/Explore_ACS_Variable_Metadata_2021_06_02.ipynb
Other ACS groups to consider
B19019	MEDIAN HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2022 INFLATION-ADJUSTED DOLLARS) BY HOUSEHOLD SIZE
B19119	MEDIAN FAMILY INCOME IN THE PAST 12 MONTHS (IN 2022 INFLATION-ADJUSTED DOLLARS) BY FAMILY SIZE	
B19037	AGE OF HOUSEHOLDER BY HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2022 INFLATION-ADJUSTED DOLLARS)
B19037 - by race and Hispanic
B19131	FAMILY TYPE BY PRESENCE OF OWN CHILDREN UNDER 18 YEARS BY FAMILY INCOME IN THE PAST 12 MONTHS (IN 2022 INFLATION-ADJUSTED DOLLARS)
B25121	HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2022 INFLATION-ADJUSTED DOLLARS) BY VALUE
B25122	HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2022 INFLATION-ADJUSTED DOLLARS) BY GROSS RENT
"""