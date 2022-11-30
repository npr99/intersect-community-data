"""
Data Structure for Baseline Housing Unit Inventory
Block Level Data for the 2010 Census

Each Dictionary represents data tables in the 2010 Census

Base API URL parameters, found at https://api.census.gov/data.html
List of all variables available in SF1 
https://api.census.gov/data/2010/dec/sf1/variables.html
"""

import numpy as np

"""
Observations with code -999 indicate that the value is not set, but can be set.
Setting -999 helps with merging. If set to np.nan the merge does not work.
This is an issue that was found with trying to predict family households.
"""
"""
By race and Hispanic groups - the Census Consistently labels variables for race with letters
Not Mutually Exclusive
"""
dec10byracehispan_groups_varstems =  { 
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
subtract_function = "BaseInventory.subtract_df"
dec10byracehispan_groups_varstems_mxpt1 =  { 
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

dec10byracehispan_groups_varstems_mxpt2 =  { 
        'I' : {'race' : 1, 'hispan' : 0, 'Label' : 'White alone, Not Hispanic', 
            'equation' : "df['I']"},
        'A-I' : {'race' : 1, 'hispan' : 1, 'Label' : 'White alone, Hispanic', 
            'equation' : subtract_function+"(df['A'],df['I'],index_col=indexvar)"},
        'H-BG' : {'race' : -999, 'hispan' : 0, 'Label' : 'Not Hispanic or Latino, any race Not white', 
            'equation' : subtract_function+"("+subtract_function+"(df[''],df['H'],"+\
                "index_col=indexvar),df['I'],index_col=indexvar)"},
        'H-A-I' : {'race' : -999, 'hispan' : 1, 'Label' : 'Hispanic or Latino, any race Not white',
            'equation' : subtract_function+"(df['H'],"+subtract_function+"(df['A'],df['I'],"+\
                "index_col=indexvar),index_col=indexvar)"}
        }

tenure_size_H16_varstem_roots = {'metadata' : {
                        'concept' : 'TENURE BY HOUSEHOLD SIZE',
                        'byracehispan' : dec10byracehispan_groups_varstems,
                        'graft_chars' : ['numprec','ownershp','race','hispan'],
                        'new_char': ['numprec','ownershp','race','hispan','family'],
                        'char_vars' : ['numprec','ownershp','family','byracehispan'],
                        'group' : 'H16',
                        'vintage' : '2010', 
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : False,
                        'mutually_exclusive_dict' : dec10byracehispan_groups_varstems_mxpt1,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'hucount',
                        'notes' : {'Number of person records range 1-7. Households with \
                                    more than 7 persons not identified. \
                                    Assume that 1 person households are not family households.'    }
                        },
                        'H016' : {
                        '003' : {'numprec' : 1,'ownershp' : 1, 'family' : 0, 'label' : 'Owner occupied 1-person'},
                        '004' : {'numprec' : 2,'ownershp' : 1, 'family' : -999, 'label' : 'Owner occupied 2-person'},
                        '005' : {'numprec' : 3,'ownershp' : 1, 'family' : -999, 'label' : 'Owner occupied 3-person'},
                        '006' : {'numprec' : 4,'ownershp' : 1, 'family' : -999, 'label' : 'Owner occupied 4-person'},
                        '007' : {'numprec' : 5,'ownershp' : 1, 'family' : -999, 'label' : 'Owner occupied 5-person'},
                        '008' : {'numprec' : 6,'ownershp' : 1, 'family' : -999, 'label' : 'Owner occupied 6-person'},
                        '009' : {'numprec' : 7,'ownershp' : 1, 'family' : -999, 'label' : 'Owner occupied 7-or-more-person'},
                        '011' : {'numprec' : 1,'ownershp' : 2, 'family' : 0, 'label' : 'Renter occupied 1-person'},
                        '012' : {'numprec' : 2,'ownershp' : 2, 'family' : -999, 'label' : 'Renter occupied 2-person'},
                        '013' : {'numprec' : 3,'ownershp' : 2, 'family' : -999, 'label' : 'Renter occupied 3-person'},
                        '014' : {'numprec' : 4,'ownershp' : 2, 'family' : -999, 'label' : 'Renter occupied 4-person'},
                        '015' : {'numprec' : 5,'ownershp' : 2, 'family' : -999, 'label' : 'Renter occupied 5-person'},
                        '016' : {'numprec' : 6,'ownershp' : 2, 'family' : -999, 'label' : 'Renter occupied 6-person'},
                        '017' : {'numprec' : 7,'ownershp' : 2, 'family' : -999, 'label' : 'Renter occupied 7-or-more-person'}
                        }
                    }



vacancy_status_H5_varstem_roots =  {'metadata' : {
                        'concept' : 'VACANCY STATUS',
                        'graft_chars' : ['vacancy'],
                        'new_char': ['vacancy'],
                        'char_vars' : ['vacancy'],
                        'group' : 'H5',
                        'vintage' : '2010', 
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'housing unit',
                        'mutually_exclusive' : True,
                        'countvar' : 'hucount',
                        'notes' : {'Include vancant housing units in total housing unit inventory.'}
                        },
                    'H005' : {
                    '002' : {'vacancy' : 1, 'label' : 'For Rent'},
                    '003' : {'vacancy' : 2, 'label' : 'Rented, not occupied'},
                    '004' : {'vacancy' : 3, 'label' : 'For sale only'},
                    '005' : {'vacancy' : 4, 'label' : 'Sold, not occupied'},
                    '006' : {'vacancy' : 5, 'label' : 'For seasonal, recreational, or occasional use'},
                    '007' : {'vacancy' : 6, 'label' : 'For migrant workers'},
                    '008' : {'vacancy' : 7, 'label' : 'Other vacant'}
                    }
                }

group_quarters_P42_varstem_roots = {'metadata' : {
                        'concept' : 'GROUP QUARTERS POPULATION BY GROUP QUARTERS TYPE',
                        'graft_chars' : ['hucount','gqtype'],
                        'new_char': ['hucount','gqtype'],
                        'char_vars' : ['hucount','gqtype'],
                        'group' : 'P42',
                        'vintage' : '2010', 
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'numprec',
                        'mutually_exclusive' : True,
                        'countvar' : 'numprec',
                        'notes' : {'Assume that population in block represents 1 housing unit. \
                                    All of the population in the block will be assigned to one \
                                    building or address point.'}
                        },
                        'P042' : {
                        '003' : {'hucount': 1, 'gqtype' : 1, 'label' : 'Correctional facilities for adults'},
                        '004' : {'hucount': 1, 'gqtype' : 2, 'label' : 'Juvenile facilities'},
                        '005' : {'hucount': 1, 'gqtype' : 3, 'label' : 'Nursing facilities/Skilled-nursing facilities'},
                        '006' : {'hucount': 1, 'gqtype' : 4, 'label' : 'Other institutional facilities'},
                        '008' : {'hucount': 1, 'gqtype' : 5, 'label' : 'College/University student housing'},
                        '009' : {'hucount': 1, 'gqtype' : 6, 'label' : 'Military quarters'},
                        '010' : {'hucount': 1, 'gqtype' : 7, 'label' : 'Other noninstitutional facilities'},
                        }
                    }

"""
Variables to identify family type
When grafting new variables need to do 1 at a time
New Char needs to have a name different from the final characteristic
This will prevent issues with merges that have common names
Only include cases where new variable is 1

"""
family_byrace_P18_varstem_roots = {'metadata' : {
                        'concept' : 'Family',
                        'byracehispan' : dec10byracehispan_groups_varstems,
                        'graft_chars' : ['race','hispan'],
                        'new_char': ['familybyP18'],
                        'char_vars' : ['familybyP18','byracehispan'],
                        'group' : 'P18',
                        'vintage' : '2010', 
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : False,
                        'mutually_exclusive_dict' : dec10byracehispan_groups_varstems_mxpt1,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'hucount',
                        'notes' : {''}
                        },
                        'P018' : {
                            '002' : {'familybyP18' : 1, 'label' : "Family Households"}
                        }
                    }

"""
Variables to identify household type
When grafting new variables need to do 1 at a time
"""

hhtype_byrace_P18_varstem_roots = {'metadata' : {
                        'concept' : 'HOUSEHOLD TYPE',
                        'byracehispan' : dec10byracehispan_groups_varstems,
                        'graft_chars' : ['race','hispan','family','numprec'],
                        'new_char': ['race','hispan','numprec','family','hhtype'],
                        'char_vars' : ['hhtype','family','numprec','byracehispan'],
                        'group' : 'P18',
                        'vintage' : '2010', 
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : False,
                        'mutually_exclusive_dict' : dec10byracehispan_groups_varstems_mxpt1,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'hucount',
                        'notes' : {''}
                        },
                        'P018' : {
                            '003' : {'hhtype' : 1, 'family' : 1, 'numprec' : -999, 'label' : "Husband-wife family"},
                            '005' : {'hhtype' : 2, 'family' : 1, 'numprec' : -999, 'label' : "Male householder, no wife present"},
                            '006' : {'hhtype' : 3, 'family' : 1, 'numprec' : -999, 'label' : "Female householder, no husband present"},
                            '008' : {'hhtype' : 4, 'family' : 0, 'numprec' : 1, 'label' : "Householder living alone"},
                            '009' : {'hhtype' : 5, 'family' : 0, 'numprec' : -999, 'label' : "Householder not living alone"},
                        }
                    }