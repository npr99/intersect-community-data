"""
Data Structure for Baseline Housing Unit Inventory
Block Level Data for the 2020 Census

Each Dictionary represents data tables in the 2020 Census

Base API URL parameters, found at https://www.census.gov/data/developers/data-sets/decennial-census.html
List of all variables available in SF1 
https://api.census.gov/data/2020/dec/sf1/variables.html
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
dec2020byracehispan_groups_varstems =  { 
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
dec2020byracehispan_groups_varstems_mxpt1 =  { 
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

dec2020byracehispan_groups_varstems_mxpt2 =  { 
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

tenure_size_H12_2020_varstem_roots = {'metadata' : {
                        'concept' : 'TENURE BY HOUSEHOLD SIZE',
                        'byracehispan' : dec2020byracehispan_groups_varstems,
                        'graft_chars' : ['numprec','ownershp','race','hispan'],
                        'new_char': ['numprec','ownershp','race','hispan','family'],
                        'char_vars' : ['numprec','ownershp','family','byracehispan'],
                        'group' : 'H12',
                        'vintage' : '2020', 
                        'dataset_name' : 'dec/dhc',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : False,
                        'mutually_exclusive_dict' : dec2020byracehispan_groups_varstems_mxpt1,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'hucount',
                        'notes' : {'Number of person records range 1-7. Households with \
                                    more than 7 persons not identified. \
                                    Assume that 1 person households are not family households.'    }
                        },
                        'H12' : {
                        '_003N' : {'numprec' : 1,'ownershp' : 1, 'family' : 0, 'label' : 'Owner occupied 1-person'},
                        '_004N' : {'numprec' : 2,'ownershp' : 1, 'family' : -999, 'label' : 'Owner occupied 2-person'},
                        '_005N' : {'numprec' : 3,'ownershp' : 1, 'family' : -999, 'label' : 'Owner occupied 3-person'},
                        '_006N' : {'numprec' : 4,'ownershp' : 1, 'family' : -999, 'label' : 'Owner occupied 4-person'},
                        '_007N' : {'numprec' : 5,'ownershp' : 1, 'family' : -999, 'label' : 'Owner occupied 5-person'},
                        '_008N' : {'numprec' : 6,'ownershp' : 1, 'family' : -999, 'label' : 'Owner occupied 6-person'},
                        '_009N' : {'numprec' : 7,'ownershp' : 1, 'family' : -999, 'label' : 'Owner occupied 7-or-more-person'},
                        '_011N' : {'numprec' : 1,'ownershp' : 2, 'family' : 0, 'label' : 'Renter occupied 1-person'},
                        '_012N' : {'numprec' : 2,'ownershp' : 2, 'family' : -999, 'label' : 'Renter occupied 2-person'},
                        '_013N' : {'numprec' : 3,'ownershp' : 2, 'family' : -999, 'label' : 'Renter occupied 3-person'},
                        '_014N' : {'numprec' : 4,'ownershp' : 2, 'family' : -999, 'label' : 'Renter occupied 4-person'},
                        '_015N' : {'numprec' : 5,'ownershp' : 2, 'family' : -999, 'label' : 'Renter occupied 5-person'},
                        '_016N' : {'numprec' : 6,'ownershp' : 2, 'family' : -999, 'label' : 'Renter occupied 6-person'},
                        '_017N' : {'numprec' : 7,'ownershp' : 2, 'family' : -999, 'label' : 'Renter occupied 7-or-more-person'}
                        }
                    }



vacancy_status_H5_2020_varstem_roots =  {'metadata' : {
                        'concept' : 'VACANCY STATUS',
                        'graft_chars' : ['vacancy'],
                        'new_char': ['vacancy'],
                        'char_vars' : ['vacancy'],
                        'group' : 'H5',
                        'vintage' : '2020', 
                        'dataset_name' : 'dec/dhc',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'housing unit',
                        'mutually_exclusive' : True,
                        'countvar' : 'hucount',
                        'notes' : {'Include vacant housing units in total housing unit inventory.'}
                        },
                    'H5' : {
                    '_002N' : {'vacancy' : 1, 'label' : 'For Rent'},
                    '_003N' : {'vacancy' : 2, 'label' : 'Rented, not occupied'},
                    '_004N' : {'vacancy' : 3, 'label' : 'For sale only'},
                    '_005N' : {'vacancy' : 4, 'label' : 'Sold, not occupied'},
                    '_006N' : {'vacancy' : 5, 'label' : 'For seasonal, recreational, or occasional use'},
                    '_007N' : {'vacancy' : 6, 'label' : 'For migrant workers'},
                    '_008N' : {'vacancy' : 7, 'label' : 'Other vacant'}
                    }
                }

group_quarters_P18_2020_varstem_roots = {'metadata' : {
                        'concept' : 'GROUP QUARTERS POPULATION BY GROUP QUARTERS TYPE',
                        'graft_chars' : ['hucount','gqtype'],
                        'new_char': ['hucount','gqtype','ageP18','sex'],
                        'char_vars' : ['hucount','gqtype','ageP18','sex'],
                        'group' : 'P18',
                        'vintage' : '2020', 
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'numprec',
                        'mutually_exclusive' : True,
                        'countvar' : 'numprec',
                        'notes' : {'Assume that population in block represents 1 housing unit. \
                                    All of the population in the block will be assigned to one \
                                    building or address point.'}
                        },
            'P18': {
                '_005N': {'hucount': 1, 'gqtype': 1, 'sex': 1, 'ageP18': 1, 'label': 'Correctional facilities for adults, Male Under 18 years'},
                '_006N': {'hucount': 1, 'gqtype': 2, 'sex': 1, 'ageP18': 1, 'label': 'Juvenile facilities, Male Under 18 years'},
                '_007N': {'hucount': 1, 'gqtype': 3, 'sex': 1, 'ageP18': 1, 'label': 'Nursing facilities/Skilled-nursing facilities, Male Under 18 years'},
                '_008N': {'hucount': 1, 'gqtype': 4, 'sex': 1, 'ageP18': 1, 'label': 'Other institutional facilities, Male Under 18 years'},
                '_010N': {'hucount': 1, 'gqtype': 5, 'sex': 1, 'ageP18': 1, 'label': 'College/University student housing, Male Under 18 years'},
                '_011N': {'hucount': 1, 'gqtype': 6, 'sex': 1, 'ageP18': 1, 'label': 'Military quarters, Male Under 18 years'},
                '_012N': {'hucount': 1, 'gqtype': 7, 'sex': 1, 'ageP18': 1, 'label': 'Other noninstitutional facilities, Male Under 18 years'},
                '_015N': {'hucount': 1, 'gqtype': 1, 'sex': 1, 'ageP18': 2, 'label': 'Correctional facilities for adults, Male 18 to 64 years'},
                '_016N': {'hucount': 1, 'gqtype': 2, 'sex': 1, 'ageP18': 2, 'label': 'Juvenile facilities, Male 18 to 64 years'},
                '_017N': {'hucount': 1, 'gqtype': 3, 'sex': 1, 'ageP18': 2, 'label': 'Nursing facilities/Skilled-nursing facilities, Male 18 to 64 years'},
                '_018N': {'hucount': 1, 'gqtype': 4, 'sex': 1, 'ageP18': 2, 'label': 'Other institutional facilities, Male 18 to 64 years'},
                '_020N': {'hucount': 1, 'gqtype': 5, 'sex': 1, 'ageP18': 2, 'label': 'College/University student housing, Male 18 to 64 years'},
                '_021N': {'hucount': 1, 'gqtype': 6, 'sex': 1, 'ageP18': 2, 'label': 'Military quarters, Male 18 to 64 years'},
                '_022N': {'hucount': 1, 'gqtype': 7, 'sex': 1, 'ageP18': 2, 'label': 'Other noninstitutional facilities, Male 18 to 64 years'},
                '_025N': {'hucount': 1, 'gqtype': 1, 'sex': 1, 'ageP18': 3, 'label': 'Correctional facilities for adults, Male 65 years and over'},
                '_026N': {'hucount': 1, 'gqtype': 2, 'sex': 1, 'ageP18': 3, 'label': 'Juvenile facilities, Male 65 years and over'},
                '_027N': {'hucount': 1, 'gqtype': 3, 'sex': 1, 'ageP18': 3, 'label': 'Nursing facilities/Skilled-nursing facilities, Male 65 years and over'},
                '_028N': {'hucount': 1, 'gqtype': 4, 'sex': 1, 'ageP18': 3, 'label': 'Other institutional facilities, Male 65 years and over'},
                '_030N': {'hucount': 1, 'gqtype': 5, 'sex': 1, 'ageP18': 3, 'label': 'College/University student housing, Male 65 years and over'},
                '_031N': {'hucount': 1, 'gqtype': 6, 'sex': 1, 'ageP18': 3, 'label': 'Military quarters, Male 65 years and over'},
                '_032N': {'hucount': 1, 'gqtype': 7, 'sex': 1, 'ageP18': 3, 'label': 'Other noninstitutional facilities, Male 65 years and over'},
                '_036N': {'hucount': 1, 'gqtype': 1, 'sex': 2, 'ageP18': 1, 'label': 'Correctional facilities for adults, Female Under 18 years'},
                '_037N': {'hucount': 1, 'gqtype': 2, 'sex': 2, 'ageP18': 1, 'label': 'Juvenile facilities, Female Under 18 years'},
                '_038N': {'hucount': 1, 'gqtype': 3, 'sex': 2, 'ageP18': 1, 'label': 'Nursing facilities/Skilled-nursing facilities, Female Under 18 years'},
                '_039N': {'hucount': 1, 'gqtype': 4, 'sex': 2, 'ageP18': 1, 'label': 'Other institutional facilities, Female Under 18 years'},
                '_041N': {'hucount': 1, 'gqtype': 5, 'sex': 2, 'ageP18': 1, 'label': 'College/University student housing, Female Under 18 years'},
                '_042N': {'hucount': 1, 'gqtype': 6, 'sex': 2, 'ageP18': 1, 'label': 'Military quarters, Female Under 18 years'},
                '_043N': {'hucount': 1, 'gqtype': 7, 'sex': 2, 'ageP18': 1, 'label': 'Other noninstitutional facilities, Female Under 18 years'},
                '_046N': {'hucount': 1, 'gqtype': 1, 'sex': 2, 'ageP18': 2, 'label': 'Correctional facilities for adults, Female 18 to 64 years'},
                '_047N': {'hucount': 1, 'gqtype': 2, 'sex': 2, 'ageP18': 2, 'label': 'Juvenile facilities, Female 18 to 64 years'},
                '_048N': {'hucount': 1, 'gqtype': 3, 'sex': 2, 'ageP18': 2, 'label': 'Nursing facilities/Skilled-nursing facilities, Female 18 to 64 years'},
                '_049N': {'hucount': 1, 'gqtype': 4, 'sex': 2, 'ageP18': 2, 'label': 'Other institutional facilities, Female 18 to 64 years'},
                '_051N': {'hucount': 1, 'gqtype': 5, 'sex': 2, 'ageP18': 2, 'label': 'College/University student housing, Female 18 to 64 years'},
                '_052N': {'hucount': 1, 'gqtype': 6, 'sex': 2, 'ageP18': 2, 'label': 'Military quarters, Female 18 to 64 years'},
                '_053N': {'hucount': 1, 'gqtype': 7, 'sex': 2, 'ageP18': 2, 'label': 'Other noninstitutional facilities, Female 18 to 64 years'},
                '_056N': {'hucount': 1, 'gqtype': 1, 'sex': 2, 'ageP18': 3, 'label': 'Correctional facilities for adults, Female 65 years and over'},
                '_057N': {'hucount': 1, 'gqtype': 2, 'sex': 2, 'ageP18': 3, 'label': 'Juvenile facilities, Female 65 years and over'},
                '_058N': {'hucount': 1, 'gqtype': 3, 'sex': 2, 'ageP18': 3, 'label': 'Nursing facilities/Skilled-nursing facilities, Female 65 years and over'},
                '_059N': {'hucount': 1, 'gqtype': 4, 'sex': 2, 'ageP18': 3, 'label': 'Other institutional facilities, Female 65 years and over'},
                '_061N': {'hucount': 1, 'gqtype': 5, 'sex': 2, 'ageP18': 3, 'label': 'College/University student housing, Female 65 years and over'},
                '_062N': {'hucount': 1, 'gqtype': 6, 'sex': 2, 'ageP18': 3, 'label': 'Military quarters, Female 65 years and over'},
                '_063N': {'hucount': 1, 'gqtype': 7, 'sex': 2, 'ageP18': 3, 'label': 'Other noninstitutional facilities, Female 65 years and over'}
                }
            }

"""
Variables to identify family type
When grafting new variables need to do 1 at a time
New Char needs to have a name different from the final characteristic
This will prevent issues with merges that have common names
Only include cases where new variable is 1

"""
family_byrace_P16_2020_varstem_roots = {'metadata' : {
                        'concept' : 'Family',
                        'byracehispan' : dec2020byracehispan_groups_varstems,
                        'graft_chars' : ['race','hispan'],
                        'new_char': ['familybyP16'],
                        'char_vars' : ['familybyP16','byracehispan'],
                        'group' : 'P16',
                        'vintage' : '2020', 
                        'dataset_name' : 'dec/dhc',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : False,
                        'mutually_exclusive_dict' : dec2020byracehispan_groups_varstems_mxpt1,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'hucount',
                        'notes' : {''}
                        },
                        'P16' : {
                            '_002N' : {'familybyP16' : 1, 'label' : "Family Households"}
                        }
                    }

"""
Variables to identify household type
When grafting new variables need to do 1 at a time
"""

hhtype_byrace_P16_2020_varstem_roots = {'metadata' : {
                        'concept' : 'HOUSEHOLD TYPE',
                        'byracehispan' : dec2020byracehispan_groups_varstems,
                        'graft_chars' : ['race','hispan','family','numprec'],
                        'new_char': ['race','hispan','numprec','family','hhtype'],
                        'char_vars' : ['hhtype','family','numprec','byracehispan'],
                        'group' : 'P16',
                        'vintage' : '2020', 
                        'dataset_name' : 'dec/dhc',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : False,
                        'mutually_exclusive_dict' : dec2020byracehispan_groups_varstems_mxpt1,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'hucount',
                        'notes' : {''}
                        },
                        'P16' : {
                            '_003N' : {'hhtype' : 1, 'family' : 1, 'numprec' : -999, 'label' : "Husband-wife family"},
                            '_005N' : {'hhtype' : 2, 'family' : 1, 'numprec' : -999, 'label' : "Male householder, no wife present"},
                            '_006N' : {'hhtype' : 3, 'family' : 1, 'numprec' : -999, 'label' : "Female householder, no husband present"},
                            '_008N' : {'hhtype' : 4, 'family' : 0, 'numprec' : 1, 'label' : "Householder living alone"},
                            '_009N' : {'hhtype' : 5, 'family' : 0, 'numprec' : -999, 'label' : "Householder not living alone"},
                        }
                    }