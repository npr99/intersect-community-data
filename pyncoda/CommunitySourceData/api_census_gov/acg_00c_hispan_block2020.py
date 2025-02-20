"""
Data Structure for Housing Unit Inventory
Block Level Data for the 2020 Census

Base API URL parameters, found at https://api.census.gov/data.html
List of all variables available in DHC
https://api.census.gov/data/2020/dec/dhc/variables.html
"""

import numpy as np

"""
Variables to identify race of householder by Hispanic or Latino
Use to graft on probability of Hispanic characteristic
"""
hispan_byrace_H7_2020_varstem_roots = {'metadata' : {
                        'concept' : 'HISPANIC OR LATINO ORIGIN OF HOUSEHOLDER BY RACE OF HOUSEHOLDER',
                        'graft_chars' : ['race'],
                        'new_char': ['hispanbyH7'],
                        'char_vars' : ['race','hispanbyH7'],
                        'group' : 'H7',
                        'vintage' : '2020', 
                        'dataset_name' : 'dec/dhc',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : True,
                        'countvar' : 'hucount',
                        'notes' : {''}
                        },
                        'H7' : {
                        '_011N' : {
                        'label' : "Hispanic or Latino householder!!Householder who is White alone",
                        'hispanbyH007' : 1,
                        'race' : 1},
                        '_012N' : {
                        'label' : "Hispanic or Latino householder!!Householder who is Black or African American alone",
                        'hispanbyH007' : 1,
                        'race' : 2},
                        '_013N' : {
                        'label' : "Hispanic or Latino householder!!Householder who is American Indian and Alaska Native alone",
                        'hispanbyH007' : 1,
                        'race' : 3},
                        '_014N' : {
                        'label' : "Hispanic or Latino householder!!Householder who is Asian alone",
                        'hispanbyH007' : 1,
                        'race' : 4},
                        '_015N' : {
                        'label' : "Hispanic or Latino householder!!Householder who is Native Hawaiian and Other Pacific Islander alone",
                        'hispanbyH007' : 1,
                        'race' : 5},
                        '_016N' : {
                        'label' : "Hispanic or Latino householder!!Householder who is Some Other Race alone",
                        'hispanbyH007' : 1,
                        'race' : 6},
                        '_017N' : {
                        'label' : "Hispanic or Latino householder!!Householder who is Two or More Races",
                        'hispanbyH007' : 1,
                        'race' : 7}
                        }
                    }  

"""
Variables to identify tenure by Hispanic or Latino
Use to graft on probability of Hispanic characteristic
Select only variables where Hispanic = 1
"""
tenure_byhispan_H11_2020_varstem_roots = {'metadata' : {
                        'concept' : 'TENURE BY HISPANIC OR LATINO ORIGIN OF HOUSEHOLDER',
                        'graft_chars' : ['ownershp'],
                        'new_char': ['hispanbyH11'],
                        'char_vars' : ['ownershp','hispanbyH11'],
                        'group' : 'H11',
                        'vintage' : '2020', 
                        'dataset_name' : 'dec/dhc',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : True,
                        'countvar' : 'hucount',
                        'notes' : {''}
                        },
                        'H11' : {
                        '004' : {
                        'label' : "Owner occupied!!Hispanic or Latino householder",
                        'hispanbyH11' : 1,
                        'ownershp' : 1},
                        '007' : {
                        'label' : "Renter occupied!!Hispanic or Latino householder",
                        'hispanbyH11' : 1,
                        'ownershp' : 2}
                        }
                    }

"""
By race and Hispanic groups - the Census Consistently labels variables for race with letters
Not Mutually Exclusive
"""
dec2020byracehispan_groups_varstems_HAI =  {
        'A' : {'race' : 1, 'hispan' : -999, 'Label' : 'White alone'},
        'I' : {'race' : 1, 'hispan' :0, 'Label' : 'White alone, not Hispanic'},
        'H' : {'race' : -999, 'hispan' : 1, 'Label' : 'Hispanic or Latino'}
        }

"""
Mutually Exclusive Race Hispanic Categeories
"""
subtract_function = "BaseInventory.subtract_df"

dec2020byracehispan_groups_varstems_HAI_mx =  {
        'H-A-I' : {'race' : -999, 'hispan' : 1, 'Label' : 'Hispanic or Latino, any race Not white',
            'equation' : subtract_function+"(df['H'],"+subtract_function+"(df['A'],df['I'],"+\
                "index_col=indexvar),index_col=indexvar)"}
        }

tenure_size_H12HAI_2020_varstem_roots = {'metadata' : {
                        'concept' : 'TENURE BY HOUSEHOLD SIZE',
                        'byracehispan' : dec2020byracehispan_groups_varstems_HAI,
                        'graft_chars' : ['numprec','ownershp'],
                        'new_char': ['hispanbyH12HAI'],
                        'char_vars' : ['numprec','ownershp','hispanbyH12HAI','byracehispan'],
                        'group' : 'H12HAI',
                        'vintage' : '2020', 
                        'dataset_name' : 'dec/dhc',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : False,
                        'mutually_exclusive_dict' : dec2020byracehispan_groups_varstems_HAI_mx,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'hucount',
                        'notes' : {'Number of person records range 1-7. Households with \
                                    more than 7 persons not identified.'}
                        },
                        'H12' : {
                        '_003N' : {'numprec' : 1,'ownershp' : 1, 'hispanbyH12HAI' : 1, 'label' : 'Owner occupied 1-person'},
                        '_004N' : {'numprec' : 2,'ownershp' : 1, 'hispanbyH12HAI' : 1, 'label' : 'Owner occupied 2-person'},
                        '_005N' : {'numprec' : 3,'ownershp' : 1, 'hispanbyH12HAI' : 1, 'label' : 'Owner occupied 3-person'},
                        '_006N' : {'numprec' : 4,'ownershp' : 1, 'hispanbyH12HAI' : 1, 'label' : 'Owner occupied 4-person'},
                        '_007N' : {'numprec' : 5,'ownershp' : 1, 'hispanbyH12HAI' : 1, 'label' : 'Owner occupied 5-person'},
                        '_008N' : {'numprec' : 6,'ownershp' : 1, 'hispanbyH12HAI' : 1, 'label' : 'Owner occupied 6-person'},
                        '_009N' : {'numprec' : 7,'ownershp' : 1, 'hispanbyH12HAI' : 1, 'label' : 'Owner occupied 7-or-more-person'},
                        '_011N' : {'numprec' : 1,'ownershp' : 2, 'hispanbyH12HAI' : 1, 'label' : 'Renter occupied 1-person'},
                        '_012N' : {'numprec' : 2,'ownershp' : 2, 'hispanbyH12HAI' : 1, 'label' : 'Renter occupied 2-person'},
                        '_013N' : {'numprec' : 3,'ownershp' : 2, 'hispanbyH12HAI' : 1, 'label' : 'Renter occupied 3-person'},
                        '_014N' : {'numprec' : 4,'ownershp' : 2, 'hispanbyH12HAI' : 1, 'label' : 'Renter occupied 4-person'},
                        '_015N' : {'numprec' : 5,'ownershp' : 2, 'hispanbyH12HAI' : 1, 'label' : 'Renter occupied 5-person'},
                        '_016N' : {'numprec' : 6,'ownershp' : 2, 'hispanbyH12HAI' : 1, 'label' : 'Renter occupied 6-person'},
                        '_017N' : {'numprec' : 7,'ownershp' : 2, 'hispanbyH12HAI' : 1, 'label' : 'Renter occupied 7-or-more-person'}
                        }
                    }