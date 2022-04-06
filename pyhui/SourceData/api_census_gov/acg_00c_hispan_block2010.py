"""
Data Structure for Housing Unit Inventory
Block Level Data for the 2010 Census

Base API URL parameters, found at https://api.census.gov/data.html
List of all variables available in SF1 
https://api.census.gov/data/2010/dec/sf1/variables.html
"""

import numpy as np

"""
Variables to identify race of householder by Hispanic or Latino
Use to graft on probability of Hispanic characteristic
"""
hispan_byrace_H7_varstem_roots = {'metadata' : {
                        'concept' : 'HISPANIC OR LATINO ORIGIN OF HOUSEHOLDER BY RACE OF HOUSEHOLDER',
                        'graft_chars' : ['race'],
                        'new_char': ['hispanbyH007'],
                        'char_vars' : ['race','hispanbyH007'],
                        'group' : 'H7',
                        'vintage' : '2010', 
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : True,
                        'countvar' : 'hucount',
                        'notes' : {''}
                        },
                        'H007' : {
                        '011' : {
                        'label' : "Hispanic or Latino householder!!Householder who is White alone",
                        'hispanbyH007' : 1,
                        'race' : 1},
                        '012' : {
                        'label' : "Hispanic or Latino householder!!Householder who is Black or African American alone",
                        'hispanbyH007' : 1,
                        'race' : 2},
                        '013' : {
                        'label' : "Hispanic or Latino householder!!Householder who is American Indian and Alaska Native alone",
                        'hispanbyH007' : 1,
                        'race' : 3},
                        '014' : {
                        'label' : "Hispanic or Latino householder!!Householder who is Asian alone",
                        'hispanbyH007' : 1,
                        'race' : 4},
                        '015' : {
                        'label' : "Hispanic or Latino householder!!Householder who is Native Hawaiian and Other Pacific Islander alone",
                        'hispanbyH007' : 1,
                        'race' : 5},
                        '016' : {
                        'label' : "Hispanic or Latino householder!!Householder who is Some Other Race alone",
                        'hispanbyH007' : 1,
                        'race' : 6},
                        '017' : {
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
tenure_byhispan_H15_varstem_roots = {'metadata' : {
                        'concept' : 'TENURE BY HISPANIC OR LATINO ORIGIN OF HOUSEHOLDER',
                        'graft_chars' : ['ownershp'],
                        'new_char': ['hispanbyH015'],
                        'char_vars' : ['ownershp','hispanbyH015'],
                        'group' : 'H15',
                        'vintage' : '2010', 
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : True,
                        'countvar' : 'hucount',
                        'notes' : {''}
                        },
                        'H015' : {
                        '004' : {
                        'label' : "Owner occupied!!Hispanic or Latino householder",
                        'hispanbyH015' : 1,
                        'ownershp' : 1},
                        '007' : {
                        'label' : "Renter occupied!!Hispanic or Latino householder",
                        'hispanbyH015' : 1,
                        'ownershp' : 2}
                        }
                    }

"""
By race and Hispanic groups - the Census Consistently labels variables for race with letters
Not Mutually Exclusive
"""
dec10byracehispan_groups_varstems_HAI =  {
        'A' : {'race' : 1, 'hispan' : -999, 'Label' : 'White alone'},
        'I' : {'race' : 1, 'hispan' :0, 'Label' : 'White alone, not Hispanic'},
        'H' : {'race' : -999, 'hispan' : 1, 'Label' : 'Hispanic or Latino'}
        }

"""
Mutually Exclusive Race Hispanic Categeories
"""
subtract_function = "BaseInventory.subtract_df"

dec10byracehispan_groups_varstems_HAI_mx =  {
        'H-A-I' : {'race' : -999, 'hispan' : 1, 'Label' : 'Hispanic or Latino, any race Not white',
            'equation' : subtract_function+"(df['H'],"+subtract_function+"(df['A'],df['I'],"+\
                "index_col=indexvar),index_col=indexvar)"}
        }

tenure_size_H16HAI_varstem_roots = {'metadata' : {
                        'concept' : 'TENURE BY HOUSEHOLD SIZE',
                        'byracehispan' : dec10byracehispan_groups_varstems_HAI,
                        'graft_chars' : ['numprec','ownershp'],
                        'new_char': ['hispanbyH16HAI'],
                        'char_vars' : ['numprec','ownershp','hispanbyH16HAI','byracehispan'],
                        'group' : 'H16HAI',
                        'vintage' : '2010', 
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : False,
                        'mutually_exclusive_dict' : dec10byracehispan_groups_varstems_HAI_mx,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'hucount',
                        'notes' : {'Number of person records range 1-7. Households with \
                                    more than 7 persons not identified.'}
                        },
                        'H016' : {
                        '003' : {'numprec' : 1,'ownershp' : 1, 'hispanbyH16HAI' : 1, 'label' : 'Owner occupied 1-person'},
                        '004' : {'numprec' : 2,'ownershp' : 1, 'hispanbyH16HAI' : 1, 'label' : 'Owner occupied 2-person'},
                        '005' : {'numprec' : 3,'ownershp' : 1, 'hispanbyH16HAI' : 1, 'label' : 'Owner occupied 3-person'},
                        '006' : {'numprec' : 4,'ownershp' : 1, 'hispanbyH16HAI' : 1, 'label' : 'Owner occupied 4-person'},
                        '007' : {'numprec' : 5,'ownershp' : 1, 'hispanbyH16HAI' : 1, 'label' : 'Owner occupied 5-person'},
                        '008' : {'numprec' : 6,'ownershp' : 1, 'hispanbyH16HAI' : 1, 'label' : 'Owner occupied 6-person'},
                        '009' : {'numprec' : 7,'ownershp' : 1, 'hispanbyH16HAI' : 1, 'label' : 'Owner occupied 7-or-more-person'},
                        '011' : {'numprec' : 1,'ownershp' : 2, 'hispanbyH16HAI' : 1, 'label' : 'Renter occupied 1-person'},
                        '012' : {'numprec' : 2,'ownershp' : 2, 'hispanbyH16HAI' : 1, 'label' : 'Renter occupied 2-person'},
                        '013' : {'numprec' : 3,'ownershp' : 2, 'hispanbyH16HAI' : 1, 'label' : 'Renter occupied 3-person'},
                        '014' : {'numprec' : 4,'ownershp' : 2, 'hispanbyH16HAI' : 1, 'label' : 'Renter occupied 4-person'},
                        '015' : {'numprec' : 5,'ownershp' : 2, 'hispanbyH16HAI' : 1, 'label' : 'Renter occupied 5-person'},
                        '016' : {'numprec' : 6,'ownershp' : 2, 'hispanbyH16HAI' : 1, 'label' : 'Renter occupied 6-person'},
                        '017' : {'numprec' : 7,'ownershp' : 2, 'hispanbyH16HAI' : 1, 'label' : 'Renter occupied 7-or-more-person'}
                        }
                    }