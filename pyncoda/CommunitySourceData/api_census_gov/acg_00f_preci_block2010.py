"""
Data Structure for Baseline Person Record Inventory
Block Level Data for the 2010 Census

Person Record is a concept that comes from public use microdata

Each Dictionary represents data tables in the 2010 Census

Base API URL parameters, found at https://api.census.gov/data.html
List of all variables available in SF1 
https://api.census.gov/data/2010/dec/sf1/variables.html
"""

import numpy as np

from pyncoda.CommunitySourceData.api_census_gov.acg_00b_hui_block2010 import *

"""
Person counts by sex by age by race and ethnicity 
"""

dictionary_age_sex_categories = {'003': {'label': 'Total!!Male!!Under 5 years',
                        'sex': 1,
                        'minageyrs': 0,
                        'maxageyrs': 4},
                        '004': {'label': 'Total!!Male!!5 to 9 years',
                        'sex': 1,
                        'minageyrs': 5,
                        'maxageyrs': 9},
                        '005': {'label': 'Total!!Male!!10 to 14 years',
                        'sex': 1,
                        'minageyrs': 10,
                        'maxageyrs': 14},
                        '006': {'label': 'Total!!Male!!15 to 17 years',
                        'sex': 1,
                        'minageyrs': 15,
                        'maxageyrs': 17},
                        '007': {'label': 'Total!!Male!!18 and 19 years',
                        'sex': 1,
                        'minageyrs': 18,
                        'maxageyrs': 19},
                        '008': {'label': 'Total!!Male!!20 years',
                        'sex': 1,
                        'minageyrs': 20,
                        'maxageyrs': 20},
                        '009': {'label': 'Total!!Male!!21 years',
                        'sex': 1,
                        'minageyrs': 21,
                        'maxageyrs': 21},
                        '010': {'label': 'Total!!Male!!22 to 24 years',
                        'sex': 1,
                        'minageyrs': 22,
                        'maxageyrs': 24},
                        '011': {'label': 'Total!!Male!!25 to 29 years',
                        'sex': 1,
                        'minageyrs': 25,
                        'maxageyrs': 29},
                        '012': {'label': 'Total!!Male!!30 to 34 years',
                        'sex': 1,
                        'minageyrs': 30,
                        'maxageyrs': 34},
                        '013': {'label': 'Total!!Male!!35 to 39 years',
                        'sex': 1,
                        'minageyrs': 35,
                        'maxageyrs': 39},
                        '014': {'label': 'Total!!Male!!40 to 44 years',
                        'sex': 1,
                        'minageyrs': 40,
                        'maxageyrs': 44},
                        '015': {'label': 'Total!!Male!!45 to 49 years',
                        'sex': 1,
                        'minageyrs': 45,
                        'maxageyrs': 49},
                        '016': {'label': 'Total!!Male!!50 to 54 years',
                        'sex': 1,
                        'minageyrs': 50,
                        'maxageyrs': 54},
                        '017': {'label': 'Total!!Male!!55 to 59 years',
                        'sex': 1,
                        'minageyrs': 55,
                        'maxageyrs': 59},
                        '018': {'label': 'Total!!Male!!60 and 61 years',
                        'sex': 1,
                        'minageyrs': 60,
                        'maxageyrs': 61},
                        '019': {'label': 'Total!!Male!!62 to 64 years',
                        'sex': 1,
                        'minageyrs': 62,
                        'maxageyrs': 64},
                        '020': {'label': 'Total!!Male!!65 and 66 years',
                        'sex': 1,
                        'minageyrs': 65,
                        'maxageyrs': 66},
                        '021': {'label': 'Total!!Male!!67 to 69 years',
                        'sex': 1,
                        'minageyrs': 67,
                        'maxageyrs': 69},
                        '022': {'label': 'Total!!Male!!70 to 74 years',
                        'sex': 1,
                        'minageyrs': 70,
                        'maxageyrs': 74},
                        '023': {'label': 'Total!!Male!!75 to 79 years',
                        'sex': 1,
                        'minageyrs': 75,
                        'maxageyrs': 79},
                        '024': {'label': 'Total!!Male!!80 to 84 years',
                        'sex': 1,
                        'minageyrs': 80,
                        'maxageyrs': 84},
                        '025': {'label': 'Total!!Male!!85 years and over',
                        'sex': 1,
                        'minageyrs': 85,
                        'maxageyrs': 110},
                        '027': {'label': 'Total!!Female!!Under 5 years',
                        'sex': 2,
                        'minageyrs': 0,
                        'maxageyrs': 4},
                        '028': {'label': 'Total!!Female!!5 to 9 years',
                        'sex': 2,
                        'minageyrs': 5,
                        'maxageyrs': 9},
                        '029': {'label': 'Total!!Female!!10 to 14 years',
                        'sex': 2,
                        'minageyrs': 10,
                        'maxageyrs': 14},
                        '030': {'label': 'Total!!Female!!15 to 17 years',
                        'sex': 2,
                        'minageyrs': 15,
                        'maxageyrs': 17},
                        '031': {'label': 'Total!!Female!!18 and 19 years',
                        'sex': 2,
                        'minageyrs': 18,
                        'maxageyrs': 19},
                        '032': {'label': 'Total!!Female!!20 years',
                        'sex': 2,
                        'minageyrs': 20,
                        'maxageyrs': 20},
                        '033': {'label': 'Total!!Female!!21 years',
                        'sex': 2,
                        'minageyrs': 21,
                        'maxageyrs': 21},
                        '034': {'label': 'Total!!Female!!22 to 24 years',
                        'sex': 2,
                        'minageyrs': 22,
                        'maxageyrs': 24},
                        '035': {'label': 'Total!!Female!!25 to 29 years',
                        'sex': 2,
                        'minageyrs': 25,
                        'maxageyrs': 29},
                        '036': {'label': 'Total!!Female!!30 to 34 years',
                        'sex': 2,
                        'minageyrs': 30,
                        'maxageyrs': 34},
                        '037': {'label': 'Total!!Female!!35 to 39 years',
                        'sex': 2,
                        'minageyrs': 35,
                        'maxageyrs': 39},
                        '038': {'label': 'Total!!Female!!40 to 44 years',
                        'sex': 2,
                        'minageyrs': 40,
                        'maxageyrs': 44},
                        '039': {'label': 'Total!!Female!!45 to 49 years',
                        'sex': 2,
                        'minageyrs': 45,
                        'maxageyrs': 49},
                        '040': {'label': 'Total!!Female!!50 to 54 years',
                        'sex': 2,
                        'minageyrs': 50,
                        'maxageyrs': 54},
                        '041': {'label': 'Total!!Female!!55 to 59 years',
                        'sex': 2,
                        'minageyrs': 55,
                        'maxageyrs': 59},
                        '042': {'label': 'Total!!Female!!60 and 61 years',
                        'sex': 2,
                        'minageyrs': 60,
                        'maxageyrs': 61},
                        '043': {'label': 'Total!!Female!!62 to 64 years',
                        'sex': 2,
                        'minageyrs': 62,
                        'maxageyrs': 64},
                        '044': {'label': 'Total!!Female!!65 and 66 years',
                        'sex': 2,
                        'minageyrs': 65,
                        'maxageyrs': 66},
                        '045': {'label': 'Total!!Female!!67 to 69 years',
                        'sex': 2,
                        'minageyrs': 67,
                        'maxageyrs': 69},
                        '046': {'label': 'Total!!Female!!70 to 74 years',
                        'sex': 2,
                        'minageyrs': 70,
                        'maxageyrs': 74},
                        '047': {'label': 'Total!!Female!!75 to 79 years',
                        'sex': 2,
                        'minageyrs': 75,
                        'maxageyrs': 79},
                        '048': {'label': 'Total!!Female!!80 to 84 years',
                        'sex': 2,
                        'minageyrs': 80,
                        'maxageyrs': 84},
                        '049': {'label': 'Total!!Female!!85 years and over',
                        'sex': 2,
                        'minageyrs': 85,
                        'maxageyrs': 110}}

sexbyage_P12_varstem_roots = {'metadata' : {
                        'concept' : 'SEX BY AGE FOR SELECTED AGE CATEGORIES',
                        'byracehispan' : dec10byracehispan_groups_varstems,
                        'graft_chars' : ['sex','minageyrs','maxageyrs','race','hispan'],
                        'new_char': ['sex','minageyrs','maxageyrs','race','hispan'],
                        'char_vars' : ['sex','minageyrs','maxageyrs','byracehispan'],
                        'group' : 'P12IA-G',
                        'vintage' : '2010', 
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'person',
                        'mutually_exclusive' : False,
                        'mutually_exclusive_dict' : dec10byracehispan_groups_varstems_mxpt1,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'preccount',
                        'notes' : {'https://api.census.gov/data/2010/dec/sf1/groups/P12.html.'    }
                        },
                        'P012': dictionary_age_sex_categories}

"""
data structures to predict Hispanic
"""

hispan_byrace_P5_varstem_roots = {'metadata' : {
                        'concept' : 'HISPANIC OR LATINO ORIGIN BY RACE',
                        'graft_chars' : ['race'],
                        'new_char': ['hispanbyP5'],
                        'char_vars' : ['race','hispanbyP5'],
                        'group' : 'P5',
                        'vintage' : '2010', 
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'person',
                        'mutually_exclusive' : True,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'preccount',
                        'notes' : {'https://api.census.gov/data/2010/dec/sf1/groups/P5.html.'    }
                        },
                        'P005': {
                        '011' : {
                        'label' : "Hispanic or Latino!!White alone",
                        'hispanbyP5' : 1,
                        'race' : 1},
                        '012' : {
                        'label' : "Hispanic or Latino!!Black or African American alone",
                        'hispanbyP5' : 1,
                        'race' : 2},
                        '013' : {
                        'label' : "Hispanic or Latino!!American Indian and Alaska Native alone",
                        'hispanbyP5' : 1,
                        'race' : 3},
                        '014' : {
                        'label' : "Hispanic or Latino!!Asian alone",
                        'hispanbyP5' : 1,
                        'race' : 4},
                        '015' : {
                        'label' : "Hispanic or Latino!!Native Hawaiian and Other Pacific Islander alone",
                        'hispanbyP5' : 1,
                        'race' : 5},
                        '016' : {
                        'label' : "Hispanic or Latino!!Some Other Race alone",
                        'hispanbyP5' : 1,
                        'race' : 6},
                        '017' : {
                        'label' : "Hispanic or Latino!!Two or More Races",
                        'hispanbyP5' : 1,
                        'race' : 7}
                        }
                        }

"""
To help predict Hispanic persons create new group
based on Hispanic minus White alone, Hispanic
"""

dec10hispannotwhite_groups_varstems =  {
        'A' : {'race' : 1, 'hispan' : -999, 'Label' : 'White alone'},
        'I' : {'race' : 1, 'hispan' :0, 'Label' : 'White alone, not Hispanic'},
        'H' : {'race' : -999, 'hispan' : 1, 'Label' : 'Hispanic or Latino'}
        }

subtract_function = "BaseInventory.subtract_df"

dec10hispannotwhite_groups_varstems_HAI =  {
        'H-A-I' : {'race' : -999, 'hispan' : 1, 'Label' : 'Hispanic or Latino, any race Not white',
            'equation' : subtract_function+"(df['H'],"+subtract_function+"(df['A'],df['I'],"+\
                "index_col=indexvar),index_col=indexvar)"}
        }

# Add HispanbyP12HAI to dictionary
import copy
dictionary_age_sex_categories_P12HAI = copy.deepcopy(dictionary_age_sex_categories)
for var_stem in dictionary_age_sex_categories_P12HAI.keys():
    dictionary_age_sex_categories_P12HAI[var_stem]['hispanbyP12HAI'] = 1

sexbyage_P12HAI_varstem_roots = {'metadata' : {
                        'concept' : 'SEX BY AGE FOR SELECTED AGE CATEGORIES',
                        'byracehispan' : dec10hispannotwhite_groups_varstems,
                        'graft_chars' : ['sex','minageyrs','maxageyrs'],
                        'new_char': ['hispanbyP12HAI'],
                        'char_vars' : ['sex','minageyrs','maxageyrs','hispanbyP12HAI','byracehispan'],
                        'group' : 'P12HAI',
                        'vintage' : '2010', 
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'person',
                        'mutually_exclusive' : False,
                        'mutually_exclusive_dict' : dec10hispannotwhite_groups_varstems_HAI,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'preccount',
                        'notes' : {'https://api.census.gov/data/2010/dec/sf1/groups/P12.html.'    }
                        },
                        'P012': dictionary_age_sex_categories_P12HAI}