"""
Data Structure for Baseline Person Record Inventory
Block Level Data for the 2020 Census

Person Record is a concept that comes from public use microdata

Each Dictionary represents data tables in the 2020 Census DHC

Base API URL parameters, found at https://api.census.gov/data.html
List of all variables available in DHC
https://api.census.gov/data/2020/dec/dhc/variables.html

2020 Census uses dec/dhc dataset instead of dec/sf1
Variable stems use _NNNN format (e.g. _003N) instead of NNN (e.g. 003)

Pattern follows acg_00f_preci_block2010.py
"""

import numpy as np
import copy

from pyncoda.CommunitySourceData.api_census_gov.acg_00b_hui_block2020 import (
    dec2020byracehispan_groups_varstems,
    dec2020byracehispan_groups_varstems_mxpt1
)

"""
Person counts by sex by age by race and ethnicity
2020 Census DHC Table P12 - SEX BY AGE
https://api.census.gov/data/2020/dec/dhc/groups/P12.html

Variable stems use _NNNN format with N suffix
Male variables: _003N through _025N
Female variables: _027N through _049N

Same age categories as 2010 P12 — same dictionary structure,
only the variable stems and dataset change.
"""

dictionary_age_sex_categories_2020 = {
    # ── Male ─────────────────────────────────────────────────────
    '_003N': {'label': 'Total!!Male!!Under 5 years',
              'sex': 1, 'minageyrs': 0,  'maxageyrs': 4},
    '_004N': {'label': 'Total!!Male!!5 to 9 years',
              'sex': 1, 'minageyrs': 5,  'maxageyrs': 9},
    '_005N': {'label': 'Total!!Male!!10 to 14 years',
              'sex': 1, 'minageyrs': 10, 'maxageyrs': 14},
    '_006N': {'label': 'Total!!Male!!15 to 17 years',
              'sex': 1, 'minageyrs': 15, 'maxageyrs': 17},
    '_007N': {'label': 'Total!!Male!!18 and 19 years',
              'sex': 1, 'minageyrs': 18, 'maxageyrs': 19},
    '_008N': {'label': 'Total!!Male!!20 years',
              'sex': 1, 'minageyrs': 20, 'maxageyrs': 20},
    '_009N': {'label': 'Total!!Male!!21 years',
              'sex': 1, 'minageyrs': 21, 'maxageyrs': 21},
    '_010N': {'label': 'Total!!Male!!22 to 24 years',
              'sex': 1, 'minageyrs': 22, 'maxageyrs': 24},
    '_011N': {'label': 'Total!!Male!!25 to 29 years',
              'sex': 1, 'minageyrs': 25, 'maxageyrs': 29},
    '_012N': {'label': 'Total!!Male!!30 to 34 years',
              'sex': 1, 'minageyrs': 30, 'maxageyrs': 34},
    '_013N': {'label': 'Total!!Male!!35 to 39 years',
              'sex': 1, 'minageyrs': 35, 'maxageyrs': 39},
    '_014N': {'label': 'Total!!Male!!40 to 44 years',
              'sex': 1, 'minageyrs': 40, 'maxageyrs': 44},
    '_015N': {'label': 'Total!!Male!!45 to 49 years',
              'sex': 1, 'minageyrs': 45, 'maxageyrs': 49},
    '_016N': {'label': 'Total!!Male!!50 to 54 years',
              'sex': 1, 'minageyrs': 50, 'maxageyrs': 54},
    '_017N': {'label': 'Total!!Male!!55 to 59 years',
              'sex': 1, 'minageyrs': 55, 'maxageyrs': 59},
    '_018N': {'label': 'Total!!Male!!60 and 61 years',
              'sex': 1, 'minageyrs': 60, 'maxageyrs': 61},
    '_019N': {'label': 'Total!!Male!!62 to 64 years',
              'sex': 1, 'minageyrs': 62, 'maxageyrs': 64},
    '_020N': {'label': 'Total!!Male!!65 and 66 years',
              'sex': 1, 'minageyrs': 65, 'maxageyrs': 66},
    '_021N': {'label': 'Total!!Male!!67 to 69 years',
              'sex': 1, 'minageyrs': 67, 'maxageyrs': 69},
    '_022N': {'label': 'Total!!Male!!70 to 74 years',
              'sex': 1, 'minageyrs': 70, 'maxageyrs': 74},
    '_023N': {'label': 'Total!!Male!!75 to 79 years',
              'sex': 1, 'minageyrs': 75, 'maxageyrs': 79},
    '_024N': {'label': 'Total!!Male!!80 to 84 years',
              'sex': 1, 'minageyrs': 80, 'maxageyrs': 84},
    '_025N': {'label': 'Total!!Male!!85 years and over',
              'sex': 1, 'minageyrs': 85, 'maxageyrs': 110},
    # ── Female ───────────────────────────────────────────────────
    '_027N': {'label': 'Total!!Female!!Under 5 years',
              'sex': 2, 'minageyrs': 0,  'maxageyrs': 4},
    '_028N': {'label': 'Total!!Female!!5 to 9 years',
              'sex': 2, 'minageyrs': 5,  'maxageyrs': 9},
    '_029N': {'label': 'Total!!Female!!10 to 14 years',
              'sex': 2, 'minageyrs': 10, 'maxageyrs': 14},
    '_030N': {'label': 'Total!!Female!!15 to 17 years',
              'sex': 2, 'minageyrs': 15, 'maxageyrs': 17},
    '_031N': {'label': 'Total!!Female!!18 and 19 years',
              'sex': 2, 'minageyrs': 18, 'maxageyrs': 19},
    '_032N': {'label': 'Total!!Female!!20 years',
              'sex': 2, 'minageyrs': 20, 'maxageyrs': 20},
    '_033N': {'label': 'Total!!Female!!21 years',
              'sex': 2, 'minageyrs': 21, 'maxageyrs': 21},
    '_034N': {'label': 'Total!!Female!!22 to 24 years',
              'sex': 2, 'minageyrs': 22, 'maxageyrs': 24},
    '_035N': {'label': 'Total!!Female!!25 to 29 years',
              'sex': 2, 'minageyrs': 25, 'maxageyrs': 29},
    '_036N': {'label': 'Total!!Female!!30 to 34 years',
              'sex': 2, 'minageyrs': 30, 'maxageyrs': 34},
    '_037N': {'label': 'Total!!Female!!35 to 39 years',
              'sex': 2, 'minageyrs': 35, 'maxageyrs': 39},
    '_038N': {'label': 'Total!!Female!!40 to 44 years',
              'sex': 2, 'minageyrs': 40, 'maxageyrs': 44},
    '_039N': {'label': 'Total!!Female!!45 to 49 years',
              'sex': 2, 'minageyrs': 45, 'maxageyrs': 49},
    '_040N': {'label': 'Total!!Female!!50 to 54 years',
              'sex': 2, 'minageyrs': 50, 'maxageyrs': 54},
    '_041N': {'label': 'Total!!Female!!55 to 59 years',
              'sex': 2, 'minageyrs': 55, 'maxageyrs': 59},
    '_042N': {'label': 'Total!!Female!!60 and 61 years',
              'sex': 2, 'minageyrs': 60, 'maxageyrs': 61},
    '_043N': {'label': 'Total!!Female!!62 to 64 years',
              'sex': 2, 'minageyrs': 62, 'maxageyrs': 64},
    '_044N': {'label': 'Total!!Female!!65 and 66 years',
              'sex': 2, 'minageyrs': 65, 'maxageyrs': 66},
    '_045N': {'label': 'Total!!Female!!67 to 69 years',
              'sex': 2, 'minageyrs': 67, 'maxageyrs': 69},
    '_046N': {'label': 'Total!!Female!!70 to 74 years',
              'sex': 2, 'minageyrs': 70, 'maxageyrs': 74},
    '_047N': {'label': 'Total!!Female!!75 to 79 years',
              'sex': 2, 'minageyrs': 75, 'maxageyrs': 79},
    '_048N': {'label': 'Total!!Female!!80 to 84 years',
              'sex': 2, 'minageyrs': 80, 'maxageyrs': 84},
    '_049N': {'label': 'Total!!Female!!85 years and over',
              'sex': 2, 'minageyrs': 85, 'maxageyrs': 110},
}

sexbyage_P12_2020_varstem_roots = {
    'metadata': {
        'concept'      : 'SEX BY AGE FOR SELECTED AGE CATEGORIES',
        'byracehispan' : dec2020byracehispan_groups_varstems,
        'graft_chars'  : ['sex', 'minageyrs', 'maxageyrs', 'race', 'hispan'],
        'new_char'     : ['sex', 'minageyrs', 'maxageyrs', 'race', 'hispan'],
        'char_vars'    : ['sex', 'minageyrs', 'maxageyrs', 'byracehispan'],
        'group'        : 'P12IA-G',          # group label used for file naming
        'vintage'      : '2020',
        'dataset_name' : 'dec/dhc',          # 2020 uses DHC not SF1
        'for_geography': 'block:*',
        'unit_of_analysis': 'person',
        'mutually_exclusive': False,
        'mutually_exclusive_dict': dec2020byracehispan_groups_varstems_mxpt1,
        'indexvar'     : ['GEO_ID', 'state', 'county', 'tract', 'block'],
        'countvar'     : 'preccount',
        'notes'        : {'https://api.census.gov/data/2020/dec/dhc/groups/P12.html.'}
    },
    'P12': dictionary_age_sex_categories_2020
}

"""
Data structure for Hispanic prediction — P5 equivalent in 2020 Census DHC
2020 Census uses table P5 for Hispanic/Latino origin by race
https://api.census.gov/data/2020/dec/dhc/groups/P5.html
Variable stems use _NNNN format
"""

hispan_byrace_P5_2020_varstem_roots = {
    'metadata': {
        'concept'      : 'HISPANIC OR LATINO ORIGIN BY RACE',
        'graft_chars'  : ['race'],
        'new_char'     : ['hispanbyP5'],
        'char_vars'    : ['race', 'hispanbyP5'],
        'group'        : 'P5',
        'vintage'      : '2020',
        'dataset_name' : 'dec/dhc',
        'for_geography': 'block:*',
        'unit_of_analysis': 'person',
        'mutually_exclusive': True,
        'indexvar'     : ['GEO_ID', 'state', 'county', 'tract', 'block'],
        'countvar'     : 'preccount',
        'notes'        : {'https://api.census.gov/data/2020/dec/dhc/groups/P5.html.'}
    },
    'P5': {
        '_011N': {'label': 'Hispanic or Latino!!White alone',
                  'hispanbyP5': 1, 'race': 1},
        '_012N': {'label': 'Hispanic or Latino!!Black or African American alone',
                  'hispanbyP5': 1, 'race': 2},
        '_013N': {'label': 'Hispanic or Latino!!American Indian and Alaska Native alone',
                  'hispanbyP5': 1, 'race': 3},
        '_014N': {'label': 'Hispanic or Latino!!Asian alone',
                  'hispanbyP5': 1, 'race': 4},
        '_015N': {'label': 'Hispanic or Latino!!Native Hawaiian and Other Pacific Islander alone',
                  'hispanbyP5': 1, 'race': 5},
        '_016N': {'label': 'Hispanic or Latino!!Some Other Race alone',
                  'hispanbyP5': 1, 'race': 6},
        '_017N': {'label': 'Hispanic or Latino!!Two or More Races',
                  'hispanbyP5': 1, 'race': 7},
    }
}

"""
Hispanic prediction helper — HAI group (Hispanic minus White alone Hispanic)
Mirrors dec10hispannotwhite_groups_varstems pattern from 2010 file
"""

dec2020hispannotwhite_groups_varstems = {
    'A': {'race':  1, 'hispan': -999, 'Label': 'White alone'},
    'I': {'race':  1, 'hispan':    0, 'Label': 'White alone, not Hispanic'},
    'H': {'race': -999, 'hispan':  1, 'Label': 'Hispanic or Latino'},
}

subtract_function = "BaseInventory.subtract_df"

dec2020hispannotwhite_groups_varstems_HAI = {
    'H-A-I': {
        'race': -999, 'hispan': 1,
        'Label': 'Hispanic or Latino, any race Not white',
        'equation': subtract_function + "(df['H']," +
                    subtract_function + "(df['A'],df['I']," +
                    "index_col=indexvar),index_col=indexvar)"
    }
}

# Add hispanbyP12HAI flag to 2020 age-sex dictionary
dictionary_age_sex_categories_2020_P12HAI = copy.deepcopy(dictionary_age_sex_categories_2020)
for var_stem in dictionary_age_sex_categories_2020_P12HAI.keys():
    dictionary_age_sex_categories_2020_P12HAI[var_stem]['hispanbyP12HAI'] = 1

sexbyage_P12HAI_2020_varstem_roots = {
    'metadata': {
        'concept'      : 'SEX BY AGE FOR SELECTED AGE CATEGORIES',
        'byracehispan' : dec2020hispannotwhite_groups_varstems,
        'graft_chars'  : ['sex', 'minageyrs', 'maxageyrs'],
        'new_char'     : ['hispanbyP12HAI'],
        'char_vars'    : ['sex', 'minageyrs', 'maxageyrs', 'hispanbyP12HAI', 'byracehispan'],
        'group'        : 'P12HAI',
        'vintage'      : '2020',
        'dataset_name' : 'dec/dhc',
        'for_geography': 'block:*',
        'unit_of_analysis': 'person',
        'mutually_exclusive': False,
        'mutually_exclusive_dict': dec2020hispannotwhite_groups_varstems_HAI,
        'indexvar'     : ['GEO_ID', 'state', 'county', 'tract', 'block'],
        'countvar'     : 'preccount',
        'notes'        : {'https://api.census.gov/data/2020/dec/dhc/groups/P12.html.'}
    },
    'P12': dictionary_age_sex_categories_2020_P12HAI
}
