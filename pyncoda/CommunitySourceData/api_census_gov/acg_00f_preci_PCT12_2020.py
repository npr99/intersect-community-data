"""
Data Structure for Person Record Inventory
Tract Level PCT12 Data for the 2020 Census DHC

SEX BY SINGLE-YEAR AGE
https://api.census.gov/data/2020/dec/dhc/groups/PCT12.html

2020 Census uses dec/dhc dataset
Variable format: PCT12_NNNN (e.g. PCT12_003N)

VERIFIED AGAINST LIVE CENSUS API (2026-04-25):
  PCT12_001N = Total
  PCT12_002N = Total!!Male  (subtotal - skip)
  PCT12_003N = Male Under 1 year (age 0)
  PCT12_004N = Male 1 year (age 1)
  ...
  PCT12_102N = Male 99 years (age 99)
  PCT12_103N = Male 100 to 104 years
  PCT12_104N = Male 105 to 109 years
  PCT12_105N = Male 110 years and over
  PCT12_106N = Total!!Female  (subtotal - skip)
  PCT12_107N = Female Under 1 year (age 0)
  PCT12_108N = Female 1 year (age 1)
  ...
  PCT12_206N = Female 99 years (age 99)
  PCT12_207N = Female 100 to 104 years
  PCT12_208N = Female 105 to 109 years
  PCT12_209N = Female 110 years and over

Total: 206 variables (100 male single ages + 3 male grouped
                    + 100 female single ages + 3 female grouped)

CORRECTION HISTORY:
  - Earlier version had ALL female variables off by one
    (started female at _108N instead of correct _107N)
  - This corrected version aligns to the official Census API exactly.
"""

import numpy as np

dictionary_single_age_sex_PCT12_2020 = {}

# === MALE === ages 0-99 (single years), then 100-104, 105-109, 110+
# _003N = age 0 (Under 1 year)
# _004N = age 1
# ...
# _102N = age 99
for age in range(100):
    var_num  = age + 3
    var_name = f'_{str(var_num).zfill(3)}N'
    label    = 'Under 1 year' if age == 0 else f'{age} {"year" if age == 1 else "years"}'
    dictionary_single_age_sex_PCT12_2020[var_name] = {
        'label'     : f'Total!!Male!!{label}',
        'sex'       : 1,
        'minageyrs' : age,
        'maxageyrs' : age,
    }
dictionary_single_age_sex_PCT12_2020['_103N'] = {
    'label': 'Total!!Male!!100 to 104 years',
    'sex': 1, 'minageyrs': 100, 'maxageyrs': 104,
}
dictionary_single_age_sex_PCT12_2020['_104N'] = {
    'label': 'Total!!Male!!105 to 109 years',
    'sex': 1, 'minageyrs': 105, 'maxageyrs': 109,
}
dictionary_single_age_sex_PCT12_2020['_105N'] = {
    'label': 'Total!!Male!!110 years and over',
    'sex': 1, 'minageyrs': 110, 'maxageyrs': 110,
}

# === FEMALE === ages 0-99 (single years), then 100-104, 105-109, 110+
# _107N = age 0 (Under 1 year)  <-- CORRECTED (was incorrectly _108N before)
# _108N = age 1
# ...
# _206N = age 99
for age in range(100):
    var_num  = age + 107
    var_name = f'_{str(var_num).zfill(3)}N'
    label    = 'Under 1 year' if age == 0 else f'{age} {"year" if age == 1 else "years"}'
    dictionary_single_age_sex_PCT12_2020[var_name] = {
        'label'     : f'Total!!Female!!{label}',
        'sex'       : 2,
        'minageyrs' : age,
        'maxageyrs' : age,
    }
dictionary_single_age_sex_PCT12_2020['_207N'] = {
    'label': 'Total!!Female!!100 to 104 years',
    'sex': 2, 'minageyrs': 100, 'maxageyrs': 104,
}
dictionary_single_age_sex_PCT12_2020['_208N'] = {
    'label': 'Total!!Female!!105 to 109 years',
    'sex': 2, 'minageyrs': 105, 'maxageyrs': 109,
}
dictionary_single_age_sex_PCT12_2020['_209N'] = {
    'label': 'Total!!Female!!110 years and over',
    'sex': 2, 'minageyrs': 110, 'maxageyrs': 110,
}

sexbyage_PCT12_2020_varstem_roots = {
    'metadata': {
        'concept'           : 'SEX BY SINGLE-YEAR AGE',
        'graft_chars'       : ['sex', 'minageyrs', 'maxageyrs'],
        'new_char'          : ['randagePCT12'],
        'char_vars'         : ['sex', 'minageyrs', 'maxageyrs'],
        'group'             : 'PCT12',
        'vintage'           : '2020',
        'dataset_name'      : 'dec/dhc',
        'for_geography'     : 'tract:*',
        'unit_of_analysis'  : 'person',
        'mutually_exclusive': True,
        'indexvar'          : ['GEO_ID', 'state', 'county', 'tract'],
        'countvar'          : 'preccount',
        'notes'             : 'https://api.census.gov/data/2020/dec/dhc/groups/PCT12.html',
    },
    'PCT12': dictionary_single_age_sex_PCT12_2020,
}
