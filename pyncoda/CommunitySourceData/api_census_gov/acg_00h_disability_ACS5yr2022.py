"""
Data Structure for Person Record Inventory
Disability Characteristics from ACS 5-year Data
Census Tract Level Data for ACS 2022 (centered on 2020)

Base API URL parameters, found at https://api.census.gov/data.html
List of all variables available in 5-year ACS
https://api.census.gov/data/2022/acs/acs5/variables.html

B18101 - SEX BY AGE BY DISABILITY STATUS
https://api.census.gov/data/2022/acs/acs5/groups/B18101.html
"""

import numpy as np

"""
Sex groups - consistent labeling for sex variable
"""
sex_groups_varstems = {
    1: {'sex': 1, 'Label': 'Male'},
    2: {'sex': 2, 'Label': 'Female'}
}

"""
Age groups for B18101 tables
These age groups are specific to the disability tables and differ
from the standard P12 age groups used in PREC core workflow.
"""
disability_age_groups_dict = {
    1: {'minageyrs':  0, 'maxageyrs':   4, 'label': 'Under 5 years'},
    2: {'minageyrs':  5, 'maxageyrs':  17, 'label': '5 to 17 years'},
    3: {'minageyrs': 18, 'maxageyrs':  34, 'label': '18 to 34 years'},
    4: {'minageyrs': 35, 'maxageyrs':  64, 'label': '35 to 64 years'},
    5: {'minageyrs': 65, 'maxageyrs':  74, 'label': '65 to 74 years'},
    6: {'minageyrs': 75, 'maxageyrs': 110, 'label': '75 years and over'}
}

"""
Main disability table B18101 - SEX BY AGE BY DISABILITY STATUS

Variable structure:
- Male variables: B18101_003E through B18101_020E
- Female variables: B18101_022E through B18101_039E

Each age group has two rows:
  - With a disability (disability = 1)
  - No disability    (disability = 0)

Variable index reference:
Male:
  003E = Male total
  004E = Male, Under 5, With disability
  005E = Male, Under 5, No disability
  006E = Male, 5-17 total
  007E = Male, 5-17, With disability
  008E = Male, 5-17, No disability
  009E = Male, 18-34 total
  010E = Male, 18-34, With disability
  011E = Male, 18-34, No disability
  012E = Male, 35-64 total
  013E = Male, 35-64, With disability
  014E = Male, 35-64, No disability
  015E = Male, 65-74 total
  016E = Male, 65-74, With disability
  017E = Male, 65-74, No disability
  018E = Male, 75+, total
  019E = Male, 75+, With disability
  020E = Male, 75+, No disability

Female:
  022E = Female total
  023E = Female, Under 5, With disability
  024E = Female, Under 5, No disability
  025E = Female, 5-17 total
  026E = Female, 5-17, With disability
  027E = Female, 5-17, No disability
  028E = Female, 18-34 total
  029E = Female, 18-34, With disability
  030E = Female, 18-34, No disability
  031E = Female, 35-64 total
  032E = Female, 35-64, With disability
  033E = Female, 35-64, No disability
  034E = Female, 65-74 total
  035E = Female, 65-74, With disability
  036E = Female, 65-74, No disability
  037E = Female, 75+, total
  038E = Female, 75+, With disability
  039E = Female, 75+, No disability

Note: Total rows (003E, 006E, etc.) are skipped - only with/without disability rows included.
"""

disability_B18101_varstem_roots_2022 = {
    'metadata': {
        'concept'          : 'SEX BY AGE BY DISABILITY STATUS',
        'char_vars'        : ['sex', 'agegroupB18101', 'disability'],
        'new_char'         : 'disability',
        'group'            : 'B18101',
        'vintage'          : '2022',
        'dataset_name'     : 'acs/acs5',
        'for_geography'    : 'tract:*',
        'unit_of_analysis' : 'person',
        'mutually_exclusive' : True,
        'indexvar'         : ['GEO_ID', 'state', 'county', 'tract'],
        'countvar'         : 'preccount',
        'notes'            : 'ACS Table B18101 - Disability status by sex and age. '
                             'Under 5 years only has with/without disability. '
                             'Total rows excluded - only mutually exclusive disability rows included.'
    },
    'B18101_': {
        # ── Male ──────────────────────────────────────────────────────────
        # Under 5 years
        '004E': {'sex': 1, 'agegroupB18101': 1, 'disability': 1,
                 'label': 'Male: Under 5 years: With a disability'},
        '005E': {'sex': 1, 'agegroupB18101': 1, 'disability': 0,
                 'label': 'Male: Under 5 years: No disability'},
        # 5 to 17 years
        '007E': {'sex': 1, 'agegroupB18101': 2, 'disability': 1,
                 'label': 'Male: 5 to 17 years: With a disability'},
        '008E': {'sex': 1, 'agegroupB18101': 2, 'disability': 0,
                 'label': 'Male: 5 to 17 years: No disability'},
        # 18 to 34 years
        '010E': {'sex': 1, 'agegroupB18101': 3, 'disability': 1,
                 'label': 'Male: 18 to 34 years: With a disability'},
        '011E': {'sex': 1, 'agegroupB18101': 3, 'disability': 0,
                 'label': 'Male: 18 to 34 years: No disability'},
        # 35 to 64 years
        '013E': {'sex': 1, 'agegroupB18101': 4, 'disability': 1,
                 'label': 'Male: 35 to 64 years: With a disability'},
        '014E': {'sex': 1, 'agegroupB18101': 4, 'disability': 0,
                 'label': 'Male: 35 to 64 years: No disability'},
        # 65 to 74 years
        '016E': {'sex': 1, 'agegroupB18101': 5, 'disability': 1,
                 'label': 'Male: 65 to 74 years: With a disability'},
        '017E': {'sex': 1, 'agegroupB18101': 5, 'disability': 0,
                 'label': 'Male: 65 to 74 years: No disability'},
        # 75 years and over
        '019E': {'sex': 1, 'agegroupB18101': 6, 'disability': 1,
                 'label': 'Male: 75 years and over: With a disability'},
        '020E': {'sex': 1, 'agegroupB18101': 6, 'disability': 0,
                 'label': 'Male: 75 years and over: No disability'},

        # ── Female ────────────────────────────────────────────────────────
        # Under 5 years
        '023E': {'sex': 2, 'agegroupB18101': 1, 'disability': 1,
                 'label': 'Female: Under 5 years: With a disability'},
        '024E': {'sex': 2, 'agegroupB18101': 1, 'disability': 0,
                 'label': 'Female: Under 5 years: No disability'},
        # 5 to 17 years
        '026E': {'sex': 2, 'agegroupB18101': 2, 'disability': 1,
                 'label': 'Female: 5 to 17 years: With a disability'},
        '027E': {'sex': 2, 'agegroupB18101': 2, 'disability': 0,
                 'label': 'Female: 5 to 17 years: No disability'},
        # 18 to 34 years
        '029E': {'sex': 2, 'agegroupB18101': 3, 'disability': 1,
                 'label': 'Female: 18 to 34 years: With a disability'},
        '030E': {'sex': 2, 'agegroupB18101': 3, 'disability': 0,
                 'label': 'Female: 18 to 34 years: No disability'},
        # 35 to 64 years
        '032E': {'sex': 2, 'agegroupB18101': 4, 'disability': 1,
                 'label': 'Female: 35 to 64 years: With a disability'},
        '033E': {'sex': 2, 'agegroupB18101': 4, 'disability': 0,
                 'label': 'Female: 35 to 64 years: No disability'},
        # 65 to 74 years
        '035E': {'sex': 2, 'agegroupB18101': 5, 'disability': 1,
                 'label': 'Female: 65 to 74 years: With a disability'},
        '036E': {'sex': 2, 'agegroupB18101': 5, 'disability': 0,
                 'label': 'Female: 65 to 74 years: No disability'},
        # 75 years and over
        '038E': {'sex': 2, 'agegroupB18101': 6, 'disability': 1,
                 'label': 'Female: 75 years and over: With a disability'},
        '039E': {'sex': 2, 'agegroupB18101': 6, 'disability': 0,
                 'label': 'Female: 75 years and over: No disability'},
    }
}

"""
B18102 - SEX BY AGE BY HEARING DIFFICULTY
https://api.census.gov/data/2022/acs/acs5/groups/B18102.html
Same age/sex structure as B18101.
"""
disability_B18102_varstem_roots_2022 = {
    'metadata': {
        'concept'          : 'SEX BY AGE BY HEARING DIFFICULTY',
        'char_vars'        : ['sex', 'agegroupB18101', 'hearing_difficulty'],
        'new_char'         : 'hearing_difficulty',
        'group'            : 'B18102',
        'vintage'          : '2022',
        'dataset_name'     : 'acs/acs5',
        'for_geography'    : 'tract:*',
        'unit_of_analysis' : 'person',
        'mutually_exclusive' : True,
        'indexvar'         : ['GEO_ID', 'state', 'county', 'tract'],
        'countvar'         : 'preccount',
        'notes'            : 'ACS Table B18102 - Hearing difficulty by sex and age.'
    },
    'B18102_': {
        # Male
        '004E': {'sex': 1, 'agegroupB18101': 1, 'hearing_difficulty': 1,
                 'label': 'Male: Under 5 years: With hearing difficulty'},
        '005E': {'sex': 1, 'agegroupB18101': 1, 'hearing_difficulty': 0,
                 'label': 'Male: Under 5 years: No hearing difficulty'},
        '007E': {'sex': 1, 'agegroupB18101': 2, 'hearing_difficulty': 1,
                 'label': 'Male: 5 to 17 years: With hearing difficulty'},
        '008E': {'sex': 1, 'agegroupB18101': 2, 'hearing_difficulty': 0,
                 'label': 'Male: 5 to 17 years: No hearing difficulty'},
        '010E': {'sex': 1, 'agegroupB18101': 3, 'hearing_difficulty': 1,
                 'label': 'Male: 18 to 34 years: With hearing difficulty'},
        '011E': {'sex': 1, 'agegroupB18101': 3, 'hearing_difficulty': 0,
                 'label': 'Male: 18 to 34 years: No hearing difficulty'},
        '013E': {'sex': 1, 'agegroupB18101': 4, 'hearing_difficulty': 1,
                 'label': 'Male: 35 to 64 years: With hearing difficulty'},
        '014E': {'sex': 1, 'agegroupB18101': 4, 'hearing_difficulty': 0,
                 'label': 'Male: 35 to 64 years: No hearing difficulty'},
        '016E': {'sex': 1, 'agegroupB18101': 5, 'hearing_difficulty': 1,
                 'label': 'Male: 65 to 74 years: With hearing difficulty'},
        '017E': {'sex': 1, 'agegroupB18101': 5, 'hearing_difficulty': 0,
                 'label': 'Male: 65 to 74 years: No hearing difficulty'},
        '019E': {'sex': 1, 'agegroupB18101': 6, 'hearing_difficulty': 1,
                 'label': 'Male: 75 years and over: With hearing difficulty'},
        '020E': {'sex': 1, 'agegroupB18101': 6, 'hearing_difficulty': 0,
                 'label': 'Male: 75 years and over: No hearing difficulty'},
        # Female
        '023E': {'sex': 2, 'agegroupB18101': 1, 'hearing_difficulty': 1,
                 'label': 'Female: Under 5 years: With hearing difficulty'},
        '024E': {'sex': 2, 'agegroupB18101': 1, 'hearing_difficulty': 0,
                 'label': 'Female: Under 5 years: No hearing difficulty'},
        '026E': {'sex': 2, 'agegroupB18101': 2, 'hearing_difficulty': 1,
                 'label': 'Female: 5 to 17 years: With hearing difficulty'},
        '027E': {'sex': 2, 'agegroupB18101': 2, 'hearing_difficulty': 0,
                 'label': 'Female: 5 to 17 years: No hearing difficulty'},
        '029E': {'sex': 2, 'agegroupB18101': 3, 'hearing_difficulty': 1,
                 'label': 'Female: 18 to 34 years: With hearing difficulty'},
        '030E': {'sex': 2, 'agegroupB18101': 3, 'hearing_difficulty': 0,
                 'label': 'Female: 18 to 34 years: No hearing difficulty'},
        '032E': {'sex': 2, 'agegroupB18101': 4, 'hearing_difficulty': 1,
                 'label': 'Female: 35 to 64 years: With hearing difficulty'},
        '033E': {'sex': 2, 'agegroupB18101': 4, 'hearing_difficulty': 0,
                 'label': 'Female: 35 to 64 years: No hearing difficulty'},
        '035E': {'sex': 2, 'agegroupB18101': 5, 'hearing_difficulty': 1,
                 'label': 'Female: 65 to 74 years: With hearing difficulty'},
        '036E': {'sex': 2, 'agegroupB18101': 5, 'hearing_difficulty': 0,
                 'label': 'Female: 65 to 74 years: No hearing difficulty'},
        '038E': {'sex': 2, 'agegroupB18101': 6, 'hearing_difficulty': 1,
                 'label': 'Female: 75 years and over: With hearing difficulty'},
        '039E': {'sex': 2, 'agegroupB18101': 6, 'hearing_difficulty': 0,
                 'label': 'Female: 75 years and over: No hearing difficulty'},
    }
}

"""
B18103 - SEX BY AGE BY VISION DIFFICULTY
https://api.census.gov/data/2022/acs/acs5/groups/B18103.html
Same age/sex structure as B18101.
"""
disability_B18103_varstem_roots_2022 = {
    'metadata': {
        'concept'          : 'SEX BY AGE BY VISION DIFFICULTY',
        'char_vars'        : ['sex', 'agegroupB18101', 'vision_difficulty'],
        'new_char'         : 'vision_difficulty',
        'group'            : 'B18103',
        'vintage'          : '2022',
        'dataset_name'     : 'acs/acs5',
        'for_geography'    : 'tract:*',
        'unit_of_analysis' : 'person',
        'mutually_exclusive' : True,
        'indexvar'         : ['GEO_ID', 'state', 'county', 'tract'],
        'countvar'         : 'preccount',
        'notes'            : 'ACS Table B18103 - Vision difficulty by sex and age.'
    },
    'B18103_': {
        # Male
        '004E': {'sex': 1, 'agegroupB18101': 1, 'vision_difficulty': 1,
                 'label': 'Male: Under 5 years: With vision difficulty'},
        '005E': {'sex': 1, 'agegroupB18101': 1, 'vision_difficulty': 0,
                 'label': 'Male: Under 5 years: No vision difficulty'},
        '007E': {'sex': 1, 'agegroupB18101': 2, 'vision_difficulty': 1,
                 'label': 'Male: 5 to 17 years: With vision difficulty'},
        '008E': {'sex': 1, 'agegroupB18101': 2, 'vision_difficulty': 0,
                 'label': 'Male: 5 to 17 years: No vision difficulty'},
        '010E': {'sex': 1, 'agegroupB18101': 3, 'vision_difficulty': 1,
                 'label': 'Male: 18 to 34 years: With vision difficulty'},
        '011E': {'sex': 1, 'agegroupB18101': 3, 'vision_difficulty': 0,
                 'label': 'Male: 18 to 34 years: No vision difficulty'},
        '013E': {'sex': 1, 'agegroupB18101': 4, 'vision_difficulty': 1,
                 'label': 'Male: 35 to 64 years: With vision difficulty'},
        '014E': {'sex': 1, 'agegroupB18101': 4, 'vision_difficulty': 0,
                 'label': 'Male: 35 to 64 years: No vision difficulty'},
        '016E': {'sex': 1, 'agegroupB18101': 5, 'vision_difficulty': 1,
                 'label': 'Male: 65 to 74 years: With vision difficulty'},
        '017E': {'sex': 1, 'agegroupB18101': 5, 'vision_difficulty': 0,
                 'label': 'Male: 65 to 74 years: No vision difficulty'},
        '019E': {'sex': 1, 'agegroupB18101': 6, 'vision_difficulty': 1,
                 'label': 'Male: 75 years and over: With vision difficulty'},
        '020E': {'sex': 1, 'agegroupB18101': 6, 'vision_difficulty': 0,
                 'label': 'Male: 75 years and over: No vision difficulty'},
        # Female
        '023E': {'sex': 2, 'agegroupB18101': 1, 'vision_difficulty': 1,
                 'label': 'Female: Under 5 years: With vision difficulty'},
        '024E': {'sex': 2, 'agegroupB18101': 1, 'vision_difficulty': 0,
                 'label': 'Female: Under 5 years: No vision difficulty'},
        '026E': {'sex': 2, 'agegroupB18101': 2, 'vision_difficulty': 1,
                 'label': 'Female: 5 to 17 years: With vision difficulty'},
        '027E': {'sex': 2, 'agegroupB18101': 2, 'vision_difficulty': 0,
                 'label': 'Female: 5 to 17 years: No vision difficulty'},
        '029E': {'sex': 2, 'agegroupB18101': 3, 'vision_difficulty': 1,
                 'label': 'Female: 18 to 34 years: With vision difficulty'},
        '030E': {'sex': 2, 'agegroupB18101': 3, 'vision_difficulty': 0,
                 'label': 'Female: 18 to 34 years: No vision difficulty'},
        '032E': {'sex': 2, 'agegroupB18101': 4, 'vision_difficulty': 1,
                 'label': 'Female: 35 to 64 years: With vision difficulty'},
        '033E': {'sex': 2, 'agegroupB18101': 4, 'vision_difficulty': 0,
                 'label': 'Female: 35 to 64 years: No vision difficulty'},
        '035E': {'sex': 2, 'agegroupB18101': 5, 'vision_difficulty': 1,
                 'label': 'Female: 65 to 74 years: With vision difficulty'},
        '036E': {'sex': 2, 'agegroupB18101': 5, 'vision_difficulty': 0,
                 'label': 'Female: 65 to 74 years: No vision difficulty'},
        '038E': {'sex': 2, 'agegroupB18101': 6, 'vision_difficulty': 1,
                 'label': 'Female: 75 years and over: With vision difficulty'},
        '039E': {'sex': 2, 'agegroupB18101': 6, 'vision_difficulty': 0,
                 'label': 'Female: 75 years and over: No vision difficulty'},
    }
}

"""
B18104 - SEX BY AGE BY COGNITIVE DIFFICULTY
https://api.census.gov/data/2022/acs/acs5/groups/B18104.html
Note: Cognitive difficulty only applies to age 5 and over (no Under 5 group).
Age groups shift: 5-17, 18-34, 35-64, 65-74, 75+
"""
disability_B18104_varstem_roots_2022 = {
    'metadata': {
        'concept'          : 'SEX BY AGE BY COGNITIVE DIFFICULTY',
        'char_vars'        : ['sex', 'agegroupB18101', 'cognitive_difficulty'],
        'new_char'         : 'cognitive_difficulty',
        'group'            : 'B18104',
        'vintage'          : '2022',
        'dataset_name'     : 'acs/acs5',
        'for_geography'    : 'tract:*',
        'unit_of_analysis' : 'person',
        'mutually_exclusive' : True,
        'indexvar'         : ['GEO_ID', 'state', 'county', 'tract'],
        'countvar'         : 'preccount',
        'notes'            : 'ACS Table B18104 - Cognitive difficulty by sex and age. '
                             'No Under 5 years group - starts at 5 to 17 years.'
    },
    'B18104_': {
        # Male (starts at 5-17, no Under 5)
        '004E': {'sex': 1, 'agegroupB18101': 2, 'cognitive_difficulty': 1,
                 'label': 'Male: 5 to 17 years: With cognitive difficulty'},
        '005E': {'sex': 1, 'agegroupB18101': 2, 'cognitive_difficulty': 0,
                 'label': 'Male: 5 to 17 years: No cognitive difficulty'},
        '007E': {'sex': 1, 'agegroupB18101': 3, 'cognitive_difficulty': 1,
                 'label': 'Male: 18 to 34 years: With cognitive difficulty'},
        '008E': {'sex': 1, 'agegroupB18101': 3, 'cognitive_difficulty': 0,
                 'label': 'Male: 18 to 34 years: No cognitive difficulty'},
        '010E': {'sex': 1, 'agegroupB18101': 4, 'cognitive_difficulty': 1,
                 'label': 'Male: 35 to 64 years: With cognitive difficulty'},
        '011E': {'sex': 1, 'agegroupB18101': 4, 'cognitive_difficulty': 0,
                 'label': 'Male: 35 to 64 years: No cognitive difficulty'},
        '013E': {'sex': 1, 'agegroupB18101': 5, 'cognitive_difficulty': 1,
                 'label': 'Male: 65 to 74 years: With cognitive difficulty'},
        '014E': {'sex': 1, 'agegroupB18101': 5, 'cognitive_difficulty': 0,
                 'label': 'Male: 65 to 74 years: No cognitive difficulty'},
        '016E': {'sex': 1, 'agegroupB18101': 6, 'cognitive_difficulty': 1,
                 'label': 'Male: 75 years and over: With cognitive difficulty'},
        '017E': {'sex': 1, 'agegroupB18101': 6, 'cognitive_difficulty': 0,
                 'label': 'Male: 75 years and over: No cognitive difficulty'},
        # Female
        '020E': {'sex': 2, 'agegroupB18101': 2, 'cognitive_difficulty': 1,
                 'label': 'Female: 5 to 17 years: With cognitive difficulty'},
        '021E': {'sex': 2, 'agegroupB18101': 2, 'cognitive_difficulty': 0,
                 'label': 'Female: 5 to 17 years: No cognitive difficulty'},
        '023E': {'sex': 2, 'agegroupB18101': 3, 'cognitive_difficulty': 1,
                 'label': 'Female: 18 to 34 years: With cognitive difficulty'},
        '024E': {'sex': 2, 'agegroupB18101': 3, 'cognitive_difficulty': 0,
                 'label': 'Female: 18 to 34 years: No cognitive difficulty'},
        '026E': {'sex': 2, 'agegroupB18101': 4, 'cognitive_difficulty': 1,
                 'label': 'Female: 35 to 64 years: With cognitive difficulty'},
        '027E': {'sex': 2, 'agegroupB18101': 4, 'cognitive_difficulty': 0,
                 'label': 'Female: 35 to 64 years: No cognitive difficulty'},
        '029E': {'sex': 2, 'agegroupB18101': 5, 'cognitive_difficulty': 1,
                 'label': 'Female: 65 to 74 years: With cognitive difficulty'},
        '030E': {'sex': 2, 'agegroupB18101': 5, 'cognitive_difficulty': 0,
                 'label': 'Female: 65 to 74 years: No cognitive difficulty'},
        '032E': {'sex': 2, 'agegroupB18101': 6, 'cognitive_difficulty': 1,
                 'label': 'Female: 75 years and over: With cognitive difficulty'},
        '033E': {'sex': 2, 'agegroupB18101': 6, 'cognitive_difficulty': 0,
                 'label': 'Female: 75 years and over: No cognitive difficulty'},
    }
}

"""
B18105 - SEX BY AGE BY AMBULATORY DIFFICULTY
https://api.census.gov/data/2022/acs/acs5/groups/B18105.html
Note: Ambulatory difficulty only applies to age 5 and over. Same structure as B18104.
"""
disability_B18105_varstem_roots_2022 = {
    'metadata': {
        'concept'          : 'SEX BY AGE BY AMBULATORY DIFFICULTY',
        'char_vars'        : ['sex', 'agegroupB18101', 'ambulatory_difficulty'],
        'new_char'         : 'ambulatory_difficulty',
        'group'            : 'B18105',
        'vintage'          : '2022',
        'dataset_name'     : 'acs/acs5',
        'for_geography'    : 'tract:*',
        'unit_of_analysis' : 'person',
        'mutually_exclusive' : True,
        'indexvar'         : ['GEO_ID', 'state', 'county', 'tract'],
        'countvar'         : 'preccount',
        'notes'            : 'ACS Table B18105 - Ambulatory difficulty by sex and age. '
                             'No Under 5 years group - starts at 5 to 17 years.'
    },
    'B18105_': {
        # Male
        '004E': {'sex': 1, 'agegroupB18101': 2, 'ambulatory_difficulty': 1,
                 'label': 'Male: 5 to 17 years: With ambulatory difficulty'},
        '005E': {'sex': 1, 'agegroupB18101': 2, 'ambulatory_difficulty': 0,
                 'label': 'Male: 5 to 17 years: No ambulatory difficulty'},
        '007E': {'sex': 1, 'agegroupB18101': 3, 'ambulatory_difficulty': 1,
                 'label': 'Male: 18 to 34 years: With ambulatory difficulty'},
        '008E': {'sex': 1, 'agegroupB18101': 3, 'ambulatory_difficulty': 0,
                 'label': 'Male: 18 to 34 years: No ambulatory difficulty'},
        '010E': {'sex': 1, 'agegroupB18101': 4, 'ambulatory_difficulty': 1,
                 'label': 'Male: 35 to 64 years: With ambulatory difficulty'},
        '011E': {'sex': 1, 'agegroupB18101': 4, 'ambulatory_difficulty': 0,
                 'label': 'Male: 35 to 64 years: No ambulatory difficulty'},
        '013E': {'sex': 1, 'agegroupB18101': 5, 'ambulatory_difficulty': 1,
                 'label': 'Male: 65 to 74 years: With ambulatory difficulty'},
        '014E': {'sex': 1, 'agegroupB18101': 5, 'ambulatory_difficulty': 0,
                 'label': 'Male: 65 to 74 years: No ambulatory difficulty'},
        '016E': {'sex': 1, 'agegroupB18101': 6, 'ambulatory_difficulty': 1,
                 'label': 'Male: 75 years and over: With ambulatory difficulty'},
        '017E': {'sex': 1, 'agegroupB18101': 6, 'ambulatory_difficulty': 0,
                 'label': 'Male: 75 years and over: No ambulatory difficulty'},
        # Female
        '020E': {'sex': 2, 'agegroupB18101': 2, 'ambulatory_difficulty': 1,
                 'label': 'Female: 5 to 17 years: With ambulatory difficulty'},
        '021E': {'sex': 2, 'agegroupB18101': 2, 'ambulatory_difficulty': 0,
                 'label': 'Female: 5 to 17 years: No ambulatory difficulty'},
        '023E': {'sex': 2, 'agegroupB18101': 3, 'ambulatory_difficulty': 1,
                 'label': 'Female: 18 to 34 years: With ambulatory difficulty'},
        '024E': {'sex': 2, 'agegroupB18101': 3, 'ambulatory_difficulty': 0,
                 'label': 'Female: 18 to 34 years: No ambulatory difficulty'},
        '026E': {'sex': 2, 'agegroupB18101': 4, 'ambulatory_difficulty': 1,
                 'label': 'Female: 35 to 64 years: With ambulatory difficulty'},
        '027E': {'sex': 2, 'agegroupB18101': 4, 'ambulatory_difficulty': 0,
                 'label': 'Female: 35 to 64 years: No ambulatory difficulty'},
        '029E': {'sex': 2, 'agegroupB18101': 5, 'ambulatory_difficulty': 1,
                 'label': 'Female: 65 to 74 years: With ambulatory difficulty'},
        '030E': {'sex': 2, 'agegroupB18101': 5, 'ambulatory_difficulty': 0,
                 'label': 'Female: 65 to 74 years: No ambulatory difficulty'},
        '032E': {'sex': 2, 'agegroupB18101': 6, 'ambulatory_difficulty': 1,
                 'label': 'Female: 75 years and over: With ambulatory difficulty'},
        '033E': {'sex': 2, 'agegroupB18101': 6, 'ambulatory_difficulty': 0,
                 'label': 'Female: 75 years and over: No ambulatory difficulty'},
    }
}

"""
B18106 - SEX BY AGE BY SELF-CARE DIFFICULTY
https://api.census.gov/data/2022/acs/acs5/groups/B18106.html
Note: Self-care difficulty only applies to age 5 and over. Same structure as B18104.
"""
disability_B18106_varstem_roots_2022 = {
    'metadata': {
        'concept'          : 'SEX BY AGE BY SELF-CARE DIFFICULTY',
        'char_vars'        : ['sex', 'agegroupB18101', 'selfcare_difficulty'],
        'new_char'         : 'selfcare_difficulty',
        'group'            : 'B18106',
        'vintage'          : '2022',
        'dataset_name'     : 'acs/acs5',
        'for_geography'    : 'tract:*',
        'unit_of_analysis' : 'person',
        'mutually_exclusive' : True,
        'indexvar'         : ['GEO_ID', 'state', 'county', 'tract'],
        'countvar'         : 'preccount',
        'notes'            : 'ACS Table B18106 - Self-care difficulty by sex and age. '
                             'No Under 5 years group - starts at 5 to 17 years.'
    },
    'B18106_': {
        # Male
        '004E': {'sex': 1, 'agegroupB18101': 2, 'selfcare_difficulty': 1,
                 'label': 'Male: 5 to 17 years: With self-care difficulty'},
        '005E': {'sex': 1, 'agegroupB18101': 2, 'selfcare_difficulty': 0,
                 'label': 'Male: 5 to 17 years: No self-care difficulty'},
        '007E': {'sex': 1, 'agegroupB18101': 3, 'selfcare_difficulty': 1,
                 'label': 'Male: 18 to 34 years: With self-care difficulty'},
        '008E': {'sex': 1, 'agegroupB18101': 3, 'selfcare_difficulty': 0,
                 'label': 'Male: 18 to 34 years: No self-care difficulty'},
        '010E': {'sex': 1, 'agegroupB18101': 4, 'selfcare_difficulty': 1,
                 'label': 'Male: 35 to 64 years: With self-care difficulty'},
        '011E': {'sex': 1, 'agegroupB18101': 4, 'selfcare_difficulty': 0,
                 'label': 'Male: 35 to 64 years: No self-care difficulty'},
        '013E': {'sex': 1, 'agegroupB18101': 5, 'selfcare_difficulty': 1,
                 'label': 'Male: 65 to 74 years: With self-care difficulty'},
        '014E': {'sex': 1, 'agegroupB18101': 5, 'selfcare_difficulty': 0,
                 'label': 'Male: 65 to 74 years: No self-care difficulty'},
        '016E': {'sex': 1, 'agegroupB18101': 6, 'selfcare_difficulty': 1,
                 'label': 'Male: 75 years and over: With self-care difficulty'},
        '017E': {'sex': 1, 'agegroupB18101': 6, 'selfcare_difficulty': 0,
                 'label': 'Male: 75 years and over: No self-care difficulty'},
        # Female
        '020E': {'sex': 2, 'agegroupB18101': 2, 'selfcare_difficulty': 1,
                 'label': 'Female: 5 to 17 years: With self-care difficulty'},
        '021E': {'sex': 2, 'agegroupB18101': 2, 'selfcare_difficulty': 0,
                 'label': 'Female: 5 to 17 years: No self-care difficulty'},
        '023E': {'sex': 2, 'agegroupB18101': 3, 'selfcare_difficulty': 1,
                 'label': 'Female: 18 to 34 years: With self-care difficulty'},
        '024E': {'sex': 2, 'agegroupB18101': 3, 'selfcare_difficulty': 0,
                 'label': 'Female: 18 to 34 years: No self-care difficulty'},
        '026E': {'sex': 2, 'agegroupB18101': 4, 'selfcare_difficulty': 1,
                 'label': 'Female: 35 to 64 years: With self-care difficulty'},
        '027E': {'sex': 2, 'agegroupB18101': 4, 'selfcare_difficulty': 0,
                 'label': 'Female: 35 to 64 years: No self-care difficulty'},
        '029E': {'sex': 2, 'agegroupB18101': 5, 'selfcare_difficulty': 1,
                 'label': 'Female: 65 to 74 years: With self-care difficulty'},
        '030E': {'sex': 2, 'agegroupB18101': 5, 'selfcare_difficulty': 0,
                 'label': 'Female: 65 to 74 years: No self-care difficulty'},
        '032E': {'sex': 2, 'agegroupB18101': 6, 'selfcare_difficulty': 1,
                 'label': 'Female: 75 years and over: With self-care difficulty'},
        '033E': {'sex': 2, 'agegroupB18101': 6, 'selfcare_difficulty': 0,
                 'label': 'Female: 75 years and over: No self-care difficulty'},
    }
}

"""
B18107 - SEX BY AGE BY INDEPENDENT LIVING DIFFICULTY
https://api.census.gov/data/2022/acs/acs5/groups/B18107.html
Note: Independent living difficulty only applies to age 18 and over.
Age groups: 18-34, 35-64, 65-74, 75+
"""
disability_B18107_varstem_roots_2022 = {
    'metadata': {
        'concept'          : 'SEX BY AGE BY INDEPENDENT LIVING DIFFICULTY',
        'char_vars'        : ['sex', 'agegroupB18101', 'indliving_difficulty'],
        'new_char'         : 'indliving_difficulty',
        'group'            : 'B18107',
        'vintage'          : '2022',
        'dataset_name'     : 'acs/acs5',
        'for_geography'    : 'tract:*',
        'unit_of_analysis' : 'person',
        'mutually_exclusive' : True,
        'indexvar'         : ['GEO_ID', 'state', 'county', 'tract'],
        'countvar'         : 'preccount',
        'notes'            : 'ACS Table B18107 - Independent living difficulty by sex and age. '
                             'Only applies to age 18 and over (agegroupB18101 3-6).'
    },
    'B18107_': {
        # Male (starts at 18-34, no Under 5 or 5-17)
        '004E': {'sex': 1, 'agegroupB18101': 3, 'indliving_difficulty': 1,
                 'label': 'Male: 18 to 34 years: With independent living difficulty'},
        '005E': {'sex': 1, 'agegroupB18101': 3, 'indliving_difficulty': 0,
                 'label': 'Male: 18 to 34 years: No independent living difficulty'},
        '007E': {'sex': 1, 'agegroupB18101': 4, 'indliving_difficulty': 1,
                 'label': 'Male: 35 to 64 years: With independent living difficulty'},
        '008E': {'sex': 1, 'agegroupB18101': 4, 'indliving_difficulty': 0,
                 'label': 'Male: 35 to 64 years: No independent living difficulty'},
        '010E': {'sex': 1, 'agegroupB18101': 5, 'indliving_difficulty': 1,
                 'label': 'Male: 65 to 74 years: With independent living difficulty'},
        '011E': {'sex': 1, 'agegroupB18101': 5, 'indliving_difficulty': 0,
                 'label': 'Male: 65 to 74 years: No independent living difficulty'},
        '013E': {'sex': 1, 'agegroupB18101': 6, 'indliving_difficulty': 1,
                 'label': 'Male: 75 years and over: With independent living difficulty'},
        '014E': {'sex': 1, 'agegroupB18101': 6, 'indliving_difficulty': 0,
                 'label': 'Male: 75 years and over: No independent living difficulty'},
        # Female
        '017E': {'sex': 2, 'agegroupB18101': 3, 'indliving_difficulty': 1,
                 'label': 'Female: 18 to 34 years: With independent living difficulty'},
        '018E': {'sex': 2, 'agegroupB18101': 3, 'indliving_difficulty': 0,
                 'label': 'Female: 18 to 34 years: No independent living difficulty'},
        '020E': {'sex': 2, 'agegroupB18101': 4, 'indliving_difficulty': 1,
                 'label': 'Female: 35 to 64 years: With independent living difficulty'},
        '021E': {'sex': 2, 'agegroupB18101': 4, 'indliving_difficulty': 0,
                 'label': 'Female: 35 to 64 years: No independent living difficulty'},
        '023E': {'sex': 2, 'agegroupB18101': 5, 'indliving_difficulty': 1,
                 'label': 'Female: 65 to 74 years: With independent living difficulty'},
        '024E': {'sex': 2, 'agegroupB18101': 5, 'indliving_difficulty': 0,
                 'label': 'Female: 65 to 74 years: No independent living difficulty'},
        '026E': {'sex': 2, 'agegroupB18101': 6, 'indliving_difficulty': 1,
                 'label': 'Female: 75 years and over: With independent living difficulty'},
        '027E': {'sex': 2, 'agegroupB18101': 6, 'indliving_difficulty': 0,
                 'label': 'Female: 75 years and over: No independent living difficulty'},
    }
}

"""
Convenience dispatch dict for the disability merge loop in acg_05b_prec_functions.py.
This is additional to the per-table dicts above (the hhinc files do not have
an analogous dispatch dict - the disability workflow merges 7 tables in a loop
rather than one at a time, so a dict keyed by table speed-name is useful).

Usage:  disability_varstem_roots_dict_2022[basevintage][f'{table}_']
"""
disability_varstem_roots_dict_2022 = {
    '2022': {
        'B18101_' : disability_B18101_varstem_roots_2022,
        'B18102_' : disability_B18102_varstem_roots_2022,
        'B18103_' : disability_B18103_varstem_roots_2022,
        'B18104_' : disability_B18104_varstem_roots_2022,
        'B18105_' : disability_B18105_varstem_roots_2022,
        'B18106_' : disability_B18106_varstem_roots_2022,
        'B18107_' : disability_B18107_varstem_roots_2022,
    }
}
