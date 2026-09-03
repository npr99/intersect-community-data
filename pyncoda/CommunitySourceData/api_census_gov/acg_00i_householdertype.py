# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

"""
Tenure by household type by age of householder - H18 in 2010, H14 in 2020.

This is where the householder's sex comes from. Everything else the housing
unit inventory needs about a householder is available from the tenure by age
table; sex is not, and without it the person record linkage loses one of its
four join keys.

Why these are written out rather than discovered
------------------------------------------------
``obtain_api_metadata`` can describe a Census table automatically, but it keeps
only the variables carrying the largest number of characteristics and treats
the rest as aggregates to be discarded. That rule assumes an evenly nested
table. This one is not evenly nested:

    Owner > Family > Married couple > Householder 15 to 34 years        (5 deep)
    Owner > Family > Other family > Male householder > Householder ...  (6 deep)
    Owner > Nonfamily > Male householder > Living alone > Householder   (6 deep)

A married couple household has no single householder sex, so its leaves sit one
level shallower and are dropped by that rule. For Grays Harbor that discarded
13,159 of 29,869 households - the largest household category in the county -
and left roughly 44% of occupied units without a householder.

Married couples and sex = -999
------------------------------
Married couple households are encoded with ``sex = -999``. This is a deliberate
value, not a missing one. The person record linkage identifies husband-wife
families by testing ``sex == -999`` when it decides which household member is a
spouse and which members to treat as children. Encoding married couples any
other way, including leaving the value absent, breaks that logic silently.

Verification
------------
Both dictionaries were generated from the API's own variable metadata and
checked against the published county totals for Grays Harbor County, WA
(FIPS 53027). The leaf variables partition each table exactly:

    H14 2020: 42 leaves, sum 29,869, published total 29,869
    H18 2010: 42 leaves, sum 28,579, published total 28,579

Because the leaves partition the table, ``mutually_exclusive`` is True: each
household is counted once and only once across the set.
"""


householdertype_H18_2010_varstem_roots = {'metadata' : {
                        'concept' : 'TENURE BY HOUSEHOLD TYPE BY AGE OF HOUSEHOLDER',
                        'graft_chars' : ['ownershp','family','sex'],
                        'new_char': ['sex','family','numprec'],
                        'char_vars' : ['ownershp','family','sex','numprec','minageyrs','maxageyrs'],
                        'group' : 'H18',
                        'vintage' : '2010',
                        'dataset_name' : 'dec/sf1',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : True,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'hucount',
                        'notes' : {'Married couple households are encoded sex = -999, the value \
                                    the linkage tests to identify husband-wife families. \
                                    numprec is a living alone flag, not a household size: \
                                    1 means the householder lives alone, -999 means either \
                                    not living alone or a family household.'}
                        },
            'H018': {
                '005': {'ownershp': 1, 'family': 1, 'sex': -999, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Family households Husband-wife family Householder 15 to 34 years'},
                '006': {'ownershp': 1, 'family': 1, 'sex': -999, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Family households Husband-wife family Householder 35 to 64 years'},
                '007': {'ownershp': 1, 'family': 1, 'sex': -999, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Family households Husband-wife family Householder 65 years and over'},
                '010': {'ownershp': 1, 'family': 1, 'sex': 1, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Family households Other family Male householder, no wife present Householder 15 to 34 years'},
                '011': {'ownershp': 1, 'family': 1, 'sex': 1, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Family households Other family Male householder, no wife present Householder 35 to 64 years'},
                '012': {'ownershp': 1, 'family': 1, 'sex': 1, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Family households Other family Male householder, no wife present Householder 65 years and over'},
                '014': {'ownershp': 1, 'family': 1, 'sex': 2, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Family households Other family Female householder, no husband present Householder 15 to 34 years'},
                '015': {'ownershp': 1, 'family': 1, 'sex': 2, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Family households Other family Female householder, no husband present Householder 35 to 64 years'},
                '016': {'ownershp': 1, 'family': 1, 'sex': 2, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Family households Other family Female householder, no husband present Householder 65 years and over'},
                '020': {'ownershp': 1, 'family': 0, 'sex': 1, 'numprec': 1, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Nonfamily households Male householder Living alone Householder 15 to 34 years'},
                '021': {'ownershp': 1, 'family': 0, 'sex': 1, 'numprec': 1, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Nonfamily households Male householder Living alone Householder 35 to 64 years'},
                '022': {'ownershp': 1, 'family': 0, 'sex': 1, 'numprec': 1, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Nonfamily households Male householder Living alone Householder 65 years and over'},
                '024': {'ownershp': 1, 'family': 0, 'sex': 1, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Nonfamily households Male householder Not living alone Householder 15 to 34 years'},
                '025': {'ownershp': 1, 'family': 0, 'sex': 1, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Nonfamily households Male householder Not living alone Householder 35 to 64 years'},
                '026': {'ownershp': 1, 'family': 0, 'sex': 1, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Nonfamily households Male householder Not living alone Householder 65 years and over'},
                '029': {'ownershp': 1, 'family': 0, 'sex': 2, 'numprec': 1, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Nonfamily households Female householder Living alone Householder 15 to 34 years'},
                '030': {'ownershp': 1, 'family': 0, 'sex': 2, 'numprec': 1, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Nonfamily households Female householder Living alone Householder 35 to 64 years'},
                '031': {'ownershp': 1, 'family': 0, 'sex': 2, 'numprec': 1, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Nonfamily households Female householder Living alone Householder 65 years and over'},
                '033': {'ownershp': 1, 'family': 0, 'sex': 2, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Nonfamily households Female householder Not living alone Householder 15 to 34 years'},
                '034': {'ownershp': 1, 'family': 0, 'sex': 2, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Nonfamily households Female householder Not living alone Householder 35 to 64 years'},
                '035': {'ownershp': 1, 'family': 0, 'sex': 2, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Nonfamily households Female householder Not living alone Householder 65 years and over'},
                '039': {'ownershp': 2, 'family': 1, 'sex': -999, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Family households Husband-wife family Householder 15 to 34 years'},
                '040': {'ownershp': 2, 'family': 1, 'sex': -999, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Family households Husband-wife family Householder 35 to 64 years'},
                '041': {'ownershp': 2, 'family': 1, 'sex': -999, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Family households Husband-wife family Householder 65 years and over'},
                '044': {'ownershp': 2, 'family': 1, 'sex': 1, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Family households Other family Male householder, no wife present Householder 15 to 34 years'},
                '045': {'ownershp': 2, 'family': 1, 'sex': 1, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Family households Other family Male householder, no wife present Householder 35 to 64 years'},
                '046': {'ownershp': 2, 'family': 1, 'sex': 1, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Family households Other family Male householder, no wife present Householder 65 years and over'},
                '048': {'ownershp': 2, 'family': 1, 'sex': 2, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Family households Other family Female householder, no husband present Householder 15 to 34 years'},
                '049': {'ownershp': 2, 'family': 1, 'sex': 2, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Family households Other family Female householder, no husband present Householder 35 to 64 years'},
                '050': {'ownershp': 2, 'family': 1, 'sex': 2, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Family households Other family Female householder, no husband present Householder 65 years and over'},
                '054': {'ownershp': 2, 'family': 0, 'sex': 1, 'numprec': 1, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Nonfamily households Male householder Living alone Householder 15 to 34 years'},
                '055': {'ownershp': 2, 'family': 0, 'sex': 1, 'numprec': 1, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Nonfamily households Male householder Living alone Householder 35 to 64 years'},
                '056': {'ownershp': 2, 'family': 0, 'sex': 1, 'numprec': 1, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Nonfamily households Male householder Living alone Householder 65 years and over'},
                '058': {'ownershp': 2, 'family': 0, 'sex': 1, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Nonfamily households Male householder Not living alone Householder 15 to 34 years'},
                '059': {'ownershp': 2, 'family': 0, 'sex': 1, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Nonfamily households Male householder Not living alone Householder 35 to 64 years'},
                '060': {'ownershp': 2, 'family': 0, 'sex': 1, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Nonfamily households Male householder Not living alone Householder 65 years and over'},
                '063': {'ownershp': 2, 'family': 0, 'sex': 2, 'numprec': 1, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Nonfamily households Female householder Living alone Householder 15 to 34 years'},
                '064': {'ownershp': 2, 'family': 0, 'sex': 2, 'numprec': 1, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Nonfamily households Female householder Living alone Householder 35 to 64 years'},
                '065': {'ownershp': 2, 'family': 0, 'sex': 2, 'numprec': 1, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Nonfamily households Female householder Living alone Householder 65 years and over'},
                '067': {'ownershp': 2, 'family': 0, 'sex': 2, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Nonfamily households Female householder Not living alone Householder 15 to 34 years'},
                '068': {'ownershp': 2, 'family': 0, 'sex': 2, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Nonfamily households Female householder Not living alone Householder 35 to 64 years'},
                '069': {'ownershp': 2, 'family': 0, 'sex': 2, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Nonfamily households Female householder Not living alone Householder 65 years and over'},
                }
            }


householdertype_H14_2020_varstem_roots = {'metadata' : {
                        'concept' : 'TENURE BY HOUSEHOLD TYPE BY AGE OF HOUSEHOLDER',
                        'graft_chars' : ['ownershp','family','sex'],
                        'new_char': ['sex','family','numprec'],
                        'char_vars' : ['ownershp','family','sex','numprec','minageyrs','maxageyrs'],
                        'group' : 'H14',
                        'vintage' : '2020',
                        'dataset_name' : 'dec/dhc',
                        'for_geography' : 'block:*',
                        'unit_of_analysis' : 'household',
                        'mutually_exclusive' : True,
                        'indexvar' : ['GEO_ID','state','county','tract','block'],
                        'countvar' : 'hucount',
                        'notes' : {'Married couple households are encoded sex = -999, the value \
                                    the linkage tests to identify husband-wife families. \
                                    numprec is a living alone flag, not a household size: \
                                    1 means the householder lives alone, -999 means either \
                                    not living alone or a family household.'}
                        },
            'H14': {
                '_005N': {'ownershp': 1, 'family': 1, 'sex': -999, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Family households Married couple Householder 15 to 34 years'},
                '_006N': {'ownershp': 1, 'family': 1, 'sex': -999, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Family households Married couple Householder 35 to 64 years'},
                '_007N': {'ownershp': 1, 'family': 1, 'sex': -999, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Family households Married couple Householder 65 years and over'},
                '_010N': {'ownershp': 1, 'family': 1, 'sex': 1, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Family households Other family Male householder, no spouse present Householder 15 to 34 years'},
                '_011N': {'ownershp': 1, 'family': 1, 'sex': 1, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Family households Other family Male householder, no spouse present Householder 35 to 64 years'},
                '_012N': {'ownershp': 1, 'family': 1, 'sex': 1, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Family households Other family Male householder, no spouse present Householder 65 years and over'},
                '_014N': {'ownershp': 1, 'family': 1, 'sex': 2, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Family households Other family Female householder, no spouse present Householder 15 to 34 years'},
                '_015N': {'ownershp': 1, 'family': 1, 'sex': 2, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Family households Other family Female householder, no spouse present Householder 35 to 64 years'},
                '_016N': {'ownershp': 1, 'family': 1, 'sex': 2, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Family households Other family Female householder, no spouse present Householder 65 years and over'},
                '_020N': {'ownershp': 1, 'family': 0, 'sex': 1, 'numprec': 1, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Nonfamily households Male householder Living alone Householder 15 to 34 years'},
                '_021N': {'ownershp': 1, 'family': 0, 'sex': 1, 'numprec': 1, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Nonfamily households Male householder Living alone Householder 35 to 64 years'},
                '_022N': {'ownershp': 1, 'family': 0, 'sex': 1, 'numprec': 1, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Nonfamily households Male householder Living alone Householder 65 years and over'},
                '_024N': {'ownershp': 1, 'family': 0, 'sex': 1, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Nonfamily households Male householder Not living alone Householder 15 to 34 years'},
                '_025N': {'ownershp': 1, 'family': 0, 'sex': 1, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Nonfamily households Male householder Not living alone Householder 35 to 64 years'},
                '_026N': {'ownershp': 1, 'family': 0, 'sex': 1, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Nonfamily households Male householder Not living alone Householder 65 years and over'},
                '_029N': {'ownershp': 1, 'family': 0, 'sex': 2, 'numprec': 1, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Nonfamily households Female householder Living alone Householder 15 to 34 years'},
                '_030N': {'ownershp': 1, 'family': 0, 'sex': 2, 'numprec': 1, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Nonfamily households Female householder Living alone Householder 35 to 64 years'},
                '_031N': {'ownershp': 1, 'family': 0, 'sex': 2, 'numprec': 1, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Nonfamily households Female householder Living alone Householder 65 years and over'},
                '_033N': {'ownershp': 1, 'family': 0, 'sex': 2, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Owner occupied Nonfamily households Female householder Not living alone Householder 15 to 34 years'},
                '_034N': {'ownershp': 1, 'family': 0, 'sex': 2, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Owner occupied Nonfamily households Female householder Not living alone Householder 35 to 64 years'},
                '_035N': {'ownershp': 1, 'family': 0, 'sex': 2, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Owner occupied Nonfamily households Female householder Not living alone Householder 65 years and over'},
                '_039N': {'ownershp': 2, 'family': 1, 'sex': -999, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Family households Married couple Householder 15 to 34 years'},
                '_040N': {'ownershp': 2, 'family': 1, 'sex': -999, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Family households Married couple Householder 35 to 64 years'},
                '_041N': {'ownershp': 2, 'family': 1, 'sex': -999, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Family households Married couple Householder 65 years and over'},
                '_044N': {'ownershp': 2, 'family': 1, 'sex': 1, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Family households Other family Male householder, no spouse present Householder 15 to 34 years'},
                '_045N': {'ownershp': 2, 'family': 1, 'sex': 1, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Family households Other family Male householder, no spouse present Householder 35 to 64 years'},
                '_046N': {'ownershp': 2, 'family': 1, 'sex': 1, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Family households Other family Male householder, no spouse present Householder 65 years and over'},
                '_048N': {'ownershp': 2, 'family': 1, 'sex': 2, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Family households Other family Female householder, no spouse present Householder 15 to 34 years'},
                '_049N': {'ownershp': 2, 'family': 1, 'sex': 2, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Family households Other family Female householder, no spouse present Householder 35 to 64 years'},
                '_050N': {'ownershp': 2, 'family': 1, 'sex': 2, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Family households Other family Female householder, no spouse present Householder 65 years and over'},
                '_054N': {'ownershp': 2, 'family': 0, 'sex': 1, 'numprec': 1, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Nonfamily households Male householder Living alone Householder 15 to 34 years'},
                '_055N': {'ownershp': 2, 'family': 0, 'sex': 1, 'numprec': 1, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Nonfamily households Male householder Living alone Householder 35 to 64 years'},
                '_056N': {'ownershp': 2, 'family': 0, 'sex': 1, 'numprec': 1, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Nonfamily households Male householder Living alone Householder 65 years and over'},
                '_058N': {'ownershp': 2, 'family': 0, 'sex': 1, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Nonfamily households Male householder Not living alone Householder 15 to 34 years'},
                '_059N': {'ownershp': 2, 'family': 0, 'sex': 1, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Nonfamily households Male householder Not living alone Householder 35 to 64 years'},
                '_060N': {'ownershp': 2, 'family': 0, 'sex': 1, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Nonfamily households Male householder Not living alone Householder 65 years and over'},
                '_063N': {'ownershp': 2, 'family': 0, 'sex': 2, 'numprec': 1, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Nonfamily households Female householder Living alone Householder 15 to 34 years'},
                '_064N': {'ownershp': 2, 'family': 0, 'sex': 2, 'numprec': 1, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Nonfamily households Female householder Living alone Householder 35 to 64 years'},
                '_065N': {'ownershp': 2, 'family': 0, 'sex': 2, 'numprec': 1, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Nonfamily households Female householder Living alone Householder 65 years and over'},
                '_067N': {'ownershp': 2, 'family': 0, 'sex': 2, 'numprec': -999, 'minageyrs': 15, 'maxageyrs': 34,
                          'label': 'Renter occupied Nonfamily households Female householder Not living alone Householder 15 to 34 years'},
                '_068N': {'ownershp': 2, 'family': 0, 'sex': 2, 'numprec': -999, 'minageyrs': 35, 'maxageyrs': 64,
                          'label': 'Renter occupied Nonfamily households Female householder Not living alone Householder 35 to 64 years'},
                '_069N': {'ownershp': 2, 'family': 0, 'sex': 2, 'numprec': -999, 'minageyrs': 65, 'maxageyrs': 110,
                          'label': 'Renter occupied Nonfamily households Female householder Not living alone Householder 65 years and over'},
                }
            }
