# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

"""
Add householder characteristics - age group and sex - to the housing unit
inventory.

Why this exists
---------------
The person-record to housing-unit linkage joins on
``agegroupH17 + sex + race + hispan``. The housing unit inventory produced by
``acg_05a_hui_functions`` carries race and hispan but not the age or sex of the
householder, so two of the four join keys are missing. The 2010 workflow did not
read them from the inventory either - it built them from two Census tables, in
code that lived outside pyncoda. This module is that step, rewritten against
current pyncoda and parameterised by vintage from the start.

Method (unchanged from the 2010 original)
-----------------------------------------
1. Tenure by age of householder gives the householder's age band.
2. Tenure by household type by age of householder gives the householder's sex,
   along with family type and a household size indicator.
3. The two are random-merged on tenure and a coarse age band, so each
   age-of-householder record gains a sex.
4. The result is random-merged onto the housing unit inventory on tenure,
   family type and household size, within race and ethnicity groups, falling
   back through progressively weaker keys and up through Block, Tract and
   County until every housing unit is matched.

Vintage handling
----------------
The 2020 Demographic and Housing Characteristics file renumbered these tables:
H17 became H13 and H18 became H14. The variable naming also differs - 2010
spells a variable ``H017003`` while 2020 spells it ``H13_003N`` - which is
handled by ``obtain_api_metadata`` and its 2020 patch, not here.

The output columns are deliberately named ``agegroupH17`` and ``agegroupH18``
for both vintages. They are labels for age bands rather than table names, the
bands are identical in 2010 and 2020, and keeping the names fixed means code
downstream of this module needs no vintage branching.

KNOWN LIMITATION - married couple households carry no householder sex
---------------------------------------------------------------------
The household type table is currently described by ``obtain_api_metadata``,
which keeps only the variables carrying the largest number of characteristics
and discards the rest as aggregates. That rule assumes an evenly nested table,
and H14/H18 is not evenly nested:

    Owner > Family > Married couple > Householder 15 to 34 years      (5 levels)
    Owner > Family > Other family > Male householder > Householder ..  (6 levels)
    Owner > Nonfamily > Male householder > Living alone > Householder  (6 levels)

A married couple household has no single householder sex, so its leaves sit one
level shallower and are dropped. For Grays Harbor that removes the largest
household category: the table yields 16,710 households against the 29,869 that
tenure by age of householder reports, and the missing 13,159 are exactly the
married couple households.

The consequence is that roughly 44% of occupied units currently fall through to
the weaker fallback rounds without a householder sex.

The fix is a hand written variable dictionary for H14/H18, in the style of
``acg_00b_hui_block2020.tenure_size_H12_2020_varstem_roots``, assigning
``sex = -999`` to married couple households. That value is not a placeholder:
the linkage identifies husband-wife families by testing ``sex == -999`` when it
decides which household members to treat as spouse and which as children, so
the dictionary has to encode it deliberately rather than leave it missing.

Until that dictionary exists this module should be treated as producing a
usable but incomplete householder assignment.
"""

import numpy as np
import pandas as pd

from pyncoda.CommunitySourceData.api_census_gov.acg_01a_BaseInventory \
    import BaseInventory
from pyncoda.CommunitySourceData.api_census_gov.acg_00a_createAPI_datastructure \
    import createAPI_datastructure
from pyncoda.CommunitySourceData.api_census_gov.acg_00a_general_datastructures \
    import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00c_hispan_block2010 \
    import hispan_byrace_H7_varstem_roots, tenure_byhispan_H15_varstem_roots
from pyncoda.CommunitySourceData.api_census_gov.acg_00c_hispan_block2020 \
    import hispan_byrace_H7_2020_varstem_roots, tenure_byhispan_H11_2020_varstem_roots
from pyncoda.CommunitySourceData.api_census_gov.acg_02a_add_categorical_char \
    import add_new_char_by_random_merge_2dfs
from pyncoda.CommunitySourceData.api_census_gov.acg_02c_agefunctions \
    import add_randage, add_H17age_groups, add_H18age_groups


# Census renumbered the householder tables between 2010 and 2020.
# Verified against the API's own groups.json for both vintages, and both
# 2020 tables confirmed available at block level.
householder_tables = {
    '2010': {
        'dataset_name'  : 'dec/sf1',
        'age_by_tenure' : 'H17',   # TENURE BY AGE OF HOUSEHOLDER
        'type_by_age'   : 'H18',   # TENURE BY HOUSEHOLD TYPE BY AGE OF HOUSEHOLDER
        'hispan_dictionaries' : [hispan_byrace_H7_varstem_roots,
                                 tenure_byhispan_H15_varstem_roots],
        },
    '2020': {
        'dataset_name'  : 'dec/dhc',
        'age_by_tenure' : 'H13',
        'type_by_age'   : 'H14',
        'hispan_dictionaries' : [hispan_byrace_H7_2020_varstem_roots,
                                 tenure_byhispan_H11_2020_varstem_roots],
        },
    }


class hui_householder_functions():
    """
    Obtain householder age group and sex, and add them to a housing unit
    inventory.
    """

    def __init__(self,
            state_county: str,
            state_county_name: str,
            seed: int = 9876,
            version: str = '2.1.0',
            version_text: str = 'v2-1-0',
            basevintage: str = '2020',
            basegeolevel: str = 'Block',
            outputfolder: str = "",
            outputfolders = {}):

        self.state_county = state_county
        self.state_county_name = state_county_name
        self.seed = seed
        self.version = version
        self.version_text = version_text
        self.basevintage = str(basevintage)
        self.basegeolevel = basegeolevel
        self.outputfolder = outputfolder
        self.outputfolders = outputfolders

        if self.basevintage not in householder_tables:
            raise ValueError(
                "No householder table mapping for vintage " + self.basevintage +
                ". Known vintages: " + str(list(householder_tables.keys())) + ".")
        self.tables = householder_tables[self.basevintage]

    @staticmethod
    def set_block_geography(datastructure_dict):
        """
        Point a discovered data structure at block level.

        obtain_api_metadata describes a table, not a request, and defaults to
        'tract:*' with a tract index. The householder tables must be pulled at
        block level, because the housing unit inventory is a block level
        product and the random merge starts at Block. Both keys have to change
        together: for_geography drives the API call, indexvar drives the
        reshape and the block id that get_apidata builds afterwards.
        """

        datastructure_dict['metadata']['for_geography'] = 'block:*'
        datastructure_dict['metadata']['indexvar'] = \
            ['GEO_ID','state','county','tract','block']

        return datastructure_dict

    def tidy_householder_agetenure(self):
        """
        Obtain tenure by age of householder (H17 in 2010, H13 in 2020) and add
        an age group for the householder.

        Returns one row per occupied housing unit, keyed on uniqueidH17.
        """

        group = self.tables['age_by_tenure']
        dataset_name = self.tables['dataset_name']

        print("\n***************************************")
        print("    Set up data structures for", group, "-", dataset_name)
        print("***************************************\n")
        agetenure_dict = createAPI_datastructure.obtain_api_metadata(
                vintage = self.basevintage,
                dataset_name = dataset_name,
                group = group,
                outputfolder = self.outputfolder,
                version_text = self.version_text)

        agetenure_dict = self.set_block_geography(agetenure_dict)

        # Graft chars are used to check the merge by variables in the grafting
        # function. Race and hispan come from the table's own race iterations.
        agetenure_dict['metadata']['graft_chars'] = \
            ['ownershp','minageyrs','maxageyrs','race','hispan']

        # The A-I race iterations of the table carry the same variable lines,
        # so one structure is reused for both the race and the ethnicity graft.
        # The A-I letter to race code mapping is unchanged between 2010 and
        # 2020, so the dec10 dictionaries apply to both vintages.
        agetenure_IAG = createAPI_datastructure.add_byracehispan(agetenure_dict,
                dec10byracehispan_All,
                dec10byracehispan_IAG_mx,
                newgroup = "IAG",
                newcharbyvar = '')

        newcharbyvar = 'hispanby' + group + 'HAI'
        agetenure_HAI = createAPI_datastructure.add_byracehispan(agetenure_dict,
                byracehispan_groups = dec10hispannotwhite_HAI,
                byracehispan_groups_mx = dec10hispannotwhite_HAI_mx,
                newgroup = "HAI",
                newcharbyvar = newcharbyvar)

        agetenure_HAI['metadata']['char_vars'].append(newcharbyvar)
        agetenure_HAI['metadata']['new_char'] = [newcharbyvar]
        # The ethnicity graft must not also match on race and hispan
        agetenure_HAI['metadata']['graft_chars'] = \
            ['ownershp','minageyrs','maxageyrs']

        print("\n***************************************")
        print("   Obtain and clean", group, "data.")
        print("***************************************\n")
        block_df = {}
        block_df['hhage'] = BaseInventory.get_apidata(
                                        state_county = self.state_county,
                                        geo_level = 'block',
                                        vintage = self.basevintage,
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [agetenure_IAG],
                                        outputfolders = self.outputfolders,
                                        outputfile = "CoreHUI_" + group + "IAG")

        block_df['hhage_hispan'] = BaseInventory.graft_on_new_char(
                                        base_inventory = block_df['hhage'],
                                        state_county = self.state_county,
                                        new_char = 'hispan',
                                        new_char_dictionaries =
                                            [agetenure_HAI] +
                                            self.tables['hispan_dictionaries'],
                                        basevintage = self.basevintage,
                                        basegeolevel = self.basegeolevel,
                                        outputfile = "hui_hhage",
                                        outputfolders = self.outputfolders)

        print("Add random age and householder age groups.")
        block_df['hhage_hispan'] = add_randage(
                                    block_df['hhage_hispan'],
                                    seed = self.seed,
                                    varname = 'randageH17')
        block_df['hhage_hispan'] = add_H17age_groups(
                                    block_df['hhage_hispan'],
                                    varname = 'randageH17')

        # Rename the primary key - leaving it as huid would collide with the
        # housing unit inventory's own huid during the random merge.
        block_df['hhage_hispan'] = block_df['hhage_hispan'].\
            rename(columns={"huid": "uniqueidH17"})

        return block_df['hhage_hispan']

    def tidy_householder_typeage(self):
        """
        Obtain tenure by household type by age of householder (H18 in 2010,
        H14 in 2020). This is where the householder's sex comes from.

        Returns one row per occupied housing unit, keyed on uniqueidH18.
        """

        group = self.tables['type_by_age']
        dataset_name = self.tables['dataset_name']

        print("\n***************************************")
        print("    Set up data structures for", group, "-", dataset_name)
        print("***************************************\n")
        typeage_dict = createAPI_datastructure.obtain_api_metadata(
                vintage = self.basevintage,
                dataset_name = dataset_name,
                group = group,
                outputfolder = self.outputfolder,
                version_text = self.version_text)

        typeage_dict = self.set_block_geography(typeage_dict)

        typeage_dict['metadata']['graft_chars'] = ['ownershp','agegroupH18']

        print("\n***************************************")
        print("   Obtain and clean", group, "data.")
        print("***************************************\n")
        typeage_df = BaseInventory.get_apidata(
                                        state_county = self.state_county,
                                        geo_level = 'block',
                                        vintage = self.basevintage,
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [typeage_dict],
                                        outputfolders = self.outputfolders,
                                        outputfile = "hui_" + group)

        print("Add random age and household type age groups.")
        typeage_df = add_randage(typeage_df,
                                 seed = self.seed,
                                 varname = 'randageH18')
        typeage_df = add_H18age_groups(typeage_df,
                                       varname = 'randageH18')

        typeage_df = typeage_df.rename(columns={"huid": "uniqueidH18"})

        return typeage_df

    def randommerge_agetenure_typeage(self, agetenure_df, typeage_df):
        """
        Give each age-of-householder record a sex, by random merging the
        household type table onto it within tenure and coarse age band.
        """

        print("\n***************************************")
        print("    Add householder sex to householder age records.")
        print("***************************************\n")

        # The age-of-householder table has finer age bands than the household
        # type table, so it needs the coarse band to merge on.
        agetenure_df = add_randage(agetenure_df,
                                   seed = self.seed,
                                   varname = 'randageH18')
        agetenure_df = add_H18age_groups(agetenure_df,
                                         varname = 'randageH18')

        add_typeage = add_new_char_by_random_merge_2dfs(
            dfs = {'primary'  : {'data': agetenure_df,
                            'primarykey' : 'uniqueidH17',
                            'geolevel' : self.basegeolevel,
                            'geovintage' : self.basevintage,
                            'notes' : 'Household level data with householder age.'},
                'secondary' : {'data': typeage_df,
                            'primarykey' : 'uniqueidH18',
                            'geolevel' : self.basegeolevel,
                            'geovintage' : self.basevintage,
                            'notes' : 'Sex and family type data.'}},
            seed = self.seed,
            common_group_vars = ['ownershp','agegroupH18'],
            new_char = 'numprec',
            extra_vars = ['sex','family'],
            geolevel = self.basegeolevel,
            geovintage = self.basevintage,
            by_groups = {'none' : {'by_variables' : []}},
            fillna_value = -888,
            state_county = self.state_county,
            outputfile = "hui_householder_agetype",
            outputfolder = self.outputfolders['RandomMerge'])

        rounds = {'options': {
                'option1' : {'notes' : 'By original common group vars and by groups variables.',
                            'common_group_vars' : add_typeage.common_group_vars,
                            'by_groups' : add_typeage.by_groups}
                                },
                'geo_levels' : [self.basegeolevel]
                }

        return add_typeage.run_random_merge_2dfs(rounds)

    def randommerge_hui_householder(self, hui_df, householder_df):
        """
        Add householder age group and sex to the housing unit inventory.

        Matches on tenure, family type and household size, within race and
        ethnicity, then falls back through weaker keys and up through Block,
        Tract and County so that every housing unit is reached.
        """

        print("\n***************************************")
        print("    Merge householder characteristics onto the housing unit inventory.")
        print("***************************************\n")

        add_householder = add_new_char_by_random_merge_2dfs(
            dfs = {'primary'  : {'data': hui_df,
                            'primarykey' : 'huid',
                            'geolevel' : self.basegeolevel,
                            'geovintage' : self.basevintage,
                            'notes' : 'Housing unit inventory without householder age or sex.'},
                'secondary' : {'data': householder_df,
                            'primarykey' : 'uniqueidH17',
                            'geolevel' : self.basegeolevel,
                            'geovintage' : self.basevintage,
                            'notes' : 'Householder age group and sex.'}},
            seed = self.seed,
            common_group_vars = ['ownershp','family','numprec'],
            new_char = 'agegroupH17',
            extra_vars = ['sex','agegroupH18'],
            geolevel = self.basegeolevel,
            geovintage = self.basevintage,
            by_groups = {'All' : {'by_variables' : ['hispan','race']}},
            fillna_value = -999,
            state_county = self.state_county,
            outputfile = "hui_householder",
            outputfolder = self.outputfolders['RandomMerge'])

        # Fallback ladder, weakening the match in a fixed order so the result
        # is reproducible: drop household size, then family type, and try race
        # only and then no by-variables at each level.
        race_only = {'Race only' : {'by_variables' : ['race']}}
        no_vars   = {'No vars'   : {'by_variables' : []}}
        rounds = {'options': {
                'option1' : {'notes' : 'Tenure, family type and size, by race and ethnicity.',
                            'common_group_vars' : ['ownershp','family','numprec'],
                            'by_groups' : add_householder.by_groups},
                'option2' : {'notes' : 'Race only - reaches single person households.',
                            'common_group_vars' : ['ownershp','family','numprec'],
                            'by_groups' : race_only},
                'option3' : {'notes' : 'No by variables, still matching size.',
                            'common_group_vars' : ['ownershp','family','numprec'],
                            'by_groups' : no_vars},
                'option4' : {'notes' : 'Drop household size.',
                            'common_group_vars' : ['ownershp','family'],
                            'by_groups' : add_householder.by_groups},
                'option5' : {'notes' : 'Drop household size, race only.',
                            'common_group_vars' : ['ownershp','family'],
                            'by_groups' : race_only},
                'option6' : {'notes' : 'Drop household size, no by variables.',
                            'common_group_vars' : ['ownershp','family'],
                            'by_groups' : no_vars},
                'option7' : {'notes' : 'Drop family type.',
                            'common_group_vars' : ['ownershp'],
                            'by_groups' : add_householder.by_groups},
                'option8' : {'notes' : 'Drop family type, race only.',
                            'common_group_vars' : ['ownershp'],
                            'by_groups' : race_only},
                'option9' : {'notes' : 'Tenure only, no by variables.',
                            'common_group_vars' : ['ownershp'],
                            'by_groups' : no_vars},
                                },
                'geo_levels' : ['Block','Tract','County']
                }

        return add_householder.run_random_merge_2dfs(rounds)

    def add_householder_characteristics(self, hui_df):
        """
        Run the whole step: obtain both tables, join them, and add householder
        age group and sex to the housing unit inventory.

        Returns the housing unit inventory with agegroupH17, agegroupH18 and
        sex added. Row count and huid set are unchanged.
        """

        agetenure_df = self.tidy_householder_agetenure()
        typeage_df = self.tidy_householder_typeage()
        householder_df = self.randommerge_agetenure_typeage(
                            agetenure_df = agetenure_df,
                            typeage_df = typeage_df)
        hui_householder_df = self.randommerge_hui_householder(
                            hui_df = hui_df,
                            householder_df = householder_df['primary'])

        return hui_householder_df

    @staticmethod
    def validate_householder_characteristics(hui_before, hui_after):
        """
        Invariants that must hold whatever path the random merge took.

        Returns a dict of check name to (passed, detail). Raises nothing, so
        that a caller can report every failure rather than only the first.
        """

        checks = {}

        same_rows = len(hui_before) == len(hui_after)
        checks['row count unchanged'] = (
            same_rows,
            "%d before, %d after" % (len(hui_before), len(hui_after)))

        before_ids = set(hui_before['huid'])
        after_ids = set(hui_after['huid'])
        checks['huid set unchanged'] = (
            before_ids == after_ids,
            "%d missing, %d added" % (len(before_ids - after_ids),
                                      len(after_ids - before_ids)))

        checks['huid still unique'] = (
            hui_after['huid'].is_unique,
            "%d duplicated" % int(hui_after['huid'].duplicated().sum()))

        for column in ['agegroupH17','agegroupH18','sex']:
            present = column in hui_after.columns
            if not present:
                checks["'%s' added" % column] = (False, "column missing")
                continue
            unset = int((hui_after[column] == -999).sum() +
                        hui_after[column].isnull().sum())
            checks["'%s' added" % column] = (
                True, "%d of %d not set" % (unset, len(hui_after)))

        # Occupied units should have a householder; vacant units should not.
        if 'vacancy' in hui_after.columns and 'agegroupH17' in hui_after.columns:
            occupied = hui_after[hui_after['numprec'] > 0]
            unmatched = int((occupied['agegroupH17'].isin([-999, 0]) |
                             occupied['agegroupH17'].isnull()).sum())
            checks['occupied units have a householder age group'] = (
                unmatched == 0,
                "%d of %d occupied units unmatched" % (unmatched, len(occupied)))

        return checks