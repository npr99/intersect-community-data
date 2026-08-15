# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

"""
Link person records to housing units - the prechui workflow.

The housing unit inventory says how many people live in each housing unit and
what the householder looks like. The person record inventory says who lives in
the county, with age, sex, race, ethnicity and disability. Neither says which
person lives in which housing unit. This module joins them, so that a person
record carries a huid and the two inventories describe the same people.

Method, following the 2021 sandbox implementation
-------------------------------------------------
1. Adjust household size. The inventory caps households at 7 people, but real
   households can be larger. Comparing block level person totals against block
   level housing unit totals gives an estimate of how much larger, which is
   assigned to the 7 person households in that block.
2. Expand each housing unit into one row per resident and number them, person 1
   being the householder.
3. Infer household structure. The expansion copies the householder's
   characteristics to everyone, which is wrong for everyone but the
   householder, so spouses and assumed children are identified and their
   inherited age and sex cleared.
4. Place group quarters residents, who have no householder and are matched
   through the group quarters table instead.
5. Random merge person records onto housing unit slots within each block,
   matching on householder age band, sex, race and ethnicity, then falling back
   through weaker keys for spouses, children and remaining members.

Vintage handling
----------------
Geography column names are built from the vintage, so 'Block2010str' or
'Block2020str'. Table differences are handled upstream: householder
characteristics in acg_05c_hui_householder, group quarters in
acg_05b_prec_functions.tidy_group_quarters.
"""

import numpy as np
import pandas as pd

from pyncoda.CommunitySourceData.api_census_gov.acg_01a_BaseInventory \
    import BaseInventory
from pyncoda.CommunitySourceData.api_census_gov.acg_02e_conditionsets \
    import create_conditionset, describe_conditionset


class prechui_workflow_functions():
    """
    Merge the housing unit inventory and the person record inventory.
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

        # 'Block2020str' or 'Block2010str'
        self.geo_id = self.basegeolevel + self.basevintage + 'str'

    @staticmethod
    def not_group_quarters(df):
        """
        Rows that are ordinary housing rather than group quarters.

        gqtype marks group quarters, but ordinary housing carries 0 in a
        polished inventory and null earlier in the workflow. Testing only one
        of them silently selects nothing, which turns any check built on it
        into a vacuous pass, so both are treated as ordinary housing here and
        everywhere else in this module.
        """

        return df['gqtype'].fillna(0) == 0

    def adjust_numprec7_hui(self, hui_df, prec_df, verify_results = False):
        """
        Raise the size of some 7 person households.

        The housing unit inventory caps household size at 7 because the Census
        tenure by household size table does. The person records are not capped,
        so where a block holds more people than its housing units account for,
        the difference is attributable to households larger than 7. That
        surplus is added to the 7 person households in the block.

        Without this step the linkage cannot place everyone: the housing units
        in such a block offer fewer seats than there are people to seat.
        """

        geo_id = self.geo_id

        # Total population implied by the housing unit inventory, by block
        total_pop_by_numprec_df = pd.pivot_table(hui_df,
                        values = 'numprec',
                        index = [geo_id],
                        aggfunc = 'sum')
        total_pop_by_numprec_df.reset_index(inplace = True)
        total_pop_by_numprec_df = total_pop_by_numprec_df.rename(
            columns = {'numprec' : 'total_population_numprec'})

        # How many housing units hold exactly 7 people, group quarters excluded
        conditions = (hui_df['numprec'] == 7) & self.not_group_quarters(hui_df)
        total_count_7numprec_df = pd.pivot_table(hui_df[conditions],
                                values = 'huid',
                                index = [geo_id],
                                aggfunc = 'count')
        total_count_7numprec_df.reset_index(inplace = True)
        total_count_7numprec_df = total_count_7numprec_df.rename(
            columns = {'huid' : 'count_7numprec'})

        fix_7numprec_df = pd.merge(left = total_pop_by_numprec_df,
                        right = total_count_7numprec_df,
                        on = geo_id,
                        how = 'left')
        fix_7numprec_df['count_7numprec'] = \
            fix_7numprec_df['count_7numprec'].fillna(value = 0)

        # Total population according to the person records, by block
        total_pop_by_preci_df = pd.pivot_table(prec_df,
                        values = 'precid',
                        index = [geo_id],
                        aggfunc = 'count')
        total_pop_by_preci_df.reset_index(inplace = True)
        total_pop_by_preci_df = total_pop_by_preci_df.rename(
            columns = {'precid' : 'total_population_prec'})

        fix_7numprec_df = pd.merge(left = fix_7numprec_df,
                        right = total_pop_by_preci_df,
                        on = geo_id,
                        how = 'left')

        fix_7numprec_df.loc[:,'pop_difference'] = \
            fix_7numprec_df['total_population_prec'] - \
            fix_7numprec_df['total_population_numprec']

        # Only a shortfall is meaningful. A block whose person records fall
        # short of its housing units cannot be fixed by making households
        # bigger, and inflating by a negative number would shrink them below 7.
        fix_7numprec_df.loc[fix_7numprec_df['pop_difference'] < 0,
                            'pop_difference'] = 0

        # The block's shortfall is SHARED between its 7 person households, not
        # given to each of them.
        #
        # The 2021 implementation computed this per household share, named it
        # difference_per7numprec, and then assigned 7 + pop_difference anyway,
        # so every 7 person household in a block received the whole block
        # shortfall. In a block with three such households and a shortfall of
        # 42 that adds 126 people instead of 42. Across Grays Harbor 2020 it
        # invented 525 people, and the error grows with the number of large
        # households in a block - exactly the dense blocks where it matters.
        #
        # The shortfall is divided evenly and the remainder handed out one per
        # household, so the block total is matched exactly rather than
        # approximately.
        hui_adjusted_numprec_df = hui_df.copy()

        is_seven = ((hui_adjusted_numprec_df['numprec'] == 7) &
                    self.not_group_quarters(hui_adjusted_numprec_df))
        # Rank the 7 person households within their block so the remainder can
        # be distributed deterministically.
        hui_adjusted_numprec_df.loc[is_seven, 'seven_rank'] = \
            hui_adjusted_numprec_df.loc[is_seven].groupby(geo_id).cumcount()

        hui_adjusted_numprec_df = pd.merge(
                        left = hui_adjusted_numprec_df,
                        right = fix_7numprec_df[[geo_id,'pop_difference','count_7numprec']],
                        on = geo_id,
                        how = 'left')

        adjustable = (hui_adjusted_numprec_df['seven_rank'].notnull() &
                      (hui_adjusted_numprec_df['count_7numprec'] > 0) &
                      (hui_adjusted_numprec_df['pop_difference'] > 0))

        shortfall = hui_adjusted_numprec_df.loc[adjustable,'pop_difference']
        households = hui_adjusted_numprec_df.loc[adjustable,'count_7numprec']
        rank = hui_adjusted_numprec_df.loc[adjustable,'seven_rank']

        even_share = (shortfall // households)
        remainder = (shortfall % households)
        extra = (rank < remainder).astype(int)

        hui_adjusted_numprec_df.loc[adjustable,'numprec'] = 7 + even_share + extra

        hui_adjusted_numprec_df = hui_adjusted_numprec_df.drop(
            ['seven_rank','pop_difference','count_7numprec'], axis = 1)

        if verify_results:
            verify_tables = {}
            verify_tables['Numprec by GQ Type'] = pd.pivot_table(
                hui_adjusted_numprec_df, values = 'huid', index = ['numprec'],
                margins = True, margins_name = 'Total',
                columns = ['gqtype'], aggfunc = 'count')
            verify_tables['Descriptive Stats'] = fix_7numprec_df.describe().T
            verify_tables['Total Population differences'] = pd.pivot_table(
                fix_7numprec_df,
                values = ['total_population_numprec','total_population_prec',
                          'pop_difference'],
                margins = True, margins_name = 'Total',
                index = ['count_7numprec'], aggfunc = 'sum')
            return hui_adjusted_numprec_df, verify_tables

        return hui_adjusted_numprec_df

    def expand_hui_to_persons(self, hui_adjusted_numprec_df):
        """
        One row per resident, numbered within the housing unit.

        pernum 1 is the householder. uniquehuid_numprec identifies the slot a
        person will eventually occupy, and is the key the random merge holds.
        """

        expand_df = hui_adjusted_numprec_df.loc[
            hui_adjusted_numprec_df['numprec'] > 0].copy()
        expected_persons = int(expand_df['numprec'].sum())

        hui_numprec = BaseInventory.expand_df(df = expand_df, expand_var = 'numprec')
        hui_numprec = hui_numprec.reset_index(drop = True)

        hui_numprec['pernum'] = hui_numprec.groupby(['huid']).cumcount() + 1

        pernum_width = len(str(int(hui_numprec['pernum'].max())))
        hui_numprec.loc[:,'uniquehuid_numprec'] = hui_numprec['huid'] + \
            hui_numprec['pernum'].apply(lambda x : str(int(x)).zfill(pernum_width))

        # expand_df consumes numprec, so put it back for later steps
        hui_numprec = pd.merge(left = hui_numprec,
                               right = expand_df[['huid','numprec']],
                               on = 'huid',
                               how = 'left')

        if len(hui_numprec) != expected_persons:
            raise ValueError(
                "expansion produced " + str(len(hui_numprec)) + " person slots "
                "for " + str(expected_persons) + " residents.")
        if not hui_numprec['uniquehuid_numprec'].is_unique:
            raise ValueError("uniquehuid_numprec is not unique after expansion.")

        return hui_numprec

    def infer_household_structure(self, hui_numprec, report = False):
        """
        Clear the householder characteristics that expansion wrongly copied.

        Expanding a housing unit copies the householder's age, sex, race and
        ethnicity to every resident. Race and ethnicity are left alone, since
        household members usually share them and the linkage matches on them.
        Age and sex are not: a householder's spouse and children do not share
        them, so they are reset to -999 and the random merge fills them from
        the person records instead.

        Two family shapes are treated differently, following the 2021 method:

        Husband-wife families carry sex -999 on the householder record, so
        person 2 is taken to be the spouse and persons 3 and beyond assumed
        children. Single parent families have a sexed householder, so person 2
        onwards are assumed children.

        Anyone who is neither householder, spouse nor assumed child has their
        age cleared as well, since nothing is known about them.
        """

        hui_numprec = hui_numprec.copy()

        # gqtype is null for ordinary housing at this stage, so the conditions
        # test it through fillna rather than against 0 directly.
        assume_child_husbandwifefamily = {
                'not_householder'    : "(df['pernum'] != 1)",
                'Family'             : "(df['family'] == 1)",
                'Husband-wife'       : "(df['sex'] == -999)",
                'Assume child obs'   : "(df['pernum'] > 2)",
                'Not Group Quarters' : "(df['gqtype'].fillna(0) == 0)"}
        assume_child_singleparent = {
                'not_householder'    : "(df['pernum'] != 1)",
                'Family'             : "(df['family'] == 1)",
                'Single Parent'      : "(df['sex'] == 1) | (df['sex'] == 2)",
                'Assume child obs'   : "(df['pernum'] > 1)",
                'Not Group Quarters' : "(df['gqtype'].fillna(0) == 0)"}

        reports = {}
        for name, conditionset in [('husband-wife', assume_child_husbandwifefamily),
                                   ('single parent', assume_child_singleparent)]:
            if report:
                reports[name] = describe_conditionset(
                    df = hui_numprec, primary_key = 'huid',
                    conditionset = conditionset)
            assume_child_obs = create_conditionset(
                df = hui_numprec, primary_key = 'huid',
                conditionset = conditionset)
            hui_numprec.loc[assume_child_obs,'agegroupH17'] = -999
            hui_numprec.loc[assume_child_obs,'agegroupH18'] = -999
            hui_numprec.loc[assume_child_obs,'sex'] = -999
            hui_numprec.loc[assume_child_obs,'child'] = 1

        hui_numprec['child'] = hui_numprec['child'].fillna(value = -999)

        notchild_or_spouse = {
                'not_householder'    : "(df['pernum'] != 1)",
                'Not Spouse'         : "(df['pernum'] != 2)",
                'Not assumed child'  : "(df['child'] != 1)",
                'Not Group Quarters' : "(df['gqtype'].fillna(0) == 0)"}
        if report:
            reports['other members'] = describe_conditionset(
                df = hui_numprec, primary_key = 'huid',
                conditionset = notchild_or_spouse)
        reset_age = create_conditionset(
                df = hui_numprec, primary_key = 'huid',
                conditionset = notchild_or_spouse)
        hui_numprec.loc[reset_age,'agegroupH17'] = -999
        hui_numprec.loc[reset_age,'agegroupH18'] = -999

        # Group quarters residents have no householder to inherit from, so any
        # characteristic still missing is marked not set rather than left null.
        is_gqtype = ~self.not_group_quarters(hui_numprec)
        for char_var in ['sex','race','hispan','agegroupH17','agegroupH18']:
            if char_var not in hui_numprec.columns:
                continue
            conditions = is_gqtype & (hui_numprec[char_var].isnull())
            hui_numprec.loc[conditions, char_var] = -999

        if report:
            return hui_numprec, reports

        return hui_numprec

    @staticmethod
    def validate_person_slots(hui_df, hui_numprec):
        """
        Invariants for the expansion and structure steps.

        Returns a dict of check name to (passed, detail), reporting every check
        rather than stopping at the first failure. Checks that select nothing
        fail rather than pass, so an empty selection cannot be mistaken for a
        clean result.
        """

        checks = {}

        expected = int(hui_df.loc[hui_df['numprec'] > 0, 'numprec'].sum())
        checks['one slot per resident'] = (
            len(hui_numprec) == expected and expected > 0,
            "%d slots for %d residents" % (len(hui_numprec), expected))

        checks['slot ids unique'] = (
            hui_numprec['uniquehuid_numprec'].is_unique,
            "%d duplicated" % int(hui_numprec['uniquehuid_numprec'].duplicated().sum()))

        occupied = hui_df[hui_df['numprec'] > 0]
        checks['every occupied unit represented'] = (
            set(hui_numprec['huid']) == set(occupied['huid']),
            "%d units missing" % len(set(occupied['huid']) - set(hui_numprec['huid'])))

        sizes = hui_numprec.groupby('huid').size()
        declared = occupied.set_index('huid')['numprec']
        mismatched = int((sizes != declared.reindex(sizes.index)).sum())
        checks['slots per unit equal numprec'] = (
            mismatched == 0, "%d units mismatched" % mismatched)

        householders = hui_numprec[hui_numprec['pernum'] == 1]
        checks['exactly one householder per unit'] = (
            len(householders) == len(occupied) and len(householders) > 0,
            "%d householders for %d units" % (len(householders), len(occupied)))

        return checks
