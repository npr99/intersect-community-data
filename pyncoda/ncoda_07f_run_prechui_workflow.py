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

KNOWN LIMITATION - the second round exhausts the slot pool
----------------------------------------------------------
The merge runs end to end and every structural invariant holds, but only about
14% of person records currently receive a huid. The cause is located but not
yet resolved, and it is not in the rounds themselves.

run_random_merge_2dfs reports two figures per round, one for the primary frame
and one for the secondary. On Grays Harbor 2020 the secondary - the pool of
housing unit person slots - runs down like this:

    round 1  householder     primary 73.24% left    secondary 89.73% left
    round 2  group quarters  primary 69.44% left    secondary  0.04% left
    rounds 3-6                primary 69.44% left    secondary  0.04% left
    round 7  catch all       primary 69.40% left    secondary  0.01% left

The group quarters round consumes 99.96% of the slot pool while placing only
2,875 people, after which no later round has anything to match against. Rounds
3 to 6 place nobody at all and the catch all places 26.

The rounds are not individually at fault. Run on its own against the same data
the catch all round places 64,516 of 75,636 - so the matching logic works and
the ordering is what fails. Filling the 12,447 null age bands on the person side
changes nothing, which rules out the obvious first suspect.

What remains is to establish what marks a secondary row as used. The count
consumed far exceeds the count matched, which suggests rows are flagged by
group rather than by pair, so a round with coarse keys burns the pool. The merge
already has a reuse_secondary switch that resets the flags when the pool empties;
whether that is the intended remedy, or whether the group quarters round should
be restricted to group quarters slots, is a question about upstream behaviour
rather than about this module.

Until then this workflow should be treated as structurally correct and
substantively incomplete: what it does assign is verifiably sound - nothing is
duplicated or lost, no unit is overfilled, everyone is housed in their own
block, and 2,901 of 2,909 group quarters residents are placed - but most people
are not yet assigned.
"""

import numpy as np
import pandas as pd

from pyncoda.CommunitySourceData.api_census_gov.acg_01a_BaseInventory \
    import BaseInventory
from pyncoda.CommunitySourceData.api_census_gov.acg_02a_add_categorical_char \
    import add_new_char_by_random_merge_2dfs
from pyncoda.CommunitySourceData.api_census_gov.acg_02c_agefunctions \
    import add_P43age_groups, add_H17age_groups, add_H18age_groups
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

    def merge_groupquarters(self, hui_numprec, groupquarters_df):
        """
        Give group quarters slots an age band and a sex.

        Group quarters residents have no householder to inherit from, so the
        slots created for them carry nothing the person merge could match on.
        This fills them from the group quarters table, matching within block on
        group quarters type.

        groupquarters_df must be person level - one row per resident, as
        returned by tidy_group_quarters(unit_of_analysis='person'). A facility
        level frame has far too few rows and would leave most residents unfilled
        while looking plausible.
        """

        print("\n***************************************")
        print("    Random merge between housing inventory and group quarters records.")
        print("***************************************\n")

        add_gq = add_new_char_by_random_merge_2dfs(
            dfs = {'primary'  : {'data': hui_numprec,
                            'primarykey' : 'uniquehuid_numprec',
                            'geolevel' : self.basegeolevel,
                            'geovintage' : self.basevintage,
                            'notes' : 'Housing unit inventory expanded by numprec.'},
                'secondary' : {'data': groupquarters_df,
                            'primarykey' : 'uniqueidP43',
                            'geolevel' : self.basegeolevel,
                            'geovintage' : self.basevintage,
                            'notes' : 'Group quarters residents.'}},
            seed = self.seed,
            common_group_vars = ['gqtype'],
            new_char = 'agegroupP43',
            extra_vars = ['sex'],
            geolevel = self.basegeolevel,
            geovintage = self.basevintage,
            by_groups = {'NA' : {'by_variables' : []}},
            fillna_value = -999,
            state_county = self.state_county,
            outputfile = "hui_groupquarters",
            outputfolder = self.outputfolders['RandomMerge'])

        rounds = {'options': {
                'option1' : {'notes' : 'Match group quarters residents on type within block.',
                            'common_group_vars' : add_gq.common_group_vars,
                            'by_groups' : add_gq.by_groups}
                                },
                'geo_levels' : [self.basegeolevel]
                }

        return add_gq.run_random_merge_2dfs(rounds)

    def prepare_prec_for_merge(self, prec_df, randage_var = 'randagePCT12'):
        """
        Put the person records on the same age bands the housing units use.

        Person records carry a single year of age; housing units carry bands
        from the householder tables. The merge needs both sides in the same
        terms, so the person ages are banded three ways - the two householder
        bandings and the group quarters banding - and a child flag is added.
        """

        if randage_var not in prec_df.columns:
            raise KeyError(
                "person records have no '" + randage_var + "' column; the age "
                "banding cannot be derived. Available: " +
                str(sorted(prec_df.columns)[:12]))

        prec_df = prec_df.copy()
        prec_df = add_P43age_groups(prec_df, varname = randage_var)
        prec_df = add_H18age_groups(prec_df, varname = randage_var)
        prec_df = add_H17age_groups(prec_df, varname = randage_var)

        # The housing unit side marks assumed children; the person side needs
        # the same flag so the child rounds have something to match on.
        prec_df.loc[prec_df[randage_var] >= 18, 'child'] = 0
        prec_df.loc[prec_df[randage_var] < 18, 'child'] = 1

        return prec_df

    def merge_prec_to_hui(self, prec_df, hui_numprec):
        """
        Attach a huid to each person record.

        Rounds weaken in a fixed order so the result is reproducible. The first
        matches householders on everything known about them. Group quarters
        residents match on sex and their own age band. Children match on race
        and ethnicity, then on nothing but the child flag. Spouses and other
        adults match on race, ethnicity and age band. The last round places
        whoever is left with no assumptions at all.
        """

        print("\n***************************************")
        print("    Random merge between person records and housing units.")
        print("***************************************\n")

        prec_hui = add_new_char_by_random_merge_2dfs(
            dfs = {'primary'  : {'data': prec_df,
                            'primarykey' : 'precid',
                            'geolevel' : self.basegeolevel,
                            'geovintage' : self.basevintage,
                            'notes' : 'Person records with race, hispan, age, sex.'},
                'secondary' : {'data': hui_numprec,
                            'primarykey' : 'uniquehuid_numprec',
                            'geolevel' : self.basegeolevel,
                            'geovintage' : self.basevintage,
                            'notes' : 'Housing unit person slots.'}},
            seed = self.seed,
            common_group_vars = ['agegroupH17','sex','race','hispan'],
            new_char = 'huid',
            extra_vars = ['gqtype','numprec','pernum','family'],
            geolevel = self.basegeolevel,
            geovintage = self.basevintage,
            by_groups = {'NA' : {'by_variables' : []}},
            fillna_value = -999,
            state_county = self.state_county,
            outputfile = "prec_hui_randomhuid",
            outputfolder = self.outputfolders['RandomMerge'])

        rounds = {'options': {
                'householderH17' : {'notes' : 'Householder on age band, sex, race and ethnicity.',
                            'common_group_vars' : ['agegroupH17','sex','race','hispan'],
                            'by_groups' : prec_hui.by_groups},
                'groupquarters' : {'notes' : 'Group quarters residents by sex and age band.',
                            'common_group_vars' : ['sex','agegroupP43'],
                            'by_groups' : prec_hui.by_groups},
                'child1' : {'notes' : 'Children by race and ethnicity.',
                            'common_group_vars' : ['race','hispan','child'],
                            'by_groups' : prec_hui.by_groups},
                'child2' : {'notes' : 'Children without race and ethnicity.',
                            'common_group_vars' : ['child'],
                            'by_groups' : prec_hui.by_groups},
                'spouse' : {'notes' : 'Other members assumed to share the householder race and ethnicity.',
                            'common_group_vars' : ['race','hispan','agegroupH17'],
                            'by_groups' : prec_hui.by_groups},
                'householderH18' : {'notes' : 'Householder on the coarser age band.',
                            'common_group_vars' : ['agegroupH18','sex','race','hispan'],
                            'by_groups' : prec_hui.by_groups},
                'others' : {'notes' : 'Whoever is left, no assumptions.',
                            'common_group_vars' : [],
                            'by_groups' : prec_hui.by_groups}
                                },
                'geo_levels' : [self.basegeolevel]
                }

        return prec_hui.run_random_merge_2dfs(rounds)

    def polish_prechui(self, prec_hui_df):
        """
        Sort and order the linked person records.
        """

        output_df = prec_hui_df.sort_values(by = ['huid','pernum'])

        primary_key_names = ['precid','huid','pernum', self.geo_id]
        columnlist = [col for col in output_df.columns
                      if col not in primary_key_names]
        ordered = [col for col in primary_key_names if col in output_df.columns]

        return output_df[ordered + columnlist]

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

    @staticmethod
    def unassigned_mask(series):
        """
        Which rows failed to receive a value from a random merge.

        A merge can leave a value as the fillna sentinel -999, as null, or - in
        the housing unit allocation - as the literal string 'missing building
        id'. Testing only one of them understates the miss rate, which is the
        kind of error that makes a linkage look better than it is.
        """

        return (series.isnull() |
                series.astype(str).isin(['-999', '-999.0', 'missing building id']))

    def validate_linkage(self, prec_df, hui_numprec, prechui_df):
        """
        Invariants for the completed linkage.

        Returns a dict of check name to (passed, detail). Every check reports,
        and a check that selects nothing fails rather than passes.
        """

        checks = {}
        geo_id = self.geo_id

        # Nothing gained, nothing lost
        checks['every person record kept'] = (
            len(prechui_df) == len(prec_df) and len(prec_df) > 0,
            "%d out for %d in" % (len(prechui_df), len(prec_df)))

        checks['precid still unique'] = (
            prechui_df['precid'].is_unique,
            "%d duplicated" % int(prechui_df['precid'].duplicated().sum()))

        checks['precid set unchanged'] = (
            set(prechui_df['precid']) == set(prec_df['precid']),
            "%d missing" % len(set(prec_df['precid']) - set(prechui_df['precid'])))

        unassigned = self.unassigned_mask(prechui_df['huid'])
        assigned_count = int((~unassigned).sum())
        checks['every person has a huid'] = (
            int(unassigned.sum()) == 0,
            "%d of %d unassigned (%.2f%% assigned)"
            % (int(unassigned.sum()), len(prechui_df),
               assigned_count / len(prechui_df) * 100 if len(prechui_df) else 0))

        assigned = prechui_df[~unassigned]

        # A housing unit cannot hold more people than it has slots
        if len(assigned):
            per_unit = assigned.groupby('huid').size()
            capacity = hui_numprec.groupby('huid').size()
            shared = per_unit.index.intersection(capacity.index)
            overfilled = int((per_unit.loc[shared] > capacity.loc[shared]).sum())
            checks['no unit holds more than its slots'] = (
                overfilled == 0 and len(shared) > 0,
                "%d of %d units overfilled" % (overfilled, len(shared)))

            unknown = len(per_unit.index.difference(capacity.index))
            checks['every assigned huid exists'] = (
                unknown == 0, "%d unknown huid" % unknown)

        # Group quarters residents belong in group quarters, and only there
        if 'gqtype' in assigned.columns and len(assigned):
            in_gq = assigned['gqtype'].fillna(0) > 0
            gq_slots = int((hui_numprec['gqtype'].fillna(0) > 0).sum())
            checks['group quarters residents placed'] = (
                int(in_gq.sum()) <= gq_slots,
                "%d residents into %d group quarters slots"
                % (int(in_gq.sum()), gq_slots))

        # Block conservation: a person should be housed in their own block
        if geo_id in assigned.columns and geo_id in hui_numprec.columns:
            slot_block = hui_numprec.set_index('huid')[geo_id]
            slot_block = slot_block[~slot_block.index.duplicated()]
            housed_block = assigned['huid'].map(slot_block)
            same_block = (housed_block == assigned[geo_id])
            moved = int((~same_block).sum())
            checks['persons housed in their own block'] = (
                moved == 0 and len(assigned) > 0,
                "%d of %d housed outside their block" % (moved, len(assigned)))

        return checks

    def validate_against_census(self, prechui_df, census_marginals):
        """
        Compare the linked result against Census tables that were NOT used as
        merge inputs.

        The merge matches on householder age band, sex, race and ethnicity, so
        agreement with those marginals is guaranteed by construction and proves
        only that the merge conserved them. Tables describing household type by
        age of householder are not inputs, so agreement with them is evidence
        about the joint distribution rather than a restatement of the inputs.

        census_marginals: dict of label to expected count.
        Returns a dataframe of label, linked value, census value, difference.
        """

        rows = []
        householders = prechui_df[prechui_df['pernum'] == 1]
        for label, (expected, mask) in census_marginals.items():
            observed = int(mask(householders).sum())
            rows.append({
                'marginal': label,
                'linked': observed,
                'census': expected,
                'difference': observed - expected,
                'percent': (observed - expected) / expected * 100 if expected else np.nan,
                })

        return pd.DataFrame(rows)
