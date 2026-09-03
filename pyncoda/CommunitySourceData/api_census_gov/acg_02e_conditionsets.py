# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

"""
Select rows by a named set of conditions.

The person record linkage repeatedly has to say things like "not the
householder, in a husband-wife family, and not group quarters". Written inline
those chains become long boolean expressions that are hard to read and harder
to check. A condition set names each clause, so the intent survives in the code
and appears in any output that reports which rule matched.

This is ported from the 2021 prechui sandbox, where it lived in
``tidy_censusapi``. The remaining functions from that module - random age and
the various age group bands - already exist in ``acg_02c_agefunctions``.
"""

import pandas as pd


def create_conditionset(df, primary_key, conditionset):
    """
    Combine a dictionary of named conditions into a single boolean mask.

    Args:
        df: the dataframe to evaluate against. The expressions refer to it as
            ``df``, so the name is part of the interface.
        primary_key (str): a column that is never null, used to seed the mask
            with every row selected before the conditions narrow it.
        conditionset (dict): name to expression string. Expressions are written
            as text and evaluated here, which is what allows them to be named
            and reported.

    Returns:
        A boolean Series aligned to df, True where every condition holds.

    Example:
        conditionset = {
                'not_householder'     : "(df['pernum'] != 1)",
                'Husband-wife family' : "(df['sex'] == -999)",
                'Assume child'        : "(df['pernum'] > 2)",
                'Not Group Quarters'  : "(df['gqtype'] == 0)"}

    Note on group quarters: whether an ordinary housing unit has ``gqtype``
    equal to 0 or null depends on how far through the workflow the inventory
    is. Conditions that test ``gqtype == 0`` will silently select nothing on an
    unpolished inventory, so prefer ``fillna(0)`` in the expression, or test
    ``gqtype > 0`` and negate.
    """

    if primary_key not in df.columns:
        raise KeyError(
            "primary_key '" + str(primary_key) + "' is not a column. "
            "It seeds the mask, so it must exist and be non-null.")

    # Start with every row selected, then narrow.
    conditions = (df[primary_key].notnull())
    for condition in conditionset.keys():
        try:
            conditions = conditions & eval(conditionset[condition])
        except Exception as error:
            raise type(error)(
                "condition '" + str(condition) + "' failed to evaluate: " +
                str(conditionset[condition]) + " - " + str(error))

    return conditions


def describe_conditionset(df, primary_key, conditionset):
    """
    Report how many rows each condition removes, in order.

    Useful when a condition set selects far fewer rows than expected: it shows
    which clause is responsible rather than leaving the whole chain to inspect.
    Returns a dataframe of condition name, rows remaining, and rows removed.
    """

    rows = []
    conditions = (df[primary_key].notnull())
    remaining = int(conditions.sum())
    rows.append({'condition': '(all rows)',
                 'remaining': remaining,
                 'removed': 0})

    for condition in conditionset.keys():
        conditions = conditions & eval(conditionset[condition])
        now = int(conditions.sum())
        rows.append({'condition': condition,
                     'remaining': now,
                     'removed': remaining - now})
        remaining = now

    return pd.DataFrame(rows)
