# Copyright (c) 2021 Nathanael Rosenheim    and others. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import numpy as np
import pandas as pd

class icd_tidy():
    def expand_df(df, expand_var):
        """
        expand dataset based on the expand variable
        """

        # Expand data by the expand variable
        df = df.reindex(df.index.repeat(df[expand_var]))

        # reset index
        df.reset_index(inplace=True, drop = True)
        
        # drop variables for expanding
        df = df.drop(columns = [expand_var])

        return df

    def add_total_sum_byvar(df, values_to_sum, by_vars, values_to_sum_col_rename):
        """
        Function adds a new column with sum of values by variables
        Used to determine numerator of probability a binary new characteristic 
        """

        # Check to make sure values to sum is not in by vars
        #print("Total sum based on :",by_vars)
        #print("   new sum to add will be called: ",values_to_sum_col_rename)
        if values_to_sum in by_vars:
            # By vars can not include the value to sum
            by_vars = [var for var in by_vars if var not in [values_to_sum]]
            print("   Fix by vars :",by_vars)

        total_sum_df = pd.pivot_table(df, values=values_to_sum, index=by_vars,
                                aggfunc=np.sum)
        total_sum_df.reset_index(inplace = True)
        total_sum_df = total_sum_df.rename(columns = {values_to_sum : values_to_sum_col_rename})

        # add probabilty denomninator to the original data frame 
        df = pd.merge(left = df,
                        right = total_sum_df,
                        left_on = by_vars,
                        right_on = by_vars,
                        how = 'outer')

        return df