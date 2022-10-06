import pandas as pd

from pyincore_data_addons.SourceData.www_data_axle_com.dataaxle_00a_datastructure \
    import *


def add_lodes_industrycode(input_df):
    naics2d_var = 'NAICS2D'
    output_df = input_df.copy()
    for naics2d in naics2d_lodes_industry_dict:
        naics2d_match = (output_df[naics2d_var] == naics2d)
        conditions = naics2d_match
        output_df.loc[conditions,'IndustryCode'] = \
            naics2d_lodes_industry_dict[naics2d]['lodesicode']
    
    return output_df