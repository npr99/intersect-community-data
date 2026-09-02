# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import numpy as np
import pandas as pd
import copy # For copying data structure dictionaries before modifying them
import os # For saving output to path
import sys

# For printing conda list in log file
from IPython import get_ipython
ipython = get_ipython()
# looks like log overlaps in the output of conda list
# try pausing for a second
import time

# Save output as a log file function
from pyncoda \
     import ncoda_00c_save_output_log as logfile
     
# Load in data structure dictionaries
from pyncoda.CommunitySourceData.api_census_gov.acg_00a_createAPI_datastructure import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00a_general_datastructures import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00f_preci_block2010 import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00f_preci_block2020 import (
    sexbyage_P12_2020_varstem_roots,
    sexbyage_P12HAI_2020_varstem_roots,
    hispan_byrace_P5_2020_varstem_roots
)
from pyncoda.CommunitySourceData.api_census_gov.acg_00f_preci_PCT12_2020 import \
    sexbyage_PCT12_2020_varstem_roots

# open, read, and execute python program with reusable commands
from pyncoda.CommunitySourceData.api_census_gov.acg_00b_hui_block2020 \
    import group_quarters_P18_2020_varstem_roots
from pyncoda.CommunitySourceData.api_census_gov.acg_01a_BaseInventory import BaseInventory
from pyncoda.CommunitySourceData.api_census_gov.acg_02a_add_categorical_char \
     import add_new_char_by_random_merge_2dfs
from pyncoda.CommunitySourceData.api_census_gov.acg_02c_agefunctions \
     import *
from pyncoda.CommunitySourceData.api_census_gov.acg_02d_polishdf \
     import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00h_disability_ACS5yr2012 import *
from pyncoda.CommunitySourceData.api_census_gov.acg_00h_disability_ACS5yr2022 import *

class prec_workflow_functions():
    """
    Function runs full process for generating the person record files
    Process runs for 1 county.
    """

    def __init__(self,
            state_county: str,
            state_county_name: str,
            seed: int = 9876,
            version: str = '3.0.0',
            version_text: str = 'v3-0-0',
            basevintage: str = '2010',
            outputfolder: str ="",
            outputfolders = {},
            savefiles: bool = True):

        self.state_county = state_county
        self.state_county_name = state_county_name
        self.seed = seed
        self.version = version
        self.version_text = version_text
        self.basevintage = basevintage
        self.outputfolder = outputfolder
        self.outputfolders = outputfolders
        self.savefiles = savefiles


    def save_environment_version_details(self):
        print("\n***************************************")
        print("    Version control - list of installed packages")
        print("***************************************\n")       

        try:
            # print a list of all installed packages and version information
            ipython.magic("conda list")
            # Give ipython a second to output results
            # this step fixes issue with conda list being split by next command
            time.sleep(1)
        except:
            print("Unable to print version information")



    """
    Code from 
    github.com\npr99\Population_Inventory\pyincore_data_addons\SourceData\api_census_gov
    """
    
    def run_prec_workflow(self, savelog=True):
        """
        Workflow to produce Person Record Inventory
        """
        # Start empty containers to store block level and tract level data
        tract_df = {}
        block_df = {}

        # Save output description as text
        output_filename = f'prec_{self.version_text}_{self.state_county}_{self.basevintage}_rs{self.seed}'
        self.output_filename = output_filename
        if savelog == True:
            log_filepath = self.outputfolders['logfiles']+"/"+output_filename+'.log'
            # start log file
            logfile.start(log_filepath)
            self.save_environment_version_details()

        print("\n***************************************")
        print("    Obtain and clean core person record characteristics for",self.state_county_name)
        print("***************************************\n")

        print(self.outputfolders)
        # Generate Person by Age, Sex, Race, and Hispanic
        block_df["preci"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'block',
                                        vintage = str(self.basevintage), 
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [sexbyage_P12_varstem_roots if str(self.basevintage) == '2010' else
                                                            sexbyage_P12_2020_varstem_roots],
                                        outputfolders = self.outputfolders,
                                        outputfile = f"CorePREC_{self.basevintage}")

        # basevintage must be passed. graft_on_new_char defaults it to "2010",
        # and it uses the vintage to build the geography column name, so a 2020
        # run without it looks for Block2010str and fails with a bare KeyError.
        # The correct 2020 dictionaries were already being selected above, which
        # is what made this look like it worked - the data was right and the
        # geography column name was not.
        #
        # HUA_Disability_2020 does this graft by hand and passes basevintage
        # explicitly, so the notebook path was unaffected and this only surfaces
        # when run_prec_workflow is called directly.
        block_df["precihispan"] = BaseInventory.graft_on_new_char(base_inventory= block_df['preci'],
                                        state_county = self.state_county,
                                        new_char = 'hispan',
                                        new_char_dictionaries = [
                                            sexbyage_P12HAI_varstem_roots if str(self.basevintage) == '2010' else sexbyage_P12HAI_2020_varstem_roots,
                                            hispan_byrace_P5_varstem_roots if str(self.basevintage) == '2010' else hispan_byrace_P5_2020_varstem_roots
                                            ],
                                        basevintage = str(self.basevintage),
                                        basegeolevel = 'Block',
                                        outputfile = f"preci_{self.basevintage}",
                                        outputfolders = self.outputfolders)

        # Generate sex by age with individual years
        vintage = str(self.basevintage)
        group = 'PCT12'
        if vintage == '2010':
            sexbyage_PCT12_varstem_roots = createAPI_datastructure.obtain_api_metadata(
                            vintage = vintage,
                            dataset_name = 'dec/sf1',
                            group = group,
                            outputfolder = self.outputfolder)
        else:
            sexbyage_PCT12_varstem_roots = sexbyage_PCT12_2020_varstem_roots

        tract_df["PCT12"] = BaseInventory.get_apidata(state_county = self.state_county,
                                        geo_level = 'tract',
                                        vintage = str(self.basevintage),
                                        mutually_exclusive_varstems_roots_dictionaries = [sexbyage_PCT12_varstem_roots],
                                        outputfolders = self.outputfolders,
                                        outputfile = f"{group}_{self.basevintage}")

        # Add random age to block_df["precihispan"]
        block_df["precihispan"] = add_randage(
                                    block_df["precihispan"],
                                    seed = self.seed,
                                    varname = 'randageP12')
        # Add agegroups to block_df["precihispan"]
        block_df["precihispan"] = add_P12age_groups(
                                    block_df["precihispan"],
                                    varname = 'randageP12')
        # Add random age to tract_df["PCT12"]
        tract_df["PCT12"] = add_randage(
                                    tract_df["PCT12"],
                                    seed = self.seed,
                                    varname = 'randagePCT12')
        # Add agegroups to tract_df["PCT12"]
        tract_df["PCT12"] = add_P12age_groups(
                                    tract_df["PCT12"],
                                    varname = 'randagePCT12')
                  
        print("\n***************************************")
        print("    Person Block by Age with Tract by Age data.")
        print("***************************************\n")

        add_age = add_new_char_by_random_merge_2dfs(
            dfs = {'primary'  : {'data': block_df["precihispan"], 
                            'primarykey' : 'precid',
                            'geolevel' : 'Block',
                            'geovintage' : str(self.basevintage),
                            'notes' : 'Block agegroup, sex, race, ethnicity data.'},
                'secondary' : {'data': tract_df["PCT12"], 
                            'primarykey' : 'uniqueidPCT12',
                            'geolevel' : 'Tract',
                            'geovintage' : str(self.basevintage),
                            'notes' : 'Tract single age years, sex data.'}},
            seed = self.seed,
            common_group_vars = ['agegroupP12'],
            new_char = 'randagePCT12',
            geolevel = "Tract",
            geovintage = str(self.basevintage),
            by_groups = {'All' : {'by_variables' : ['sex']}},
            fillna_value= -999,
            state_county = self.state_county,
            outputfile = f"preci_randomage_{self.basevintage}",
            outputfolder = self.outputfolders['RandomMerge'])

        # Set up round options
        rounds = {'options': {
                'option1' : {'notes' : 'By original common group vars and by groups variables.',
                            'common_group_vars' : 
                                    add_age.common_group_vars,
                            'by_groups' :
                                    add_age.by_groups}
                                },
                'geo_levels' : ['Tract']                         
                }

        prec_age_df = add_age.run_random_merge_2dfs(rounds)

        print("\n***************************************")
        print("    Add Disability Characteristics from ACS Tract Data")
        print("***************************************\n")

        # Retrieve tract-level disability data
        tract_df["B18101"] = BaseInventory.get_apidata(
                                        state_county = self.state_county,
                                        geo_level = 'tract',
                                        vintage = str(int(self.basevintage)+2),
                                        # The 2022 dictionary is named
                                        # disability_B18101_varstem_roots_2022;
                                        # the components were transposed here,
                                        # so this raised NameError for 2020.
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [disability_B18101_varstem_roots if str(self.basevintage) == '2010' else
                                                            disability_B18101_varstem_roots_2022],
                                        outputfolders = self.outputfolders,
                                        outputfile = f"B18101_disability_{self.basevintage}")

        # Add B18101 age groups to person records
        print("Adding B18101 age groups for disability matching...")
        prec_age_df['primary'] = add_B18101age_groups(
                                    prec_age_df['primary'],
                                    varname = 'randagePCT12')

        print("Random merging disability data...")
        add_disability = add_new_char_by_random_merge_2dfs(
            dfs = {'primary'  : {'data': prec_age_df['primary'],
                            'primarykey' : 'precid',
                            'geolevel' : 'Block',
                            'geovintage' : str(self.basevintage),
                            'notes' : 'Person-level data without disability'},
                'secondary' : {'data': tract_df["B18101"],
                            'primarykey' : 'uniqueidB18101',
                            'geolevel' : 'Tract',
                            'geovintage' : str(self.basevintage),
                            'notes' : 'Tract-level disability counts by sex and age'}},
            seed = self.seed,
            common_group_vars = ['agegroupB18101'],
            new_char = 'disability',
            geolevel = "Tract",
            geovintage = str(self.basevintage),
            by_groups = {'All' : {'by_variables' : ['sex']}},
            fillna_value = -999,
            state_county = self.state_county,
            outputfile = f"prec_disability_{self.basevintage}",
            outputfolder = self.outputfolders['RandomMerge'],
            savefiles = self.savefiles)

        # Set the rounds explicitly rather than taking the generic defaults.
        #
        # make_round_options_dict includes a round that groups by race, which
        # suits tables that carry a race dimension. B18101 does not - it is sex
        # by age by disability status - so that round tries to sort the tract
        # data by a column it does not have and raises KeyError: 'race'.
        #
        # This ladder matches the one HUA_Disability_2020 uses and is known to
        # work: tract with sex and age band, then tract with sex, then tract
        # alone, then county as the final fallback.
        rounds = {'options': {
                'option1' : {'notes' : 'Tract, sex and age band.',
                            'common_group_vars' : ['agegroupB18101'],
                            'by_groups' : {'All' : {'by_variables' : ['sex']}}},
                'option2' : {'notes' : 'Tract and sex, drop the age band.',
                            'common_group_vars' : [],
                            'by_groups' : {'All' : {'by_variables' : ['sex']}}},
                'option3' : {'notes' : 'Tract alone, drop sex.',
                            'common_group_vars' : [],
                            'by_groups' : {'All' : {'by_variables' : []}}},
                'option4' : {'notes' : 'County, the broadest fallback.',
                            'common_group_vars' : [],
                            'by_groups' : {'All' : {'by_variables' : []}}}
                                },
                'geo_levels' : ['Tract','Tract','Tract','County']
                }

        # Run multi-round random merge
        # Flags automatically created:
        # - disability_flagsetrm: Overall assignment flag (0 = not assigned, 1 = assigned)
        # - disability_Tract2010_flagsetrm: Round-specific flag (0, 1, 2, 3, etc.)
        # These track which persons received disability assignments and in which round
        prec_disability_df = add_disability.run_random_merge_2dfs(rounds)

        # Update return variable to include disability
        prec_age_df = prec_disability_df

        print("\n***************************************")
        print("    Try to polish final hui data.")
        print("***************************************\n")

        prec_df = self.final_polish_prec(prec_age_df['primary'])

        print("\n***************************************")
        print("    Save cleaned data file.")
        print("***************************************\n")

        csv_filepath = self.outputfolders['top']+"/"+output_filename+'.csv'

        savefile = os.path.join(os.getcwd(), csv_filepath)
        prec_df.to_csv(savefile, index=False)
        print("File saved:",savefile)

        # Stop log file
        logfile.stop()

        return prec_df

    # Function not currently used - might be in PRECHUI Workflow
    def tidy_group_quarters(self, unit_of_analysis: str = 'person'):
        """
        Obtain group quarters population by sex, age and group quarters type.

        This is the hook the person record linkage uses to place group quarters
        residents, who have no householder and so cannot be matched through the
        household tables.

        Vintage handling. The 2020 Demographic and Housing Characteristics file
        renumbered this table, so the group name is mapped rather than assumed:

            2010  dec/sf1  P43  GROUP QUARTERS POPULATION BY SEX BY AGE BY
                                GROUP QUARTERS TYPE
            2020  dec/dhc  P18  GROUP QUARTERS POPULATION BY SEX BY AGE BY
                                MAJOR GROUP QUARTERS TYPE

        Despite the "major" in the 2020 title the seven group quarters types are
        the same, so the linkage needs no vintage branching. Requesting P43 from
        dec/dhc, as this function previously did, fails with an unknown variable
        error - the dataset name was switched by vintage but the group name was
        not.

        Output columns are named for the 2010 table (agegroupP43) in both
        vintages, matching the convention used for the householder age groups:
        they label age bands rather than tables, and the bands are identical.

        unit_of_analysis
        ----------------
        'person' (default) returns one row per group quarters resident, which
        is what the linkage needs: it matches person records one to one, so a
        facility level frame would supply far too few rows to match against.

        'housingunit' returns one row per facility, with numprec holding the
        resident count. This is the shape the housing unit workflow uses, where
        a block's group quarters population is treated as a single unit.

        The distinction is easy to miss and quiet when wrong. For Grays Harbor
        2020 the facility frame has 142 rows and the person frame 2,909; the
        first happens to equal the number of group quarters units in the
        housing unit inventory, so a facility frame passed to the linkage looks
        plausible and silently leaves most residents unmatched.
        """

        if unit_of_analysis not in ('person', 'housingunit'):
            raise ValueError(
                "unit_of_analysis must be 'person' or 'housingunit', got "
                + repr(unit_of_analysis))

        vintage = str(self.basevintage)
        dataset_name = 'dec/sf1' if vintage == '2010' else 'dec/dhc'
        group = 'P43' if vintage == '2010' else 'P18'

        print("\n***************************************")
        print("    Set up data structures for", group, "-", dataset_name)
        print("***************************************\n")

        if vintage == '2020':
            # Already written out, with the seven group quarters types and the
            # same three age bands P43 uses.
            groupquarters_dict = copy.deepcopy(group_quarters_P18_2020_varstem_roots)
        else:
            groupquarters_dict = createAPI_datastructure.obtain_api_metadata(
                    vintage = vintage,
                    dataset_name = dataset_name,
                    group = group,
                    outputfolder = self.outputfolder,
                    version_text = self.version_text)
            # obtain_api_metadata describes a table, not a request, and defaults
            # to tract geography. Without this the data returns at tract level
            # and, because this outputfile is not a "Core" file, no block id is
            # built and nothing raises - the geography is simply wrong.
            groupquarters_dict['metadata']['for_geography'] = 'block:*'
            groupquarters_dict['metadata']['indexvar'] = \
                ['GEO_ID','state','county','tract','block']

        # Graft chars are used to check the merge by variables in the grafting
        # function. Previously misspelled 'gqytpe', which silently checked
        # nothing.
        groupquarters_dict['metadata']['graft_chars'] = ['gqtype']

        print("\n***************************************")
        print("   Obtain and clean", group, "data.")
        print("***************************************\n")
        block_df = {}
        block_df[group] = BaseInventory.get_apidata(
                                        state_county = self.state_county,
                                        geo_level = 'block',
                                        vintage = vintage,
                                        mutually_exclusive_varstems_roots_dictionaries =
                                                            [groupquarters_dict],
                                        outputfolders = self.outputfolders,
                                        outputfile = f"{group}_{vintage}")

        output_df = block_df[group]

        if vintage == '2020':
            # The 2020 dictionary carries the age band and sex directly, so no
            # random age is drawn. Rename to the names the linkage expects.
            rename_map = {}
            if 'ageP18' in output_df.columns:
                rename_map['ageP18'] = 'agegroupP43'
            if 'sexP18' in output_df.columns:
                rename_map['sexP18'] = 'sex'
            output_df = output_df.rename(columns=rename_map)
        else:
            # The 2010 table gives an age range, so draw a random age within it
            # and band it, the same way the person records are aged.
            print("Add random age and group quarters age groups.")
            output_df = add_randage(output_df,
                                    seed = self.seed,
                                    varname = 'randageP43')
            output_df = add_P43age_groups(output_df,
                                          varname = 'randageP43')

        if unit_of_analysis == 'person' and 'numprec' in output_df.columns:
            # One row per resident, so the frame can be matched against person
            # records one to one.
            residents = int(output_df['numprec'].sum())
            print("Expand", len(output_df), "group quarters facilities to",
                  residents, "residents.")
            output_df = BaseInventory.expand_df(df = output_df,
                                                expand_var = 'numprec')
            output_df = output_df.reset_index(drop = True)

            # A key the random merge can hold onto, mirroring the huid built
            # for housing units: block, group quarters type, then a counter.
            output_df['gq_counter'] = \
                output_df.groupby(['GEO_ID','gqtype']).cumcount() + 1
            counter_width = len(str(int(output_df['gq_counter'].max())))
            output_df['uniqueidP43'] = (
                output_df['GEO_ID'].astype(str)
                + 'G' + output_df['gqtype'].astype(int).astype(str)
                + output_df['gq_counter'].apply(
                    lambda x: str(int(x)).zfill(counter_width)))

            if len(output_df) != residents:
                raise ValueError(
                    "group quarters expansion produced " + str(len(output_df))
                    + " rows for " + str(residents) + " residents.")

        return output_df

    def hui_tidy_P43(self):
        """
        Deprecated name kept so existing callers keep working.

        The table is P43 only in 2010; use tidy_group_quarters, which maps the
        group name by vintage.
        """

        return self.tidy_group_quarters()

    def final_polish_prec(self, input_df):

        print("\n***************************************")
        print("    Try to polish final prec data.")
        print("***************************************\n")

        print("Drop extra columns.")                                                                  
        prec_df = drop_extra_columns(input_df)

        print("\n***************************************")
        print("    Save cleaned data file.")
        print("***************************************\n")

        if self.savefiles == True:
            csv_filepath = self.outputfolders['top']+"/"+self.output_filename+'.csv'

            savefile = os.path.join(os.getcwd(), csv_filepath)
            prec_df.to_csv(savefile, index=False)
            print("File saved:",savefile)
        
        return prec_df
