"""
Patches for 2020 DHC support in pyncoda.

Two patches:
  1. createAPI_datastructure.obtain_api_metadata
        - Use 'PCT12_001N' format for variable discovery (2010 used 'PCT12001')
        - Strip leading '_' and trailing 'N' from variable_linenumber so the
          dictionary keys are plain digits ('001' etc.), matching the 2010 shape

  2. BaseInventory.get_data_based_on_varstems_and_roots
        - For 2020 DHC, the static dictionaries (e.g., acg_00f_preci_block2020.py)
          use varroot keys like '_003N' (with underscore + N suffix). Building API
          names is then simple concatenation: 'P12A' + '_003N' = 'P12A_003N'.
        - The internal rename target also follows: 'P12' + '_003N' = 'P12_003N'
        - This patch's main job is to fix the rename loop so column dtypes get
          converted to int and the rename actually runs (the original 2010 code
          uses `varstem_race + varroot_str` for both API var and rename source —
          we keep that pattern).

Usage:
    from pyncoda.CommunitySourceData.api_census_gov.acg_00a_createAPI_datastructure_2020_patch import (
        patch_obtain_api_metadata_for_2020,
        patch_get_data_for_2020,
    )
    patch_obtain_api_metadata_for_2020()
    patch_get_data_for_2020()
"""

import os
import json
import urllib.request
import requests
import pandas as pd

from pyncoda.CommunitySourceData.api_census_gov.acg_00a_createAPI_datastructure import (
    createAPI_datastructure,
)
from pyncoda.CommunitySourceData.api_census_gov.acg_01a_BaseInventory import (
    BaseInventory,
)
try:
    from pyncoda.CommunitySourceData.api_census_gov.acg_00a_createAPI_datastructure import (
        group_quarters_valueLabels,
    )
except ImportError:
    group_quarters_valueLabels = {'metadata': {'label': 'gqtype'}}


# =============================================================================
# Patch 1: obtain_api_metadata — variable discovery for 2020 DHC
# =============================================================================

def obtain_api_metadata_2020_aware(
    vintage: str = "2010",
    dataset_name: str = "dec/sf1",
    group: str = "P12",
    outputfolder: str = "",
    version_text: str = 'v0-2-0',
):
    """Drop-in replacement that supports 2020 DHC variable naming."""

    datastructure_folder = outputfolder + '/00_datastructures'
    os.makedirs(datastructure_folder, exist_ok=True)
    api_datastructure_folder = outputfolder + '/00_datastructures/api_census_gov'
    os.makedirs(api_datastructure_folder, exist_ok=True)

    dict_filename = f'{group}_{vintage}_datastructure'
    dict_filepath = api_datastructure_folder + "/" + dict_filename + version_text + '.txt'

    if os.path.exists(dict_filepath):
        with open(dict_filepath) as f:
            data = f.read()
        result = json.loads(data)
        print("Dictionary file", dict_filepath, "Already exists - Skipping API Call.")
        return result

    datastructure_dict = {
        'metadata': {
            'concept': '',
            'vintage': vintage,
            'dataset_name': dataset_name,
            'group': group,
            'notes': [],
            'mutually_exclusive': True,
        }
    }

    is_2020_dhc = (dataset_name == "dec/dhc")

    if is_2020_dhc:
        for_geography = 'tract:*'
        indexvar = ['GEO_ID', 'state', 'county', 'tract']
    elif dataset_name == "dec/sf1":
        if "CT" in group:
            for_geography = 'tract:*'
            indexvar = ['GEO_ID', 'state', 'county', 'tract']
        else:
            for_geography = 'block:*'
            indexvar = ['GEO_ID', 'state', 'county', 'tract', 'block']
    else:
        for_geography = 'tract:*'
        indexvar = ['GEO_ID', 'state', 'county', 'tract']

    datastructure_dict['metadata']['for_geography'] = for_geography
    datastructure_dict['metadata']['indexvar'] = indexvar

    first_letter_of_group = group[0:1]
    if dataset_name in ("dec/sf1", "dec/dhc"):
        if first_letter_of_group == 'P':
            unit_of_analysis = 'person'
            countvar = 'preccount'
        elif first_letter_of_group == 'H':
            unit_of_analysis = 'household'
            countvar = 'hucount'
        else:
            unit_of_analysis = 'person'
            countvar = 'preccount'
    else:
        unit_of_analysis = 'household'
        countvar = 'hucount'
    datastructure_dict['metadata']['unit_of_analysis'] = unit_of_analysis
    datastructure_dict['metadata']['countvar'] = countvar

    group_api_page = f'https://api.census.gov/data/{vintage}/{dataset_name}/groups/{group}.html.'
    datastructure_dict['metadata']['notes'].append(group_api_page)
    print("Obtaining data structure for", group)
    print("Check weblink for variable list:", group_api_page)

    if dataset_name == 'dec/dhc':
        varstem = group  # 'PCT12' stays 'PCT12'
    elif dataset_name == 'dec/sf1':
        group_head = group.rstrip('0123456789')
        group_tail = group[len(group_head):]
        varstem = group_head + str(group_tail).zfill(3)
    else:
        varstem = group

    datastructure_dict[varstem] = {}

    request_error = 0
    i = 1
    while request_error == 0:
        if dataset_name == 'dec/dhc':
            variable = f"{varstem}_{str(i).zfill(3)}N"
        elif dataset_name == 'acs/acs5':
            variable = f"{varstem}_{str(i).zfill(3)}E"
        else:
            variable = varstem + str(i).zfill(3)

        variable_metadata_hyperlink = (
            f'https://api.census.gov/data/{vintage}/{dataset_name}/variables/{variable}.json'
        )

        if requests.get(variable_metadata_hyperlink, timeout=30).status_code == 404:
            request_error = 1
            print("Assume reached the end of variable list with", variable)
            break

        with urllib.request.urlopen(variable_metadata_hyperlink) as url:
            variable_metadata = json.load(url)
        i += 1

        census_concept_string = str(variable_metadata.get("concept", ""))
        if datastructure_dict['metadata']['concept'] != census_concept_string:
            datastructure_dict['metadata']['concept'] = census_concept_string

        census_label_string = str(variable_metadata.get("label", ""))

        # Strip group prefix to get the linenumber
        # 2020 DHC: PCT12_107N -> 107   (strip '_' prefix and 'N' suffix)
        # 2010 SF1: PCT012107 -> 107
        # ACS:      B18101_007E -> 007
        variable_linenumber = variable.replace(varstem, "")
        if dataset_name == 'dec/dhc':
            variable_linenumber = variable_linenumber.lstrip('_').rstrip('N')
        elif dataset_name == 'acs/acs5':
            variable_linenumber = variable_linenumber.replace("_", "").rstrip('E')

        datastructure_dict[varstem][variable_linenumber] = {}
        datastructure_dict[varstem][variable_linenumber]['label'] = census_label_string

        for substring in census_label_string.split("!!"):
            substring = substring.strip().rstrip(':').strip()
            if substring == "Total" or substring == "":
                continue
            if 'Male' in substring:
                datastructure_dict[varstem][variable_linenumber]['sex'] = 1
            if 'Female' in substring:
                datastructure_dict[varstem][variable_linenumber]['sex'] = 2
            if substring == 'Owner occupied':
                datastructure_dict[varstem][variable_linenumber]['ownershp'] = 1
            if substring == 'Renter occupied':
                datastructure_dict[varstem][variable_linenumber]['ownershp'] = 2
            if substring == 'Family households':
                datastructure_dict[varstem][variable_linenumber]['family'] = 1
            if substring == 'Nonfamily households':
                datastructure_dict[varstem][variable_linenumber]['family'] = 0
            if substring == 'Living alone':
                datastructure_dict[varstem][variable_linenumber]['numprec'] = 1
            if substring == 'Not living alone':
                datastructure_dict[varstem][variable_linenumber]['numprec'] = -999
            if substring == 'Husband-wife family':
                datastructure_dict[varstem][variable_linenumber]['sex'] = -999
                datastructure_dict[varstem][variable_linenumber]['numprec'] = -999
            if substring == 'Other family':
                datastructure_dict[varstem][variable_linenumber]['numprec'] = -999

            for value_labels in [group_quarters_valueLabels]:
                for var_label_key in value_labels.keys():
                    if var_label_key != 'metadata':
                        for value in value_labels[var_label_key].keys():
                            checkstring = value_labels[var_label_key][value].get('label', '')
                            if checkstring and checkstring in substring:
                                datastructure_dict[varstem][variable_linenumber][var_label_key] = value

            if "year" in substring.lower():
                ages = [int(x) for x in substring.split() if x.isdigit()]
                if len(ages) == 2:
                    datastructure_dict[varstem][variable_linenumber]['minageyrs'] = ages[0]
                    datastructure_dict[varstem][variable_linenumber]['maxageyrs'] = ages[1]
                elif len(ages) == 1:
                    if "Under" in substring or "under" in substring:
                        datastructure_dict[varstem][variable_linenumber]['minageyrs'] = 0
                        datastructure_dict[varstem][variable_linenumber]['maxageyrs'] = ages[0] - 1
                        if "Householder" in substring:
                            datastructure_dict[varstem][variable_linenumber]['minageyrs'] = 15
                            datastructure_dict['metadata']['notes'].append(
                                'Assume min age of household is 15 years'
                            )
                    elif "and over" in substring:
                        datastructure_dict[varstem][variable_linenumber]['minageyrs'] = ages[0]
                        datastructure_dict[varstem][variable_linenumber]['maxageyrs'] = 110
                        datastructure_dict['metadata']['notes'].append(
                            'Assume max age of 110 years'
                        )
                    else:
                        datastructure_dict[varstem][variable_linenumber]['minageyrs'] = ages[0]
                        datastructure_dict[varstem][variable_linenumber]['maxageyrs'] = ages[0]

            if "$" in substring:
                clean_sub = substring.replace(',', '').replace('$', '')
                dollars = [int(x) for x in clean_sub.split() if x.isdigit()]
                if len(dollars) == 2:
                    datastructure_dict[varstem][variable_linenumber]['mindollars'] = dollars[0]
                    datastructure_dict[varstem][variable_linenumber]['maxdollars'] = dollars[1]
                elif len(dollars) == 1:
                    if "Less than" in substring:
                        datastructure_dict[varstem][variable_linenumber]['mindollars'] = 0
                        datastructure_dict[varstem][variable_linenumber]['maxdollars'] = dollars[0] - 1
                    elif "or more" in substring:
                        datastructure_dict[varstem][variable_linenumber]['mindollars'] = dollars[0]
                        datastructure_dict[varstem][variable_linenumber]['maxdollars'] = dollars[0] + 50000
                        datastructure_dict['metadata']['notes'].append(
                            'Assume max dollars is $50,000 more than max'
                        )
                    else:
                        datastructure_dict[varstem][variable_linenumber]['mindollars'] = dollars[0]
                        datastructure_dict[varstem][variable_linenumber]['maxdollars'] = dollars[0]

    print(datastructure_dict.keys())
    max_char_count = 1
    for dict_key in datastructure_dict.keys():
        if dict_key != 'metadata':
            for variable in datastructure_dict[dict_key].keys():
                char_count = len(datastructure_dict[dict_key][variable].keys())
                if char_count > max_char_count:
                    max_char_count = char_count

    for dict_key in datastructure_dict.keys():
        if dict_key != 'metadata':
            remove_vars = []
            for var_stem_key in datastructure_dict[dict_key].keys():
                char_count = len(datastructure_dict[dict_key][var_stem_key].keys())
                if char_count < max_char_count:
                    print("Remove", var_stem_key, "from dictionary.")
                    remove_vars.append(var_stem_key)
            for remove_var in remove_vars:
                datastructure_dict[dict_key].pop(remove_var)

    for dict_key in datastructure_dict.keys():
        if dict_key != 'metadata':
            keys_in_group = list(datastructure_dict[dict_key].keys())
            if not keys_in_group:
                print(f"WARNING: No variables remain in {dict_key} after filtering.")
                continue
            first_key = keys_in_group[0]
            vars_set = datastructure_dict[dict_key][first_key].keys()
            char_vars = [v for v in vars_set if v != 'label']
            datastructure_dict['metadata']['char_vars'] = char_vars

    keys_to_check = [k for k in datastructure_dict.keys() if k != 'metadata']
    for dict_key in keys_to_check:
        number_of_vars_in_group = len(datastructure_dict[dict_key].keys())
        print("Data structure has", number_of_vars_in_group, "variables.")
        if number_of_vars_in_group > 45:
            print("Data structure for API call will have too many variables.")
            print("Census API limits data to 50 variables - including GEOID and geovars.")
            print("Need to split up varstem into parts.")
            partcount = 1
            varcount = 1
            new_data_structure_dict = {'metadata': datastructure_dict['metadata']}
            varstem_local = dict_key
            varstem_part = varstem_local + '_part' + str(partcount).zfill(2)
            new_data_structure_dict[varstem_part] = {}
            for var in datastructure_dict[dict_key].keys():
                new_data_structure_dict[varstem_part][var] = datastructure_dict[varstem_local][var]
                varcount += 1
                if varcount == 45:
                    partcount += 1
                    print("Max variables for API call reached. Starting new varstem part", partcount)
                    varcount = 1
                    varstem_part = varstem_local + '_part' + str(partcount).zfill(2)
                    new_data_structure_dict[varstem_part] = {}
            datastructure_dict = new_data_structure_dict
        else:
            print("Data structure has fewer than max variables for API call.")

    with open(dict_filepath, 'w') as convert_file:
        convert_file.write(json.dumps(datastructure_dict))

    return datastructure_dict


def patch_obtain_api_metadata_for_2020():
    createAPI_datastructure.obtain_api_metadata = staticmethod(obtain_api_metadata_2020_aware)
    print("[OK] createAPI_datastructure.obtain_api_metadata patched for 2020 DHC support")


# =============================================================================
# Patch 2: get_data_based_on_varstems_and_roots — fixed for 2020 column naming
# =============================================================================
#
# This is the original 2010 logic with TWO fixes for 2020 DHC:
#   1. The varroot keys ('_003N') concatenate naturally with the varstem
#      ('P12A' + '_003N' = 'P12A_003N') - simple concat, same as 2010 was '003'
#   2. After fetch, columns must be renamed from API name to internal name:
#      'P12A_003N' -> 'P12_003N' (stripping the race letter)
#      Strings -> int conversion for arithmetic operations
# 

def get_data_based_on_varstems_and_roots_2020_aware(
    state_county: str,
    varstems_roots_dictionary: dict = None,
    outputfolder: str = "popinv_workflow",
    outputfolders=None,
):
    if varstems_roots_dictionary is None:
        varstems_roots_dictionary = {}
    if outputfolders is None:
        outputfolders = {'TidyCommunitySourceData': 'countydata/popinv_workflow'}

    concept            = varstems_roots_dictionary['metadata']['concept']
    vintage            = varstems_roots_dictionary['metadata']['vintage']
    dataset_name       = varstems_roots_dictionary['metadata']['dataset_name']
    for_geography      = varstems_roots_dictionary['metadata']['for_geography']
    char_vars          = varstems_roots_dictionary['metadata']['char_vars']
    mutually_exclusive = varstems_roots_dictionary['metadata']['mutually_exclusive']
    countvar           = varstems_roots_dictionary['metadata']['countvar']
    group              = varstems_roots_dictionary['metadata']['group']
    unit_of_analysis   = varstems_roots_dictionary['metadata']['unit_of_analysis']

    csv_filename = f'{group}_{state_county}_{vintage}'
    csv_filepath = outputfolders['TidyCommunitySourceData'] + "/" + csv_filename + '.csv'

    if os.path.exists(csv_filepath):
        df = pd.read_csv(csv_filepath, low_memory=False)
        print("File", csv_filepath, "Already exists - Skipping API Call.")
        return df

    df_varstems = []
    for varstem in varstems_roots_dictionary:
        if varstem == 'metadata':
            continue

        varrootlist = varstems_roots_dictionary[varstem]

        print(char_vars)
        if 'byracehispan' in char_vars:
            racehispn_groups_dictionary = varstems_roots_dictionary['metadata']['byracehispan']
        else:
            racehispn_groups_dictionary = {'': {'Label': 'Race and Hispanic Not Applicable'}}

        if '_part' in varstem:
            varstem_api = varstem
            part_number = varstem[-2:]
            varstem = varstem.replace('_part' + part_number, '')
        else:
            varstem_api = varstem

        df = {}
        print("\n**********************************")
        print("Obtain data from Census API", concept)
        for racehispangroup in racehispn_groups_dictionary:
            get_vars = 'GEO_ID'
            get_var_count = 1

            varstem_race = varstem + racehispangroup

            label = racehispn_groups_dictionary[racehispangroup]['Label']
            print("    Obtaining data for", varstem_race, concept, "by", label)

            # Build the variable list — simple concatenation works for both 2010 and 2020
            #   2010: 'P12A' + '003' = 'P12A003'
            #   2020: 'P12A' + '_003N' = 'P12A_003N'
            for varroot_str in varrootlist:
                api_var = varstem_race + varroot_str
                get_vars = get_vars + ',' + api_var
                get_var_count += 1
                if get_var_count > 50:
                    print("too many variables")
                    return

            df[racehispangroup] = BaseInventory.obtain_census_api(
                state_county=state_county,
                vintage=vintage,
                dataset_name=dataset_name,
                var_stem=varstem_api + racehispangroup,
                get_vars=get_vars,
                for_geography=for_geography,
                outputfolders=outputfolders,
            )

            # Rename columns: API name -> internal name
            #   2010: 'P12A003' -> 'P12003'  (drop race letter)
            #   2020: 'P12A_003N' -> 'P12_003N'  (drop race letter from middle)
            for varroot_str in varrootlist:
                api_var    = varstem_race + varroot_str   # e.g. 'P12A_003N' or 'P12A003'
                rename_var = varstem + varroot_str        # e.g. 'P12_003N'  or 'P12003'
                if api_var in df[racehispangroup].columns:
                    # Convert to int for arithmetic (API returns strings)
                    df[racehispangroup][api_var] = df[racehispangroup][api_var].astype('int')
                    # Rename to internal name
                    df[racehispangroup] = df[racehispangroup].rename(
                        columns={api_var: rename_var}
                    )

        # Mutually exclusive logic
        if mutually_exclusive is False:
            mx_df = {}
            mutually_exclusive_dict = varstems_roots_dictionary['metadata']['mutually_exclusive_dict']
            indexvar = varstems_roots_dictionary['metadata']['indexvar']
            racehispn_groups_dictionary = varstems_roots_dictionary['metadata']['mutually_exclusive_dict']

            print("\n**********************************")
            print("Create mutually exclusive dataframe for:")
            for mxdf_key in mutually_exclusive_dict:
                label = mutually_exclusive_dict[mxdf_key]['Label']
                function = mutually_exclusive_dict[mxdf_key]['equation']
                mx_df[mxdf_key] = eval(function)
                mx_df[mxdf_key].label = label
                print("     ", label)
        else:
            mx_df = {}
            for key in df.keys():
                mx_df[key] = df[key].copy(deep=True)
                mx_df[key].label = concept

        print("\n**********************************")
        print("Reshape dataframe to convert unit of analysis")
        for key in mx_df.keys():
            geoid_count = mx_df[key].shape[0]
            print("   For mutually exclusive dataframe for:", mx_df[key].label)
            df_reshape = BaseInventory.reshape_geoid_to_countvar(
                wide_df=mx_df[key],
                newvar='precode',
                stem=varstem,
                countvar=countvar,
            )
            new_count = df_reshape.shape[0]
            geo_level = for_geography.replace(':*', '')
            print("       Unit of analysis converted from", geoid_count, geo_level, "s, to",
                  new_count, unit_of_analysis)

            for char_var in char_vars:
                if char_var not in ['byracehispan']:
                    df_reshape[char_var] = df_reshape['precode'].apply(
                        lambda x: varrootlist[x][char_var]
                    )
                if char_var == 'byracehispan':
                    df_reshape['race'] = racehispn_groups_dictionary[key]['race']
                    df_reshape['hispan'] = racehispn_groups_dictionary[key]['hispan']

            df_varstems.append(df_reshape)

    df_return = pd.concat(df_varstems)
    df_return = df_return.drop(columns=['precode'])

    savefile = os.path.join(os.getcwd(), csv_filepath)
    df_return.to_csv(savefile, index=False)

    return df_return


def patch_get_data_for_2020():
    BaseInventory.get_data_based_on_varstems_and_roots = staticmethod(
        get_data_based_on_varstems_and_roots_2020_aware
    )
    print("[OK] BaseInventory.get_data_based_on_varstems_and_roots patched for 2020 DHC support")
