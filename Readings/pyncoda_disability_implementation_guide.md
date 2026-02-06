# Extending pyncoda: Adding Disability Characteristics to PREC (Person Records)

**Date:** February 5, 2026  
**Discussion Topic:** Understanding pyncoda workflows and adding disability status from ACS data
Nathanael Rosenheim guiding Claude Sonnet 4.5 in VS Code Plan and Agent mode.

## Overview

This document explains how pyncoda disaggregates tract-level American Community Survey (ACS) data to block-level household or person observations using random merge techniques. It provides guidance for extending the Person Record (PREC) workflow to include disability characteristics from Census Tables such as [B18101](https://api.census.gov/data/2012/acs/acs5/groups/B18101.html) and related tables.

## Background Question

**Initial Request:** Explain how household income is added to the housing unit inventory and how to extend pyncoda to include disability status.

**Key Clarification:** Disability characteristics should be added to **PREC (person-level)** records, not HUA (household-level) records, using ACS Tables such as the B18101 series.

---

## Part 1: Understanding the Two Workflows

### HUA Workflow (Housing Unit Analysis)
- **Unit of Observation:** Housing units (households)
- **Primary Key:** `huid` (Housing Unit ID)
- **Main File:** [acg_05a_hui_functions.py](../pyncoda/CommunitySourceData/api_census_gov/acg_05a_hui_functions.py)
- **Characteristics Added:** 
  - Tenure (owner/renter)
  - Household size (numprec)
  - Race of householder
  - Hispanic ethnicity
  - Family status
  - **Household income** (from ACS B19001/B19101)

### PREC Workflow (Person Record Analysis)
- **Unit of Observation:** Individual persons
- **Primary Key:** `precid` (Person Record ID)
- **Main File:** [acg_05b_prec_functions.py](../pyncoda/CommunitySourceData/api_census_gov/acg_05b_prec_functions.py)
- **Characteristics Added:**
  - Sex
  - Age (ranges, then specific age)
  - Race
  - Hispanic ethnicity
  - **Disability status** ← *This is where we're adding new functionality*

_NOTE_ Current PREC only working for the 2010 data. PREC needs to be extended to run for the 2020 files.
---

## Part 2: HUA Workflow - Household Income Example

### Workflow Summary (Lines 105-241 in acg_05a_hui_functions.py)

#### Step 1: Create Block-Level Housing Units (Lines 142-149)
```python
block_df['core'] = BaseInventory.get_apidata(
    state_county = self.state_county, 
    geo_level = 'Block',
    vintage = str(self.basevintage),
    mutually_exclusive_varstems_roots_dictionaries = [
        tenure_size_H16_varstem_roots,
        vacancy_status_H5_varstem_roots,
        group_quarters_P42_varstem_roots
    ]
)
```
- **Function:** [BaseInventory.get_apidata()](../pyncoda/CommunitySourceData/api_census_gov/acg_01a_BaseInventory.py#L221-L263)
- **Core Function:** [get_data_based_on_varstems_and_roots()](../pyncoda/CommunitySourceData/api_census_gov/acg_01a_BaseInventory.py#L20-L217)
- **Process:** Expands block-level counts to individual housing unit records
- **Output:** Each row = one housing unit with tenure, size, race, vacancy status

#### Step 2: Add Family Characteristics (Lines 151-158)
```python
block_df['family'] = BaseInventory.graft_on_new_char(
    base_inventory = block_df['core'],
    new_char = 'family',
    new_char_dictionaries = [family_byrace_P18_varstem_roots]
)
```
- **Function:** [graft_on_new_char()](../pyncoda/CommunitySourceData/api_census_gov/acg_01a_BaseInventory.py#L555-L900)
- **Process:** Probabilistically assigns family status based on race/Hispanic distributions
- **Data Structure:** [family_byrace_P18_varstem_roots](../pyncoda/CommunitySourceData/api_census_gov/acg_00b_hui_block2010.py#L171-L193)

#### Step 3: Add Hispanic Characteristics (Lines 161-168)
Uses multiple cross-tabulations to refine Hispanic coding

#### Step 4: Retrieve Tract-Level Household Income (Lines 170-177)
```python
tract_df["B19001"] = BaseInventory.get_apidata(
    state_county = self.state_county,
    geo_level = 'tract',
    vintage = str(int(self.basevintage)+2),
    mutually_exclusive_varstems_roots_dictionaries = [hhinc_B19001_varstem_roots_2022]
)
```
- **Census Table:** B19001 - Household Income by Race/Hispanic
- **Data Structure:** [acg_00d_hhinc_ACS5yr2022.py](../pyncoda/CommunitySourceData/api_census_gov/acg_00d_hhinc_ACS5yr2022.py)
- **Variables:** 16 income groups × 9 race/Hispanic groups
- **Geography:** Tract level (too sparse at block level)

#### Step 5: Retrieve Tract-Level Family Income (Lines 179-186)
```python
tract_df["B19101"] = BaseInventory.get_apidata(
    geo_level = 'tract',
    mutually_exclusive_varstems_roots_dictionaries = [hhincfamily_B19101_varstem_roots_2022]
)
```
- **Census Table:** B19101 - Family Income by Race/Hispanic
- **Purpose:** Distinguish family vs non-family households within income groups

#### Step 6: Random Merge Income Datasets (Tract→Household) (Lines 188-213)
```python
income_by_family = add_new_char_by_random_merge_2dfs(
    dfs = {
        'primary': {'data': tract_df['B19001']},     # All households
        'secondary': {'data': tract_df['B19101']}    # Family households
    },
    common_group_vars = ['incomegroup'],
    new_char = 'family',
    by_groups = {'All': {'by_variables': ['race','hispan']}}
)
```
- **Class:** [add_new_char_by_random_merge_2dfs](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L17-L1008)
- **Purpose:** Add family status to income records
- **Matching:** Within same tract + race + Hispanic + income group
- **Output:** Household income data with family characteristic added
- **Flags Created:** 
  - `family_flagsetrm`: Overall flag (0 or 1)
  - `family_Tract2010_flagsetrm`: Round-specific flag
  - Code: [Lines 603-639](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L603-L639)

#### Step 7: Random Merge Income to Block Data (Tract→Block→Household) (Lines 215-241)
```python
block_income = add_new_char_by_random_merge_2dfs(
    dfs = {
        'primary': {'data': block_df['hispan']},              # Block households
        'secondary': {'data': tract_income_match['primary']}  # Tract income
    },
    common_group_vars = ['family'],
    new_char = 'incomegroup',
    by_groups = {
        'Hispanic': {'by_variables': ['hispan']},
        'not Hispanic': {'by_variables': ['race']}
    },
    reuse_secondary = True  # Tract data can match multiple block households
)
```
- **Geographic Matching:** Extract Tract2010 from Block2010 ID (first 11 digits)
- **Stratification:** Hispanic households by ethnicity, non-Hispanic by race
- **Multi-Round Process:** Progressive relaxation of matching criteria if needed
- **Flags Created:** Multiple tracking flags monitor assignment status:
  - `{new_char}_flagsetrm`: Overall flag indicating characteristic was assigned (1) or not (0)
  - `{new_char}_{geolevel}_flagsetrm`: Geographic-specific flag for each merge round
  - Example: `incomegroup_flagsetrm`, `incomegroup_Tract2010_flagsetrm`
  - Created in [setup_run_random_merge_2dfs()](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L603-L639)
  - Updated in [merge_groups()](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L445-L485) after each successful merge
  - Prevents re-assignment in subsequent rounds

### Key Random Merge Process

The **random merge** is the critical innovation that enables disaggregation:

1. **Add Geographic IDs:** Extract tract from block ID
   - Example: `Tract2010` from first 11 digits of `Block2010` ID
   - Code: [setup_run_random_merge_2dfs()](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L528-L639)

2. **Add Primary Keys:** Unique identifiers for tracking
   - Ensures each record can be matched and verified
   - Examples: `huid` (households), `precid` (persons), `uniqueidB19001` (tract income)

3. **Initialize Flags:** Create tracking variables before merge
   - `{new_char}_flagsetrm = 0` for all records (not yet assigned)
   - `{new_char}_{geolevel}_flagsetrm = 0` for geographic-specific tracking
   - Code: [Lines 603-639 in acg_02a_add_categorical_char.py](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L603-L639)

4. **Random Sort:** Within each stratification group (tract + demographics)
   - Uses `np.random.seed()` for reproducibility
   - Code: [prepare_randommerge()](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L93-L166)

5. **Assign Merge Order:** Sequential counter within groups
   - Variable: `random_mergeorder` = row number within group
   - Enables 1:1 matching between geography levels

6. **Join:** Match on geography + demographics + merge order
   - Left merge from primary to secondary dataset
   - Code: [merge_groups()](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L445-L485)

7. **Update Flags:** Mark successfully matched records
   - Set `{new_char}_flagsetrm = 1` for assigned records
   - Set `{new_char}_{geolevel}_flagsetrm = round_number`
   - Code: [Lines 475-485 in merge_groups()](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L475-L485)

8. **Validate:** Check merge quality
   - Count matched vs unmatched records
   - Verify no duplicate matches occurred
   - Report match rates by geography

9. **Multiple Rounds:** If some records unmatched, relax criteria and repeat
   - Only merge records where `{new_char}_flagsetrm == 0` (not yet assigned)
   - Progressive relaxation: drop age group, then sex, then geography level
   - Code: [run_random_merge_2dfs()](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L885-L1008)

**Files to Reference:**
- [setup_run_random_merge_2dfs()](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L528-L639)
- [prepare_randommerge()](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L93-L166)
- [merge_groups()](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L445-L485)
- [run_random_merge_2dfs()](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L885-L1008)

### Flag Naming Convention and Examples

**Standard Flag Pattern:**
```python
{new_char}_flagsetrm           # Overall assignment flag (binary: 0 or 1)
{new_char}_{geolevel}_flagsetrm  # Round-specific flag (integer: 0, 1, 2, 3...)
```

**Concrete Examples:**

**HUA Income Workflow:**
- `incomegroup_flagsetrm`: Indicates household received income assignment
- `incomegroup_Tract2010_flagsetrm`: Records which round assigned income
  - 0 = Not assigned
  - 1 = Assigned in Round 1 (Tract + race + hispan + family)
  - 2 = Assigned in Round 2 (Tract + race + hispan)
  - 3 = Assigned in Round 3 (Tract + race)
  - 4 = Assigned in Round 4 (Tract only)

**PREC Age Workflow:**
- `randagePCT12_flagsetrm`: Person received specific age
- `randagePCT12_Tract2010_flagsetrm`: Round number for age assignment

**PREC Disability Workflow (to be implemented):**
- `disability_flagsetrm`: Person received disability status
- `disability_Tract2010_flagsetrm`: Round number for disability assignment
  - 1 = Assigned in Round 1 (Tract + sex + agegroupB18101)
  - 2 = Assigned in Round 2 (Tract + sex)
  - 3 = Assigned in Round 3 (Tract only)

**Flag Creation Code:**
```python
# In setup_run_random_merge_2dfs() - Lines 603-639
# Initialize overall flag
df_primary[f'{new_char}_flagsetrm'] = 0

# Initialize geographic flag for each level
for geolevel in geolevel_list:
    df_primary[f'{new_char}_{geolevel}_flagsetrm'] = 0

# After merge in merge_groups() - Lines 475-485
# Update flags for matched records
df_primary.loc[matched_indices, f'{new_char}_flagsetrm'] = 1
df_primary.loc[matched_indices, f'{new_char}_{current_geolevel}_flagsetrm'] = round_number
```

**Using Flags in Analysis:**
```python
# Check assignment quality
print(f"Overall match rate: {(df['disability_flagsetrm'] == 1).mean():.2%}")

# Check distribution across rounds
round_dist = df['disability_Tract2010_flagsetrm'].value_counts().sort_index()
print("Assignments by round:")
for round_num, count in round_dist.items():
    if round_num > 0:
        print(f"  Round {round_num}: {count:,} persons ({count/len(df):.2%})")

# Subset analysis by assignment method
high_precision = df[df['disability_Tract2010_flagsetrm'] == 1]  # Best matches
low_precision = df[df['disability_Tract2010_flagsetrm'] >= 3]   # County-level matches
```

---

## Part 3: PREC Workflow - Person Records with Disability

### Workflow Summary (Lines 86-212 in acg_05b_prec_functions.py)

#### Step 1: Create Block-Level Person Records (Lines 105-112)
```python
block_df['preci'] = BaseInventory.get_apidata(
    state_county = self.state_county,
    geo_level = 'Block',
    vintage = str(self.basevintage),
    mutually_exclusive_varstems_roots_dictionaries = [sexbyage_P12_varstem_roots]
)
```
- **Census Table:** P12 - Sex by Age (Decennial Census)
- **Data Structure:** [sexbyage_P12_varstem_roots](../pyncoda/CommunitySourceData/api_census_gov/acg_00f_preci_block2010.py#L207-L228)
- **Process:** Expands block counts to individual person records
- **Output:** Each row = one person with `sex`, `minageyrs`, `maxageyrs`, `race`

#### Step 2: Add Hispanic Ethnicity (Lines 114-120)
```python
block_df['precihispan'] = BaseInventory.graft_on_new_char(
    base_inventory = block_df['preci'],
    new_char = 'hispan',
    new_char_dictionaries = [sexbyage_P12HAI_varstem_roots, hispan_P5_varstem_roots]
)
```
- **Process:** Uses graft method (different from random merge)
- **Flags Created:** 
  - `hispan_flagsetprob`: Probabilistic assignment flag
  - `hispan_Block2010_flagsetprob`: Block-level assignment tracking
  - Different flag naming for graft vs random merge methods

#### Step 3: Retrieve Tract-Level Detailed Age Data (Lines 123-137)
```python
tract_df["PCT12"] = BaseInventory.get_apidata(
    geo_level = 'tract',
    mutually_exclusive_varstems_roots_dictionaries = [sexbyage_PCT12_varstem_roots]
)
```
- **Census Table:** PCT12 - Sex by Single-Year Age
- **Purpose:** Provides specific ages (not just ranges) at tract level
- **Geography:** Tract (more detailed than block P12)

#### Step 4: Add Age Groups for Matching (Lines 140-161)
```python
# Add random age within block range
block_df["precihispan"] = add_randageP12(block_df["precihispan"], self.seed)

# Add age group for matching
block_df["precihispan"] = add_P12age_groups(block_df["precihispan"], 'randageP12')
```
- **Functions:** [add_randageP12()](../pyncoda/CommunitySourceData/api_census_gov/acg_02c_agefunctions.py#L4-L22), [add_P12age_groups()](../pyncoda/CommunitySourceData/api_census_gov/acg_02c_agefunctions.py#L25-L69)
- **Purpose:** Create common age group variable for tract-block matching

#### Step 5: Random Merge Tract Age to Block Persons (Lines 163-205)
```python
add_age = add_new_char_by_random_merge_2dfs(
    dfs = {
        'primary': {'data': block_df["precihispan"], 'primarykey': 'precid'},
        'secondary': {'data': tract_df["PCT12"], 'primarykey': 'uniqueidPCT12'}
    },
    common_group_vars = ['agegroupP12'],
    new_char = 'randagePCT12',
    by_groups = {'All': {'by_variables': ['sex']}}
)
```
- **Matching:** Tract + sex + age group + random order
- **Output:** Person records with specific age from tract distribution
- **Flags Created:**
  - `randagePCT12_flagsetrm`: Indicates if specific age assigned (0 or 1)
  - `randagePCT12_Tract2010_flagsetrm`: Tracks which round assigned the age
  - Used to prevent re-assignment in subsequent rounds
- **This is the pattern to follow for disability!**

---

## Part 4: Implementation Guide - Adding Disability to PREC

### Census Tables to Use

From https://api.census.gov/data/2012/acs/acs5/variables.html:

| Table | Description | Cross-Tabulations |
|-------|-------------|-------------------|
| [**B18101**](https://api.census.gov/data/2012/acs/acs5/groups/B18101.html) | Sex by Age by Disability Status | Sex × Age (6 groups) × Disability |
| **B18101A-I** | Age by Disability Status by Race | Race × Age × Disability *(no sex)* |
| **B18102** | Sex by Age by Hearing Difficulty | Sex × Age × Hearing |
| **B18103** | Sex by Age by Vision Difficulty | Sex × Age × Vision |
| **B18104** | Sex by Age by Cognitive Difficulty | Sex × Age × Cognitive |
| **B18105** | Sex by Age by Ambulatory Difficulty | Sex × Age × Ambulatory |
| **B18106** | Sex by Age by Self-Care Difficulty | Sex × Age × Self-Care |
| **B18107** | Sex by Age by Independent Living Difficulty | Sex × Age × Independent Living |

**B18101 Age Groups:**
1. Under 5 years (0-4)
2. 5 to 17 years
3. 18 to 34 years
4. 35 to 64 years
5. 65 to 74 years
6. 75 years and over

**B18101 Sex Groups:**
1. Male
2. Female

### Step-by-Step Implementation

#### 1. Create Data Structure File

**New File:** `pyncoda/CommunitySourceData/api_census_gov/acg_00h_disability_ACS5yr2012.py`

```python
"""
Disability Characteristics from ACS 5-year Data
Census Tract Level Data for ACS 2012 (centered on 2010)
API: https://api.census.gov/data/2012/acs/acs5/variables.html
"""

# Age groups for B18101 tables
disability_age_groups_dict = {
    1: {'minageyrs': 0, 'maxageyrs': 4, 'label': 'Under 5 years'},
    2: {'minageyrs': 5, 'maxageyrs': 17, 'label': '5 to 17 years'},
    3: {'minageyrs': 18, 'maxageyrs': 34, 'label': '18 to 34 years'},
    4: {'minageyrs': 35, 'maxageyrs': 64, 'label': '35 to 64 years'},
    5: {'minageyrs': 65, 'maxageyrs': 74, 'label': '65 to 74 years'},
    6: {'minageyrs': 75, 'maxageyrs': 110, 'label': '75 years and over'}
}

# Main disability table
disability_B18101_varstem_roots = {
    'metadata': {
        'concept': 'SEX BY AGE BY DISABILITY STATUS',
        'char_vars': ['sex', 'agegroupB18101', 'disability'],
        'new_char': 'disability',
        'group': 'B18101',
        'vintage': '2012',
        'dataset_name': 'acs/acs5',
        'for_geography': 'tract:*',
        'unit_of_analysis': 'person',
        'mutually_exclusive': True,
        'indexvar': ['GEO_ID', 'state', 'county', 'tract'],
        'countvar': 'preccount',
        'notes': 'ACS Table B18101 - Disability status by sex and age'
    },
    'B18101': {
        # Male variables
        '004E': {'sex': 1, 'agegroupB18101': 1, 'disability': 1, 
                 'label': 'Male!!Under 5 years!!With a disability'},
        '005E': {'sex': 1, 'agegroupB18101': 1, 'disability': 0, 
                 'label': 'Male!!Under 5 years!!No disability'},
        '007E': {'sex': 1, 'agegroupB18101': 2, 'disability': 1, 
                 'label': 'Male!!5 to 17 years!!With a disability'},
        '008E': {'sex': 1, 'agegroupB18101': 2, 'disability': 0, 
                 'label': 'Male!!5 to 17 years!!No disability'},
        # ... continue for all age groups and sex
        # Female variables start around 023E
    }
}

# Specific disability types (optional extension)
disability_B18102_varstem_roots = {
    'metadata': {
        'concept': 'SEX BY AGE BY HEARING DIFFICULTY',
        'char_vars': ['sex', 'agegroupB18102', 'hearing_difficulty'],
        'new_char': 'hearing_difficulty',
        'group': 'B18102',
        # ... similar structure
    }
}
# Similarly for B18103-B18107
```

**Pattern Reference:** Follow structure in [acg_00d_hhinc_ACS5yr2022.py](../pyncoda/CommunitySourceData/api_census_gov/acg_00d_hhinc_ACS5yr2022.py)

#### 2. Create Age Group Mapping Function

**Modify:** `pyncoda/CommunitySourceData/api_census_gov/acg_02c_agefunctions.py`

**Add Function:**
```python
def add_B18101age_groups(input_df, varname):
    """
    Add age groups for ACS Table B18101 (Disability)
    
    Parameters:
    - input_df: DataFrame with person records
    - varname: Column containing age (e.g., 'randagePCT12')
    
    Returns:
    - DataFrame with new column 'agegroupB18101'
    """
    output_df = input_df.copy()
    
    agegroupB18101_dict = {
        1: {'minageyrs': 0, 'maxageyrs': 4},
        2: {'minageyrs': 5, 'maxageyrs': 17},
        3: {'minageyrs': 18, 'maxageyrs': 34},
        4: {'minageyrs': 35, 'maxageyrs': 64},
        5: {'minageyrs': 65, 'maxageyrs': 74},
        6: {'minageyrs': 75, 'maxageyrs': 110}
    }
    
    output_df['agegroupB18101'] = 0  # Default for missing
    
    for agegroup in agegroupB18101_dict:
        min_age = agegroupB18101_dict[agegroup]['minageyrs']
        max_age = agegroupB18101_dict[agegroup]['maxageyrs']
        
        conditions = (output_df[varname] >= min_age) & (output_df[varname] <= max_age)
        output_df.loc[conditions, 'agegroupB18101'] = agegroup
    
    return output_df
```

**Pattern Reference:** Similar to [add_P12age_groups()](../pyncoda/CommunitySourceData/api_census_gov/acg_02c_agefunctions.py#L25-L69)

#### 3. Modify PREC Workflow

**File:** `pyncoda/CommunitySourceData/api_census_gov/acg_05b_prec_functions.py`

**Location:** After line 205 (after age assignment completed)

**Add Imports at Top:**
```python
from pyncoda.CommunitySourceData.api_census_gov.acg_00h_disability_ACS5yr2012 import *
```

**Add Code Block:**
```python
# After line 205 in run_prec_workflow()

print("\n***************************************")
print("    Add Disability Characteristics from ACS Tract Data")
print("***************************************\n")

# Retrieve tract-level disability data
tract_df["B18101"] = BaseInventory.get_apidata(
    state_county = self.state_county,
    geo_level = 'tract',
    vintage = str(int(self.basevintage)+2),  # e.g., 2012 for 2010 blocks
    mutually_exclusive_varstems_roots_dictionaries = [disability_B18101_varstem_roots],
    outputfolders = self.outputfolders,
    outputfile = "B18101_disability"
)

# Add B18101 age groups to person records
print("Adding B18101 age groups for disability matching...")
prec_age_df['primary'] = add_B18101age_groups(
    prec_age_df['primary'],
    varname = 'randagePCT12'
)

# Also add to tract data if needed
tract_df["B18101"] = add_B18101age_groups(
    tract_df["B18101"],
    varname = 'randagePCT12'  # Adjust based on tract data structure
)

# Random merge disability data to person records
print("Random merging disability data...")
add_disability = add_new_char_by_random_merge_2dfs(
    dfs = {
        'primary': {
            'data': prec_age_df['primary'],
            'primarykey': 'precid',
            'geolevel': 'Block',
            'geovintage': str(self.basevintage),
            'notes': 'Person-level data without disability'
        },
        'secondary': {
            'data': tract_df["B18101"],
            'primarykey': 'uniqueidB18101',
            'geolevel': 'Tract',
            'geovintage': str(self.basevintage),
            'notes': 'Tract-level disability counts by sex and age'
        }
    },
    seed = self.seed,
    common_group_vars = ['agegroupB18101'],
    new_char = 'disability',
    geolevel = "Tract",
    geovintage = str(self.basevintage),
    by_groups = {'All': {'by_variables': ['sex']}},
    fillna_value = -999,
    state_county = self.state_county,
    outputfile = "prec_disability",
    outputfolder = self.outputfolders['RandomMerge'],
    savefiles = self.savefiles
)

# Set up round options
rounds = add_disability.make_round_options_dict()

# Run multi-round random merge
prec_disability_df = add_disability.run_random_merge_2dfs(rounds)

# Flags automatically created:
# - disability_flagsetrm: Overall assignment flag (0 = not assigned, 1 = assigned)
# - disability_Tract2010_flagsetrm: Round-specific flag (0, 1, 2, 3, etc.)
# These track which persons received disability assignments and in which round

# Update return variable to include disability
prec_age_df = prec_disability_df
```

#### 4. Update PREC Data Structure

**File:** `pyncoda/CommunitySourceData/api_census_gov/acg_00g_prec_datastructure.py`

**Add Variable Definitions:**
```python
'disability': {
    'label': 'Disability Status',
    'DataType': 'Int',
    'pyType': 'category',
    'categorical': True,
    'AnalysisUnit': 'Person',
    'MeasureUnit': 'Person',
    'categories_dict_v2': {
        0: {'longlabel': '0. No disability', 'shortlabel': 'No disability'},
        1: {'longlabel': '1. With a disability', 'shortlabel': 'With disability'},
        -999: {'longlabel': '-999. Unable to predict', 'shortlabel': 'Unknown'}
    },
    'notes': '\n'.join([
        '1. Disability predicted from ACS 5-year Table B18101.',
        '2. Random merge based on tract, sex, and B18101 age group.',
        '3. Values of -999 indicate inability to predict.',
        '4. Verify results by comparing to ACS Table B18101 tract totals.'
    ]),
    'primary_key': 'precid',
    'source': 'ACS 5-year',
    'source_table': 'B18101'
},

'agegroupB18101': {
    'label': 'Age Group (B18101 categories)',
    'DataType': 'Int',
    'pyType': 'category',
    'categorical': True,
    'categories_dict_v2': {
        1: {'longlabel': 'Under 5 years', 'minageyrs': 0, 'maxageyrs': 4},
        2: {'longlabel': '5 to 17 years', 'minageyrs': 5, 'maxageyrs': 17},
        3: {'longlabel': '18 to 34 years', 'minageyrs': 18, 'maxageyrs': 34},
        4: {'longlabel': '35 to 64 years', 'minageyrs': 35, 'maxageyrs': 64},
        5: {'longlabel': '65 to 74 years', 'minageyrs': 65, 'maxageyrs': 74},
        6: {'longlabel': '75 years and over', 'minageyrs': 75, 'maxageyrs': 110}
    },
    'notes': 'Age groups matching ACS Table B18101 structure'
}
```

---

## Part 5: Validation and Testing

### Validation Strategy

1. **Aggregate Back to Tract Level**
   ```python
   # Check disability totals by tract
   validation = output_df.groupby(['Tract2010', 'disability']).size().reset_index(name='count')
   
   # Compare with original ACS data
   # Should match within sampling error
   ```

2. **Cross-Tabulation Checks**
   ```python
   # Disability by sex
   pd.crosstab(output_df['sex'], output_df['disability'], margins=True)
   
   # Disability by age group
   pd.crosstab(output_df['agegroupB18101'], output_df['disability'], margins=True)
   
   # Disability by race
   pd.crosstab(output_df['race'], output_df['disability'], margins=True)
   ```

3. **Flag Variable Checks**
   ```python
   # Check what percentage of persons have disability assigned
   flag_summary = output_df['disability_flagsetrm'].value_counts()
   print(f"Disability assignment rate: {flag_summary[1] / len(output_df) * 100:.2f}%")
   
   # Check which round assigned disability (0 = not assigned, 1 = round 1, etc.)
   round_summary = output_df['disability_Tract2010_flagsetrm'].value_counts().sort_index()
   print("Assignments by round:")
   print(round_summary)
   
   # Check for -999 values (unable to predict)
   missing_disability = (output_df['disability'] == -999).sum()
   print(f"Persons without disability prediction: {missing_disability}")
   
   # Cross-check: flag=0 should correspond to disability=-999
   unassigned = (output_df['disability_flagsetrm'] == 0).sum()
   assert unassigned == missing_disability, "Flag and -999 count mismatch!"
   ```

4. **Reproducibility Test**
   ```python
   # Run with different seeds
   # Aggregate statistics should be similar but individual assignments different
   ```

### Expected Results

- **Match Rate:** >95% of person records should receive disability assignment (not -999)
  - Check: `(df['disability_flagsetrm'] == 1).mean()` should be > 0.95
- **Tract Totals:** Should match ACS B18101 within 1-2% (accounting for sampling error)
- **Distribution:** Disability rates should vary by age group (higher for older ages)
- **Flag Validation:**
  - All records should have either `disability_flagsetrm = 0` or `= 1`
  - Records with `flagsetrm = 0` should have `disability = -999`
  - Records with `flagsetrm = 1` should have valid disability values (0 or 1)
  - Round flags should show most assignments in early rounds (Round 1 preferred)

---

## Part 6: Key Technical Considerations

### 1. Age Group Alignment Challenge

**Issue:** B18101 age groups don't perfectly match P12/PCT12 age groups

| B18101 Groups | P12/PCT12 Groups |
|---------------|------------------|
| 0-4 | 0-4, 5-9 (split needed) |
| 5-17 | 5-9, 10-14, 15-17 |
| 18-34 | 18-19, 20, 21, ..., 34 |
| 35-64 | Large range, many P12 groups |
| 65-74 | 65-66, 67-69, 70-74 |
| 75+ | 75-79, 80-84, 85+ |

**Solution:** Use `randagePCT12` (specific age in years) to assign `agegroupB18101` - this bypasses P12 grouping issues

### 2. Multi-Round Merging

Like the income workflow, disability merging should use multiple rounds:

**Round 1:** Match on [Tract + Sex + Age Group B18101]  
- Most precise matching
- Flags updated: `disability_Tract2010_flagsetrm = 1` for matched records
- Only unmatched records (`disability_flagsetrm = 0`) proceed to Round 2

**Round 2:** Match on [Tract + Sex] (drop age group)  
- Broader matching for unassigned records
- Flags updated: `disability_Tract2010_flagsetrm = 2`

**Round 3:** Match on [Tract] only (drop sex)  
- Even broader for remaining unmatched records
- Flags updated: `disability_Tract2010_flagsetrm = 3`

**Round 4:** Match on [County] (if still unmatched)
- Final attempt at county level
- Flags updated: `disability_County_flagsetrm = 4`

**Flag Tracking Logic:**
- Records with `{new_char}_flagsetrm = 1` are skipped in subsequent rounds
- Geographic flags (`{geolevel}_flagsetrm`) record which round succeeded
- Code implementation: [run_random_merge_2dfs()](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py#L885-L1008)

This ensures maximum coverage while maintaining best possible accuracy and complete audit trail.

### 3. Vintage Mismatch

**Block Data:** 2010 or 2020 Decennial Census  
**ACS Data:** 5-year estimates centered on different year

**Options:**
- **Option A:** Use 2012 ACS 5-year (2010-2014) for 2010 blocks
- **Option B:** Use 2022 ACS 5-year (2020-2024) for 2020 blocks  
- **Option C:** Use latest ACS for current disability rates (accept temporal mismatch)

**Recommendation:** Match vintages when possible (Option A/B)

### 4. Specific Disability Types (B18102-B18107)

**If Implementing All Tables:**
- Run separate random merges for each disability type
- Use same matching strategy (sex + age)
- Result: Multiple disability columns per person
- Can create composite disability measures

**Single Table Approach:**
- Start with B18101 only (overall disability)
- Simpler, covers most use cases
- Can extend later if needed

### 5. Race-Specific Validation

Use B18101A-I tables for validation:
- These have Race × Age × Disability (no sex)
- Can't use for primary merge (missing sex cross-tab)
- Useful for checking race-specific disability rates
- Compare aggregate rates by race

---

## Part 7: File Summary and References

### Files to Create

| File | Purpose |
|------|---------|
| `acg_00h_disability_ACS5yr2012.py` | Data structures for B18101 series (2010 vintage) |
| `acg_00h_disability_ACS5yr2022.py` | Data structures for B18101 series (2020 vintage) |

### Files to Modify

| File | Lines | Changes |
|------|-------|---------|
| [acg_05b_prec_functions.py](../pyncoda/CommunitySourceData/api_census_gov/acg_05b_prec_functions.py) | After 205 | Add disability merge workflow |
| [acg_02c_agefunctions.py](../pyncoda/CommunitySourceData/api_census_gov/acg_02c_agefunctions.py) | End | Add `add_B18101age_groups()` |
| [acg_00g_prec_datastructure.py](../pyncoda/CommunitySourceData/api_census_gov/acg_00g_prec_datastructure.py) | Variables section | Add disability variable definitions |

### Key Reference Files (Do Not Modify - Use as Patterns)

| File | Purpose |
|------|---------|
| [acg_05a_hui_functions.py](../pyncoda/CommunitySourceData/api_census_gov/acg_05a_hui_functions.py) | HUA workflow pattern (income example) |
| [acg_05b_prec_functions.py](../pyncoda/CommunitySourceData/api_census_gov/acg_05b_prec_functions.py) | PREC workflow pattern (age example) |
| [acg_00d_hhinc_ACS5yr2022.py](../pyncoda/CommunitySourceData/api_census_gov/acg_00d_hhinc_ACS5yr2022.py) | Data structure pattern for ACS tables |
| [acg_01a_BaseInventory.py](../pyncoda/CommunitySourceData/api_census_gov/acg_01a_BaseInventory.py) | Core data retrieval functions |
| [acg_02a_add_categorical_char.py](../pyncoda/CommunitySourceData/api_census_gov/acg_02a_add_categorical_char.py) | Random merge engine |
| [acg_02c_agefunctions.py](../pyncoda/CommunitySourceData/api_census_gov/acg_02c_agefunctions.py) | Age group functions pattern |

---

## Part 8: Next Steps and Questions

### Implementation Checklist

- [ ] Research Census API variable codes for B18101 (check 2012 and 2022 APIs)
- [ ] Create `acg_00h_disability_ACS5yr2012.py` with complete variable mappings
- [ ] Add `add_B18101age_groups()` function to `acg_02c_agefunctions.py`
- [ ] Test age group function with sample data
- [ ] Modify `acg_05b_prec_functions.py` to add disability merge workflow
- [ ] Add disability variables to `acg_00g_prec_datastructure.py`
- [ ] Run test with single county (small geography)
- [ ] Validate tract-level aggregations match ACS
- [ ] Test reproducibility with multiple seeds
- [ ] Document methodology and validation results
- [ ] (Optional) Extend to B18102-B18107 for specific difficulty types

### Open Questions for Implementation

1. **Which ACS vintage?** 2012 5-year or accept temporal mismatch with 2022?
2. **Single table or full suite?** Start with B18101 only or implement all B18101-B18107?
3. **Age group granularity:** Accept B18101's 6 age groups or attempt finer disaggregation?
4. **Race validation:** Use B18101A-I for validation or skip?
5. **Output schema:** Add to existing PREC output or create separate disability-enhanced version?

---

## Conclusion

The random merge approach used in pyncoda enables disaggregation of tract-level ACS data to block-level person records while preserving statistical distributions. The same pattern used for household income (HUA workflow) and detailed age (PREC workflow) can be applied to disability characteristics.

**Key Success Factors:**
1. Use existing person records with sex and age already assigned
2. Map ages to B18101 categories using `add_B18101age_groups()`
3. Random merge on Tract + Sex + Age Group B18101
4. Multiple rounds ensure high match rates
5. Validate by aggregating back to tract level

**The methodology is proven and extensible** - disability is a natural fit for the PREC workflow structure.

---

## Additional Resources

- **Census API Documentation:** https://api.census.gov/data.html
- **ACS 2012 Variables:** https://api.census.gov/data/2012/acs/acs5/variables.html
- **ACS 2022 Variables:** https://api.census.gov/data/2022/acs/acs5/variables.html
- **Disability Tables Guide:** https://www.census.gov/topics/health/disability/guidance/data-collection-acs.html

---

**Document prepared:** February 5, 2026  
**Author:** Discussion with GitHub Copilot (Claude Sonnet 4.5)  
**Purpose:** Guide for extending pyncoda to include disability characteristics from ACS data
