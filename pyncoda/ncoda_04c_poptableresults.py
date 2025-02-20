import numpy as np
import pandas as pd
from pyncoda.ncoda_00d_cleanvarsutils import *


'''
    Dictionaries with conditions for labeling categorical variables
'''
gqtype_valuelabels = {'categorical_variable': {'variable' : 'gqtype',
                            'variable_label' : 'Group Quarters Type',
                            'notes' : ''},
                'value_list' : {
                    0 : {'value': 0, 'value_label' : '0. NA (non-group quarters)'},
                    1 : {'value': 1, 'value_label': "1 Correctional facilities for adults"},
                    2 : {'value': 2, 'value_label': "2. Juvenile facilities"},
                    3 : {'value': 3, 'value_label': "3. Nursing facilities/Skilled-nursing facilities"},
                    4 : {'value': 4, 'value_label': "4. Other institutional facilities"},
                    5 : {'value': 5, 'value_label': "5. College/University student housing"},
                    6 : {'value': 6, 'value_label': "6. Military quarters"},
                    7 : {'value': 7, 'value_label': "7. Other noninstitutional facilities"}}
                }

race_valuelabels = {'categorical_variable': {'variable' : 'race',
                            'variable_label' : 'Race',
                            'notes' : 'Race categories based on 2010 Census'},
                'value_list' : {
                    0 : {'value': 0, 'value_label' : '0. Missing'},
                    1 : {'value': 1, 'value_label': '1. White'},
                    2 : {'value': 2, 'value_label': '2. Black or African American'},
                    3 : {'value': 3, 'value_label': '3. American Indian and Alaska Native'},
                    4 : {'value': 4, 'value_label': '4. Asian'},
                    5 : {'value': 5, 'value_label': '5. Native Hawaiian and Other Pacific Islander'},
                    6 : {'value': 6, 'value_label': '6. Some Other Race'},
                    7 : {'value': 7, 'value_label': '7. Two or More Races'}}
        }

hispan_valuelabels = {'categorical_variable': {'variable' : 'hispan',
                            'variable_label' : 'Hispanic',
                            'notes' : 'Hispanic Origin'},
                'value_list' : {
                    0 : {'value': 0, 'value_label' : '0 Not Hispanic or Latino'},
                    1 : {'value': 1, 'value_label': '1 Hispanic or Latino'}}
                }

class PopResultsTable:
    """Utility methods for Population related data:
        Housing Unit Inventory
        Person Record Inventory
        Job inventory
        Housing Unit Allocation
        Creates tables for data exploration and visualization
    """
    @staticmethod
    def visualize(dataset, **kwargs):
        """visualize Population related dataframe.
        Args:
            dataset (obj): Housing unit Inventory,
                Person Record, or Job Inventory dataset object.
            kwargs (kwargs): Keyword arguments for visualization title.
            who (str): Who does the data represent (unit of analysis):
                options: Total Households,
                         Total Population by Householder.
                         Total Population by Persons
                         Total Jobs
                         Median Household Income
            what (str): What does the data represent ex: "by Race, Ethnicity"
            when (str): What year is the source data ex: "2010"
            where (str): Where does the data represent ex: "Robeson County, NC"
            row_index (str): Variable to label rows ex: 'Race Ethnicity
            col_index (str): Variable to label columns ex: 'Tenure'
            row_percent (str): Variable to calculate row percentage
                ex: "1 Dislocates" to see percentage dislocation by row
        Returns:
            None
        """
        pop_df = pd.read_csv(dataset.get_file_path('csv'), header="infer", low_memory=False)
        table = PopResultsTable.pop_results_table(pop_df, **kwargs)

        return table

    @staticmethod
    def add_race_ethnicity_to_pop_df(df):
        """add race and ethnicity information to Pop dataframe.
        Args:
            df (obj): Pandas DataFrame object.
        Returns:
            object: Pandas DataFrame object.
        """

        # Ensure 'race' and 'hispan' columns are numeric for comparison
        df['race'] = pd.to_numeric(df['race'], errors='coerce')
        df['hispan'] = pd.to_numeric(df['hispan'], errors='coerce')
        df['gqtype'] = pd.to_numeric(df['hispan'], errors='coerce')

        df['Race Ethnicity'] = "0 Vacant HU No Race Ethnicity Data"
        df['Race Ethnicity'].notes = "Identify Race and Ethnicity Housing Unit Characteristics."

        df.loc[(df['race'] == 1) & (df['hispan'] == 0), 'Race Ethnicity'] = "1 White alone, Not Hispanic"
        df.loc[(df['race'] == 2) & (df['hispan'] == 0), 'Race Ethnicity'] = "2 Black alone, Not Hispanic"
        df.loc[(df['race'] == 3) & (df['hispan'] == 0), 'Race Ethnicity'] = "3 American Indian and Alaska " \
                                                                            "Native alone, Not Hispanic"
        df.loc[(df['race'] == 4) & (df['hispan'] == 0), 'Race Ethnicity'] = "4 Asian alone, Not Hispanic"
        df.loc[(df['race'].isin([5, 6, 7])) & (df['hispan'] == 0), 'Race Ethnicity'] = "5 Other Race, Not Hispanic"
        df.loc[(df['hispan'] == 1), 'Race Ethnicity'] = "6 Any Race, Hispanic"

        # Check if group quarters variable is in dataframe
        if 'gqtype' in df.columns:
            df.loc[(df['gqtype'] >= 1) & (df['Race Ethnicity'] == "0 Vacant HU No Race Ethnicity Data"), 'Race Ethnicity'] \
                = "7 Group Quarters no Race Ethnicity Data"
        # Set variable to missing if structure is vacant - makes tables look nicer
        df.loc[(df['Race Ethnicity'] == "0 Vacant HU No Race Ethnicity Data"), 'Race Ethnicity'] = np.nan

        return df

    @staticmethod
    def add_vacancy_to_pop_df(df):
        """add vacancy information to Pop dataframe.
        Args:
            df (obj): Pandas DataFrame object.
        Returns:
            object: Pandas DataFrame object.
        """

        df['Vacancy Type'] = "0 Occupied Housing Unit"
        df['Vacancy Type'].notes = "Identify Vacancy Type Housing Unit Characteristics."

        df.loc[(df['vacancy'] == 1), 'Vacancy Type'] = "1 For Rent"
        df.loc[(df['vacancy'] == 2), 'Vacancy Type'] = "2 Rented, not occupied"
        df.loc[(df['vacancy'] == 3), 'Vacancy Type'] = "3 For sale only"
        df.loc[(df['vacancy'] == 4), 'Vacancy Type'] = "4 Sold, not occupied"
        df.loc[(df['vacancy'] == 5), 'Vacancy Type'] = "5 For seasonal, recreational, or occasional use"
        df.loc[(df['vacancy'] == 6), 'Vacancy Type'] = "6 For migrant workers"
        df.loc[(df['vacancy'] == 7), 'Vacancy Type'] = "7 Other vacant"
        # Set variable to missing if structure is occupied - makes tables look nicer
        df.loc[(df['Vacancy Type'] == "0 Occupied Housing Unit"), 'Vacancy Type'] = np.nan

        return df

    @staticmethod
    def add_tenure_to_pop_df(df):
        """add tenure information to Pop dataframe.
        Args:
            df (obj): Pandas DataFrame object.
        Returns:
            object: Pandas DataFrame object.
        """

        df['Tenure Status'] = "0 No Tenure Status"
        df['Tenure Status'].notes = "Identify Renter and Owner Occupied Housing Unit Characteristics."

        df.loc[(df['ownershp'] == 1), 'Tenure Status'] = "1 Owner Occupied"
        df.loc[(df['ownershp'] == 2), 'Tenure Status'] = "2 Renter Occupied"
        # Set variable to missing if structure is vacant - makes tables look nicer
        df.loc[(df['Tenure Status'] == "0 No Tenure Status"), 'Tenure Status'] = np.nan

        return df

    @staticmethod
    def add_family_to_pop_df(df):
        """add family status information to population dataframe.
        Args:
            df (obj): Pandas DataFrame object.
        Returns:
            object: Pandas DataFrame object.
        """

        df['Family Type'] = "0 No Family Data"
        df['Family Type'].notes = "Identify Family and Non-Family Characteristics."

        df.loc[(df['family'] == 1), 'Family Type'] = "1 Family Household"
        df.loc[(df['family'] == 0), 'Family Type'] = "0 Non-Family Household"
        # Set variable to missing if structure is vacant - makes tables look nicer
        df.loc[(df['Family Type'] == "0 No Family Data"), 'Family Type'] = np.nan

        return df

    @staticmethod
    def add_dislocates_pd_df(df):
        """
        Population dislocation requires data on building damage and
        population characteristics
        If the observation does not have building data then population dislocation
        is set to missing.
        Args:
            df (obj): Pandas DataFrame object.
        Returns:
            object: Pandas DataFrame object.
        """
        df['Population Dislocation'] = "No Data"
        df['Population Dislocation'].notes = "Identify Population Dislocation."

        df.loc[(df['dislocated'] == False) & (df['guid'].notnull()), 'Population Dislocation'] = "0 Does not dislocate"
        df.loc[(df['dislocated'] == True) & (df['guid'].notnull()), 'Population Dislocation'] = "1 Dislocates"

        # Set dislocates to missing if no building data- makes tables look nicer
        df.loc[(df['Population Dislocation'] == "No Data"), 'Population Dislocation'] = np.nan

        return df

    @staticmethod
    def add_jobtype_df(df):
        """add job type information to dataframe.
        Args:
            df (obj): Pandas DataFrame object.
        Returns:
            object: Pandas DataFrame object.
        """

        df['Job Type'] = "0 No Job Type Information"
        df['Job Type'].notes = "Identify Job Type Characteristics."

        df.loc[(df['jobtype'] == 'JT03'), 'Job Type'] = "Private Primary Jobs"
        df.loc[(df['jobtype'] == 'JT09'), 'Job Type'] = "Private Non-primary Jobs"
        df.loc[(df['jobtype'] == 'JT05'), 'Job Type'] = "Federal Primary Jobs"
        df.loc[(df['jobtype'] == 'JT10'), 'Job Type'] = "Federal Non-primary Jobs"
        df.loc[(df['jobtype'] == 'JT07'), 'Job Type'] = "Public Sector Primary Jobs"
        df.loc[(df['jobtype'] == 'JT11'), 'Job Type'] = "Public Sector Non-primary Jobs"
        # Set variable to missing if structure is occupied - makes tables look nicer
        df.loc[(df['Job Type'] == "0 No Job Type Information"), 'Job Type'] = np.nan

        return df

    @staticmethod
    def add_industrycode_df(df):
        """add industry code information to dataframe.
        Args:
            df (obj): Pandas DataFrame object.
        Returns:
            object: Pandas DataFrame object.
        """

        df['NAICS Industry Sector'] = "0 No NAICS Industry Sector"
        df['NAICS Industry Sector'].notes = "Identify NAICS Industry Sector."

        df.loc[(df['IndustryCode'] == 1), 'NAICS Industry Sector'] = "11 Agriculture, Forestry, Fishing and Hunting"
        df.loc[(df['IndustryCode'] == 2), 'NAICS Industry Sector'] = "21 Mining, Quarrying, and Oil and Gas Extraction"
        df.loc[(df['IndustryCode'] == 3), 'NAICS Industry Sector'] = "22 Utilities"
        df.loc[(df['IndustryCode'] == 4), 'NAICS Industry Sector'] = "23 Construction"
        df.loc[(df['IndustryCode'] == 5), 'NAICS Industry Sector'] = "31-33 Manufacturing"
        df.loc[(df['IndustryCode'] == 6), 'NAICS Industry Sector'] = "42 Wholesale Trade"
        df.loc[(df['IndustryCode'] == 7), 'NAICS Industry Sector'] = "44-45 Retail Trade"
        df.loc[(df['IndustryCode'] == 8), 'NAICS Industry Sector'] = "48-49 Transportation and Warehousing"
        df.loc[(df['IndustryCode'] == 9), 'NAICS Industry Sector'] = "51 Information"
        df.loc[(df['IndustryCode'] == 10), 'NAICS Industry Sector'] = "52 Finance and Insurance"
        df.loc[(df['IndustryCode'] == 11), 'NAICS Industry Sector'] = "53 Real Estate and Rental and Leasing"
        df.loc[(df['IndustryCode'] == 12), 'NAICS Industry Sector'] = "54 Professional, Scientific, and Technical " \
                                                                      "Services"
        df.loc[(df['IndustryCode'] == 13), 'NAICS Industry Sector'] = "55 Management of Companies and Enterprises"
        df.loc[(df['IndustryCode'] == 14), 'NAICS Industry Sector'] = "56 Administration & Support, Waste Management " \
                                                                      "and Remediation"
        df.loc[(df['IndustryCode'] == 15), 'NAICS Industry Sector'] = "61 Educational Services"
        df.loc[(df['IndustryCode'] == 16), 'NAICS Industry Sector'] = "62 Health Care and Social Assistance"
        df.loc[(df['IndustryCode'] == 17), 'NAICS Industry Sector'] = "71 Arts, Entertainment, and Recreation"
        df.loc[(df['IndustryCode'] == 18), 'NAICS Industry Sector'] = "72 Accommodation and Food Services"
        df.loc[(df['IndustryCode'] == 19), 'NAICS Industry Sector'] = "81 Other Services " \
                                                                      "(excluding Public Administration)"
        df.loc[(df['IndustryCode'] == 20), 'NAICS Industry Sector'] = "92 Public Administration"
        # Set variable to missing if structure is occupied - makes tables look nicer
        df.loc[(df['NAICS Industry Sector'] == "0 No NAICS Industry Sector"), 'NAICS Industry Sector'] = np.nan

        return df

    @staticmethod
    def add_colpercent(df, sourcevar, formatedvar):
        """add column percentage to help visualize data.
        Args:
            df (obj): Pandas DataFrame object.
            sourcevar (obj): Pandas Pivottable Column object.
            formatedvar (str): Column name.
        Returns:
            object: Pandas DataFrame object.
        """

        df['%'] = (df[sourcevar] / (df[sourcevar].sum()/2) * 100)
        df['(%)'] = df.agg('({0[%]:.1f}%)'.format, axis=1)
        df['value'] = df[sourcevar]
        df['format value'] = df.agg('{0[value]:,.0f}'.format, axis=1)
        df[formatedvar] = df['format value'] + '\t ' + df['(%)']

        df = df.drop(columns=[sourcevar, '%', '(%)', 'value', 'format value'])

        return df


    @staticmethod
    def pop_results_table(input_df, **kwargs):
        """Explore and visualize population data with a formatted table.
        Args:
            df (obj): Pandas DataFrame object.
            kwargs (kwargs): Keyword arguments for visualization title.
            who (str): Who does the data represent (unit of analysis):
                options: Total Households,
                         Total Population by Householder.
                         Total Population by Persons
                         Total Jobs
                         Median Household Income
            what (str): What does the data represent ex: "by Race, Ethnicity"
            when (str): What year is the source data ex: "2010"
            where (str): Where does the data represent ex: "Robeson County, NC"
            row_index (str): Variable to label rows ex: 'Race Ethnicity
            col_index (str): Variable to label columns ex: 'Tenure'
            row_percent (str): Variable to calculate row percentage
                ex: "1 Dislocates" to see percentage dislocation by row
        Returns:
            object: Styled Table Pandas DataFrame object.
        """

        # Create copy of input dataframe
        # Copy prevents original dataframe from being altered
        df = input_df.copy()

        who = ""
        what = ""
        when = ""
        where = ""
        row_index = "race"
        col_index = "hispan"
        row_percent = ""

        if "who" in kwargs.keys():
            who = kwargs["who"]
        if "what" in kwargs.keys():
            what = kwargs["what"]
        if "when" in kwargs.keys():
            when = kwargs["when"]
        if "where" in kwargs.keys():
            where = kwargs["where"]
        if "row_index" in kwargs.keys():
            row_index = kwargs["row_index"]
        if "col_index" in kwargs.keys():
            col_index = kwargs["col_index"]
        if "row_percent" in kwargs.keys():
            row_percent = kwargs["row_percent"]

        # check current column list and add categorical descriptions
        current_col_list = list(df.columns)
        #print(current_col_list)
        # Add labels to variable categories = makes tables easier to read
        if all(col in current_col_list for col in ['race', 'hispan']):
            #print("Add race ethnicity labels") 
            df = PopResultsTable.add_race_ethnicity_to_pop_df(df)
        if 'race' in current_col_list:
            #print("Add race labels")
            df = add_label_cat_values_df(df, valuelabels = race_valuelabels)
        if 'hispan' in current_col_list:
            #print("Add hispanic labels")
            df = add_label_cat_values_df(df, valuelabels = hispan_valuelabels)
        if 'ownershp' in current_col_list:
            df = PopResultsTable.add_tenure_to_pop_df(df)
        if 'vacancy' in current_col_list:
            df = PopResultsTable.add_tenure_to_pop_df(df)
        if all(col in current_col_list for col in ['guid', 'dislocated']):
            df = PopResultsTable.add_dislocates_pd_df(df)
        if 'jobtype' in current_col_list:
            df = PopResultsTable.add_jobtype_df(df)
        if 'family' in current_col_list:
            df = PopResultsTable.add_family_to_pop_df(df)
        if 'IndustryCode' in current_col_list:
            df = PopResultsTable.add_industrycode_df(df)
        if 'hhinc' in current_col_list:
            df = PopResultsTable.add_hhinc_df(df)
        if 'poverty' in current_col_list:
            df = PopResultsTable.add_poverty_df(df)
        if 'gqtype' in current_col_list:
            df = add_label_cat_values_df(df, valuelabels = gqtype_valuelabels)

        #print("Set up who, what, when, where")
        if who == "Total Households":
            variable = 'huid'
            function = 'count'
            renamecol = {'Total': who, 'sum': ''}
            num_format = "{:,.0f}"
        elif who == "Total Population by Households":
            variable = 'numprec'
            function = 'sum'
            renamecol = {'Total': who, 'sum': ''}
            num_format = "{:,.0f}"
        elif who == "Total Population by Persons":
            variable = 'precid'
            function = 'count'
            renamecol = {'Total': who, 'sum': ''}
            num_format = "{:,.0f}"
        elif who == "Total Jobs":
            variable = 'uniquejobid'
            function = 'count'
            renamecol = {'Total': who, 'sum': ''}
            num_format = "{:,.0f}"
        elif who == "Median Household Income":
            variable = 'randincome'
            function = 'median'
            renamecol = {'Total': who}
            num_format = "${:,.0f}"
        else:
            variable = 'huid'
            function = 'count'
            renamecol = {'Total': who, 'sum': ''}
            num_format = "{:,.0f}"

        # Generate table
        table = pd.pivot_table(df, values=variable, index=[row_index],
                               margins=True, margins_name='Total',
                               columns=[col_index], aggfunc=function).rename(columns=renamecol)
        table_title = "Table. " + who + " " + what + ", " + where + ", " + when + "."
        varformat = {(who): num_format}
        for col in table.columns:
            varformat[col] = num_format

        # Add percent row column
        if row_percent != '':
            numerator = table[row_percent]
            denominator = table[who]
            table['row_pct'] = numerator/denominator * 100
            table['Percent Row ' + '\n' + row_percent] = \
                table.agg('{0[row_pct]:.1f}%'.format, axis=1)
            table = table.drop(columns=['row_pct'])

        # Add Column Percents
        if "Total" in who:
            # add column percent to all columns except the percent row column
            row_pct_vars = [col for col in table if col.startswith('Percent Row ')]
            columns = [col for col in table if col not in row_pct_vars]
            for col in columns:
                formated_column_name = col + ' (%)'
                table = PopResultsTable.add_colpercent(table, col, formated_column_name)

        # Move row percent to last column
        if row_percent != '':
            row_pct_vars = [col for col in table if col.startswith('Percent Row ')]
            columns = [col for col in table if col not in row_pct_vars]
            table = table[columns + row_pct_vars]

        # Caption Title Style
        styles = [dict(selector="caption",
                       props=[("text-align", "center"), ("caption-side", "top"),  ("font-size", "150%")])]

        table = table.style \
            .set_caption(table_title) \
            .set_table_styles(styles) \
            .format(varformat)

        return table

    @staticmethod
    def add_hhinc_df(df):
        """add Household Income Group information to Pop dataframe.
        Args:
            df (obj): Pandas DataFrame object.
        Returns:
            object: Pandas DataFrame object.
        """
        df['Household Income Group'] = "No Data"
        df['Household Income Group'].notes = "Identify Household Income Groups Housing Unit Characteristics."
        df.loc[(df['hhinc'] == 1), 'Household Income Group'] = "1 Less than $15,000"
        df.loc[(df['hhinc'] == 2), 'Household Income Group'] = "2 $15,000 to $24,999"
        df.loc[(df['hhinc'] == 3), 'Household Income Group'] = "3 $25,000 to $74,999"
        df.loc[(df['hhinc'] == 4), 'Household Income Group'] = "4 $75,000 to $99,999"
        df.loc[(df['hhinc'] == 5), 'Household Income Group'] = "5 $100,000 or more"
        # Set variable to missing if no data- makes tables look nicer
        df.loc[(df['Household Income Group'] == "No Data"),
               'Household Income Group'] = np.nan
        return df

    @staticmethod
    def add_poverty_df(df):
        """add poverty status information to Pop dataframe.
        Args:
            df (obj): Pandas DataFrame object.
        Returns:
            object: Pandas DataFrame object.
        """
        df['Poverty Status'] = "No Data"
        df['Poverty Status'].notes = "Identify Poverty Status Housing Unit Characteristics."
        df.loc[(df['poverty'] == 0), 'Poverty Status'] = "0 At or above poverty level"
        df.loc[(df['poverty'] == 1), 'Poverty Status'] = "1 Below poverty level"
        # Set variable to missing if no data- makes tables look nicer
        df.loc[(df['Poverty Status'] == "No Data"),
               'Poverty Status'] = np.nan
        return df

'''
CODE TO INCLUDE IN YOUR SCRIPT
from
https://github.com/npr99/IN-CORE_notebooks/blob/main/Galveston_testbed_2022-09-19.ipynb

# Add household income group categories for table
# Code will be added to next release of pop_results_table on pyincore-viz

hhinc_valuelabels = {'categorical_variable': {'variable' : 'hhinc',
                                'variable_label' : 'Household Income Group',
                                'notes' : '5 Household Income Groups based on random income.'},
                    'value_list' : {
                        1 : {'value': 1, 'value_label': "1 Less than $15,000"},
                        2 : {'value': 2, 'value_label': "2 $15,000 to $24,999"},
                        3 : {'value': 3, 'value_label': "3 $25,000 to $74,999"},
                        4 : {'value': 4, 'value_label': "4 $75,000 to $99,999"},
                        5 : {'value': 5, 'value_label': "5 $100,000 or more"}}
                    }

pd_df = add_label_cat_values_df(pd_df, valuelabels = hhinc_valuelabels)

ds3_conditions = {'cat_var' : {'variable_label' : 'Probability Complete Failure',
                         'notes' : 'Probability of complete failure based on damage state 3'},
              'condition_list' : {
                1 : {'condition': "(df['DS_3'] == 0)", 'value_label': "0 0%"},
                2 : {'condition': "(df['DS_3'] > 0)", 'value_label': "1 Less than 20%"},
                3 : {'condition': "(df['DS_3'] > .2)", 'value_label': "2 20-40%"},
                4 : {'condition': "(df['DS_3'] > .4)", 'value_label': "3 40-60%"},
                5 : {'condition': "(df['DS_3'] > .6)", 'value_label': "4 60-80%"},
                6 : {'condition': "(df['DS_3'] > .8)", 'value_label': "5 80-100%"},
                7 : {'condition': "(df['DS_3'] == 1)", 'value_label': "6 100%"}}
            }

      
dsf_valuelabels = {'categorical_variable' : {'variable' : 'd_sf',
                   'variable_label' : 'Single Family Dwelling',
                   'notes' : 'Categories for single family dwellings'},
              'value_list' : {
                1 : {'value': 0, 'value_label': "0 Not Single Family"},
                2 : {'value': 1, 'value_label': "1 Single Family"}}
            }
pd_df = add_label_cat_conditions_df(pd_df, conditions = ds3_conditions)
pd_df = add_label_cat_values_df(pd_df, valuelabels = dsf_valuelabels)
'''