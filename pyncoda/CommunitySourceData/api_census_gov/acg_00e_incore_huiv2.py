"""
Data Structure for Housing Unit Inventory
used in pyincore v.0.9.0

Trying to follow Data Documentation Initiative Controlled Vocabularies:
https://ddialliance.org/controlled-vocabularies

pyType = Python Type - needed to set correct data type in Python

"""

incore_v2_DataStructure = {
    'huid'  : 
    {   'label' : 'Housing Unit ID', 
        'DataType'  : 'String',
        'pyType' : str,
        'AnalysisUnit' : 'Housing unit',
        'MeasureUnit' : 'Housing units',
        'notes' : '\n'.join([
            '1. Housing Unit ID is the Primary Key for the HU Inventory. '
            'Use HUID to sort data before Random Merge Order is assigned. \n \n'
            '2. One Housing Unit could be an occupied house, '
            'a single unit in a multi-family structure, a vacant house or '
            'a group quarters with a number of persons. \n \n'
            '3. The Housing Unit ID is a combination of the census block ID, '
            'the letter H and a cumulative counter for housing units in the block.'
                ])},
    'blockid' : 
    {   'label' : 'Block ID' , 
        'huiv3-0-0' : 'Block2010',
        'formula' : "\n".join([
                        "output_df['blockid']."
                        "apply(lambda x :"
                        "str(int(x)).zfill(15))"]),
        'DataType'  : 'String',
        'pyType' : str,
        'AnalysisUnit' : 'Geographic unit',
        'MeasureUnit' : 'Housing unit in census block',
        'length' : 15,
        'zero_padded' : True,
        'notes' :
            '1. 2010 Census Block ID'},
    'bgid'    : 
    {   'label' : '2010 Census Block Group ID', 
        'huiv3-0-0' : 'BlockGroup2010', 
        'formula' : "\n".join([
                        "output_df['blockid']."
                        "apply(lambda x :"
                        "str(int(x)).zfill(15)[0:12])"]),
        'DataType'  : 'String',
        'pyType' : str,
        'AnalysisUnit' : 'Geographic unit',
        'MeasureUnit' : 'Housing unit in census block group',
        'length' : 12,
        'zero_padded' : True,
        'notes' : '\n'.join([
            '1. 12 Character String that contains 2010 census block group ID. \n \n'
            '2. Foreign Key with Block Group Data for population dislocation model.'
                ])},
    'tractid' : {
        'label' : '2010 Census Tract ID', 
        'huiv3-0-0' : 'Tract2010',
        'formula' : "\n".join([
                        "output_df['blockid']."
                        "apply(lambda x :"
                        "str(int(x)).zfill(15)[0:11])"]),
        'DataType'  : 'String',
        'pyType' : str,
        'AnalysisUnit' : 'Geographic unit',
        'MeasureUnit' : 'Housing unit in census tract',
        'length' : 11,
        'zero_padded' : True,
        'notes' :
            '1. 11 Character String that contains 2010 census tract ID.'},
    'FIPScounty': 
    {   'label' : 'County FIPS Code', 
        'huiv3-0-0' : 'County2010',
        'formula' : "\n".join([
                        "output_df['blockid']."
                        "apply(lambda x :"
                        "str(int(x)).zfill(15)[0:5])"]),
        'DataType'  : 'String',
        'pyType' : str,
        'AnalysisUnit' : 'Geographic unit',
        'MeasureUnit' : 'Housing unit in county',
        'length' : 5,
        'zero_padded' : True,
        'notes' : 
            '1. 5 digit County FIPS code. Every county in the US has a unique code.'},
    'numprec' : 
    {   'label' : 'Number of Person Records',
        'DataType'  : 'Int',
        'pyType' : int,
        'AnalysisUnit' : 'Housing unit',
        'MeasureUnit' : 'Persons',
        'notes' : '\n'.join([
            '1. For occupied housing units the max numprec is 7. \n \n'
            '2. Households with more than 7 people could have higher numprec values. \n \n'
            '3. Housing units with numprec = 0 are vacant - not occupied housing units. \n \n'
            '4. Housing units with numprec > 7 are group quarters. '
            'Numprec represents the number of people in the group quarters. '
                ])},
    'ownershp' : 
    {   'label' : 'Tenure Status',
        'DataType'  : 'Int',
        'pyType' : "category",
        'categorical' : True,
        'AnalysisUnit' : 'Household',
        'MeasureUnit' : 'Housing unit',
        'categories_dict' : {
            1 : '1. Owned or being bought (loan)',
            2 : '2. Rented'},
        'categories' : 
        [   '1. Owned or being bought (loan)',
            '2. Rented'],
        'notes' : '\n'.join([
            '1. Based on 2010 Census SF1 Table H16. \n \n'
            '2. Tenure status is not applicable for vacant not occupied housing units. \n \n'
            '3. Tenure status is not applicable for group quarters. \n \n'
            '4. To verify results compare table to: \n \n'
            'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=DECENNIALSF12010.H16.'
                ]),
        'primary_key' : 'huid',
        'pop_var' : 'numprec'},
    'race' : 
    {   'label' : 'Race of Householder',
        'DataType'  : 'Int',
        'pyType' : "category",
        'categorical' : True,
        'AnalysisUnit' : 'Householder',
        'MeasureUnit' : 'Person',
        'categories_dict' : {
            1 : '1. White alone', 
            2 : '2. Black or African American alone', 
            3 : '3. American Indian and Alaska Native alone', 
            4 : '4. Asian alone' ,
            5 : '5. Native Hawaiian and Other Pacific Islander alone', 
            6 : '6. Some Other Race alone',
            7 : '7. Two or More Races'},
        'categories_dict_v2' : {
            1 : {'longlabel' : '1. White alone', 'shortlabel' : 'White'},
            2 : {'longlabel' : '2. Black or African American alone', 'shortlabel' : 'Black'},
            3 : {'longlabel' : '3. American Indian and Alaska Native alone','shortlabel' :  'American Indian'},
            4 : {'longlabel' : '4. Asian alone' , 'shortlabel' : 'Asian'},
            5 : {'longlabel' : '5. Native Hawaiian and Other Pacific Islander alone', 'shortlabel' : 'Pacific Islander'},
            6 : {'longlabel' : '6. Some Other Race alone', 'shortlabel' : 'Some Other Race'},
            7 : {'longlabel' : '7. Two or More Races', 'shortlabel' : 'Two or More Races'}},
        'categories' : 
        [   '1. White alone', 
            '2. Black or African American alone', 
            '3. American Indian and Alaska Native alone', 
            '4. Asian alone' ,
            '5. Native Hawaiian and Other Pacific Islander alone', 
            '6. Some Other Race alone',
            '7. Two or More Races'],
        'short_labels' : 
        [   'White', 
            'Black', 
            'American Indian', 
            'Asian' ,
            'Pacific Islander', 
            'Some Other Race',
            'Two or More Races'],
        'notes' : '\n'.join([
            '1. Race is based on the race of the householder. \n \n'
            '2. Race of householder is not applicable for vacant not occupied housing units. \n \n'
            '3. Race is missing for population living in group quarters. \n \n'
            '4. Race is based on 2010 Census SF1 Tables H16 by race. \n \n'
            '5. To verify results compare table to: \n \n'
            'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=DECENNIALSF12010.H6.'
            ]),
        'primary_key' : 'huid',
        'pop_var' : 'numprec'},
    'hispan' : 
    {   'label' : 'Hispanic Householder',
        'DataType'  : 'Int',
        'pyType' : "category",
        'categorical' : True,
        'AnalysisUnit' : 'Householder',
        'MeasureUnit' : 'Person',
        'categories_dict' : {
            0 : '0. Householder Not Hispanic',
            1 : '1.  Householder Hispanic or Latino'},
        'categories_dict_v2' : {
            0 : {'longlabel' : '0. Householder Not Hispanic', 'shortlabel' : 'Not Hispanic'},
            1 : {'longlabel' : '1.  Householder Hispanic or Latino', 'shortlabel' : 'Hispanic'}},
        'categories' : 
        [   '0. Householder Not Hispanic', 
            '1. Householder Hispanic or Latino'],
        'short_labels' : 
        [   'Not Hispanic', 
            'Hispanic or Latino'],
        'notes' : '\n'.join([
            '1. Hispanic is based on the ethnicity of the householder. \n \n'
            '2. Hispanic is missing for vacant not occupied housing units. \n \n'
            '3. Hispanic is missing for population living in group quarters.\n \n'
            '4. Hispanic is predicted based on 2010 Census SF1 Tables H7, H15, and H16. \n \n'
            '5. Hispanic totals will not match the US Census data, but they should be close. \n \n'
            '6. To verify results compare table to: \n \n'
            'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=DECENNIALSF12010.H7.'
            ]),
        'primary_key' : 'huid',
        'pop_var' : 'numprec'},
    'family' : 
    {   'label' : 'Family Household',
        'DataType'  : 'Int',
        'pyType' : "category",
        'categorical' : True,
        'AnalysisUnit' : 'Household',
        'MeasureUnit' : 'Household',
        'categories_dict' : {
            0 : '0. Nonfamily Household',
            1 : '1.  Family Household'},
        'categories_dict_v2' : {
            0 : {'longlabel' : '0. Nonfamily Household', 'shortlabel' : 'Nonfamily'},
            1 : {'longlabel' : '1. Family Household', 'shortlabel' : 'Family'}},
        'categories' : 
        [   '0. Nonfamily Household', 
            '1. Family Household'],
        'short_labels' : 
        [   'Nonfamily', 
            'Family'],
        'notes' : '\n'.join([
            '1. Family household is based on 2010 Census SF1 Tables P18 by race and ethnicity. \n \n'
            '2. To verify results compare table to: \n \n'
            'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=DECENNIALSF12010.P18.'
            ]),
        'primary_key' : 'huid',
        'pop_var' : 'numprec'},
    'vacancy' : 
    {   'label' : 'Vacancy Type',
        'DataType'  : 'Int',
        'pyType' : "category",
        'categorical' : True,
        'AnalysisUnit' : 'Housing unit',
        'MeasureUnit' : 'Housing unit',
        'categories_dict' : {
            0 : '0. NA Not Vacant', 
            1 : '1. For rent',
            2 : '2. Rented, not occupied',
            3 : '3. For sale only',
            4 : '4. Sold, not occupied',
            5 : '5. For seasonal, recreational, or occasional use',
            6 : '6. For migrant workers',
            7 : '7. Other vacant'},
        'categories' : 
        [   '0. NA Not Vacant', 
            '1. For rent',
            '2. Rented, not occupied',
            '3. For sale only',
            '4. Sold, not occupied',
            '5. For seasonal, recreational, or occasional use',
            '6. For migrant workers',
            '7. Other vacant'],
        'notes' : '\n'.join([
            '1.  Based on 2010 Census SF1 Table H5. \n \n'
            '2. To verify results compare table to: \n \n'
            'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=DECENNIALSF12010.H5.'
            ]),
        'primary_key' : 'huid',
        'pop_var' : 'numprec'},
    'gqtype' : 
    {   'label' : 'Group Quarters Type',
        'DataType'  : 'Int',
        'pyType' : "category",
        'categorical' : True,
        'AnalysisUnit' : 'Group quarters',
        'MeasureUnit' : 'Housing unit',
        'categories_dict' : {
            0 : '0. NA (non-group quarters)', 
            1 : '1. Correctional facilities for adults',
            2 : '2. Juvenile facilities',
            3 : '3. Nursing facilities/Skilled-nursing facilities',
            4 : '4. Other institutional facilities',
            5 : '5. College/University student housing',
            6 : '6. Military quarters',
            7 : '7. Other noninstitutional facilities'},
        'categories' : 
        [   '0. NA (non-group quarters households)', 
            '1. Correctional facilities for adults',
            '2. Juvenile facilities',
            '3. Nursing facilities/Skilled-nursing facilities',
            '4. Other institutional facilities',
            '5. College/University student housing',
            '6. Military quarters',
            '7. Other noninstitutional facilities'],
        'notes' : '\n'.join([
            '1. Based on 2010 Census SF1 Table P42. \n \n'
            '2. Counts represent number of group quarters. '
                'Sum by numprec to get population counts. \n \n'
            '3. To verify results compare table to: \n \n'
            'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=DECENNIALSF12010.P42.'
            ]),
        'primary_key' : 'huid',
        'pop_var' : 'numprec'},
    'incomegroup' : 
    {   'label' : 'Detailed Household Income Group',
        'DataType'  : 'Int',
        'pyType' : "category",
        'categorical' : True,
        'AnalysisUnit' : 'Household',
        'MeasureUnit' : '2012 INFLATION-ADJUSTED DOLLARS',
        'categories_dict' : {
            0 : '0. NA vacant or group quarters',
            1 : '1. Less than $10,000',
            2 : '2. $10,000 to $14,999',
            3 : '3. $15,000 to $19,999',
            4 : '4. $20,000 to $24,999',
            5 : '5. $25,000 to $29,999',
            6 : '6. $30,000 to $34,999',
            7 : '7. $35,000 to $39,999',
            8 : '8. $40,000 to $44,999',
            9 : '9. $45,000 to $49,999',
            10 : '10. $50,000 to $59,999',
            11 : '11. $60,000 to $74,999',
            12 : '12. $75,000 to $99,999',
            13 : '13. $100,000 to $124,999',
            14 : '14. $125,000 to $149,999',
            15 : '15. $150,000 to $199,999',
            16 : '16. $200,000 or more'},
        'categories' : 
        [   '0. NA vacant or group quarters',
            '1. Less than $10,000',
            '2. $10,000 to $14,999',
            '3. $15,000 to $19,999',
            '4. $20,000 to $24,999',
            '5. $25,000 to $29,999',
            '6. $30,000 to $34,999',
            '7. $35,000 to $39,999',
            '8. $40,000 to $44,999',
            '9. $45,000 to $49,999',
            '10. $50,000 to $59,999',
            '11. $60,000 to $74,999',
            '12. $75,000 to $99,999',
            '13. $100,000 to $124,999',
            '14. $125,000 to $149,999',
            '15. $150,000 to $199,999',
            '16. $200,000 or more'],
        'notes' : '\n'.join([
            '1. Based on 2012 5-year ACS tables B19001 and B19101. \n \n'
            '2. This variable is designed to provide household income data that are comparable '
            'to income distributions by race, Hispanic and families. '
            'There are numerous assumptions associated with this variable. '
            'The random allocation of income to a physical location '
            'does not represent the actual income for the household. '
            'This information is designed to be aggregated. \n \n'
            '3. Totals by household income group will not match to ACS data. \n \n'
            '4. Compare that the inventory counts fall within the ACS '
            'the 90% CI (confidence interval) of the estimate. \n \n'
            '5. To verify results compare table to: \n \n'
            'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=ACSDT5Y2012.B19001'
            ]),
        'primary_key' : 'huid',
        'pop_var' : 'numprec'},
    'hhinc' : 
    {   'label' : 'Five Household Income Groups',
        'DataType'  : 'Int',
        'pyType' : "category",
        'categorical' : True,
        'AnalysisUnit' : 'Household',
        'MeasureUnit' : '2012 INFLATION-ADJUSTED DOLLARS',
        'categories_dict' : {
            0 : '0. NA vacant or group quarters',
            1 : '1. Less than $15,000',
            2 : '2. $15,000 to $24,999',
            3 : '3. $25,000 to $74,999',
            4 : '4. $75,000 to $99,999',
            5 : '5. $100,000 or more'},
        'categories' : 
        [   '0. NA vacant or group quarters',
            '1. Less than $15,000',
            '2. $15,000 to $24,999',
            '3. $25,000 to $74,999',
            '4. $75,000 to $99,999',
            '5. $100,000 or more'],
        'notes' : '\n'.join([
                '1. Based on 2012 5-year ACS tables B19001 and B19101.  \n \n'
                '2. Based on incomegroup variable - with 5 groups versus 16.'
            ]),
        'primary_key' : 'huid',
        'pop_var' : 'numprec'},
    'randincome' : 
    {   'label' : 'Random Household Income',
        'huiv3-0-0' : 'randincomeB19101', 
        'DataType'  : 'Float',
        'pyType' : float,
        'AnalysisUnit' : 'Household',
        'MeasureUnit' : '2012 INFLATION-ADJUSTED DOLLARS',
        'notes' : '\n'.join([
                '1.  Household income top coded at $250,000. \n \n'
                '2.  Based on 2012 5-year ACS tables B19001 and B19101. \n \n'
                '3.  Missing cases are vacant housing units or group quaters. '
                'The US Census does not collect income data for '
                'populations living in group quarters.  \n \n'
                '4.  To verify results compare median income to: \n \n'
                'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=ACSDT5Y2012.B19013'
                ])},
    'poverty' : 
        {   'label' : 'Poverty Status',
            'DataType'  : 'Int',
            'pyType' : "category",
            'categorical' : True,
            'AnalysisUnit' : 'Household',
            'MeasureUnit' : 'Household',
            'categories_dict' : {
                0 : '0. At or above poverty level',
                1 : '1. Below poverty level'},
            'categories' : 
            [   '0. At or above poverty level', 
                '1.  Below poverty level'],
            'short_labels' : 
            [   'Above poverty', 
                'Below poverty'],
            'notes' : '\n'.join([
                '1. Based on 2012 US Census Poverty Thresholds  \n \n',
                'https://www.census.gov/topics/income-poverty/poverty/guidance/poverty-measures.html  \n \n',
                'https://www2.census.gov/programs-surveys/cps/tables/time-series/historical-poverty-thresholds/thresh12.xls  \n \n',
                '2. Based on household size and random income, assuming random income represents household income in the past 12 months.  \n \n',
                '3. To verify results compare table to: \n \n'
                'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=ACSDT5Y2012.B17019'
                ]),
            'primary_key' : 'huid',
            'pop_var' : 'numprec'}
    }