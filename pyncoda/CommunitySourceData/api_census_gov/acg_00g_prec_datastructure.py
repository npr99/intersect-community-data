"""
Data Structure for Person Record File

Trying to follow Data Documentation Initiative Controlled Vocabularies:
https://ddialliance.org/controlled-vocabularies

pyType = Python Type - needed to set correct data type in Python

"""

prec_v300_DataStructure = {
    'precid'  : 
    {   'label' : 'Person Record ID', 
        'DataType'  : 'String',
        'pyType' : str,
        'AnalysisUnit' : 'Person',
        'MeasureUnit' : 'Person',
        'notes' : '\n'.join([
            '1. Person Record ID is the Primary Key for the Person Record File. '
            'Use precid to sort data before Random Merge Order is assigned. \n \n'
            '2. Every person in the file has a precid which unique and non-missing. \n \n'
            '3. One person record represents one person. \n \n'
            '4. For persons in study area, the Person Record ID is a combination '
            'of the census block ID, '
            'the letter P and a cumulative counter for persons in the block.'
            '5. For persons outside study area, the Person Record ID is a combination '
            'of the census block ID, '
            'the letters PJ - for person job and a cumulative counter for jobs in the block.'
            '6. The person record file includes all persons that live in the study area, '
            'and all persons that are employed in the study area. \n \n'
                ])},
    'huid'  : 
    {   'label' : 'Housing Unit ID', 
        'DataType'  : 'String',
        'pyType' : str,
        'AnalysisUnit' : 'Housing unit',
        'MeasureUnit' : 'Housing units',
        'notes' : '\n'.join([
            '1. Housing Unit ID is the Primary Key for the HU Inventory. '
            'Use HUID to merge person record file with the housing unit inventory. \n \n'
            '2. One Housing Unit could be an occupied house, '
            'a single unit in a multi-family structure, a vacant house or '
            'a group quarters with a number of persons. \n \n'
            '3. The Housing Unit ID is a combination of the census block ID, '
            'the letter H and a cumulative counter for housing units in the block. \n \n'
            '4. Missing huids (nan values) occur when a person lives outside of the study area, '
            'but works in the study area. Persons without huid can not be assigned to a '
            'to a housing unit in the study area.'
                ])},
    'Block2010str' : 
    {   'label' : '2010 Block ID' , 
        'DataType'  : 'String',
        'pyType' : str,
        'AnalysisUnit' : 'Geographic unit',
        'MeasureUnit' : 'Person in census block',
        'length' : 16,
        'zeropaded' : False,
        'notes' : '\n'.join([
            '1. 2010 Home Block ID as a string '
            '2. The 2010 Block ID  is a combination of the letter B and the census block ID. \n \n'
            '3. The Census Block is a zeropadded 15 character string of numbers. \n \n'
            '4. The first 5 characters of the Block ID represent the county. '
            'For example if the block id string = B371559602012079, the county = 37155. \n \n'
            '5. The first 11 characters of the Block ID represent the census tract. '
            'For example if the block id string = B371559602012079, '
            'the census tract id = 37155960201. \n \n'
            '6. The first 12 characters of the Block ID represent the block group. '
            'For example if the block id string = B371559602012079, '
            'the block group id = 371559602012. \n \n'
            '7. The python code to generate the other census geography ids from the blockid string are: \n \n'
            "           df['blockid']           = df['Block2010str'].apply(lambda x : str(int(x[1:16])).zfill(15)) \n \n"
            "           df['countyid']         = df['Block2010str'].apply(lambda x : str(int(x[1:16])).zfill(15)[0:5]) \n \n"
            "           df['tractid']            = df['Block2010str'].apply(lambda x : str(int(x[1:16])).zfill(15)[0:11]) \n \n"
            "           df['blockgroupid'] = df['Block2010str'].apply(lambda x : str(int(x[1:16])).zfill(15)[0:12]) \n \n"
                ])},
    'hcb_lat' : 
    {   'label' : 'Home Census Block Latitude',
        'DataType'  : 'Float',
        'pyType' : float,
        'AnalysisUnit' : 'census block',
        'MeasureUnit' : 'WGS84 degrees',
        'notes' : '\n'.join([
            '1. Spatial coordinate for 2010 census block. \n \n'
            '2. Coordinate Reference System is WGS84: EPSG:4326.'
            ])},
    'hcb_lon' : 
    {   'label' : 'Home Census Block Longitude',
        'DataType'  : 'Float',
        'pyType' : float,
        'AnalysisUnit' : 'census block',
        'MeasureUnit' : 'WGS84 degrees',
        'notes' : '\n'.join([
            '1. Spatial coordinate for 2010 census block. \n \n'
            '2. Coordinate Reference System is WGS84: EPSG:4326.'
            ])},
    'sex' : 
    {   'label' : 'Sex',
        'DataType'  : 'Int',
        'pyType' : "category",
        'categorical' : True,
        'AnalysisUnit' : 'Person',
        'MeasureUnit' : 'Person',
        'categories_dict_v2' : {
            1 : {'longlabel' : '1 Male',   'shortlabel' : 'Male'},
            2 : {'longlabel' : '2 Female', 'shortlabel' : 'Female'}},
        'notes' : '\n'.join([
            '1. Sex is based on the sex of the person. \n \n'
            '2. Sex is based on 2010 Census SF1 Tables P12 sex by age by race. \n \n'
            '5. To verify results compare table to: \n \n'
            'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=DECENNIALSF12010.P12.'
            ]),
        'primary_key' : 'precid'},
    'race' : 
    {   'label' : 'Race',
        'DataType'  : 'Int',
        'pyType' : "category",
        'categorical' : True,
        'AnalysisUnit' : 'Person',
        'MeasureUnit' : 'Person',
        'categories_dict_v2' : {
            1 : {'longlabel' : '1. White alone', 'shortlabel' : 'White'},
            2 : {'longlabel' : '2. Black or African American alone', 'shortlabel' : 'Black'},
            3 : {'longlabel' : '3. American Indian and Alaska Native alone','shortlabel' :  'American Indian'},
            4 : {'longlabel' : '4. Asian alone' , 'shortlabel' : 'Asian'},
            5 : {'longlabel' : '5. Native Hawaiian and Other Pacific Islander alone', 'shortlabel' : 'Pacific Islander'},
            6 : {'longlabel' : '6. Some Other Race alone', 'shortlabel' : 'Some Other'},
            7 : {'longlabel' : '7. Two or More Races', 'shortlabel' : 'Two or More'}},
        'notes' : '\n'.join([
            '1. Race is based on the race of the person. \n \n'
            '2. Race is based on 2010 Census SF1 Tables P12 sex by age by race. \n \n'
            '5. To verify results compare table to: \n \n'
            'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=DECENNIALSF12010.P12.'
            ]),
        'primary_key' : 'precid'},
    'hispan' : 
    {   'label' : 'Hispanic',
        'DataType'  : 'Int',
        'pyType' : "category",
        'categorical' : True,
        'AnalysisUnit' : 'Person',
        'MeasureUnit' : 'Person',
        'categories_dict_v2' : {
            0 : {'longlabel' : '0. Householder Not Hispanic', 'shortlabel' : 'Not Hispanic'},
            1 : {'longlabel' : '1.  Householder Hispanic or Latino', 'shortlabel' : 'Hispanic'}},
        'notes' : '\n'.join([
            '1. Hispanic is based on the ethnicity of the person. \n \n'
            '2. Hispanic is predicted based on 2010 Census SF1 Tables P12 and P5. \n \n'
            '3. Hispanic totals will not match the US Census data, but they should be close. \n \n'
            '4. To verify results compare table to: \n \n'
            'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=DECENNIALSF12010.P5.'
            ]),
        'primary_key' : 'precid'},
    'randagePCT12' : 
    {   'label' : 'Random Age',
        'DataType'  : 'Float',
        'pyType' : float,
        'AnalysisUnit' : 'Person',
        'MeasureUnit' : 'years',
        'notes' : '\n'.join([
            '1. Random age is based on age groups from block level data in P12 '
            'and census tract level data in PCT12. Age is based on sex, race, and ethnicity \n \n'
            "2. Decennial Census Question: What is Person's Age on April 1, 2010. \n \n"
            "3. Missing values represent people that live outside of the study area. \n \n"
            '4. To verify results compare table to: \n \n'
            'https://data.census.gov/cedsci/table?g=0500000US{state_county}&tid=DECENNIALSF12010.PCT12.'
            ])},
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
        'primary_key' : 'precid'}}