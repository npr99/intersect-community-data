"""
### Create NCES CCD Data Structure Loop
The NCES appears to have versions for the data structuters.
2009-2010 should be version v.2a

"""

def ccd_v2a_datastructure():
    ccd_v2a = {}
    ccd_v2a['gradelevels'] ={'PK' : {'gradelevel' : 'PK', 'label' : 'Prekindergarten students'},
                            'KG' : {'gradelevel' : 'KG', 'label' : 'Kindergarten students'},
                            '01' : {'gradelevel' : 'G01', 'label' : 'Grade 1 students'}}
    # Add grade 2 - 12 students
    for grade in range(2,13):
        #print(grade)
        grade_str = str(grade).zfill(2)
        ccd_v2a['gradelevels'][grade_str] = {}
        ccd_v2a['gradelevels'][grade_str] ['gradelevel'] = 'G'+grade_str
        ccd_v2a['gradelevels'][grade_str] ['label'] = 'Grade '+str(grade)+' students'
    ccd_v2a['gradelevels']['UG'] = {'gradelevel' : 'UG', 'label' : 'Ungraded students'}


    ccd_v2a['RACECAT_5'] = {'AM' : {'racecat5' : 1, 'race' : 3, 'hispan' : 0, 
                            'label' : 'American Indian/Alaska Native'}}
    ccd_v2a['RACECAT_5']['AS'] =  {'racecat5' : 2, 'race' : 8, 'hispan' : 0, 
                            'label' : 'Asian/Hawaiian Native/Pacific Islander',
                            'notes' : 'Combine race categories 4 and 5 in Census data'}
    ccd_v2a['RACECAT_5']['HI'] =  {'racecat5' : 3, 'race' : -999, 
                                    'hispan' : 1, 'label' : 'Hispanic'}
    ccd_v2a['RACECAT_5']['BL'] =  {'racecat5' : 4, 'race' : 2, 
                                    'hispan' : 0, 'label' : 'Black, non-Hispanic'}
    ccd_v2a['RACECAT_5']['WH'] =  {'racecat5' : 5, 'race' : 1, 
                                    'hispan' : 0, 'label' : 'White, non-Hispanic'}

    ccd_v2a['RACECAT_7'] = {'AM' : {'racecat7' : 1, 'race' : 3, 'hispan' : 0, 
                            'label' : 'American Indian/Alaska Native'}}
    ccd_v2a['RACECAT_7']['AS'] =  {'racecat7' : 2, 'race' : 4, 'hispan' : 0, 
                                        'label' : 'Asian'}
    ccd_v2a['RACECAT_7']['HI'] =  {'racecat7' : 3, 'race' : -999, 'hispan' : 1, 'label' : 'Hispanic'}
    ccd_v2a['RACECAT_7']['BL'] =  {'racecat7' : 4, 'race' : 2, 'hispan' : 0, 'label' : 'Black'}
    ccd_v2a['RACECAT_7']['WH'] =  {'racecat7' : 5, 'race' : 1, 'hispan' : 0, 'label' : 'White'}
    ccd_v2a['RACECAT_7']['HP'] =  {'racecat7' : 6, 'race' : 5, 'hispan' : 0, 
                                    'label' : 'Hawaiian Native/Pacific Islander'}
    ccd_v2a['RACECAT_7']['TR'] =  {'racecat7' : 7, 'race' : 9, 'hispan' : 0,
                                    'label' : 'Two or more races',
                                    'notes' : 'Combine race categories 6 and 7 in Census data'}

    ccd_v2a['Sex'] = { 'F' : {'sex' : 2, 'label' : 'Female'}}
    ccd_v2a['Sex']['M'] = {'sex' : 1, 'label' : 'Male'}

    return ccd_v2a

def ccd_v2a_widevars(ccd_v2a):
    # Create Variable List with mutually Exclusive values
    year = '09'
    wide_vars = []
    for race in ccd_v2a['RACECAT_7']:
        for grade in ccd_v2a['gradelevels']:
            for sex in ccd_v2a['Sex']:
                wide_vars.append(race+grade+sex+year)
    return wide_vars