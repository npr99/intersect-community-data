"""Dictionary of standard LODES data structures."""

# Orgin-or-Destination File Groups 
# Loop through od first to get full list of possible work and home counties
all_ods = {    'od'  : 'Origin-Destination data',
               'wac' : 'Workplace Area Characteristic data',
               'rac' : 'Residence Area Characteristic data'
       }

all_segparts = {'Segments':
            {
             'Earnings' : 
             {
              'SE01' : 'jobs with earnings \\$1250/month or less',
              'SE02' : 'jobs with earnings \\$1251/month to \\$3333/month',
              'SE03' : 'jobs with earnings greater than \\$3333/month'
              },
             'Age' : 
             {
              'SA01' : 'jobs of workers age 29 or younger',
              'SA02' : 'jobs for workers age 30 to 54',
              'SA03' : 'jobs for workers age 55 or older'
              },
             'SuperSector' : 
             {
              'SI01' : 'Goods Producing industry sectors',
              'SI02' : 'Trade, Transportation, and Utilities industry sectors',
              'SI03' : 'All Other Services industry sectors'
              },
            },
             'Parts':
              {
               'mainaux' : 
              {'main': 'jobs with both workplace and residence in state',
               'aux' : 'jobs with workplace in the state and residence outside of state'
              },
              }
            }

# Variable stems used to reshape LODES data
longvar = {'CA' : 'Age',
            'CE' : 'Earnings',
            'CNS': 'IndustryCode',
            'CR' : 'Race',
            'CT' : 'Ethnicity',
            'CD' : 'Education',
            'CS' : 'Sex'
        }

# Chararacter stems
all_charstems = {
            'Age'  : 'CA',
            'Earnings' : 'CE',
            'Race' : 'CR',
            'Sex' : 'CS',
            'Ethnicity': 'CT',
            'Education' : 'CD'
           }

all_stems = {'Age' : 'CA',
            'Earnings' : 'CE',
            'IndustryCode' : 'CNS',
            'Race' : 'CR',
            'Sex' : 'CS',
            'Ethnicity': 'CT',
            'Education' : 'CD'
           }  

# Segment Characteristics
all_segstems = {'Earnings' : 'SE',
            'Age' : 'SA',
            'SuperSector' : 'SI',
            'mainaux' : 'na'
           }

# Jobtypes in LODES data
all_jobtypes = {  'JT00' : 'All Jobs',
              'JT01' : 'Primary Jobs',
              'JT02' : 'All Private Jobs',
              'JT03' : 'Private Primary Jobs',
              'JT04' : 'All Federal Jobs',
              'JT05' : 'Federal Primary Jobs'
           }

# Mutually Exculsive Job Types
all_mxjobtypes = {'JT03' : 'Private Primary Jobs',
              'JT09' : 'Private Non-primary Jobs',
              'JT05' : 'Federal Primary Jobs',
              'JT10' : 'Federal Non-primary Jobs',
              'JT07' : 'Public Sector Primary Jobs',
              'JT11' : 'Public Sector Non-primary Jobs'
             }

