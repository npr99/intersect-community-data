"""
Data Structure for Building Archetypes
#### Residential Building Variable
What variable has information to determine if building is residential or not?
County observations by variable

"""

Nofal_residential_archetypes = { 
        1 : 'One-story sf residential building on a crawlspace foundation',
        2 : 'One-story mf residential building on a slab-on-grade foundation',
        3 : 'Two-story sf residential building on a crawlspace foundation',
        4 : 'Two-story mf residential building on a slab-on-grade foundation'}

Nofal_residential_archetypesv2 = { 
        1 : {'Description' : 
                'One-story sf residential building on a crawlspace foundation',
             'HU estimate' : 1},
        2 : {'Description' : 
                'One-story mf residential building on a slab-on-grade foundation',
              'HU estimate' : 1},
        3 : {'Description' : 
                'Two-story sf residential building on a crawlspace foundation',
            'HU estimate' : 1},
        4 : {'Description' : 
                'Two-story mf residential building on a slab-on-grade foundation',
            'HU estimate' : 1}
            }


# HAZUS Archetypes for residential buildings include an estimate of housing units
HAZUS_residential_archetypes = { 
    "RES1-1SNB" : {'Description' : "Single Family Dwelling", 'HU estimate' : 1},
    "RES1-1SWB" : {'Description' : "Single Family Dwelling", 'HU estimate' : 1},
    "RES1-2SNB" : {'Description' : "Single Family Dwelling", 'HU estimate' : 1},
    "RES1-2SWB" : {'Description' : "Single Family Dwelling", 'HU estimate' : 1},
    "RES1-3SNB" : {'Description' : "Single Family Dwelling", 'HU estimate' : 1},
    "RES1-3SWB" : {'Description' : "Single Family Dwelling", 'HU estimate' : 1},
    "RES1-SLNB" : {'Description' : "Single Family Dwelling", 'HU estimate' : 1},
    "RES1-SLWB" : {'Description' : "Single Family Dwelling", 'HU estimate' : 1},
    "RES1" : {'Description' : "Single Family Dwelling", 'HU estimate' : 1},
    "RES2" : {'Description' : "Mobile / Manufactured Home", 'HU estimate' : 1},
    "RES3A" : {'Description' : "Duplex", 'HU estimate' : 2},
    "RES3B" : {'Description' : "3-4 Units", 'HU estimate' : 3},
    "RES3C" : {'Description' : "5-9 Units", 'HU estimate' : 7},
    "RES3D" : {'Description' : "10-19 Units", 'HU estimate' : 15},
    "RES3E" : {'Description' : "20-49 Units", 'HU estimate' : 30},
    "RES3F" : {'Description' : "50+ Units", 'HU estimate' : 50},
    "RES4" : {'Description' : "Temporary Lodging", 'HU estimate' : 1},
    "RES5" : {'Description' : "Institutional Dormitory", 'HU estimate' : 1},
    "RES6" : {'Description' : "Nursing Home", 'HU estimate' : 1}
    }