"""
Data Structure for Housing Unit Allocation

Set up list of communities with proper dictionary structure

This code was taking up a lot of space in my notebooks
Thought I would move it here.

In the future some of the items in the dictionary could be 
added automatically.

"""

from pyncoda.ncoda_00h_bldg_archetype_structure import *
import ipywidgets as widgets

# Define the communities
communities_dictionary = {
    'Lumberton_NC' : {
                    'community_name' : 'Lumberton, NC',
                    'focalplace_name' : 'Lumberton',
                    'STATE' : 'NORTH_CAROLINA',
                    'years' : ['2010'],
                    'counties' : { 
                        1 : {'FIPS Code' : '37155', 'Name' : 'Robeson County, NC'}
                    },
                    'building_inventory' : { 
                        'use_incore' : True,
                        'id' : '62ab7dcbf328861e25ffea9e',
                        'note' : 'Building inventory for Robeson County, NC',
                        'archetype_var' : 'archetype',
                        'residential_archetypes' : Nofal_residential_archetypesv2,
                        'building_area_var' : 'sq_foot'
                    }
                },
    'Galveston_County_TX' : {
                    'community_name' : 'Galveston, TX',
                    'focalplace_name' : 'Galveston',
                    'STATE' : 'TEXAS',
                    'years' : ['2010'],
                    'counties' : { 
                        1 : {'FIPS Code' : '48167', 'Name' : 'Galveston County, TX'}
                        },
                    'building_inventory' : { 
                        'use_incore' : True,
                        'id' : '63053ddaf5438e1f8c517fed',
                        'note' : 'Building inventory for Galveston County, TX',
                        'archetype_var' : 'arch_flood',
                        'residential_archetypes' : Nofal_residential_archetypesv2,
                        'building_area_var' : 'sq_foot'
                        }
                    },
    'Galveston_Island_TX' : {
                    'community_name' : 'Galveston, TX',
                    'focalplace_name' : 'Galveston',
                    'STATE' : 'TEXAS',
                    'years' : ['2010'],
                    'counties' : { 
                        1 : {'FIPS Code' : '48167', 'Name' : 'Galveston County, TX'}
                        },
                    'building_inventory' : { 
                        'use_incore' : True,
                        'id' : '63ff6b135c35c0353d5ed3ac',
                        'note' : 'Building inventory for Galveston Island, TX',
                        'archetype_var' : 'arch_flood',
                        'residential_archetypes' : Nofal_residential_archetypesv2,
                        'building_area_var' : 'sq_foot',
                        'bldg_uniqueid' : 'guid',
                        'building_area_cutoff' : 300
                        }
                    },
    'Mayfield_KY' : {
                    'community_name' : 'Mayfield, KY',
                    'focalplace_name' : 'Mayfield',
                    'STATE' : 'KENTUCKY',
                    'years' : ['2010'],
                    'counties' : { 
                        1 : {'FIPS Code' : '21083', 'Name' : 'Graves County, KY'}
                        },
                    'building_inventory' : {
                        'use_incore' : False,
                        'id' : 'NSI',
                        'note' : 'NSI Building inventory for Graves County, KY',
                        'archetype_var' : 'occtype',
                        'bldg_uniqueid' : 'fd_id_bid',
                        'residential_archetypes' : HAZUS_residential_archetypes,
                        'building_area_var' : 'sqft',
                        'building_area_cutoff' : 300
                    }
                },
    'Beaumont_TX_NSI' : {
                    'community_name' : 'Beaumont, TX',
                    'focalplace_name' : 'Beaumont',
                    'STATE' : 'TEXAS',
                    'years' : ['2010'],
                    'counties' : { 
                        1 : {'FIPS Code' : '48245', 'Name' : 'Jefferson, TX'}
                        },
                    'building_inventory' : { 
                        'use_incore' : False,
                        'id' : 'NSI',
                        'note' : 'NSI Building inventory for Jefferson County, TX',
                        'archetype_var' : 'occtype',
                        'bldg_uniqueid' : 'fd_id_bid',
                        'residential_archetypes' : HAZUS_residential_archetypes,
                        'building_area_var' : 'sqft',
                        'building_area_cutoff' : 300
                    }
                },
    'Beaumont_TX_shp' : {
                    'community_name' : 'Beaumont, TX',
                    'focalplace_name' : 'Beaumont',
                    'STATE' : 'TEXAS',
                    'years' : ['2010'],
                    'counties' : { 
                        1 : {'FIPS Code' : '48245', 'Name' : 'Jefferson, TX'}
                        },
                    'building_inventory' : { 
                        'use_incore' : False,
                        'id' : 'Safayet',
                        'filename' : "G:\Shared drives\HRRC_IN-CORE\Tasks\P5.2 Social Institution Resilience\WorkNPR\IN-CORE_2bv1_prep_bldgfile_hua_2023-06-23\IN-CORE_2bv1_prep_bldgfile_hua_2023-06-23.shp",
                        'note' : 'Safayet Building inventory for Jefferson County, TX',
                        'archetype_var' : 'res',
                        'bldg_uniqueid' : 'bldg_id',
                        'residential_archetypes' : basic_residential_archetypes,
                        'building_area_var' : 'sqft',
                        'building_area_cutoff' : 300
                    }
                },
    'Oceana_MI' : {
                    'community_name' : 'Pentwater, MI',
                    'focalplace_name' : 'Pentwater',
                    'STATE' : 'MICHIGAN',
                    'years' : ['2010'],
                    'counties' : { 
                        1 : {'FIPS Code' : '26127', 'Name' : 'Oceana County, MI'}
                        },
                    'building_inventory' : {
                        'use_incore' : False, 
                        'id' : 'NSI',
                        'note' : 'NSI Building inventory for Oceana County, MI',
                        'archetype_var' : 'occtype',
                        'bldg_uniqueid' : 'fd_id_bid',
                        'residential_archetypes' : HAZUS_residential_archetypes,
                        'building_area_var' : 'sqft',
                        'building_area_cutoff' : 300
                        }
                    },
    'Seaside_OR_NSI' : {
                'community_name' : 'Seaside, OR',
                'focalplace_name' : 'Seaside',
                'STATE' : 'OREGON',
                'years' : ['2010'],
                'counties' : { 
                    1 : {'FIPS Code' : '41007', 'Name' : 'Clatsop County, OR'}
                    },
                'building_inventory' : {
                    'use_incore' : False, 
                    'id' : 'NSI',
                    'note' : 'NSI Building inventory for Clatsop County, OR',
                    'archetype_var' : 'occtype',
                    'bldg_uniqueid' : 'fd_id_bid',
                    'residential_archetypes' : HAZUS_residential_archetypes,
                    'building_area_var' : 'sqft',
                    'building_area_cutoff' : 300
                    }
                },
    'Eugene_OR_NSI' : {
                'community_name' : 'Lane County, OR',
                'focalplace_name' : 'Eugene',
                'STATE' : 'OREGON',
                'years' : ['2010'],
                'counties' : { 
                    1 : {'FIPS Code' : '41039', 'Name' : 'Lane County, OR'}
                    },
                'building_inventory' : {
                    'use_incore' : False, 
                    'id' : 'NSI',
                    'note' : 'NSI Building inventory for Lane County, OR',
                    'archetype_var' : 'occtype',
                    'bldg_uniqueid' : 'fd_id_bid',
                    'residential_archetypes' : HAZUS_residential_archetypes,
                    'building_area_var' : 'sqft',
                    'building_area_cutoff' : 300
                    }
                }
    }

def create_community_dropdown(dict_communities):
    community_names = [details['community_name'] for key, details in communities_dictionary.items()]
    # add note from building inventory
    bldg_inventory_notes = [details['building_inventory']['note'] for key, details in communities_dictionary.items()]
    # combine the two lists
    community_options = [f"{community_names[i]}: {bldg_inventory_notes[i]}" for i in range(len(community_names))]
    dropdown = widgets.Dropdown(
        options=community_options,
        value=community_options[0],  # Default value
        description='Community:',
        disabled=False,
    )
    return dropdown

# Create a reverse mapping from community names to IDs
name_to_id = {details['community_name']: id for id, details in communities_dictionary.items()}

# Function to find a community ID by its name
def get_community_id_by_name(community_option):
    for id, details in communities_dictionary.items():
        # create community_options from community name and building inventory note
        community_name = details['community_name']
        building_inventory_note = details['building_inventory']['note']
        community_option_check = f"{community_name}: {building_inventory_note}" 
        if community_option == community_option_check:
            print(f"Selected community ID: {id}")
            print(f"{community_name} is in {communities_dictionary[id]['STATE']}")
            focalplace = communities_dictionary[id]['focalplace_name']
            print(f"Focal place: {focalplace}")
            for county in communities_dictionary[id]['counties']:
                countyname = communities_dictionary[id]['counties'][county]['Name']
                countyfips = communities_dictionary[id]['counties'][county]['FIPS Code']
                print(f"{community_name} is in {countyname} with FIPS code {countyfips}")
            use_incore = communities_dictionary[id]['building_inventory']['use_incore']
            print(f"Use IN-CORE: {use_incore}")
            return id, focalplace, countyname, countyfips
    print(f"Community {community_name} not found")
    return "ID not found"