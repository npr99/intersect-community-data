# Copyright (c) 2021 Nathanael Rosenheim. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import pandas as pd
import geopandas as gpd # For reading in shapefiles
import numpy as np
import os
import sys # For displaying package versions

# open, read, and execute python program with reusable commands
from pyncoda.ncoda_00b_directory_design import directory_design
from pyncoda.ncoda_07a_generate_hui import generate_hui_functions
from pyncoda.ncoda_07c_generate_addpt import generate_addpt_functions
from pyncoda.ncoda_07d_run_hua_workflow import hua_workflow_functions
from pyncoda.CommunitySourceData.nsi_sec_usace_army_mil.nsi_01a_downloadfiles import download_nsi_files


class process_community_workflow():
    """
    The following code will produce the following outputs:
    1. Housing Unit Inventory
    2. Address Point Inventory
    3. Housing Unit Allocation
    """

    def __init__(self,
            communities,
            seed: int = 9876,
            version: str = '2.0.0',
            version_text: str = 'v2-0-0',
            basevintage: str = '2010',
            outputfolder: str ="OutputData",
            outputfolders = {},
            savefiles: bool = True):

        self.communities = communities
        self.seed = seed
        self.version = version
        self.version_text = version_text
        self.basevintage = basevintage
        self.outputfolder = outputfolder
        self.outputfolders = outputfolders
        self.savefiles = savefiles


    def generate_hui(self, communities, use_incore):
        ## Read in Housing Unit Inventory or create a new one
        generate_hui_df = generate_hui_functions(
                            communities =   communities,
                            seed =          self.seed,
                            version =       self.version,
                            version_text=   self.version_text,
                            basevintage=    self.basevintage,
                            outputfolder=   self.outputfolder,
                            use_incore=     use_incore
                            )

        hui_dataset_id = generate_hui_df.generate_hui_v2_for_incore()

        # If using IN-CORE
        if use_incore:
            from pyincore import Dataset
            # Housing Unit inventory
            housing_unit_inv_id = hui_dataset_id
            from pyncoda.ncoda_06d_INCOREDataService import loginto_incore_dataservice
            # set IN-CORE data service
            data_service = loginto_incore_dataservice()
            # load housing unit inventory as pandas dataframe
            housing_unit_inv = Dataset.from_data_service(housing_unit_inv_id, data_service)
            filename = housing_unit_inv.get_file_path('csv')
            print("The IN-CORE Dataservice has saved the Housing Unit Inventory on your local machine: "+filename)

            # Convert CSV to Pandas Dataframe
            housing_unit_inv_df = pd.read_csv(filename, header="infer")
        else:
            housing_unit_inv_df = hui_dataset_id
            hui_dataset_id = 'local'
        
        return housing_unit_inv_df, hui_dataset_id


    def load_building_inventory(self, 
                                community_dict):
        """
        Loads building inventory for a given community from specified sources.
        
        Parameters:
        - community: The community name.
        - community_config: Configuration dictionary for the community.
        - data_service: IN-CORE DataService instance for accessing datasets.
        
        Returns:
        - Geopandas GeoDataFrame of the loaded building inventory.
        """
        # Logic to load building inventory based on community_config
        # This includes handling IN-CORE data service, file sources, or NSI datasets
        
        use_incore = community_dict['building_inventory']['use_incore']
        bldg_inv_id = community_dict['building_inventory']['id']

        # load building inventory
        # If using IN-CORE
        if use_incore:
            # Functions from IN-CORE
            from pyincore import Dataset
            from pyncoda.ncoda_06d_INCOREDataService import loginto_incore_dataservice
            # set IN-CORE data service
            data_service = loginto_incore_dataservice()

            # Get the Unique ID
            bldg_uniqueid = 'guid'
            # Building inventory ID
            bldg_inv = Dataset.from_data_service(bldg_inv_id, data_service)
            filename = bldg_inv.get_file_path('shp')
            print("The IN-CORE Dataservice has saved the Building Inventory on your local machine: "+filename)
            bldg_inv_gdf = gpd.read_file(filename)
        # Check if building inventory is comes  from a filename if filename key exists
        elif 'filename' in community_dict['building_inventory'].keys():
            # Get the Unique ID
            bldg_uniqueid = community_dict['building_inventory']['bldg_uniqueid']
            
            print("Building inventory is from a file")
            bldg_filename = community_dict['building_inventory']['filename']
            bldg_inv_gdf = gpd.read_file(bldg_filename)
        # Check if building inventory is from NSI
        elif 'NSI' in bldg_inv_id:
            # Get the Unique ID
            bldg_uniqueid = community_dict['building_inventory']['bldg_uniqueid']
            # make an empty dictionary for saving county NSI files
            county_nsi_gdf = {}
            for county in community_dict['counties'].keys():
                county_fips = community_dict['counties'][county]['FIPS Code']
                state_county_name  = community_dict['counties'][county]['Name']
                print("Downloading NSI files for:")
                print(state_county_name,': county FIPS Code',county_fips)
                county_nsi_gdf[county_fips] = download_nsi_files(county_fips=county_fips)
            # merge all counties into one geodataframe
            bldg_inv_gdf = pd.concat(county_nsi_gdf.values(), 
                                        ignore_index=True, axis=0)
        
        return bldg_inv_gdf, bldg_inv_id, bldg_uniqueid

    def generate_and_process_addpt(self,
                                    community, 
                                    housing_unit_inv_df, 
                                    bldg_inv_gdf, 
                                    community_dict,
                                    communities_dict):
        


        archetype_var = community_dict['building_inventory']['archetype_var']
        building_area_var = community_dict['building_inventory']['building_area_var']
        building_area_cutoff = community_dict['building_inventory']['building_area_cutoff']
        residential_archetypes = community_dict['building_inventory']['residential_archetypes']
        bldg_inv_id = community_dict['building_inventory']['id']
        bldg_uniqueid = community_dict['building_inventory']['bldg_uniqueid']
        use_incore = community_dict['building_inventory']['use_incore']

        print("Generate Address point inventory for: "+community)
        print("Based on building inventory: "+bldg_inv_id)
        generate_addpt_df = generate_addpt_functions(
                            community =   community,
                            communities = communities_dict,
                            hui_df = housing_unit_inv_df,
                            bldg_inv_gdf = bldg_inv_gdf,
                            bldg_inv_id = bldg_inv_id,
                            residential_archetypes = residential_archetypes,
                            bldg_uniqueid = bldg_uniqueid,
                            archetype_var = archetype_var,
                            building_area_var = building_area_var,
                            building_area_cutoff = building_area_cutoff,
                            seed =          self.seed,
                            version =       self.version,
                            version_text=   self.version_text,
                            basevintage=    self.basevintage,
                            outputfolder=   self.outputfolder,
                            use_incore=     use_incore
                            )

        addpt_dataset_id = generate_addpt_df.generate_addpt_v2_for_incore()

        ### Read in Address Point Inventory
        '''
        The address point inventory is an intermediate file based on the building inventory. 
        The address point inventory acts as the bridge between the building inventory 
        and the housing unit inventory.
        '''
        # Check if addpt_dataset_id is string
        if isinstance(addpt_dataset_id, str):
            print("The Address Point Inventory ID is a pandas string")
            # Functions from IN-CORE
            from pyincore import Dataset
            # Functions from IN-CORE
            from pyncoda.ncoda_06d_INCOREDataService import loginto_incore_dataservice
            data_service = loginto_incore_dataservice()
            # Address Point inventory
            addpt_inv_id = addpt_dataset_id
            # load housing unit inventory as pandas dataframe
            addpt_inv = Dataset.from_data_service(addpt_inv_id, data_service)
            filename = addpt_inv.get_file_path('csv')
            print("The IN-CORE Dataservice has saved the Address Point Inventory on your local machine: "+filename)
            addpt_inv_df = pd.read_csv(filename, header="infer")
        # else if addpt_dataset_id is a dataframe
        elif isinstance(addpt_dataset_id, pd.DataFrame):
            addpt_inv_df = addpt_dataset_id
            print("The Address Point Inventory ID contains a pandas dataframe")
        else:
            print("The Address Point Inventory is not a string or pandas dataframe")

        return addpt_inv_df, addpt_dataset_id
    
    def generate_and_process_hua(self,
                                community,
                                community_dict,
                                housing_unit_inv_df,
                                bldg_inv_id,
                                bldg_uniqueid,
                                bldg_inv_gdf,
                                addpt_inv_df,
                                outputfolders
                                ):
        '''
        ### Run Housing Unit Allocation
        '''
        # year is the last two digits of the basevintage
        yr = str(self.basevintage)[2:4]

        archetype_var = community_dict['building_inventory']['archetype_var']
        use_incore = community_dict['building_inventory']['use_incore']

        print("Housing Unit Allocation for: "+community)
        print("Based on building inventory: "+bldg_inv_id)

        run_hua_gdf = hua_workflow_functions(
                                community =   community,
                                hui_df = housing_unit_inv_df,
                                bldg_gdf = bldg_inv_gdf,
                                bldg_inv_id = bldg_inv_id,
                                addpt_df = addpt_inv_df,
                                bldg_uniqueid = bldg_uniqueid,
                                archetype_var = archetype_var,
                                seed =          self.seed,
                                version =       self.version,
                                version_text=   self.version_text,
                                basevintage=    self.basevintage,
                                outputfolder=   self.outputfolder,
                                outputfolders = outputfolders,
                                use_incore=     use_incore
                                )

        hua_gdf = run_hua_gdf.housing_unit_allocation_workflow()


        ## Merge Housing Unit Allocation with Housing Unit Inventory
        # Merge HUA with HUI
        hua_cols = ['huid', bldg_uniqueid,
                    f'placeNAME{yr}','huestimate','x','y']
        # check HUA Cols
        print(hua_cols)
        hua_hui_df = pd.merge(left = housing_unit_inv_df,
                            right = hua_gdf[hua_cols],
                            on='huid',
                            how='left')

        # Replace missing bldg_uniqueid 
        hua_hui_df[bldg_uniqueid] = \
             hua_hui_df[bldg_uniqueid].fillna('missing building id')

        # Keep if huid is not missing
        hua_hui_df = hua_hui_df[hua_hui_df['huid'].notna()]

        ## Save Housing Unit Allocation to CSV and Upload to IN-CORE Dataservice
        # save hua_hui_gdf to csv
        check_folder = self.outputfolder
        output_filename = \
            f'hua_{self.version_text}_{community}_{self.basevintage}_rs{self.seed}_{bldg_inv_id}'
        csv_filepath = check_folder+"/"+output_filename+'.csv'
        savefile = os.path.join(os.getcwd(), csv_filepath)
        # Resave results for community name
        hua_hui_df.to_csv(savefile, index=False)

        # make a county list for community
        county_list = ''
        for county in community_dict['counties'].keys():
            state_county = community_dict['counties'][county]['FIPS Code']
            state_county_name  = community_dict['counties'][county]['Name']
            print(state_county_name,': county FIPS Code',state_county)
            county_list = county_list + state_county_name+': county FIPS Code '+state_county
        county_list
        
        title = "Housing Unit Allocation v2.0.0 data for "+community + " " + str(self.basevintage)
        title

        if use_incore:
            # Upload to IN-CORE Dataservice
            run_hua_gdf.upload_hua_file_to_incore(title =title,
                                county_list = county_list,
                                csv_filepath = csv_filepath,
                                output_filename = output_filename)
        return hua_hui_df
                                       
    def process_communities(self):
        """
        Processes multiple communities to generate and allocate housing units based on 
        building inventories, either loaded from IN-CORE DataService, a file, or the NSI dataset.
        
        Parameters:
        - communities: Dictionary containing community-specific parameters and building inventory details.
        - housing_unit_inv_df: DataFrame containing housing unit inventory data.
        - data_service: IN-CORE DataService instance for accessing and uploading datasets.
        """
        for community in self.communities.keys():

            # set output folder
            outputfolders = directory_design(
                                state_county_name = community,
                                outputfolder = self.outputfolder)

            # community dictionary
            community_dict = self.communities[community]
            use_incore = community_dict['building_inventory']['use_incore']

            # Load Housing Unit Inventory
            housing_unit_inv_df, hui_dataset_id = \
                self.generate_hui(self.communities, use_incore)
        
            # Extract community-specific configuration
            bldg_inv_id = community_dict['building_inventory']['id']
            
            # Load building inventory based on source configuration
            bldg_inv_gdf, bldg_inv_id, bldg_uniqueid = \
                self.load_building_inventory(community_dict) 

            print(f"Generate Address point inventory for: {community}")
            print(f"Based on building inventory: {bldg_inv_id}")
            
            # Generate Address Point Inventory
            addpt_inv_df, addpt_dataset_id = \
                self.generate_and_process_addpt(community, 
                                                housing_unit_inv_df, 
                                                bldg_inv_gdf, 
                                                community_dict,
                                                self.communities)
            
            # Generate and process housing unit allocation
            hua_hui_df = self.generate_and_process_hua(community,
                                                        community_dict,
                                                        housing_unit_inv_df,
                                                        bldg_inv_id,
                                                        bldg_uniqueid,
                                                        bldg_inv_gdf,
                                                        addpt_inv_df,
                                                        outputfolders
                                                        )   


            # convert hui_hua_df to geodataframe
            crs = "EPSG:4326"
            hua_hui_gdf = gpd.GeoDataFrame(hua_hui_df, 
                geometry=gpd.points_from_xy(hua_hui_df.x, hua_hui_df.y), crs=crs)
            
            return hua_hui_gdf
