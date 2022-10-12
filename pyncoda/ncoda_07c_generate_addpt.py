"""
# Address Point Inventory Workflow

## Overview
Functions to obtain and clean data required for the Address Point Inventory - 
which is a key component of the Housing Unit Allocation process. 
The address point inventory predicts the number of housing units in each structure in a building inventory. 


### Resources and references:
For an overview of the address point inventory and housing unit allocation method see:

Rosenheim, N., Guidotti, R., Gardoni, P., & Peacock, W. G. (2021). Integration of detailed household and housing unit characteristic data with critical infrastructure for post-hazard resilience modeling. Sustainable and Resilient Infrastructure, 6(6), 385-401.

## Required Inputs
Program requires the following inputs:
1. program obtains and cleans US Census Block Data
2. program requires a geocoded building inventory
    - Future version of ICD will provide tools for generating a building inventory file
    - Current version will require users to have an IN-CORE account
3. Housing Unit Inventory - for expected address point counts by block
4. program will use the block data, expected counts, and building inventory to generate an address point inventory.
    
## Output Description
The output of this workflow is a CSV file with the address point inventory and a codebook that describes the data.

The output CSV is designed to be used in the Interdependent Networked Community Resilience Modeling Environment (IN-CORE) for the housing unit allocation model.

IN-CORE is an open source python package that can be used to model the resilience of a community. To download IN-CORE, see:

https://incore.ncsa.illinois.edu/


## Instructions
Users can run the workflow by executing each block of code in the notebook.

## Description of Program
- program:    ncoda_07bv1_addresspoint_workflow
- task:       Run the Address Point Workflow
- See github commits for description of program updates
- Current Version:    
- project:    Interdependent Networked Community Resilience Modeling Environment (IN-CORE), Subtask 5.2 - Social Institutions
- funding:	  NIST Financial Assistance Award Numbers: 70NANB15H044 and 70NANB20H008 
- author:     Nathanael Rosenheim

- Suggested Citation:
Rosenheim, Nathanael (2021) “Detailed Household and Housing Unit Characteristics: Data and Replication Code.” DesignSafe-CI. 
https://doi.org/10.17603/ds2-jwf6-s535.

"""
import pandas as pd
import geopandas as gpd # For reading in shapefiles
import numpy as np
import sys # For displaying package versions
import os # For managing directories and file paths if drive is mounted

# Use shapely.wkt loads to convert WKT to GeoSeries
from shapely.wkt import loads


# Functions from IN-CORE
from pyincore import IncoreClient, Dataset, FragilityService, MappingSet, DataService

# open, read, and execute python program with reusable commands
from pyncoda.ncoda_00b_directory_design import directory_design
from pyncoda.ncoda_00e_geoutilities import *
from pyncoda.ncoda_02b_cleanblockdata import *
from pyncoda.ncoda_02d_addresspoint import *
from pyncoda.ncoda_06d_INCOREDataService import *


class generate_addpt_functions():
    """
    Function runs full process for generating the address point inventory
    Process runs for multiple counties.

    Outputs CSV files and Codebooks
    """

    """
    # Example Residential Archetypes List
    residential_archetypes = { 
        1 : 'One-story sf residential building on a crawlspace foundation',
        2 : 'One-story mf residential building on a slab-on-grade foundation',
        3 : 'Two-story sf residential building on a crawlspace foundation',
        4 : 'Two-story mf residential building on a slab-on-grade foundation'}
    """

    def __init__(self,
            community,
            communities,
            hui_df,
            bldg_inv_gdf,
            bldg_inv_id,
            residential_archetypes,
            archetype_var: str = 'arch_flood',
            building_area_var: str = 'sq_foot',
            building_area_cutoff: int = 300,
            seed: int = 9876,
            version: str = '2.0.0',
            version_text: str = 'v2-0-0',
            basevintage: str = 2010,
            outputfolder: str ="",
            savefiles: bool = True):

        self.community = community
        self.communities = communities
        self.hui_df = hui_df
        self.bldg_inv_gdf = bldg_inv_gdf
        self.bldg_inv_id = bldg_inv_id
        self.residential_archetypes = residential_archetypes
        self.archetype_var = archetype_var
        self.building_area_var = building_area_var
        self.building_area_cutoff = building_area_cutoff
        self.seed = seed
        self.version = version
        self.version_text = version_text
        self.basevintage = basevintage
        self.outputfolder = outputfolder
        self.savefiles = savefiles
        self.year = basevintage


    def obtain_census_block_place_puma_gdf(self, community, year):
        """
        Create one file that has the census block, place, and puma for each block
        for all counties in the community.
        """
        print("***************")
        print("Obtaining Census Block, Place, and PUMA Data")
        print("***************")
        print("")
        yr = year[2:4]
        check_folder = self.outputfolder
        output_filename = f'tl_{year}_{community}_tabblockplacepuma{yr}EPSG4269'
        csv_filepath = check_folder+"/"+output_filename+'.csv'
        savefile = sys.path[0]+"/"+csv_filepath

        # Check if file exists
        if os.path.exists(savefile):
            print("File already exists: "+savefile)
            census_block_place_puma_df = pd.read_csv(savefile)

            # Convert df to gdf
            census_block_place_puma_gdf = df2gdf_WKTgeometry(df = census_block_place_puma_df, 
                        projection = "epsg:4269", 
                        reproject ="epsg:4269",
                        geometryvar = 'blk104269')
            return census_block_place_puma_gdf
            
        # Create empty container to store outputs for in-core
        # Will use these to combine multiple counties
        county_df = {}
        # Workflow for generating Address Point Inventory data for IN-CORE
        for county in self.communities[community]['counties'].keys():
            state_county = self.communities[community]['counties'][county]['FIPS Code']
            state_county_name  = self.communities[community]['counties'][county]['Name']
            state_caps = self.communities[community]['STATE']
            print(state_county_name,': county FIPS Code',state_county)

            # create output folders for hui data generation
            outputfolders = directory_design(state_county_name = state_county_name,
                                                outputfolder = self.outputfolder)

            output_folder = outputfolders['CommunitySourceData']
            # Read in Census Block PUMA Place Data
            county_df[state_county] = \
                obtain_join_block_place_puma_data(
                                county_fips = state_county,
                                state = state_caps,
                                year = year,
                                output_folder = output_folder,
                                replace = False)
            
        # Combine all counties into one dataframe
        census_block_place_puma_gdf = pd.concat(county_df.values(), 
                                    ignore_index=True, axis=0)
        
        #Save results for community name
        savefile = sys.path[0]+"/"+csv_filepath
        census_block_place_puma_gdf.to_csv(savefile, index=False)

        return census_block_place_puma_gdf

    def predict_housingunit_estimate(self,
                                    community,
                                    year,
                                    census_block_place_puma_gdf):

        print("***************")
        print("Predicting Housing Unit Estimates")
        print("***************")
        print("")

        check_folder = self.outputfolder
        output_filename = f'huest_{self.version_text}_{community}_{year}_{self.bldg_inv_id}'
        csv_filepath = check_folder+"/"+output_filename+'.csv'
        savefile = sys.path[0]+"/"+csv_filepath
        if os.path.exists(savefile):
            print("File already exists"+savefile)
            huesimate_df = pd.read_csv(savefile)
            return huesimate_df


        yr = year[2:4]
        # Merge Building Inventory and Census Block Data
        join_column_list = [f'BLOCKID{yr}',f'BLOCKID{yr}_str',
                            f'placeGEOID{yr}',f'placeNAME{yr}']
        geolevel = 'block'
        
        # add representative point to buildings
        bldg_inv_gdf_point = add_representative_point(self.bldg_inv_gdf,year=year)
        building_to_block_gdf = spatial_join_points_to_poly(
                    points_gdf = bldg_inv_gdf_point,
                    polygon_gdf = census_block_place_puma_gdf,
                    point_var = f'rppnt{yr}4326',
                    poly_var = f'blk{yr}4326',
                    geolevel = geolevel,
                    join_column_list = join_column_list)

        # Housing unit inventory needs the block string variable
        self.hui_df[f'BLOCKID{yr}_str'] = \
                self.hui_df[f'blockid'].\
                    apply(lambda x : "B"+str(int(x)).zfill(15))

        # Run Address Point Algorithm
        huesimate_df = predict_residential_addresspoints(
                        building_to_block_gdf = building_to_block_gdf,
                        hui_df = self.hui_df,
                        hui_blockid = f'BLOCKID{yr}_str',
                        bldg_blockid = 'blockBLOCKID10_str',
                        bldg_uniqueid = 'guid',
                        placename_var = 'blockplaceNAME10',
                        archetype_var = self.archetype_var,
                        residential_archetypes = self.residential_archetypes,
                        building_area_var = self.building_area_var,
                        building_area_cutoff = self.building_area_cutoff
                        )

        # Check errors
        #huesimate_df['apcount'].groupby(huesimate_df['ErrorCheck1_int']).describe()
        #huesimate_df['apcount'].groupby(huesimate_df['ErrorCheck2_int']).describe()
        #huesimate_df['apcount'].groupby(huesimate_df['ErrorCheck3_int']).describe()

        # Add Single Family Dummy Variable
        condition1 = (huesimate_df["huestimate"] > 1)
        condition2 = ~(huesimate_df["guid"].isna())
        condition = condition1 & condition2
        huesimate_df.loc[condition,'d_sf'] = 0
        condition1 = (huesimate_df["huestimate"] == 1)
        condition = condition1 & condition2
        huesimate_df.loc[condition, 'd_sf'] = 1 
        #pd.crosstab(huesimate_df['huestimate'], 
        #    huesimate_df['d_sf'],
        #    margins=True, 
        #    margins_name="Total")

        # Identify Unincorporated Areas with Place Name
        # There are many address points that fall just outside of city limits 
        # in Unincorporated places.
        # For these areas use the county information to label 
        # the place names as the County Name.
        huesimate_df.loc[(huesimate_df['blockplaceNAME10'].isna()),
                    'blockplaceNAME10'] = f"Unincorporated"

        huesimate_df.to_csv(savefile, index=False)

        return huesimate_df

    def upload_addpt_file_to_incore(self,
                        community,
                        year,
                        county_list,
                        csv_filepath,
                        output_filename):
        '''
        Metadata and upload to incore dataservice 
        for address point inventory
        '''
        ## Upload Address Point Inventory to IN-CORE
        # Upload CSV file to IN-CORE and save dataset_id
        # note you have to put the correct dataType as well as format
        title = "Address Point Inventory v2.0.0 data for " + community + " " + str(year)
        addpt_description =  '\n'.join(["2010 Address Point Inventory v2.0.0 with required IN-CORE columns. " 
                "Compatible with pyincore v1.4. " 
                "Unit of observation is address point. " 
                "Each address point is associated with a building in the building inventory. "
                "Building Inventory ID is the last part of the address point filename. " 
                "Rosenheim, Nathanael. (2022). npr99/intersect-community-data. Zenodo. " 
                "https://doi.org/10.5281/zenodo.6476122. "
                "File includes data for "+county_list])

        dataset_metadata = {
            "title":title,
            "description": addpt_description,
            "dataType": "incore:addressPoints",
            "format": "table"
            }

        data_service_addpt = loginto_incore_dataservice()
        created_dataset = data_service_addpt.create_dataset(properties = dataset_metadata)
        dataset_id = created_dataset['id']
        print('dataset is created with id ' + dataset_id)

        ## Attach files to the dataset created
        files = [csv_filepath]
        try:
            data_service_addpt.add_files_to_dataset(dataset_id, files)

            print('The file(s): '+ output_filename +" have been uploaded to IN-CORE")
            print("Dataset now on IN-CORE, use dataset_id:",dataset_id)
            print("Dataset is only in personal account, contact IN-CORE to make public")
        except:
            print("Error uploading file to IN-CORE")

            print("Delete dataset from IN-CORE: "+dataset_id)
            data_service_addpt.delete_dataset(dataset_id)
            dataset_idv2 = "No Dataset ID"
            print("Dataset Id set to: "+dataset_idv2)
            return dataset_idv2

        return dataset_id

    def generate_addpt_v2_for_incore(self):
        """
        Generate Address Point data for IN-CORE
        """
        print("***************")
        print("Address Point Inventory Workflow")
        print("***************")
        print("")

        # set year
        year = str(self.year)
        
        # Set community
        community = self.community
        title = "Address Point Inventory v2.0.0 data for "+self.communities[community]['community_name']
        print("Generating",title)
        output_filename = f'addpt_{self.version_text}_{community}_{year}_{self.bldg_inv_id}'
        csv_filepath = self.outputfolder+"/"+output_filename+'.csv'
        savefile = sys.path[0]+"/"+csv_filepath

        # Check if file exists on IN-CORE
        dataset_id = return_dataservice_id(title, output_filename)

        # if dataset_id is not None, return id
        if dataset_id is not None:
            print("Dataset already exists on IN-CORE, use dataset_id:",dataset_id)
            return dataset_id

        # Workflow for generating Address Point Inventory data for IN-CORE
        census_block_place_puma_gdf = \
            self.obtain_census_block_place_puma_gdf(community = community, 
                                                    year = year)

        huesimate_df = \
            self.predict_housingunit_estimate(community = community,
                                            year = year,
                                            census_block_place_puma_gdf = census_block_place_puma_gdf)

        # make a county list for community
        county_list = ''
        for county in self.communities[community]['counties'].keys():
            state_county = self.communities[community]['counties'][county]['FIPS Code']
            state_county_name  = self.communities[community]['counties'][county]['Name']
            print(state_county_name,': county FIPS Code',state_county)
            county_list = county_list + state_county_name+': county FIPS Code '+state_county

            # Check if file exists on local drive
        if os.path.exists(savefile):
            print("File already exists on local drive but "+
                "not on incore dataservice: "
                +savefile)
            # upload file to INCORE dataservice
            dataset_id_final = self.upload_addpt_file_to_incore(
                community = community,
                year = year,
                county_list = county_list,
                csv_filepath = csv_filepath,
                output_filename = output_filename)
            
            if dataset_id_final == "No Dataset ID":
                print("Could not upload file to INCORE")
                print("dataset_id is set to the dataframe")
                # Read in csv as dataframe
                address_point_df = pd.read_csv(csv_filepath, low_memory=False)
                return address_point_df
                
            return dataset_id_final
        """
        Convert the Building inventory into a list of address points

        """

        # Set up census block place puma gdf for merge
        select_cols = ['BLOCKID10_str','BLOCKID10','geometry','rppnt104269']
        census_blocks_df_cols = census_block_place_puma_gdf[select_cols].copy(deep=True)

        # Set up housing unit estimate file for merge
        select_cols = ['guid','blockBLOCKID10_str','huestimate']
        huesimate_df_cols = huesimate_df[select_cols].copy(deep=True)

        # Look at address point count by block
        hui_blockid = 'blockid'
        bldg_blockid = 'BLOCKID10'
        hua_block_counts = self.hui_df[[hui_blockid,'huid']].groupby(hui_blockid).agg('count')
        hua_block_counts.reset_index(inplace = True)
        hua_block_counts = hua_block_counts.\
            rename(columns={'huid': "tothupoints", hui_blockid : bldg_blockid })
        # Sum tothupoints
        hua_apcount = hua_block_counts['tothupoints'].sum()
        print("Total number of expected housing unit address points in county:",hua_apcount)

        # merge address point counts by block with building data
        census_blocks_df_cols = pd.merge(right = census_blocks_df_cols,
                        left = hua_block_counts,
                        right_on = bldg_blockid,
                        left_on =  bldg_blockid,
                        how = 'outer')   

        # fill in missing tothupoints with 0 values
        census_blocks_df_cols['tothupoints'] = census_blocks_df_cols['tothupoints'].fillna(value=0)

        ### Prepare Building Inventory to Expand Based on Housing Unit Estimate
        '''
        For the address point inventory to work there needs to be 
        one observation for each possible housing unit. 
        This means that for buildings that have multiple housing units 
        there will be one address point for each housing unit.

        For places that do not have buildings but have people the 
        address point inventory will provide details on housing units 
        impacted outside of the study area.
        '''

        # If the residentialAP3v1 is used to expand the dataset observations without residential address points will be lost.
        # To keep all buildings add an expand variable
        huesimate_df_cols.loc[(huesimate_df_cols['huestimate']==0),'expandvar'] = 1
        huesimate_df_cols.loc[(huesimate_df_cols['huestimate']>0),'expandvar'] = huesimate_df_cols['huestimate']
        # Check to make sure expand variable was generated correctly
        #pd.crosstab(huesimate_df_cols['expandvar'].loc[huesimate_df_cols['expandvar']<=3],
        #            huesimate_df_cols['huestimate'], margins=True, margins_name="Total")

        ## Expand GUID List
        # Using the expand variable expand building inventory.
        # Clean up missing values for expand variable
        ### Expand var can not be negative
        huesimate_df_cols.loc[(huesimate_df_cols['expandvar']<0)] = 0
        ### Expand var cannot be missing
        huesimate_df_cols.loc[(huesimate_df_cols.expandvar.isna(),'expandvar')] = 1

        # The address point inventory is the expanded housing unit estimate dataframe
        # code to expand dataframe using .repeat() method
        huesimate_df_cols_expand = huesimate_df_cols.reindex(
            huesimate_df_cols.index.repeat(huesimate_df_cols['expandvar']))
        
        # Expand Residential Address Point Count File
        ## Using the count of address point variable expand residential address point count file
        # The expand variable can not have missing values
        census_blocks_df_cols.loc[(census_blocks_df_cols['tothupoints'].isna()) \
            ,'expandvar'] = 0
        census_blocks_df_cols.loc[(census_blocks_df_cols['tothupoints']>=0),\
            'expandvar'] = census_blocks_df_cols['tothupoints']

        # Expand data using repeat method
        census_blocks_df_cols_expand = census_blocks_df_cols.reindex(
            census_blocks_df_cols.index.repeat(census_blocks_df_cols['expandvar']))

        ## Merge Two Address Point Files
        '''
        Combing the address points based on building inventory and 
        the address points based on the 2010 Census will create one file 
        that has address points for the entire county.

        The combined file will show where the building inventory may 
        be missing information within the study community. 
        The combined file will also help to show the populations 
        impacted both inside the study community and in neighboring areas.

        To merge the two files need to add a counter to each file by blockid.
        '''
        # Add counter by block id - use cumulative count method
        census_blocks_df_cols_expand['blockidcounter'] = \
            census_blocks_df_cols_expand.groupby('BLOCKID10').cumcount()

        # Add counter by block id - use cumulative count method
        huesimate_df_cols_expand['blockidcounter'] = \
            huesimate_df_cols_expand.groupby('blockBLOCKID10_str').cumcount()

        # Merge 2 files based on blockid and blockid counter - 
        # keep all observations from both files with full outer join
        address_point_inventory = pd.merge(left = huesimate_df_cols_expand, 
                                        right = census_blocks_df_cols_expand,
                                        left_on=['blockBLOCKID10_str','blockidcounter'], 
                                        right_on=['BLOCKID10_str','blockidcounter'], how='outer')

        '''
        # Check merge - examples were Building Id is missing
        displaycols = ['guid','BLOCKID10']
        condition = address_point_inventory['guid'].isna()
        address_point_inventory[displaycols].loc[condition].head()
        # Check merge - examples were there is no census data
        displaycols = ['guid','BLOCKID10']
        condition = address_point_inventory['tothupoints'].isnull()
        address_point_inventory[displaycols].loc[condition].head()
        '''

        # Fix issue with missing blockid vs BLOCKID10
        address_point_inventory.loc[address_point_inventory.BLOCKID10_str.isna(),
            'BLOCKID10_str'] = address_point_inventory['blockBLOCKID10_str']

        cols = [col for col in address_point_inventory]
        #### The Address Point ID is based on the building id first then the block id
        '''
        In the best case scenario every address point is connected to a 
        building but in cases where the building id is missing 
        then the address point is based on the Census Block ID.
        '''
        address_point_inventory.loc[(address_point_inventory['guid'].isna()),
            'strctid'] = address_point_inventory.\
            apply(lambda x: "C"+ str(x['BLOCKID10_str']).zfill(36), axis=1)
        address_point_inventory.loc[(address_point_inventory['guid'].notna()),
            'strctid'] = address_point_inventory.\
            apply(lambda x: "ST"+ str(x['guid']).zfill(36), axis=1)

        # Sort Address Points by The first part of the address point 
        address_point_inventory.sort_values(by=['strctid'])
        # Add Counter by Building
        address_point_inventory['apcounter'] = address_point_inventory.groupby('strctid').cumcount()

        '''
        To make a unique id for the address points need to have a combination of unique values. 
        The first part of the address point id is based on either the building id or the block id.
        Within each Building or Census Block the counter variable provides a way to 
        identify address points within a block.
        '''
        address_point_inventory['addrptid'] = address_point_inventory.apply(lambda x: x['strctid'] + "AP" +
                                                                str(int(x['apcounter'])).zfill(6), axis=1)
        # Move Primary Key Column to first Column
        cols = ['addrptid']  + [col for col in address_point_inventory if col != 'addrptid']
        address_point_inventory = address_point_inventory[cols]

        # Confirm Primary Key is Unique and Non-Missing
        # address_point_inventory.addrptid.describe()

        #### Generate Flag Variables
        # For the merged dataset identify cases where either building or census data is missing.
        # Create Address Poing Flag Variable
        address_point_inventory['flag_ap'] = 0
        address_point_inventory.loc[(address_point_inventory['tothupoints'].isnull()),'flag_ap'] = 1
        address_point_inventory.loc[(address_point_inventory['guid'].isna()),'flag_ap'] = 2
        address_point_inventory.loc[(address_point_inventory['BLOCKID10_str'].isnull()),'flag_ap'] = 3
        # Check to make sure expand variable was generated correctly
        address_point_inventory.groupby(['flag_ap']).count()

        ## Identify observations that represent the primary building
        '''
        In some future exploration cases it would be of interest to run cross 
        tabulations on just the buildings, instead of all of the address points. 
        To identify the buildings it is possible to use the address point counter (apcounter) 
        and the address point flag (flag_ap). If the counter is 0 and the flag is 0 or 1 then the 
        address point observation is the first address point in a building.
        '''
        # create a binary variable 0 - not the primary building observation, 1 - use to count buildings
        address_point_inventory['bldgobs'] = 0
        # If the ap count is 0 and the flag is 0 or 1 then the bldgobs should be 1
        address_point_inventory.loc[(address_point_inventory['apcounter'] == 0) &
                                    (address_point_inventory['flag_ap'] <= 1), 'bldgobs'] = 1

        ## Set Geometry for Address Points
        '''
        The location of the address point will be important for identifying the 
        hazard impact. There are two options for the address point location.

        If there is a building representative point use the building representative point
        If there building data is missing use the representative point from the census block
        '''
        # Merge guid and geometry from building inventory to address point inventory
        address_point_inventory_geo = pd.merge(left = address_point_inventory,
                                            right = self.bldg_inv_gdf[['guid','geometry']],
                                            left_on=['guid'],
                                            right_on=['guid'], 
                                            how='left')
        # Rename geometry column to block geometry
        address_point_inventory_geo.rename(columns={'geometry_x':'block10_geometry'}, inplace=True)

        # Rename geometry column to building geometry
        address_point_inventory_geo.rename(columns={'geometry_y':'building_geometry'}, inplace=True)

        ### Identify Residential Address Points
        '''
        For Address Points that have an estimate for the number of housing units, or if the building data is
        missing then the address point is likely to be a residential address point.

        The knowledge that an address point is residential will help prioritize the 
        allocation of housing units to address points.

        For address points in buildings with more than housing unit the number of 
        housing units also provides a way to prioritize renters and owners. 
        With renters more likely to be allocated to buildings with greater numbers of housing units.
        '''
        address_point_inventory_geo['residential'] = 0
        # If the building id is missing then the address point is residential
        address_point_inventory_geo.loc[(address_point_inventory_geo['guid'].isna()),'residential'] = 1
        # The the variable residentialAP3v1 is greater than 0 then the address point is residential
        address_point_inventory_geo.loc[(address_point_inventory_geo['huestimate']>0),'residential'] = 1
        # Check new variable
        # address_point_inventory_geo[['flag_ap','residential']].groupby(['flag_ap']).sum()

        ## Keep primary columns
        '''
        The address point county file has many columns but only a few are needed to 
        generate the address point inventory.
        '''
        ## Create block id variable from substring of BLOCKID10_str
        address_point_inventory_geo['blockid'] = address_point_inventory_geo['BLOCKID10_str'].str[1:16]
        select_cols = ['addrptid','strctid','guid','blockid','BLOCKID10_str',
            'building_geometry','block10_geometry','rppnt104269',
            'huestimate','residential','bldgobs','flag_ap']
        address_point_inventory_cols = address_point_inventory_geo[select_cols].copy(deep=True)

        ### Merge Address Point inventory with Building and Census Data
        '''
        To analyze the impact of the hazard the address point inventory needs to include 
        building information and census place information. 
        The building information will include building type, year built, 
        and appraised values (when available). 
        The Census information will include city name and count information.
        '''
        # Keep columns for merge
        merge_cols = ['guid',self.archetype_var]
        building_df_merge_cols = self.bldg_inv_gdf[merge_cols]

        # merge selected columns from building inventory to address point inventory
        address_point_inventory_cols_bldg = pd.merge(
                                        address_point_inventory_cols, 
                                        building_df_merge_cols,
                                        left_on='guid', 
                                        right_on='guid', 
                                        how='left')

        # For the merge only need a select number of columns
        merge_cols = ['BLOCKID10_str','placeGEOID10','placeNAME10','COUNTYFP10']
        census_blocks_df_merge_cols = census_block_place_puma_gdf[merge_cols]

        # merge selected columns from building inventory to address point inventory
        address_point_inventory_cols_bldg_block = pd.merge(
                                        address_point_inventory_cols_bldg, 
                                        census_blocks_df_merge_cols,
                                        left_on='BLOCKID10_str', 
                                        right_on='BLOCKID10_str', 
                                        how='left')

        # address_point_inventory_cols_bldg_block[['placeNAME10','guid']].groupby(['placeNAME10']).count()
        ### Identify Unincorporated Areas with Place Name
        '''
        There are many address points that fall just outside of city limits in unincorporated places. 
        For these areas use the county information to label the place names as the County Name.
        '''
        condition1 = (address_point_inventory_cols_bldg_block['placeNAME10'].isna())
        address_point_inventory_cols_bldg_block.loc[
                        condition1, 'placeNAME10'] = "Unincorporated"

        # Check new variable
        #pd.crosstab(address_point_inventory_cols_bldg_block['placeNAME10'], 
        #            address_point_inventory_cols_bldg_block['COUNTYFP10'], margins=True, margins_name="Total")


        ### Save Work as CSV
        '''
        A CSV file with the Well Known Text (WKT) geometry provides flexibility for saving and working with files.
        '''
        # Move Foreign Key Columns Block ID State, County, Tract to first Columns
        first_columns = ['addrptid','guid','strctid','blockid','placeGEOID10','placeNAME10','COUNTYFP10']
        cols = first_columns + [col for col in address_point_inventory_cols_bldg_block if col not in first_columns]
        address_point_inventory_cols_bldg_block = address_point_inventory_cols_bldg_block[cols]

        #Save results for community name
        address_point_inventory_cols_bldg_block.to_csv(savefile, index=False)

        # Save second set of files in common directory
        #common_directory = self.outputfolder+"/../"+output_filename
        #address_point_inventory_cols_bldg_block.to_csv(common_directory+'.csv', index=False)

        #### Add X Y variables
        '''
        To be consistent with previous address point inventories add X and Y variables

        Issue with missing geometries could not be fixed earlier because 
        data frames were geodataframes. 
        Now that the data frame is a regular data frame can use geometry columns as string to fix the issue.
        '''
        ## read in the address point inventory csv file
        address_point_df = pd.read_csv(csv_filepath, low_memory=False)

        # Set Address Point Geometry
        # The default geometry is the building representative point
        address_point_df['geometry'] = address_point_df['building_geometry']
        # When the building representative point is missing use the Census Block Representative Point
        address_point_df.loc[(address_point_df['geometry'].isnull()),'geometry'] = address_point_df['rppnt104269']

        # Convert Data Frame to Geodataframe
        address_point_gdf = gpd.GeoDataFrame(address_point_df)
        address_point_gdf['geometry'] = address_point_gdf['geometry'].apply(lambda x: loads(x))

        # Add X and Y variables
        address_point_gdf['x'] = address_point_gdf['geometry'].x
        address_point_gdf['y'] = address_point_gdf['geometry'].y

        ### ISSUE - There are buildings on the edge of the county
        '''
        These observations do not geocode inside the county boundary.

        Observations with missing values can not be converted to integer and therefore will have the trailing .0 - since they are a float.

        The address point inventory needs to have all buildings
        in the inventory. If a building is missing then the housing unit 
        allocation method will not work.
        '''
        # Set observations outside of the county to with filled in values
        condition1 = (address_point_gdf['COUNTYFP10'].isna())
        address_point_gdf.loc[condition1,'COUNTYFP10'] = 999
        address_point_gdf.loc[condition1,'placeNAME10'] = "Outside County"
        address_point_gdf.loc[condition1,'blockid'] = 999999999999999
        address_point_gdf.loc[condition1,'BLOCKID10_str'] = 'B999999999999999'
        # Check if placeGEOID10 is missing
        condition1 = (address_point_gdf['placeGEOID10'].isna())
        address_point_gdf.loc[condition1,'placeGEOID10'] = 9999999

        # Remove .0 from data
        address_point_gdfv2 = address_point_gdf.\
            applymap(lambda cell: int(cell) if str(cell).endswith('.0') else cell)
        # drop columns not needed for analysis
        address_point_gdfv2.drop(['geometry','building_geometry','block10_geometry','rppnt104269'], \
            axis=1, inplace=True)
        
        # Resave results for community name
        address_point_gdfv2.to_csv(savefile, index=False)

        # upload file to INCORE dataservice
        # try to upload file to INCORE dataservice
        dataset_id_final = self.upload_addpt_file_to_incore(
            community = community,
            year = year,
            county_list = county_list,
            csv_filepath = csv_filepath,
            output_filename = output_filename)

        if dataset_id_final == "No Dataset ID":
            print("Could not upload file to INCORE")
            print("dataset_id is the dataframe")
            return address_point_gdfv2

        return dataset_id_final

    


