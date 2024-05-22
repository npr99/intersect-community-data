# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

# New functions in that could be added to https://github.com/IN-CORE/pyincore/blob/master/pyincore/utils/geoutil.py

import pandas as pd
import geopandas as gpd
import geopandas as gpd # For obtaining and cleaning spatial data


def df2gdf_WKTgeometry(df: pd.DataFrame, projection = "epsg:4326", reproject ="epsg:4326",
                       geometryvar = 'geometry'):

    """Function to convert dataframe with WKT Geometry to Geodata Frame
    
    Tested Python Environment:
        Python Version      3.7.10
        geopandas version:  0.9.0
        pandas version:     1.2.4
        shapely version:    1.7.1
    Args:
        :param df: dataframe with Well Known Text (WKT) geometry
        :param projection: String with Coordinate Reference System - default is epsg:4326
        :help projection: https://spatialreference.org/ref/epsg/wgs-84/
            Use UTM for measuring distances and area in meters
            Common Universal Transverse Mercator (UTM) for North America
            UTM zone 10N = West Coast     = epsg:26910
            UTM zone 17N = North Carolina = epsg:26917
            UTM zone 19N = Maine          = epsg:26919
            https://spatialreference.org/ref/epsg/26910/
        :param geometryvar: Variable with WKT geometry
    
    Returns:
        geopandas geodataframe in CRS
    
    @author: Nathanael Rosenheim - nrosenheim@arch.tamu.edu
    @version: 2021-06-19T1217
    """
    
    # Check version replication # to do - add way to print warning when version is not same as test
    # check_package_version(gpd,"0.9.0") - example call of function - returns confirmation or warning

    # Use shapely.wkt loads to convert WKT to GeoSeries
    from shapely.wkt import loads

    gdf = gpd.GeoDataFrame(df).copy(deep=True) # deep copy to ensure not linked to source df
    gdf[geometryvar] = gdf[geometryvar].apply(lambda x: loads(x))

    # Geodata frame requires geometry and CRS to be set
    gdf = gdf.set_geometry(gdf[geometryvar])
    # new method https://pyproj4.github.io/pyproj/stable/gotchas.html#axis-order-changes-in-proj-6
    from pyproj import CRS
    gdf.crs = CRS(projection)
    
    # reproject data
    gdf = gdf.to_crs(reproject)
    
    return gdf

# Nearest neighboring point
import numpy as np
import scipy as scipy
from scipy.spatial import KDTree # required to find nearest point location


def nearest_pt_search(gdf_a: gpd.GeoDataFrame, gdf_b: gpd.GeoDataFrame, 
                    uniqueid_a: str, uniqueid_b: str, k=1,
                    dist_cutoff = 99999):
        """Given two sets of points add unique id from locations a to locations b
        Inspired by: https://towardsdatascience.com/using-scikit-learns-binary-trees-to-efficiently-find-latitude-and-longitude-neighbors-909979bd929b
        
        This function is used to itdentify buildings associated with businesses, schools, hospitals.
        The locations of businesses might be geocoded by address and may not overlap
        the actual structure. This function helps resolve this issue.
        
        Tested Python Enviroment:
            Python Version      3.7.10
            geopandas version:  0.9.0
            pandas version:     1.2.4
            scipy version:     1.6.3
            numpy version:      1.20.2
        Args:
            gdf_a: Geodataframe with list of locations with unique id
            uniqueid_a: Unique ID for gdf with locations a
            gdf_b: Geodataframe with list of locations with unique id
            uniqueid_b: Unique ID for gdf with locations b
            k : The amount of neighbors to return
            dist_cutoff : Integer value distance - meters if CRS UTM
                if distance is greater than cutoff neighbor is not considererd
                initially set to 9999 which will set the value to outliers
        Returns:
            GeoDataFrame: Locations b with nearest unique id from Locations a Geopandas DataFrame object
        
        @author: Nathanael Rosenheim - nrosenheim@arch.tamu.edu
        @version: 2021-06-21T1021
        """
        
        # Check if both gdf have the same CRS
        if gdf_a.crs == gdf_b.crs:
            save_crs = gdf_a.crs
        
        # set up locations a
        locations_a = gdf_a[[uniqueid_a,'geometry']].copy()
        
        locations_a['LON'] = locations_a.geometry.apply(lambda p: p.centroid.x)
        locations_a['LAT'] = locations_a.geometry.apply(lambda p: p.centroid.y)
        
        # Critical step: reset index for locations a
        locations_a = locations_a.reset_index()
        
        # set up locations b
        locations_b = gdf_b[[uniqueid_b,'geometry']].copy()
        
        locations_b['LON'] = locations_b.geometry.apply(lambda p: p.centroid.x)
        locations_b['LAT'] = locations_b.geometry.apply(lambda p: p.centroid.y)
        
        # Takes the first group's latitude and longitude values to construct
        # the kd tree.
        kd = KDTree(locations_a[["LAT", "LON"]].values)

        # Executes a query with the second group. This will return two
        # arrays.
        distances, indices = kd.query(locations_b[["LAT", "LON"]], k=k)
        
        # add distance to output - what unit is distance? 
        # if projection is UTM the distance is in meters
        # place distance array columns in new dataframe columns
        
        # Identify outliers based on mean and standard deviation of nearest neighbors
        # outlier was considered but distances were too large and created errors
        outlier = distances[:,0].mean() + distances[:,0].std()*3
        
        if dist_cutoff == 9999:
            dist_cutoff = outlier
        
        # start an empty container to hold all of the locations
        append_locations = []
        for i in range(0,k):
            # copy locations b to new dataframe
            locations = locations_b.copy(deep=True)
            
            # Which neighbor is this match
            locations['neighbor'] = i+1
            
            # Crtitical step - select column with distances for neighbor i
            locations['distance'] = distances[:,i]
            # look for distances greater than cutoff  
            locations['distoutlier'] = np.where(locations['distance'] > dist_cutoff, True, False)
            
            # add location a index
            # Crtitical step - select column with indices for neighbor i
            locations['location a index'] = indices[:,i]
                       
            # clear index if distance is greater than cutoff
            locations.loc[(locations['distoutlier']==True), 'location a index'] = np.nan
            
            # Merge by index
            # https://thispointer.com/pandas-how-to-merge-dataframes-by-index-using-dataframe-merge-part-3/
            locations = locations.merge(locations_a, left_on = 'location a index', 
                                             right_index=True, how='inner')
            
            # append location data
            append_locations.append(locations)
        
        # Create dataframe from appended locations data
        locationmatch = pd.concat(append_locations)
        
        # Set geodateframe crs
        from pyproj import CRS
        locationmatch.crs = CRS(save_crs)

    
        return locationmatch

def spatial_join_polygon_point(points_gdf,
                                polygon_gdf,
                                epsg: int = 4326,
                                join_column_list: list = []):
    """
    Original code idea from 
    https://geoffboeing.com/2016/10/r-tree-spatial-index-python/
    """

    # Ensure both points and polygons have the same CRS
    points_gdf = points_gdf.to_crs(epsg=epsg)
    polygon_gdf = polygon_gdf.to_crs(epsg=epsg)

    # build the r-tree index - for Points
    sindex_points_gdf = points_gdf.sindex

    #Loops for spatial join are time consuming
    #Here is a way to know that the loop is running and how long it takes to run
    #https://cmdlinetips.com/2018/01/two-ways-to-compute-executing-time-in-python/

    # find the points that intersect with each subpolygon
    for index, polygon in polygon_gdf.iterrows():
    # find approximate matches with r-tree, then precise matches from those 
    # approximate ones
        possible_matches_index = list(sindex_points_gdf.\
            intersection(polygon['geometry'].bounds))
        possible_matches = points_gdf.iloc[possible_matches_index]
        precise_matches = possible_matches[possible_matches.\
            intersects(polygon['geometry'])]

        for col in join_column_list:
            points_gdf.loc[precise_matches.index,col] = polygon[col]

    return points_gdf

""" Test application with establishments and lodes
## Attempt Nearest Point Merge with Jobs and Establishments
# Run merge on Industry Code and then recombine

jobi_gdf_rmerge = df2gdf_WKTgeometry(df = jobi_df_rmerge, \
     projection = "epsg:4326",reproject="epsg:26910", geometryvar= 'w_geometry')
jobi_gdf_rmerge.head()

# Create list of Industry Codes in Joblist
industry_code_list = jobi_gdf_rmerge['IndustryCode'].unique().tolist()
# Create empty dictionary to store merge results by industry
job_estab_gdf_icode = {}
# Loop through unique industry codes
for industry_code in industry_code_list:
    # Split establishment list by industry code
    estab_gdf_icode = estab_gdf.loc\
        [estab_gdf['IndustryCode'] == industry_code]
    # Split job list by industry code
    jobi_gdf_rmerge_icode = jobi_gdf_rmerge.loc\
        [jobi_gdf_rmerge['IndustryCode'] == industry_code]
    # Run nearest neighbor search using industy code dataframes
    job_estab_gdf_icode[industry_code] = nearest_pt_search(gdf_a = estab_gdf_icode,
                                    gdf_b = jobi_gdf_rmerge_icode,
                                    uniqueid_a = 'estabid',
                                    uniqueid_b = 'jobid',
                                    k = 2,
                                    dist_cutoff = 500)
# Recombine industry code dataframes
job_estab_gdf = pd.concat(job_estab_gdf_icode.values(), ignore_index=True)

## Add Establishment ID to Joblist
# Keep only first neighbor
job_estab_gdf_neighbor1 = job_estab_gdf.loc[job_estab_gdf['neighbor']==1].copy()
job_estab_gdf_neighbor1.jobid.describe()

# Keep the closest neighbor
job_estab_gdf_neighbor1 = job_estab_gdf_neighbor1.sort_values(by=['jobid','distance'])
job_estab_gdf_neighbor1['uniquecounter'] = \
    job_estab_gdf_neighbor1.groupby(['jobid']).cumcount() + 1
job_estab_gdf_neighbor1.loc[job_estab_gdf_neighbor1['uniquecounter']!=1]
joblist_estab_gdf = pd.merge(right = jobi_df_rmerge,
                               left  = job_estab_gdf_neighbor1[\
                                   ['jobid','estabid','geometry_y',
                                   'LON_y','LAT_y','distance']],
                               on = 'jobid',
                               how = 'right')
joblist_estab_gdf[['jobid','estabid']].astype(str).describe()

joblist_estab_gdf = pd.merge(right = joblist_estab_gdf,
                               left  = estab_gdf[\
                                   ['estabid','SIName','NAICS4D','Employee_Size_Location']],
                               on = 'estabid',
                               how = 'right')
joblist_estab_gdf.head()
table_df = joblist_estab_gdf
table = pd.pivot_table(table_df, 
                        values = ['jobid','Employee_Size_Location','distance'],
                        index=['estabid','SIName'], 
                        aggfunc={'jobid':'count',
                                'Employee_Size_Location': np.mean,
                                'distance': np.mean}, 
                        margins=True, margins_name = 'Total')
table
joblist_estab_gdf.groupby(['IndustryCode','NAICS4D','estabid']).\
        aggregate({'jobid':'count'})
"""