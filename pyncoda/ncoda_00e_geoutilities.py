# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

# New functions in that could be added to https://github.com/IN-CORE/pyincore/blob/master/pyincore/utils/geoutil.py

import pandas as pd
import geopandas as gpd
import geopandas as gpd # For obtaining and cleaning spatial data
import folium as fm # folium has more dynamic maps - but requires internet connection
# Nearest neighboring point
import numpy as np
import scipy as scipy
from scipy.spatial import KDTree # required to find nearest point location

# Use shapely.wkt loads to convert WKT to GeoSeries
from shapely.wkt import loads
# new method 
# https://pyproj4.github.io/pyproj/stable/gotchas.html#axis-order-changes-in-proj-6
from pyproj import CRS

def df2gdf_WKTgeometry(df: pd.DataFrame, 
                       projection = "epsg:4326", 
                       reproject ="epsg:4326",
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

    print(f"Converting {geometryvar} to Geodataframe")
    gdf = gpd.GeoDataFrame(df).copy(deep=True) 
        # deep copy to ensure not linked to source df
    gdf[geometryvar] = gdf[geometryvar].apply(lambda x: loads(x))

    # Geodata frame requires geometry and CRS to be set
    gdf = gdf.set_geometry(gdf[geometryvar])

    gdf.crs = CRS(projection)
    
    # reproject data
    gdf = gdf.to_crs(reproject)
    
    return gdf

def add_representative_point(polygon_gdf,
                             year,
                             epsg: int = 4269,):
    """
    Add representative point to a polygon

    epsg: 4326 or 4269 for lat/lon point
    https://gis.stackexchange.com/questions/170839/is-re-projection-needed-from-srid-4326-wgs-84-to-srid-4269-nad-83

    epsg default is 4269 but use 4326 if that is the current projection
    EPSG 4269 uses NAD 83, and is the US Census default, which will have slightly different lat lon points 
    when compared to EPSG 4326 which uses WGS 84.
    """

    test_crs = polygon_gdf.crs
    if test_crs == "epsg:4269":
        epsg = 4269
    elif test_crs == "epsg:4326":
        epsg = 4326
    else:
        # Ensure both points and polygons have the same CRS
        polygon_gdf = polygon_gdf.to_crs(epsg=epsg)

    yr = year[2:4]

    # Add Representative Point
    polygon_gdf[f'rppnt{yr}{epsg}'] =\
        polygon_gdf['geometry'].representative_point()
    polygon_gdf[f'rppnt{yr}{epsg}'].label = \
        f"Representative Point {year} EPSG {epsg} (WKT)"
    polygon_gdf[f'rppnt{yr}{epsg}'].notes = \
        f"Internal Point within census block {year} poly EPSG {epsg}"

    # Add Column that Duplicates Polygon Geometry - 
    # allows for switching between point and polygon geometries for spatial join
    polygon_gdf[f'blk{yr}{epsg}'] = polygon_gdf['geometry']
    polygon_gdf[f'blk{yr}{epsg}'].label = \
        f"{year} Census Block Polygon EPSG {epsg} (WKT)"
    polygon_gdf[f'blk{yr}{epsg}'].notes = \
        f"Polygon Shape Points for {year} Census Block EPSG {epsg}"

    return polygon_gdf

def nearest_pt_search(gdf_a: gpd.GeoDataFrame, gdf_b: gpd.GeoDataFrame, 
                    uniqueid_a: str, uniqueid_b: str, k=1,
                    dist_cutoff = 99999):
        """Given two sets of points add unique id from locations a to locations b
        Inspired by: https://towardsdatascience.com/using-scikit-learns-binary-trees-to-efficiently-find-latitude-and-longitude-neighbors-909979bd929b
        
        This function is used to identify buildings associated with businesses, schools, hospitals.
        The locations of businesses might be geocoded by address and may not overlap
        the actual structure. This function helps resolve this issue.
        
        Tested Python Environment:
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
                if distance is greater than cutoff neighbor is not considered
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
            
            # Critical step - select column with distances for neighbor i
            locations['distance'] = distances[:,i]
            # look for distances greater than cutoff  
            locations['distoutlier'] = np.where(locations['distance'] > dist_cutoff, True, False)
            
            # add location a index
            # Critical step - select column with indices for neighbor i
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


def spatial_join_points_to_poly(points_gdf, 
                                polygon_gdf, 
                                point_var, 
                                poly_var, 
                                geolevel, 
                                epsg: int = 4326,
                                join_column_list: list = [],
                                buffer_dist: int = 0.001):
    """
    Function adds polygon variables to block points.
    point_var: Variable with WKT Point Geometry for Polygon GDF
    poly_var: Variable with WKT Polygon Geometry for Polygon GDF
    geolevel: Geography level of the polygon - example Place or PUMA
    join_column_list: Variable list to be transferer from Polygon File to Block File
    buffer_dist: buffer distance in degrees for lat/lon around point bounds
     - https://en.wikipedia.org/wiki/Decimal_degrees
     - example: 0.001 for 1/1000th of a degree or approximately 100 meters
     - example: 0.0001 for 1/10000th of a degree or approximately 10 meters
    Original code idea from 
    https://geoffboeing.com/2016/10/r-tree-spatial-index-python/

    Geopandas has a function sjoin that could be used
    however sjoin can be slow and does not allow for selecting columns

    future improvement: if there are multiple polygons for a point,
    the function could create multiple rows for the point.
    """
    # make copies of input df and gdf
    copy_point_gdf = points_gdf.copy(deep=True)
    copy_polygon_gdf = polygon_gdf.copy(deep=True)

    # Ensure both points and polygons have the same CRS
    copy_point_gdf = copy_point_gdf.to_crs(epsg=epsg)
    copy_polygon_gdf = copy_polygon_gdf.to_crs(epsg=epsg)

    # Find the bounds of the point File

    minx = copy_point_gdf.bounds.minx.min() - buffer_dist # subtract buffer from minimum values
    miny = copy_point_gdf.bounds.miny.min() - buffer_dist
    maxx = copy_point_gdf.bounds.maxx.max() + buffer_dist
    maxy = copy_point_gdf.bounds.maxy.max() + buffer_dist
    copy_point_gdf_bounds = [minx, miny, maxx, maxy]

    # Select polygons within Bounds of Study Area
    # build the r-tree index - for polygon file
    print("Polygon file has",copy_polygon_gdf.shape[0],geolevel,"polygons.")
    sindex_poly_gdf = copy_polygon_gdf.sindex
    possible_matches_index = list(sindex_poly_gdf.intersection(copy_point_gdf_bounds))
    area_poly_gdf = copy_polygon_gdf.iloc[possible_matches_index]
    print("Identified",area_poly_gdf.shape[0],geolevel,"polygons to spatially join.")

    # build the r-tree index - Using Representative Point
    copy_point_gdf['geometry'] = copy_point_gdf[point_var]
    sindex_copy_point_gdf = copy_point_gdf.sindex

    #Loops for spatial join are time consuming
    #Here is a way to know that the loop is running and how long it takes to run
    #https://cmdlinetips.com/2018/01/two-ways-to-compute-executing-time-in-python/
    # find the points that intersect with each subpolygon and add ID to Point
    for index, polygon in area_poly_gdf.iterrows():
        if index%100==0:
            print('.', end ="")

        # find approximate matches with r-tree, then precise matches from those approximate ones
        possible_matches_index = \
            list(sindex_copy_point_gdf.intersection(polygon['geometry'].bounds))
        possible_matches = copy_point_gdf.iloc[possible_matches_index]
        precise_matches = \
            possible_matches[possible_matches.intersects(polygon['geometry'])]
        for col in join_column_list:
            copy_point_gdf.loc[precise_matches.index,geolevel+col] = polygon[col]

    # Switch Geometry back to Polygon
    copy_point_gdf['geometry'] = copy_point_gdf[poly_var]

    return copy_point_gdf

def single_layer_folium_map(gdf,layer_name, output_folder):   
    # Find the bounds of the Census Block File
    minx = gdf.bounds.minx.min()
    miny = gdf.bounds.miny.min()
    maxx = gdf.bounds.maxx.max()
    maxy = gdf.bounds.maxy.max()

    # plot the intersections and the city
    gdf_map = fm.Map(location=[(miny+maxy)/2,(minx+maxx)/2], zoom_start=10)
    #fm.GeoJson(gdf).add_to(gdf_map)

    style_function = lambda x: {'color':'green','fillColor': 'transparent' }

    fm.GeoJson(gdf[['geometry']],
              name=layer_name,
              style_function=style_function,).add_to(gdf_map)
    
    fm.LayerControl().add_to(gdf_map)
    
    gdf_map.save(output_folder+'/'+layer_name+'.html')
    
    return gdf_map

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