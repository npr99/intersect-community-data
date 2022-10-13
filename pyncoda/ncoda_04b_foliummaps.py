# Copyright (c) 2022 Nathanael Rosenheim and others. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

import pandas as pd
import geopandas as gpd
import folium as fm
from folium import plugins # Add minimap and search plugin functions to maps
from folium.map import *

def folium_marker_layer_map(gdf,
                             gdfvar,
                             layername,
                             color_levels):
    """
    Map geodataframe with folium
    Color code markers based on variable levels
    """
    
    # Check projection is epsg:4326
    
    # Find the bounds of the Census Block File
    minx = gdf.bounds.minx.min()
    miny = gdf.bounds.miny.min()
    maxx = gdf.bounds.maxx.max()
    maxy = gdf.bounds.maxy.max()

    map = fm.Map(location=[(miny+maxy)/2,(minx+maxx)/2], zoom_start=16)

    colorlist = ['red', 'blue', 'green', 'purple', 'orange', 'darkred',
                'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue', 
                'darkpurple', 'white', 'pink', 'lightblue', 'lightgreen', 
                'gray', 'black', 'lightgray']

    #  Check if color levels is larger than color list
    if len(color_levels) > len(colorlist):
        print(f"Color levels = {len(color_levels)} is larger than color list ({len(colorlist)})")
        # increase the color list to match the number of color levels
        colorlist = colorlist * (len(color_levels) // len(colorlist) + 1)
        
    i = 0
    for color_level in color_levels:
        layer_map_name=layername+' '+ str(i+1)
        feature_group = FeatureGroup(name=layer_map_name)
        locations = gdf.loc[gdf[gdfvar] == color_level]
        
        for idx, row in locations.iterrows():
            # Get lat and lon of points
            lon = row['geometry'].x
            lat = row['geometry'].y

            # Get NAME information
            label = row[gdfvar]
            # Add marker to the map
            feature_group.add_child(Marker([lat, lon], 
                                        popup=label,
                                        icon=fm.Icon(color=colorlist[i],
                                            icon ='home')))
        
        i = i + 1
        map.add_child(feature_group)
    

    # Add Layer Control to the Map
    fm.LayerControl(collapsed=False, autoZIndex=False).add_to(map)

    # Add minimap
    plugins.MiniMap().add_to(map)

    # How should the map be bound - look for the southwest and 
    # northeast corners of the data
    sw_corner = [gdf.bounds.miny.min(),gdf.bounds.minx.min()]
    ne_corner = [gdf.bounds.maxy.max(),gdf.bounds.maxx.max()]
    map.fit_bounds([sw_corner, ne_corner])

    return map

def count_huid_by_building(hua_df, 
                            blocknum = '',
                            groupby_vars = ['x','y']):
    """
    Select housing units in a block
    """

    # Check if Block2010 is missing
    if 'Block2010' not in hua_df.columns:
        # Add block id 2010 as string
        # zero pad block id to 15 digits
        print("Adding Block2010")
        hua_df['Block2010'] = hua_df['blockid'].apply(lambda x : str(int(x)).zfill(15))

    # check if block number is set
    if blocknum != '':
        condition1 = (hua_df['Block2010'] == blocknum)
        block_df = hua_df.loc[condition1].copy()
    else:
        block_df = hua_df.copy()

    # count by geometry
    huid_count_df = block_df[['huid']+groupby_vars].groupby(groupby_vars).count()
    # reset index
    huid_count_df = huid_count_df.reset_index()

    return huid_count_df

def map_selected_block(hua_df, blocknum):

    # count huid by building
    huid_count_df = count_huid_by_building(hua_df, blocknum)

    # convert points to gdf
    huid_count_gdf = gpd.GeoDataFrame(
        huid_count_df, geometry=gpd.points_from_xy(huid_count_df.x, huid_count_df.y))

    """
    # convert dataframe to geodataframe using geometry
    from shapely.wkt import loads
    huid_count_gdf = gpd.GeoDataFrame(huid_count_df).copy(deep=True)
    huid_count_gdf['geometry'] = huid_count_gdf['geometry'].apply(lambda x : loads(x))
    """

    # Create list if HUID count unique values
    huid_count_levels = huid_count_gdf['huid'].unique().tolist()
    print("Housing unit counts",huid_count_levels)


    # Map housing units in block
    map = folium_marker_layer_map(gdf = huid_count_gdf,
                            gdfvar="huid",
                            layername = "HUID count",
                            color_levels = huid_count_levels)
    return map
    