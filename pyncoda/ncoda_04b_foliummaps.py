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
import contextily as cx
import os
import matplotlib.pyplot as plt
import contextily as cx

def folium_marker_layer_map(gdf,
                             gdfvar,
                             layername,
                             color_levels):
    """
    Map geodataframe with folium
    Color code markers based on variable levels
    """
    
    # Check that gdf is a geodataframe
    if not isinstance(gdf, gpd.geodataframe.GeoDataFrame):
        # convert points to gdf
        gdf = gpd.GeoDataFrame(
            gdf, geometry=gpd.points_from_xy(gdf.x, gdf.y), crs="EPSG:4326")
    # Check projection is epsg:4326
    if gdf.crs != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
    
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

def count_gdfvar_by_building(hua_df, 
                            blocknum = '',
                            groupby_vars = ['x','y'],
                            gdvar: str = 'huid'):
    """
    Select housing units in a block
    """

    # Check if Block2010 is missing
    if 'Block2010' not in hua_df.columns:
        # Add block id 2010 as string
        # zero pad block id to 15 digits
        print("Adding Block2010")
        hua_df['Block2010'] = hua_df['blockid'].apply(lambda x : str(int(x)).zfill(15))
    # Check if Block2010 is a string
    if hua_df['Block2010'].dtype != 'object':
        print("Converting Block2010 to string")
        hua_df['Block2010'] = hua_df['Block2010'].apply(lambda x : str(int(x)).zfill(15))

    # check if block number is set
    if blocknum != '':
        condition1 = (hua_df['Block2010'] == blocknum)
        block_df = hua_df.loc[condition1].copy()
    else:
        block_df = hua_df.copy()

    # count by geometry
    gdvar_count_df = block_df[[gdvar]+groupby_vars].groupby(groupby_vars).count()
    # reset index
    gdvar_count_df = gdvar_count_df.reset_index()

    return gdvar_count_df

def map_selected_block(df, 
                        blocknum, 
                        gdfvar: str = "huid",
                        laryername: str = "HUID count"):

    # count gdfvar by building
    gdfvar_count_df = count_gdfvar_by_building(df, blocknum, gdvar=gdfvar)

    # convert points to gdf
    gdfvar_count_gdf = gpd.GeoDataFrame(
        gdfvar_count_df, geometry=gpd.points_from_xy(gdfvar_count_df.x, gdfvar_count_df.y))

    """
    # convert dataframe to geodataframe using geometry
    from shapely.wkt import loads
    huid_count_gdf = gpd.GeoDataFrame(huid_count_df).copy(deep=True)
    huid_count_gdf['geometry'] = huid_count_gdf['geometry'].apply(lambda x : loads(x))
    """

    # Create list if gdfvar count unique values
    gdfvar_count_levels = gdfvar_count_gdf[gdfvar].unique().tolist()
    #print("unique counts",gdfvar_count_levels)


    # Map housing units in block
    map = folium_marker_layer_map(gdf = gdfvar_count_gdf,
                            gdfvar=gdfvar,
                            layername = laryername,
                            color_levels = gdfvar_count_levels)
    return map
    
def racedot_map(gdf, column, category):
    gdf = gdf.to_crs(epsg=3857)
    ax = gdf.plot(figsize=(10, 10), column=column,
                      categorical=category, legend=True, markersize = 0.1)
    cx.add_basemap(ax, source=cx.providers.Stamen.TonerLite)
    cx.add_basemap(ax, source=cx.providers.Stamen.TonerLabels)

    return ax

def plot_dotmap_map(gdf, 
                    map_var, 
                    mapname,
                    outputfolder, 
                    bldg_inv_id,
                    community,
                    place,
                    condition_id):
    """
    Mapped plotting of GeoDataFrame rows that met the given location and selection conditions,
    using matplotlib for the sub-plotting and contextily for basemap tiling.
    
    :param gdf: Input GeoDataFrame with all necessary data and values.
    :param map_var: String, the name of the map column with the dataset.
    :param outputfolder: String, the main directory path for results' saving.
    :param place: String, a particular label in the dataset for the plot's title.
    :param bldg_inv_id: The label representing the current boundary or point of interest's focus.
    :param community: The short key or label, likely as a parameter or factored from a switch.
    """


    marker_size = calculate_marker_size(gdf)  # You should define or update this function.

    output_folder = os.path.join(outputfolder, f"{community}/06_Explore")
    output_filename = f'{mapname}_{community}_{condition_id}_{bldg_inv_id}'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder, exist_ok=True)  # Create the intermediate paths as well.
    filepath = os.path.join(output_folder, output_filename)
    
    ax = plot_data_with_contextily(gdf, 
                                   map_var,
                                   marker_size, 
                                   place)  
    # handle contextily plotting manually or refactor into the called logic.

    for extension in ['svg', 'png']:
        ax.figure.savefig(
            f"{filepath}.{extension}", 
            bbox_inches="tight", format=extension, dpi=600)
    
    return filepath
        
def calculate_marker_size(hua_data):
    # Define the way you want to compute the marker size.
    x_range = hua_data.bounds.maxx.max() - hua_data.bounds.minx.min()
    return .01 / x_range
    
def plot_data_with_contextily(gdf, 
                              map_var, 
                              marker_size, 
                              place,
                              source=cx.providers.OpenStreetMap.HOT):
    ax = gdf.plot(column=map_var, 
                  categorical=True, 
                  legend=True, 
                  markersize=marker_size)
    cx.add_basemap(ax, crs=gdf.crs, 
                   source=source)
    plt.title(f"{map_var} for {place}", size=18)
    return ax