# Copyright (c) 2022 Nathanael Rosenheim and others. All rights reserved.
#
# This program and the accompanying materials are made available under the
# terms of the Mozilla Public License v2.0 which accompanies this distribution,
# and is available at https://www.mozilla.org/en-US/MPL/2.0/

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
    for i in range(0,color_levels):
        layer_map_name=layername+' '+str(i+1)
        feature_group = FeatureGroup(name=layer_map_name)
        locations = gdf.loc[gdf[gdfvar] == i+1]
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