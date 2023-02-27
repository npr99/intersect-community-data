# UNDER DEVELOPMENT

Goals for project:
1. Read in Microsoft's [Building Data](https://github.com/Microsoft/USBuildingFootprints)
2. Select subset of data for one county
3. Add census blockid (2000, 2010, 2020) to buildings
4. Intersect data with OpenStreetMap data - this will add some landuse data (e.g. commercial structures often have OSM data)
5. Consider intersecting with county parcel data
6. Output will provide input for housing unit allocation model

Additional intersections possible:
1. Reference USA data to add NAICS codes to buildings
2. Flood plain data to add flood risk to buildings
3. Building type to identify building codes and risk for tornados/earthquakes/hurricanes (some of this information is in IN-CORE)
4. First floor elevation to identify building codes and risk for floods


# Building data from Microsoft Building Footprint Project

https://github.com/Microsoft/USBuildingFootprints

Testing download with

https://usbuildingdata.blob.core.windows.net/usbuildings-v2/DistrictofColumbia.geojson.zip

## Example Projects

https://heremaps.github.io/pptk/tutorials/viewer/building_footprints.html

https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/DLGP7Y

https://www.maximfortin.com/project/bpl-us/

https://github.com/nexeons/buildingpopulationlayer/blob/main/BPL-US-beta.py 

# Resource for downloading data
https://automaticknowledge.co.uk/resources/#USBuildings

> "US Building Footprints
We’re huge fans of the Microsoft Building Footprints project, but it isn’t always easy for GIS users to work with the data in the format it’s published in. For this reason, we’ve converted each state buildings dataset from GeoJSON to GeoPackage format and put them on a separate download page. We’ve also added a county code and name column to each file. This means users can more easily filter the data. There are around 130 million buildings in the complete dataset and our version comes in at more than 30GB for the whole country, but like the original source it’s divided into single states. Despite the size of the files, you should find that even the largest ones load fairly quickly in QGIS. They can also be used in other software, such as ArcGIS Pro or R.

Source: Microsoft Building Footprints, licensed under the Open Data Commons Open Database License (ODbL) v1.0."

Original post: https://github.com/microsoft/USBuildingFootprints/issues/76 
