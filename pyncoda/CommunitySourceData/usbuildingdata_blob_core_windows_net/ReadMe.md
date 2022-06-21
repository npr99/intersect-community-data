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
