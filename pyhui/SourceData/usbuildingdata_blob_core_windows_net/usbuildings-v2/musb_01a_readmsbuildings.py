# https://heremaps.github.io/pptk/tutorials/viewer/building_footprints.html


import pptk
import json
import pyproj
import numpy as np

# Read in data using wget
#https://usbuildingdata.blob.core.windows.net/usbuildings-v2/DistrictofColumbia.geojson.zip
url = "https://usbuildingdata.blob.core.windows.net/usbuildings-v2/"
filename = "DistrictofColumbia.geojson"
url_file = url + filename
# save file to local directory
!wget -O $filename $url_file

# unzip file

with open('DistrictofColumbia.geojson', 'rb') as fd:
    data = json.load(fd)