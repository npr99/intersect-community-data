import wget
import pandas as pd
import sys
import os
import geopandas as gpd
# creat the geometry column for the data: 
from shapely.geometry import Point

def obtain_dataaxel(year: str = '2010',
                    FIPS_Code: str = 37155,
                    State: str = 'NC',
                    outputfolder: str = "",
                    folderpath: str = 'C:\\MyProjects\\HRRCProjects\\IN-CORE\\WorkNPR\\'):
    """
    Download data from web and read into memory
    
    ### Found correct encoding
    Opened data in Stata and found that the encoding was ISO-8859-1.

    Also learned that python has issues with decoding some characters - 
    need to check the final data to make sure it looks correct.

    # Data must be downloaded manually from
    https://library.tamu.edu/resources/helpfiles/dataaxle/dataaxle.html

    # Data should only be saved on local machine and deleted 
    after extraction.

    # Notes:
    Filesize is very large - 4-6 GB
    Check file by reading in part of the file first
    pd.read_csv(filename, nrows = 1000, encoding='ISO-8859-1')
    Help with reading in large CSV files:
    https://medium.com/analytics-vidhya/optimized-ways-to-read-large-csvs-in-python-ab2b36a7914e

    help from https://stackoverflow.com/questions/28239529/conditional-row-read-of-csv-in-pandas
    
    NAICS Code List
    https://www.bls.gov/iag/tgs/iag_index_naics.htm
    """

    # Check if output file already exists
    FIPS_Code_str = str(FIPS_Code).zfill(5)
    output_filename = f"dataaxle_01a_obtain_{FIPS_Code_str}_{year}"
    csv_filepath = sys.path[0]+"/"+outputfolder+"/"+output_filename+".csv"

    # Check if selected data already exists - if yes read in saved file
    if os.path.exists(csv_filepath):
        output_df = pd.read_csv(csv_filepath, low_memory=False,
            dtype = {'estabid' : str,
                    'NAICS2D' : str,
                    'NAICS4D' : str,
                    'Employee_Size_Location' : int,
                    'geometry' : str
                    })
        # If file already exists return csv as dataframe
        print("File",csv_filepath,"Already exists - Skipping Obtain data-axel.")
        return output_df

    filename = f"{folderpath}BusinessData{year}.csv.zip"

    print("Reading data-axel data from",filename)
    chunk = pd.read_csv(filename,chunksize=1000000, 
            encoding='ISO-8859-1', low_memory=False)
    pd_df = pd.concat(chunk)

    print("Selecting data for county",str(FIPS_Code_str))
    #select_pd_df = pd_df.loc[pd_df['FIPS_Code'] == 48167].copy(deep=True)
    select_pd_df = pd_df.loc[(pd_df['FIPS_Code'] == FIPS_Code) &
                            (pd_df['State'] == State)].copy(deep=True)

    # Create 2-digit NAICS Code
    select_pd_df.loc[:,'Primary_NAICS_Code'] = \
        select_pd_df['Primary_NAICS_Code'].fillna(00000)
    select_pd_df['NAICS2D'] = select_pd_df['Primary_NAICS_Code'].\
        apply(lambda x : str(int(x))[0:2])
    # Add 4-digit NAICS Codes for more detail
    # Example Education Services: NAICS 61
    #  Elementary and Secondary Schools: NAICS 6111 
    select_pd_df['NAICS4D'] = select_pd_df['Primary_NAICS_Code'].\
        apply(lambda x : str(int(x))[0:4])

    """ # Explore option
    select_pd_df.groupby('NAICS2D').\
        aggregate({'Employee_Size_Location':np.sum})
    """

    # Keep Company Name if Education or Health Care Services
    # Social Institution Name
    select_pd_df['SIName'] = ""
    education_services = (select_pd_df['NAICS2D'] == '61')
    select_pd_df.loc[education_services, 'SIName'] = select_pd_df['Company']
    health_care = (select_pd_df['NAICS2D'] == '62')
    select_pd_df.loc[health_care, 'SIName'] = select_pd_df['Company']

    # Add Unique Establishment ID
    select_pd_df = add_estabid(select_pd_df)

    # Drop if Employee_Size_Location is missing
    missing_employees = (select_pd_df['Employee_Size_Location'].isnull())
    print("Data has",missing_employees.sum(),"Establishments without employee data.")
    # Select the opposite of the missing employee data
    select_pd_df = select_pd_df.loc[~missing_employees]

    # Add geometry column to data
    geometry = [Point(xy) for xy in zip(select_pd_df.Longitude, select_pd_df.Latitude)]
    select_pd_df = gpd.GeoDataFrame(select_pd_df, geometry=geometry)

    # Only save a portion of the original file
    keep_vars = ['estabid','NAICS2D','NAICS4D','SIName',
        'Latitude','Longitude','Employee_Size_Location','geometry']
    # Save Work at this point as CSV
    select_pd_df[keep_vars].to_csv(csv_filepath, index=False)

    return select_pd_df[keep_vars]

def add_estabid(add_estabid_df):
    add_estabid_df.loc[:,'FIPS_Code_str'] = \
        add_estabid_df['FIPS_Code'].apply(lambda x : str(int(x)).zfill(5))
    add_estabid_df.loc[:,'estabid_part1'] = "E"+add_estabid_df['FIPS_Code_str'] + \
        "N"+add_estabid_df['NAICS2D'].apply(lambda x : str(int(x)).zfill(2))
    # Part2 of unique id is a counter based on the part1
    # The counter ensures that values are unique within part1
    # Add unique ID based on group vars
    add_estabid_df.loc[:,'estabid_part2'] = \
        add_estabid_df.groupby(['estabid_part1']).cumcount() + 1
    # Need to zero pad part2 find the max number of characters
    part2_max = add_estabid_df['estabid_part2'].max()
    part2_maxdigits = len(str(part2_max))
    add_estabid_df.loc[:,'estabid'] = add_estabid_df['estabid_part1'] + \
        "C" + add_estabid_df['estabid_part2'].apply(lambda x : \
           str(int(x)).zfill(part2_maxdigits))

    return add_estabid_df
