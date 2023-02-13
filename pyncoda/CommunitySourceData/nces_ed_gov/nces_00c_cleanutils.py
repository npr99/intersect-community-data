from pyproj import CRS
import numpy as np

def select_var(data, selectvar: str, selectlist):
    """
    
    Args:
        :param data: data to select from
        :type data: pandas dataframe or geopandas dataframe
        :param selectvar: Variable to select from
        :param selectlist: List of values to select       
    
    Returns:
        dataframe: selected values from data
    """
    
    # Make a copy of object - deep = True - creates a new object
    data_selected = data[data[selectvar].isin(selectlist)].copy(deep=True)
    
    # How many observations selected 
    obs = len(data_selected.index)
    print(obs,"observations selected using ",selectvar,
            " in list ",selectlist)
    
    # Return data with job count
    return data_selected

# Select School Attendance Boundary data
def select_NCES_sabs(data,NCESSCH_list,LEAID_list):
    '''
    Select School Attendance Boundary data
    based on the list of school ids (NCESSCH) and 
    school district ids (LEAID)
    '''
    
    data['slcncessch'] = np.where(data['ncessch'].isin(NCESSCH_list),1,0)
    data['slcleaid']   = np.where(data['leaid'].isin(LEAID_list),1,0)
    
    data_selected = data[(data['slcncessch'] == 1) |
                         (data['slcleaid'] == 1)].copy(deep=True)
    
    # How many observations selected 
    obs = len(data_selected.index)
    print(obs,"observations selected")
    
    return data_selected

def prepare_nces_data_for_append(gdf,
                            copyvars,
                            level,
                            schtype,
                            years):
    '''
    individual nces files have different column names
    this function renames the columns to match the
    column names in the appended file
    '''
    append_gdf = gdf[copyvars].copy()

    # All data frames need to have the same column names
    colnames = ['ncesid','name','addr','city','stabbr','zip','cnty15','geometry']
    append_gdf.columns = colnames
    
    # level relates to the level of the school
    # 1 = elementary to 5 = postsecondary
    append_gdf['level'] = level

    # Set geometry
    append_gdf.crs = CRS("epsg:4326")
    append_gdf = append_gdf.to_crs("epsg:4326")
    # School type 
    # 1 = district, 2 = public, 3 = charter, 4 = private
    append_gdf['schtype'] = schtype
    # save centroid coordinates
    # Use UTM zone for centroid to avoid projection error
    #append_gdf['centroidUTM'] = gdf.to_crs('epsg:26917').\
    #    centroid.to_crs(append_gdf.crs)
    append_gdf['lat'] = append_gdf['geometry'].y
    append_gdf['lon'] = append_gdf['geometry'].x
    append_gdf['schyr'] = years
    
    return append_gdf

def split_SAB_gradelevel(df, outputfolder, year):
    '''
    ### Split SABs by level and open enrollment

    The SAB files has 5 different levels (`level`)

    - 1 = Primary
    - 2 = Middle
    - 3 = High
    - 4 = Other
    - N = Not Applicable

    The SAB files have a flag for schools 
    that allow open enrollment `openEnroll`.

    The SAB file can be split into non-overlapping 
    files that represent the different levels and 
    if the school allows open enrollment.
    '''

    sab_boundaries = {}
    condition2 = (df['openEnroll']=='0')

    sab_boundaries[('Primary SAB', year)] = \
        df[(df['level']=='1') & condition2].copy(deep=True)
    sab_boundaries[('Middle SAB', year)] = \
        df[(df['level']=='2') & condition2].copy(deep=True)
    sab_boundaries[('High SAB', year)] = \
        df[(df['level']=='3') & condition2].copy(deep=True)
    sab_boundaries[('Other SAB', year)] = \
        df[(df['level']=='4') & condition2].copy(deep=True)
    sab_boundaries[('Open Enroll SAB', year)] = \
        df[(df['openEnroll']=='1')].copy(deep=True)
    
    for key in sab_boundaries:
        print(key)
        # Set Coordinate Reference System to to WGS84    
        sab_boundaries[key] = \
            sab_boundaries[key].to_crs("epsg:4326") 
        # save as shapefile
        # sab_boundaries[key].to_file(outputfolder+"/"+newfilename)

    
    return sab_boundaries