import seaborn as sns
import matplotlib.pyplot as plt # For plotting and making graphs
import pandas as pd
from IPython import display

from pyincore_data_addons.SourceData.api_census_gov.acg_00e_incore_huiv010 \
    import incore_v010_DataStructure

def income_distribution(input_df,
                        variable: str = "randincome",
                        by_variable: str = "race",
                        datastructure = incore_v010_DataStructure,
                        communities: dict = {},
                        community: str = "",
                        year: int = 2010,
                        outputfolders = {}):

    # Set figure size (width, height) in inches
    #plt.figure(figsize = ( 14 , 12 ))
    plt.rcParams["figure.figsize"] = [11.00, 8.5]
    plt.rcParams["figure.autolayout"] = True

    plot = sns.displot(input_df, 
                    x=variable, 
                    hue=by_variable, 
                    stat="count", 
                    element="step",
                    facet_kws={'legend_out': True},
                    palette='tab10')

    label = datastructure[variable]['label'] 
    by_label = datastructure[by_variable]['label'] 
    # title
    new_title = by_label
    plot._legend.set_title(new_title)
    # replace labels
    categories = datastructure[by_variable]['short_labels']
    new_labels = categories

    for t, l in zip(plot._legend.texts, new_labels):
        t.set_text(l)

    # Set label for x-axis
    plt.xlabel( "Random Household Income" , size = 12 )
    
    # Set label for y-axis
    plt.ylabel( "Count" , size = 12 )
    
    # Figure Title
    who = f"{label} distribution"
    what = f"by {by_label}"
    where = county_list_for_tabletitle(communities,community)
    when = str(year)
    table_title = who +" "+ what+", \n "+ where +", "+ when +"."
    

    # Set title for figure
    plt.title( table_title, size = 18)

    # Add notes to figure
    notes = datastructure[variable]['notes']

    # Add county fips to hyperlinks in notes
    if '{state_county}' in notes:
        county_list = county_list_for_datacensusgov(communities,community)
        #print('adding',county_list,'to',notes)
        notes = notes.format(state_county = county_list)

    plt.gcf().text(0, 0, notes, 
                    va = 'bottom', ha='left',
                    fontstyle='italic', size=6)

    # Save figure
    countylist = county_list_for_filename(communities,community)
    filename = variable+'_by_'+by_variable+countylist+'.svg'
    filepath = outputfolders['Explore']+"/"+filename

    plt.tight_layout()
    plt.subplots_adjust(top=0.85)     # Add space at top
    plt.savefig(filepath, bbox_inches="tight",format = 'svg', dpi=600)


    return plt

def county_list_for_datacensusgov(communities,community):
    """
    data.census.gov can display mulptiple geographies
    the FIPS Codes need to be seperated by a comma
    """
    counties_to_display = []
    for county in communities[community]['counties'].keys():
        state_county = communities[community]['counties'][county]['FIPS Code']
        counties_to_display.append(state_county)

    counties_to_display_joined = ",".join(counties_to_display)

    return counties_to_display_joined

def county_list_for_tabletitle(communities,community):
    """
    Table titles should include the county names
    """
    counties_to_display = []
    for county in communities[community]['counties'].keys():
        state_county = communities[community]['counties'][county]['Name']
        counties_to_display.append(state_county)

    counties_to_display_joined = " & ".join(counties_to_display)

    return counties_to_display_joined

def county_list_for_filename(communities,community):
    """
    the FIPS Codes need to be seperated by a _
    """
    counties_to_display = []
    for county in communities[community]['counties'].keys():
        state_county = communities[community]['counties'][county]['FIPS Code']
        counties_to_display.append(state_county)

    counties_to_display_joined = "_".join(counties_to_display)

    return counties_to_display_joined


def plot_groups(input_df):
    # Scatter Plot
    plt.scatter(input_df['randageP12'], input_df['randagePCT12'])
    plt.title('Scatter plot random age by P12 vs PCT12')
    plt.xlabel('randageP12')
    plt.ylabel('randagePCT12')
    plt.show()

    # Scatter Plot
    plt.scatter(input_df['incomegroup'], input_df['randincome'])
    plt.title('Scatter plot random houseohld income by income gorups B19')
    plt.xlabel('Income Groups')
    plt.ylabel('Random Income')
    plt.show()