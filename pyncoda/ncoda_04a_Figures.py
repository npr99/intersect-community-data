import seaborn as sns
import matplotlib.pyplot as plt # For plotting and making graphs
from matplotlib.ticker import StrMethodFormatter
import matplotlib.ticker as ticker

import pandas as pd
from IPython import display


def income_distribution(input_df,
                        variable: str = "randincome",
                        by_variable: str = "race",
                        xlabel_string: str = "Household Income",
                        datastructure: dict = {},
                        communities: dict = {},
                        community: str = "",
                        year: int = 2010,
                        savefile: bool = True,
                        outputfolders = {},
                        notes = False):

    # Set figure size (width, height) in inches
    #plt.figure(figsize = ( 14 , 12 ))
    plt.rcParams["figure.figsize"] = [11.00, 8.5]
    plt.rcParams["figure.autolayout"] = True

    # Label values for plot
    # Fix to problem when by var is missing values
    # Created a v2 dictionary that has short and long labels
    output_df = input_df.copy()
    categories_dict = datastructure[by_variable]['categories_dict_v2']
    categories_df = pd.DataFrame.from_dict(categories_dict, orient='index')
    categories_df.reset_index(inplace = True)
    # Rename columns
    categories_df = categories_df.rename(columns={'index':by_variable,
                                'longlabel' : by_variable+' Long Label', 'shortlabel' : by_variable+' Short Label'})

    # Merge count and categories tables
    output_df = \
        output_df.merge(categories_df, on=by_variable, how='left')

    # Sort by long label
    output_df.sort_values(by=[by_variable+' Long Label'], inplace=True)

      
    plot = sns.displot(output_df, 
                    x=variable, 
                    hue=by_variable+' Short Label', 
                    stat="count", 
                    element="step",
                    facet_kws={'legend_out': True},
                    palette='tab10')

    # Set the title
    label = datastructure[variable]['label'] 
    by_label = datastructure[by_variable]['label'] 
    # title
    new_title = by_label
    plot._legend.set_title(new_title)

    # Set label for x-axis
    plt.xlabel( xlabel_string , size = 12 )

    # Set label for y-axis
    plt.ylabel( "Count" , size = 12 )

    # Add commas to the y and x-axis and $ sign to the x-axis
    for ax in plot.axes.flat:
        ax.xaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
        ax.yaxis.set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))        

    # Figure Title
    who = f"{label} distribution"
    what = f"by {by_label}"
    where = county_list_for_tabletitle(communities,community)
    when = str(year)
    table_title = who +" "+ what+", \n "+ where +", "+ when +"."
    

    # Set title for figure
    plt.title( table_title, size = 18)

    # Add notes to figure
    if notes == True:
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
    if savefile == True:
        countylist = county_list_for_filename(communities,community)
        filename = variable+'_by_'+by_variable+countylist
        filepath = outputfolders['Explore']+"/"+filename

        plt.tight_layout()
        plt.subplots_adjust(top=0.85)     # Add space at top
        plt.savefig(filepath+'.svg', bbox_inches="tight",format = 'svg', dpi=600)
        plt.savefig(filepath+'.png', bbox_inches="tight",format = 'png', dpi=600)

        return filename
    else:
        return plot

def county_list_for_datacensusgov(communities,community):
    """
    data.census.gov can display multiple geographies
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
    plt.title('Scatter plot random household income by income groups B19')
    plt.xlabel('Income Groups')
    plt.ylabel('Random Income')
    plt.show()