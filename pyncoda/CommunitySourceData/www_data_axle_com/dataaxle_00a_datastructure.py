""""
## Background on Data

https://platform.data-axle.com/places/attributes

Possible Citation:
Data Axel (2020) Technical Overview: Data Axel Reference Solutions – Historical Database. https://referencesolutions.data-axle.com/wp-content/uploads/2020/09/us-historical-business-1.pdf

### Other universities with data access

https://lib.msu.edu/about/data/referenceusahistorical/

Infogroup, 2014, "ReferenceUSA Business Historical Data Files", 
https://doi.org/10.7910/DVN/GW2P3G, Harvard Dataverse, V13

### ReferenceUSA Historical Business (1997-2020)
The ReferenceUSA Historical Business data collection provides 
access to company profiles from 1997 through 2020. 
Variables include company name, address, SIC/NAICS codes, employee size, 
sales, latitude/longitude, and more. ReferenceUSA is a product of Data Axle, 
formerly Infogroup.

### Access
Access to ReferenceUSA Historical Business data is limited to 
current TAMU affiliates only and requires login with your
TAMU netID and password. Redistribution is not permitted.

### Guidelines for Acceptable Use
Prohibition of commercial use, and guidelines for acceptable use

The data is only for academic or private study in compliance 
with applicable laws. You are strictly prohibited from redistributing or
reselling of the data. You may publish limited excerpts and a summarized or 
aggregated statistics or analysis of the data in academic and scholarly works but 
not the data in raw form.

BY CONTINUING TO USE THIS DATA COLLECTION YOU ARE AGREEING TO ACCEPT THESE TERMS.

## Guidelines described well on MSU website
I have replaced the MSU with TAMU - seems to apply no matter what the university is.
https://libguides.lib.msu.edu/c.php?g=95386&p=623717.

### Notice to users of licensed databases
### Prohibition of commercial use, and guidelines for acceptable use
The [TAMU] Libraries subscribe to licensed database products to support the 
educational and research needs of library users. 
In some cases, these databases are educational versions of commercial products. 
Users are advised that access to these materials is controlled by license agreements: 
violation of license terms by individual library users potentially jeopardizes 
future campus access for all students and faculty, and exposes violaters to sanctions.

The content of these databases are made available only for the individual 
educational and research purposes of authorized users. 
By proceeding to the database itself, you as the user are indicating that you are 
aware of the following terms and conditions, and agree to conduct your use of this 
material accordingly.

#### Uses that are allowed:
- You may use the database for purposes of academic research or private study only.
- You may browse and search the database, and display its contents on the screen.
- You may make and save a digital copy of limited extracts from the database for 
academic purposes.
- You may print out copies of limited extracts from the database for academic purposes.
- You may reproduce or quote limited portions of the database contents for reports, 
essays, projects, and similar materials created for academic purposes, 
with appropriate acknowledgement of the source (such as footnotes, 
endnotes or other citations).
- These limited extracts may be shared with other academic users.

#### Uses that are NOT allowed:
- You may not sell or otherwise re-distribute data to third parties without express 
permission.This includes but is not limited to posting on public sources 
like Google Docs, Tableau, etc.
- You may not use the database or any part of the information comprised in the 
database content for commercial research, for example, 
research that is done under a funding or consultant contract, internship, 
or other relationship in which the results are delivered to a for-profit organization.
- You may not engage in bulk reproduction or distribution of the 
licensed materials in any form.
- You may not engage in extensive downloading or copying of content.
- You may not use automated searching or querying, including, but not limited to, 
the use of spiders or other external software for text and data mining.
- You may not store a vast amount of data on your personal computers.
"""

naics2d_lodes_industry_dict = {
'11' : {'lodesicode' : 1,   'label' : 'NAICS sector 11 (Agriculture, \
                                        Forestry, Fishing and Hunting)'},
'21' : {'lodesicode' : 2 ,  'label' : 'NAICS sector 21 (Mining, Quarrying, \
                                        and Oil and Gas Extraction)'},
'22' : {'lodesicode' : 3 ,  'label' : 'NAICS sector 22 (Utilities)'},
'23' : {'lodesicode' : 4 ,  'label' : 'NAICS sector 23 (Construction)'},
'31' : {'lodesicode' : 5 ,  'label' : 'NAICS sector 31-33 (Manufacturing)'},
'32' : {'lodesicode' : 5 ,  'label' : 'NAICS sector 31-33 (Manufacturing)'},
'33' : {'lodesicode' : 5 ,  'label' : 'NAICS sector 31-33 (Manufacturing)'},
'42' : {'lodesicode' : 6 ,  'label' : 'NAICS sector 42 (Wholesale Trade)'},
'44' : {'lodesicode' : 7 ,  'label' : 'NAICS sector 44-45 (Retail Trade)'},
'45' : {'lodesicode' : 7 ,  'label' : 'NAICS sector 44-45 (Retail Trade)'},
'48' : {'lodesicode' : 8 ,  'label' : 'NAICS sector 48-49 (Transportation and Warehousing)'},
'49' : {'lodesicode' : 8 ,  'label' : 'NAICS sector 48-49 (Transportation and Warehousing)'},
'51' : {'lodesicode' : 9 ,  'label' : 'NAICS sector 51 (Information)'},
'52' : {'lodesicode' : 10 , 'label' : 'NAICS sector 52 (Finance and Insurance)'},
'53' : {'lodesicode' : 11 , 'label' : 'NAICS sector 53 (Real Estate and Rental \
                                        and Leasing)'},
'54' : {'lodesicode' : 12 , 'label' : 'NAICS sector 54 (Professional, Scientific, \
                                        and Technical Services)'},
'55' : {'lodesicode' : 13 , 'label' : 'NAICS sector 55 (Management of Companies \
                                        and Enterprises)'},
'56' : {'lodesicode' : 14 , 'label' : 'NAICS sector 56 (Administrative and Support \
                                        and Waste Management and Remediation Services)'},
'61' : {'lodesicode' : 15 , 'label' : 'NAICS sector 61 (Educational Services)'},
'62' : {'lodesicode' : 16 , 'label' : 'NAICS sector 62 (Health Care and Social Assistance)'},
'71' : {'lodesicode' : 17 , 'label' : 'NAICS sector 71 (Arts, Entertainment, and Recreation)'},
'72' : {'lodesicode' : 18 , 'label' : 'NAICS sector 72 (Accommodation and Food Services)'},
'81' : {'lodesicode' : 19 , 'label' : 'NAICS sector 81 (Other Services \
                                        [except Public Administration])'},
'92' : {'lodesicode' : 20 , 'label' : 'NAICS sector 92 (Public Administration)'},
'99' : {'lodesicode' : 0 ,  'label' : 'NAICS sector unkown'},
'0' :  {'lodesicode' : 0 ,  'label' : 'NAICS sector unkown'}
}