[![DOI](https://zenodo.org/badge/478737101.svg)](https://zenodo.org/badge/latestdoi/478737101)

![IN-CORE HRRC Banner](https://github.com/npr99/intersect-community-data/blob/main/IN-CORE_HRRC_Banner.png?raw=true)

# Repository Title: Intersect Community Data (pyncoda)

### Point of Contact: Nathanael Rosenheim: nrosenheim@arch.tamu.edu

### Required Citation: 
Rosenheim, Nathanael, Roberto Guidotti, Paolo Gardoni & Walter Gillis Peacock. (2021). Integration of detailed household and housing unit characteristic data with critical infrastructure for post-hazard resilience modeling. __Sustainable and Resilient Infrastructure__. 6(6), 385-401. https://doi.org/10.1080/23789689.2019.1681821

Rosenheim, Nathanael. (2022). "Detailed Household and Housing Unit Characteristics: Data and Replication Code." [Version 2] __DesignSafe-CI__. https://doi.org/10.17603/ds2-jwf6-s535 [Project-2961](https://www.designsafe-ci.org/data/browser/public/designsafe.storage.published/PRJ-2961)

## Description 
People are the most important part of community resilience planning. However, models for community resilience planning tend to focus on buildings and infrastructure. This repository provides a solution that connects people to buildings for community resilience models. The housing unit inventory method transforms aggregated population data into disaggregated housing unit data that includes occupied and vacant housing unit characteristics. Detailed household characteristics include size, race, ethnicity, income, group quarters type, vacancy type and census block. Applications use the housing unit allocation method to assign the housing unit inventory to structures within each census block through a reproducible and randomized process. The benefits of the housing unit inventory include community resilience statistics that intersect detailed population characteristics with hazard impacts on infrastructure; uncertainty propagation; and a means to identify gaps in infrastructure data such as limited building data. This repository includes all of the python code files. Python is an open source programming language and the code files provide future users with the tools to generate a 2010 housing unit inventory for any county in the United States. Applications of the method are reproducible in IN-CORE (Interdependent Networked Community Resilience Modeling Environment).

### Funding Source: Award name and number
Center for Risk-Based Community Resilience Planning : NIST-70NANB20H008, NIST-70NANB15H044 (http://resilience.colostate.edu/)

Department of Energy (DOE) Southeast Texas Urban Integrated Field Lab (IFL), DE-SC0023216 (https://setx-uifl.org/)

### Related Works

Interdependent Networked Community Resilience Modeling Environment (IN-CORE) : https://incore.ncsa.illinois.edu/

Hazard Reduction and Recovery Center (HRRC): https://www.arch.tamu.edu/impact/centers-institutes-outreach/hazard-reduction-recovery-center/

**Keywords**: Social and Economic Population Data, Housing Unit, US Census, Synthetic Population

## License

**Output Data License**: Open Data Commons Attribution License (https://opendatacommons.org/licenses/by/summary/)

**Program Code**: Mozilla Public License Version 2.0 (https://www.mozilla.org/en-US/MPL/2.0/)

## Setup Environment 
1. [Install Anaconda](https://www.anaconda.com/products/individual)
2. Install VS Code
3. Download Repository
4. See `environment.yml` file for dependencies
5. Run the primary Jupyter Notebook in the main folder


## DesignSafe-CI project identification number


## Important Acronyms
- acg = api.census.gov - primary data source
- HRRC = Hazard Reduction and Recovery Center
- hui = housing unit inventory
- ncoda = Intersect Community Data
- IN-CORE = Interdependent Networked Community Resilience Modeling Environment

## Census Data Terms of Use
This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau. https://www.census.gov/data/developers/about/terms-of-service.html

"Users will not use these data, alone or in combination with any other Census or non-Census data, to:
1. identify any individual person, household, business or other entity
2. not link or combine these data with information in any other Census or non-Census dataset in a manner that identifies an individual person, household, business or other entity
3. not publish information from these data files, particularly in combination with any other Census or non-Census data, in a manner that identifies any individual person, household, business or other entity; and
4. not use the identity of any person or establishment discovered inadvertently and advise the Director of the Census Bureau of any such discovery by calling the U.S. Census Bureau’s Policy Coordination Office at 301-763-6440."



## Acknowledgements
[Work on acknowledgments]
Mastura Safayet - PhD Student Texas A&M University [has fork of repository running in VS Code on local machine]
