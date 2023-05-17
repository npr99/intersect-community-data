

US Army Corps of Engineers: Hydrologic Engineering Center

Burns, J. N., Bausch, D., Sharma, U., Lehman, W., & Rozelle, J. (2019, December). Developing a National Structure Database for Flood Risk Assessment across the United States: an Inter-Agency Collaboration. In AGU Fall Meeting Abstracts (Vol. 2019, pp. PA31B-07).



Download Tool:
https://nsi.sec.usace.army.mil/downloads/ 

Github Repository:
https://github.com/HydrologicEngineeringCenter/nsi-ui

Technical Documentation:
https://www.hec.usace.army.mil/confluence/nsi/technicalreferences/2022/technical-documentation

API Reference Guid https://www.hec.usace.army.mil/confluence/nsi/technicalreferences/latest/api-reference-guide

Example API Call:
https://nsi.sec.usace.army.mil/nsiapi/structures?fips=48245

Format of API Data GeoJSON
Geopandas can directly read GeoJSON from api call

## Occupancy Types
https://www.hec.usace.army.mil/confluence/nsi/technicalreferences/latest/technical-documentation

|Damage Category|Occupancy Type Name|Description                                  |
|-------------|-----------|-------------------------------------------------------|
| Residential | RES1-1SNB | Single Family Residential, 1 story, no basement       |
| Residential | RES1-1SWB | Single Family Residential, 1 story, with basement     |
| Residential | RES1-2SNB | Single Family Residential, 2 story, no basement       |
| Residential | RES1-2SWB | Single Family Residential, 2 story, with basement     |
| Residential | RES1-3SNB | Single Family Residential, 3 story, no basement       |
| Residential | RES1-3SWB | Single Family Residential, 3 story, with basement     |
| Residential | RES1-SLNB | Single Family Residential, split-level, no basement   |
| Residential | RES1-SLWB | Single Family Residential, split-level, with basement |
| Residential | RES2      | Manufactured Home                                     |
| Residential | RES3A     | Multi-Family housing 2 units                          |
| Residential | RES3B     | Multi-Family housing 3-4 units                        |
| Residential | RES3C     | Multi-Family housing 5-10 units                       |
| Residential | RES3D     | Multi-Family housing 10-19 units                      |
| Residential | RES3E     | Multi-Family housing 20-50 units                      |
| Residential | RES3F     | Multi-Family housing 50 plus units                    |
| Residential | RES4      | Average Hotel                                         |
| Residential | RES5      | Nursing Home                                          |
| Residential | RES6      | Nursing Home                                          |
| Commercial  | COM1      | Average Retail                                        |
| Commercial  | COM2      | Average Wholesale                                     |
| Commercial  | COM3      | Average Personal & Repair Services                    |
| Commercial  | COM4      | Average Professional Technical Services               |
| Commercial  | COM5      | Bank                                                  |
| Commercial  | COM6      | Hospital                                              |
| Commercial  | COM7      | Average Medical Office                                |
| Commercial  | COM8      | Average Entertainment/Recreation                      |
| Commercial  | COM9      | Average Theater                                       |
| Commercial  | COM10     | Garage                                                |
| Industrial  | IND1      | Average Heavy Industrial                              |
| Industrial  | IND2      | Average light industrial                              |
| Industrial  | IND3      | Average Food/Drug/Chemical                            |
| Industrial  | IND4      | Average Metals/Minerals processing                    |
| Industrial  | IND5      | Average High Technology                               |
| Industrial  | IND6      | Average Construction                                  |
| Commercial  | AGR1      | Average Agricultural                                  |
| Commercial  | REL1      | Church                                                |
| Public      | GOV1      | Average Government Services                           |
| Public      | GOV2      | Average Emergency Response                            |
| Public      | EDU1      | Average School                                        |
| Public      | EDU2      | Average College/University                            |

* Table generator 
https://www.tablesgenerator.com/markdown_tables 