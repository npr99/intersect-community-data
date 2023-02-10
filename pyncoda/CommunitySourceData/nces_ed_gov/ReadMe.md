<!-----
NEW: Check the "Suppress top comment" option to remove this info from the output.

Conversion time: 1.442 seconds.


Using this Markdown file:

1. Paste this output into your source file.
2. See the notes and action items below regarding this conversion run.
3. Check the rendered output (headings, lists, code blocks, tables) for proper
   formatting and use a linkchecker before you publish this page.

Conversion notes:

* Docs to Markdown version 1.0β30
* Thu Aug 19 2021 06:15:08 GMT-0700 (PDT)
* Source doc: NCES_SourceData_Overview
* Tables are currently converted to HTML tables.
* This document has images: check for >>>>>  gd2md-html alert:  inline image link in generated source and store images to your server. NOTE: Images in exported zip file from Google Docs may not appear in  the same order as they do in your doc. Please check the images!

----->

# National Center for Education Statistics

Authors: Nathanael Rosenheim, [Michelle Stanley]


## Project Objective:

A set of automated programs that will obtain, clean and explore school related data. The final data set will have the list of schools in a selected area (county or counties) with their school attendance boundaries. The file file can have the count of teachers (Full time equivalent) and counts of students by grade level and race+ethnicity+gender (where available). The teacher counts will allow the LODES data to be checked - statistically determine the correlation. The student data will provide a statistical basis for probabilistically connecting households allocated to residential structures to schools. The jupyter notebooks and sample data files will be archived on either DesignSafe or OpenICPSR. Code developed for this project will be maintained within [pyncoda](https://github.com/npr99/intersect-community-data) functions on github. 


## Overview of Data and Data Citations

The National Center for Education Statistics (NCES), U.S. Department of Education, part of the Institute of Education Sciences (IES), the nation's leading source for rigorous, independent education research, evaluation and statistics. The NCES has school location, attendance boundaries, and characteristics data. These data are sourced from two programs Education Demographic and Geographic Estimates (EDGE) and Common Core of Data (CCD).

The EDGES program provides GIS shapefiles for school locations and School Attendance Boundaries for public, private, and post secondary schools. The CCD program provides basic information such as staff, membership (students), and lunch programs in public schools. 


### References for Data Documentation:


    Geverdt, D., (2018a). Education Demographic and Geographic Estimates (EDGE) Geocodes: Public Schools and Local Education Agencies, (NCES 2018-080). U.S. Department of Education. Washington, DC: National Center for Education Statistics. Retrieved [2021-06-04] from http://nces.ed.gov/pubsearch/.


    Geverdt, D., (2018b). Education Demographic and Geographic Estimates (EDGE) Program, Geocodes: Private Schools (NCES 2018- 084). U.S. Department of Education. Washington, DC: National Center for Education Statistics. Retrieved [2021-06-04] from http://nces.ed.gov/pubsearch/.


    Geverdt, D., (2018c). Education Demographic and Geographic Estimates (EDGE) Program, Geocodes: Postsecondary Schools (NCES 2018-084). U.S. Department of Education. Washington, DC: National Center for Education Statistics. Retrieved [2021-06-04] from http://nces.ed.gov/pubsearch/.


    Geverdt, D., (2018d). School Attendance Boundary Survey (SABS) File Documentation: 2015-16 (NCES 2018-099). U.S. Department of Education. Washington, DC: National Center for Education Statistics. Retrieved [2021-06-02] from http://nces.ed.gov/pubsearch.


    Broughman, S.P., Rettig, A., and Peterson, J. (2017). Private School Universe Survey (PSS): Public-Use Data File User’s Manual for School Year 2015–16 (NCES 2017-160). U.S. Department of Education. Washington, DC: National Center for Education Statistics.

**Table NCES.1.  National Center for Education Statistics (NCES) available data.**


<table>
  <tr>
   <td>Program
   </td>
   <td>Description
   </td>
   <td>Spatial Scale
   </td>
   <td>Longitudinal annual data by school year
   </td>
   <td>Equity Data
   </td>
  </tr>
  <tr>
   <td><a href="https://nces.ed.gov/programs/edge/Geographic/DistrictBoundaries">EDGES</a>
   </td>
   <td>School District Boundaries
   </td>
   <td>Polygon
   </td>
   <td>1995-2020
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><a href="https://nces.ed.gov/programs/edge/sabs">EDGES</a>
   </td>
   <td>School Boundary
   </td>
   <td>Polygons by grade level
   </td>
   <td>2009, 2010, 2013, 2015
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><a href="https://nces.ed.gov/programs/edge/Geographic/SchoolLocations">EDGES</a>
   </td>
   <td>Post Secondary Schools Locations
   </td>
   <td>Geocoded Lat, Lon based on address
   </td>
   <td>2015-2020
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><a href="https://nces.ed.gov/programs/edge/Geographic/SchoolLocations">EDGES</a>
   </td>
   <td>Private Schools Locations
   </td>
   <td>Geocoded Lat, Lon based on address
   </td>
   <td>2017-2018, 2015-2016
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><a href="https://nces.ed.gov/programs/edge/Geographic/SchoolLocations">EDGES</a>
   </td>
   <td>Public School Locations
   </td>
   <td>Geocoded Lat, Lon based on address
   </td>
   <td>2015-2019
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><a href="https://nces.ed.gov/programs/edge/Geographic/SchoolLocations">EDGES</a>
   </td>
   <td>School District Locations
   </td>
   <td>Geocoded Lat, Lon based on address
   </td>
   <td>2015-2019
   </td>
   <td>
   </td>
  </tr>
  <tr>
   <td><a href="https://nces.ed.gov/ipeds/">IPDES</a>
   </td>
   <td>Integrated Postsecondary Education Data System
   </td>
   <td>Address and unique id 
   </td>
   <td>1980-2019
   </td>
   <td>Student characteristics
<p>
race, ethnicity, and gender
   </td>
  </tr>
  <tr>
   <td><a href="https://nces.ed.gov/ccd/ccddata.asp">CCD</a>
   </td>
   <td>Common Core of Data (Public Schools and School Districts)
   </td>
   <td>Address and unique id 
   </td>
   <td>1986-2021
   </td>
   <td>Student characteristics by race, ethnicity, gender, grade level, and lunch program eligibility
   </td>
  </tr>
  <tr>
   <td><a href="https://nces.ed.gov/surveys/pss/index.asp">PSS</a>
   </td>
   <td>Private School Survey
   </td>
   <td>Address and unique id 
   </td>
   <td>1989-2018
   </td>
   <td>Student characteristics race, ethnicity, gender, grade level
   </td>
  </tr>
  <tr>
   <td>CCD
   </td>
   <td>Fiscal Data
   </td>
   <td>
   </td>
   <td>
   </td>
   <td>Might be a source of financial capital by school district
   </td>
  </tr>
</table>



## Data Use Agreements 

When I clicked on the file layout download for the PSS survey data [https://nces.ed.gov/surveys/pss/pssdata.asp](https://nces.ed.gov/surveys/pss/pssdata.asp) the popup warning appeared to agree to the terms of use. 



<p id="gdcalert1" ><span style="color: red; font-weight: bold">>>>>>  gd2md-html alert: inline image link here (to images/image1.png). Store image on your image server and adjust path/filename/extension if necessary. </span><br>(<a href="#">Back to top</a>)(<a href="#gdcalert2">Next alert</a>)<br><span style="color: red; font-weight: bold">>>>>> </span></p>


![alt_text](images/image1.png "image_tooltip")


I am a little confused by the data use agreement - the private school survey data is reported by school and provides the school name and location for each observation. 


## Output Example

G:\Shared drives\HRRC_IN-CORE\Tasks\P4.9 Testebeds\Lumberton_LaborMarketAllocation\SourceData\nces.ed.gov\WorkNPR\NCES_2av1_SelectCountySchools_2021-06-06\NCES_2av1_SelectCountySchools_2021-06-06.html 

### Estimated School Building and Administrative Office Locations
> Purpose of School Location Data: "The National Center for Education Statistics (NCES) Education Demographic and Geographic Estimates (EDGE) program develops data resources and information to help data users investigate the social and
spatial context of education. School point locations (latitude/longitude values) are a key component of the NCES data collection." (Geverdt, 2018a p. 1) 

School location data is collected for:
* Public schools
    - elementary
    - public secondary
    - location education agencies
* Private schools
    - elementary
    - secondary
* Postsecondary Schools that participates in the federal student financial aid programs
    - college
    - university
    - technical and vocational institution 
    
Note a Location Education Agency is "A public board of education or other public authority within a state that maintains administrative control of public elementary or secondary schools in a city, county, township, school district, or other political subdivision of a state. School districts and county offices of education are both LEAs. Under the Local Control Funding Formula, charter schools are increasingly treated as LEAs. (Ed Source https://edsource.org/glossary/local-education-agency-lea)"

The school location data can be linked to:
* Public schools (Geverdt, 2018a p. 1) 
    - Common Core of Data (CCD) school and agency universe
    - enrollment, staffing, and program participation 
* Private schools (Geverdt, 2018b p. 1) 
    - Private School Survey (PSS) collection
    - Biennial provides data about enrollment, staffing, type of program, and other basic administrative features
* Postsecondary Schools (Geverdt, 2018c p. 1) 
    - Integrated Postsecondary Education Data System (IPEDS) collection
    - enrollments, program completions, graduation rates, faculty and staff, finances, institutional prices, and student financial aid

### School Attendance Boundaries
> "The School Attendance Boundaries Survey (SABS) was an experimental survey conducted by the U.S.
Department of Education’s (ED) National Center for Education Statistics (NCES) with assistance from the
U.S. Census Bureau to collect school attendance boundaries for regular schools in the 50 states and the
District of Columbia. Attendance boundaries, sometimes known as school catchment areas, define the
geographic extent served by a local school for the purpose of student assignments. School district
administrators create attendance areas to help organize and plan district-wide services, and districts
may adjust individual school boundaries to help balance the physical capacity of local schools with
changes in the local school-age population." (Geverdt, 2018d p. 1)

## Literature Review of Data Uses Examples 

[Michell Stanley]

Ask Michelle Stanley to work on a literature review of NCES data articles. What are some of the ways academics have used the data. 

Pub search on NCES website

[https://nces.ed.gov/pubsearch/](https://nces.ed.gov/pubsearch/)
