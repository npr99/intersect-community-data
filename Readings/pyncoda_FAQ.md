
From Dr. Himadri Sen Gupta 2026-01-22:

### Does the HUA method capture correlations between housing unit characteristics and household demographics?
- Yes, the HUA method builds up from US Census tables that provide counts of households by number of people, tenure, race and Hispanic. By building up from the block level tables the HUA maintains the intersectionality of the characteristics and therefore the correlations. See [Rosenheim et al. 2021](https://doi.org/10.1080/23789689.2019.1681821) for details on how the block level data is transformed into housing unit level files. 
- For income the HUA method uses American Community Survey (ACS) census tract level tables that provide income by race, Hispanic, and family across 16 income groups. The inventory of households by income, race, ethnicity and family type are then randomly allocated to the block level data using the common variables of race, Hispanic, and family. The income data is correlated across the characteristics and due to strong correlations with tenure status validation of HUA results has shown that income by tenure is also correlated with US Census statistics.  

| Year | Characteristics | File | Source |
|------|----------------|------|--------|
| 2010 | Household size (1-7+ persons), Tenure (owner/renter), Race (7 categories), Hispanic/Latino origin, Tenure, Group Quarters Type, Vacancy Type | [acg_00b_hui_block2010.py](../pyncoda/CommunitySourceData/api_census_gov/acg_00b_hui_block2010.py) | Decennial Census SF1 |
| 2020 | Household size (1-7+ persons), Tenure (owner/renter), Race (7 categories), Hispanic/Latino origin, Tenure, Group Quarters Type, Vacancy Type | [acg_00b_hui_block2020.py](../pyncoda/CommunitySourceData/api_census_gov/acg_00b_hui_block2020.py) | Decennial Census DHC |
| 2010 | Race of householder by Hispanic/Latino origin, Tenure by Hispanic/Latino origin | [acg_00c_hispan_block2010.py](../pyncoda/CommunitySourceData/api_census_gov/acg_00c_hispan_block2010.py) | Decennial Census SF1 |
| 2020 | Race of householder by Hispanic/Latino origin, Tenure by Hispanic/Latino origin | [acg_00c_hispan_block2020.py](../pyncoda/CommunitySourceData/api_census_gov/acg_00c_hispan_block2020.py) | Decennial Census DHC |
| 2008-2012 | Household income (16 income groups), Family income by race and Hispanic/Latino origin | [acg_00d_hhinc_ACS5yr2012.py](../pyncoda/CommunitySourceData/api_census_gov/acg_00d_hhinc_ACS5yr2012.py) | ACS 5-Year |
| 2018-2022 | Household income (16 income groups), Family income by race and Hispanic/Latino origin | [acg_00d_hhinc_ACS5yr2022.py](../pyncoda/CommunitySourceData/api_census_gov/acg_00d_hhinc_ACS5yr2022.py) | ACS 5-Year |


### Does the HUA account for relationships like building type (single-family vs. multi-family) correlated with tenure (owner vs. renter)?

Yes, the HUA method explicitly attempts to place owners in single-family structures and renters in multi-family structures by using an initial prediction of ownership based on the number of housing units estimated for each building. The following functions implement this logic:
- [`predict_residential_addresspoints()`](../pyncoda/ncoda_02d_addresspoint.py#L5) - Predicts housing units in a structure by running three rounds of checks and assigns single-family dummy variable based on housing unit estimate (d_sf = 1 if huestimate==1, else 0)
- [`predict_housingunit_estimate()`](../pyncoda/ncoda_07c_generate_addpt.py#L251) - Calls `predict_residential_addresspoints()` to estimate housing units and generates single-family indicator variable (d_sf) to flag single-family buildings
- [`check_addpt_predictownershp()`](../pyncoda/ncoda_07d_run_hua_workflow.py#L74) - Predicts structure ownership (tenure) based on number of housing units: assumes owner-occupied if 1-2 units, renter-occupied if >2 units
- [`update_addpt_predictownershp()`](../pyncoda/ncoda_07d_run_hua_workflow.py#L88) - Updates predicted ownership based on the tenure characteristics of the first round housing units allocated to the structure

The initial ownership prediction assumes that buildings with 1-2 housing units are owner-occupied single-family or duplex structures, while buildings with more than 2 housing units are renter-occupied multi-family structures. However, this initial assignment is then refined through three rounds of random merging with Census data to ensure the HUA results match the Census statistics for tenure by household size and tenure by race/ethnicity at the block level.

### Does the HUA account for relationships like building type correlated with household age or other demographics?

The HUA does not explicitly model building type correlations with household age or other demographic characteristics beyond tenure status. The method focuses on capturing intersectionality across household size, tenure, race, ethnicity and income from Census tables, but building archetypes are used primarily to identify residential structures rather than to correlate with specific demographic characteristics. 

The HUA method does implicitly have relationships between building type due to the nature of census block data. At the census block the number of buildings tends to be small (average 10-40 structures - numbers with vary for specific counties). If one assumes that building age and structure are homogenous at the block level then the demographic characteristics will correlated with the building characteristics. Note that the base HUA uses the [National Structure Inventory](https://github.com/npr99/intersect-community-data/tree/main/pyncoda/CommunitySourceData/nsi_sec_usace_army_mil).

### Annotated Bibliography with Articles that Discuss Correlations

| Citation | Correlations | Finding |
|----------|-------------|---------|
| [Rosenheim et al. 2021](https://doi.org/10.1080/23789689.2019.1681821) | Building characteristics ↔ occupant characteristics (race, tenure); Tenure status ↔ displacement risk | Renters occupy precarious housing; tenure critical for displacement |
| [Fereshtehnejad et al. 2021](https://doi.org/10.1061/(ASCE)NH.1527-6996.0000459) | Evacuation decisions ↔ income, tenure, race; Race/ethnicity ↔ median household income | >70% Black households below median income; identifies vulnerable populations |
| [Mazumder et al. 2023](https://doi.org/10.1016/j.rcns.2023.07.005) | Prolonged dislocation ↔ low-income households; Social vulnerability ↔ poverty, tenure, minority status | Low-income households experience extended homelessness after floods |
| [Nofal et al. 2024](https://doi.org/10.1111/mice.13135) | Disaster impact ↔ socioeconomic status (race, income, tenure); Social characteristics ↔ building damage/utility loss | Disasters affect populations unequally based on social characteristics |
| [Roohi et al. 2021](https://doi.org/10.1080/15732479.2020.1845753) | Minority renters ↔ older multi-unit structures; Building type ↔ tenure (renters in multi-unit, owners in single-family) | Building accuracy critical for modeling renter-occupied housing impacts |
| [Wang et al. 2021](https://doi.org/10.1061/(ASCE)IS.1943-555X.0000642) | Retrofit benefits ↔ income and tenure; Low-income renters ↔ rental properties | Retrofits benefit homeowners; renters see no direct benefit |

#### Rosenheim, N., Guidotti, R., Gardoni, P., & Peacock, W. G. (2021). Integration of detailed household and housing unit characteristic data with critical infrastructure for post-hazard resilience modeling. Sustainable and Resilient Infrastructure, 6(6), 385–401. https://doi.org/10.1080/23789689.2019.1681821

This seminal work details the "housing unit inventory" approach, which links US Census data to specific residential structures. The authors observe that US building characteristics have a strong relationship with occupant characteristics, such as race and tenure. Specifically, lower-income and minority households are more likely to be renters living in precarious housing. The article further notes that tenure status is a critical determinant of displacement, as renters often have weaker property rights and fewer resources to avoid long-term dislocation.

#### Fereshtehnejad, E., Gidaris, I., Rosenheim, N., Tomiczek, T., Padgett, J. E., Cox, D. T., Van Zandt, S., & Peacock, W. G. (2021). Probabilistic risk assessment of coupled natural-physical-social systems: Cascading impact of hurricane-induced damages to civil infrastructure in Galveston, Texas. Natural Hazards Review, 22(3), 04021013. https://doi.org/10.1061/(ASCE)NH.1527-6996.0000459

This article highlights that household evacuation decisions following hurricanes are significantly correlated with income, tenure status, and race. Utilizing data from Galveston Island, the authors demonstrate that race and ethnicity are deeply intertwined with median household income, noting that over 70% of Black or African American households lived below the median income, while over 60% of non-Hispanic White households lived above it. The framework integrates these sociodemographic variables to identify "hot households" where physically vulnerable buildings coincide with socially vulnerable residents who are less likely to evacuate.

#### Mazumder, R. K., Rosenheim, N., Enderami, S. A., Sutley, E. J., Stanley, M., & Meyer, M. (2023). Estimating long-term K-12 student homelessness after a catastrophic flood disaster. Resilient Cities and Structures, 2(2), 82–92. https://doi.org/10.1016/j.rcns.2023.07.005

This study focuses on the disparate disaster trajectories for students, noting that those experiencing prolonged dislocation are primarily from low-income households. The authors identify poverty, housing tenancy (tenure), and minority racial status as the primary indicators of social vulnerability that hinder a household's capacity to anticipate and recover from hazards. The methodology links school attendance boundaries to a housing unit inventory to track how these socioeconomic factors influence student homelessness following a flood.

#### Nofal, O. M., Rosenheim, N., Kameshwar, S., Patil, J., Zhou, X., van de Lindt, J. W., Dueñas-Osorio, L., Cha, E. J., Endrami, A., Sutley, E., Cutler, H., Lu, T., Wang, C., & Jeon, H. (2024). Community-level post-hazard functionality methodology for buildings exposed to floods. Computer-Aided Civil and Infrastructure Engineering, 1–24. https://doi.org/10.1111/mice.13135

The authors present a methodology for building functionality that emphasizes that disasters do not affect all populations equally. They argue that individuals with different socioeconomic statuses—including race, income, and tenure—respond to and are impacted by natural hazards in distinct ways. By linking households to housing units, the model facilitates an analysis of how these social characteristics intersect with building damage and utility loss to determine total post-hazard functionality.

#### Roohi, M., van de Lindt, J. W., Rosenheim, N., Hu, Y., & Cutler, H. (2021). Implication of building inventory accuracy on physical and socio-economic resilience metrics for informed decision-making in natural hazards. Structure and Infrastructure Engineering, 17(4), 534–554. https://doi.org/10.1080/15732479.2020.1845753

This research examines how building inventory accuracy affects resilience metrics, finding that minority renters are statistically more likely to reside in older, multi-unit structures. In the Memphis testbed, the data revealed that renters are more likely to live in buildings with more than one story, whereas owner-occupied housing is more frequently single-family wood dwellings. The study concludes that missing or inaccurate building data disproportionately affects the modeling of impacts for renter-occupied households.

#### Wang, W., van de Lindt, J. W., Rosenheim, N., Cutler, H., Hartman, B., Lee, J. S., & Calderon, D. (2021). Effect of residential building wind retrofits on social and economic community-level resilience metrics. Journal of Infrastructure Systems, 27(4), 04021034. https://doi.org/10.1061/(ASCE)IS.1943-555X.0000642

This article investigates the socioeconomic impact of building retrofits, finding that low-income households in Joplin are heavily comprised of renters. Because these households occupy rental properties rather than owning them, they saw virtually no direct benefit from building retrofit strategies, which primarily stabilized the incomes of middle- and high-income homeowners. The study uses these correlations to demonstrate how physical infrastructure improvements can have uneven benefits across different socioeconomic groups.