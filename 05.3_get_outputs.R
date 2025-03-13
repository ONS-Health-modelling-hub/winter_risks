### Load packages
library(tidyverse) # version 1.3.2
library(sparklyr) # version 1.7.8, mutate(across()) works with this version

### User Inputs

# whether to bootstrap CIs
bootstrap_CIs <- FALSE
check_stability <- FALSE
n_iterations <- 300
width_iterations <- 10
save_iterations <- 7

# whether to use 2ndary disclosure tool
run_2ndary_disclosure_tool <- FALSE

# directories
project_directory <- "cen_dth_gps/Winter Risk Factor"
lookup_directory <- paste0(project_directory, "/lookup_files")
final_fit_tidy_file_name <- "final_glm_categorical_tidy.csv" # S:\cen_dth_gps\Winter Risk Factors\input_files
bootstrapped_coefficients_file_name <- "bootstrapped_coefficients.csv" # S:\cen_dth_gps\Winter Risk Factors\input_files
functions_directory <- paste0(project_directory, "/_functions")
oa_to_region_lookup_path <- paste0("/dap/landing_zone/ons/geography/lookup/",
                                   "v1/oa_lsoa_msoa_ew_dec_2021_lu_v5.csv")
lsoa11_to_oa21_path <- paste0("/dap/landing_zone/lookup_files/",
                              "oa_2021_to_lsoa_2011_look_up/v1/OA2021_to_LSOA2011_lookup.csv")
imd_path <- "/dap/landing_zone/ons/hale_lookups/imd/v1/IMD_2019.csv"
census21_OA_path <- "2021_census_furd_sdc.furd_2a_v3"
census21_res_path <- "2021_census_furd_attributes.furd_2b_resident"
cen21_spine_pds_path <- "cen_dth_gps.cen21_spine_pds"
census21_hh_path <- "2021_census_furd_attributes.furd_2b_household"
cen21_hh_poverty_probabilites_path <- "cen_dth_gps.analytical_winterriskfactor_hh_poverty_probability"
winter_risk_factor_gpes_flags_path <- "cen_dth_gps.analytical_winterriskfactor_gpes_flags"
cen21_pds_linkage_ipws_path <- "cen_dth_gps.analytical_winterriskfactor_cen21_pds_linkage_IPWs"
ESP_path <- "/dap/landing_zone/ons/hale_lookups/age_standard_by_ethnicity/v1/ESP.csv"
poisson_path <- "/dap/landing_zone/ons/hale_lookups/age_standard_by_ethnicity/v1/Poisson.csv"

# For outputs
outcome <- "low60ahc_probability"

breakdown_vars <- c("all",
                    "sex",
                    "age_group",
                    "ethnicity_agg",
                    "heating_type_agg",
                    "imd_decile",
                    "region",
                    "disability",
                    "health_in_general",
                    "activity_last_week"
                    )

census_vars <- c(# ID variables
                 "resident_id", "household_id", "response_id",
                 # Filtering variables
                 "usual_resident_ind", "residence_type",
                 # Imputation flags
                 "proxy_answer",
                 # Personal characteristics
                 "resident_age", "sex", "ns_sec", "disability", "health_in_general",
                 "highest_qualification", "activity_last_week", "ethnic_group_tb", "ethnic05_20",
                 # Household characteristics
                 "heating_type")

gpes_conditions <- c("Asthma_flag",
                     "CysticFibrosisBronchiectasisAlveolitis_flag",
                     "HeartFailure_flag",
                     "PulmonaryHypertensionOrFibrosis_flag",
                     "PeripheralVascularDisease_flag",
                     "AtrialFibrillation_flag",
                     "Copd_flag",
                     "StrokeOrTia_flag",
                     "CongenitalHeartProblem_flag",
                     "CoronaryHeartDisease_flag")

cardiovascular_conditions <- c("HeartFailure_flag",
                                "PulmonaryHypertensionOrFibrosis_flag",
                                "PeripheralVascularDisease_flag",
                                "AtrialFibrillation_flag",
                                "StrokeOrTia_flag",
                                "CongenitalHeartProblem_flag",
                                "CoronaryHeartDisease_flag")

respiratory_conditions <- c("CysticFibrosisBronchiectasisAlveolitis_flag",
                            "Copd_flag",
                            "Asthma_flag")

# For staging
username <- Sys.getenv('HADOOP_USER_NAME')
staging_folder <- "WinterRiskFactor"

### Functions
for (fun_ in list.files(functions_directory)) {
  
  source(paste0(functions_directory, "/", fun_))
  
}

rm(fun_); gc()

### User Input checks

## user written functions
if (sum(gsub(".R", "", list.files(functions_directory)) %in% ls()) != length(list.files(functions_directory))) {
  
  stop(paste0(paste(list.files(functions_directory), collapse = " and "),
              " not found in global environment"))
  
}

### Spark session

if (bootstrap_CIs == TRUE) {
  
  ## Extra large session
  xl_config <- sparklyr::spark_config()
  xl_config$spark.executor.memory <- "20g"
  xl_config$spark.yarn.executor.memoryOverhead <- "2g"
  xl_config$spark.executor.cores <- 5
  xl_config$spark.dynamicAllocation.enabled <- "true"
  xl_config$spark.dynamicAllocation.maxExecutors <- 12
  xl_config$spark.sql.shuffle.partitions <- 240
  xl_config$spark.shuffle.service.enabled <- "true"

  sc <- sparklyr::spark_connect(
    master = "yarn-client",
    app_name = "xl-session",
    config = xl_config)
  
} else {
  
  large_config <- sparklyr::spark_config()
  large_config$spark.executor.memory <- "10g"
  large_config$spark.yarn.executor.memoryOverhead <- "1g"
  large_config$spark.executor.cores <- 5
  large_config$spark.dynamicAllocation.enabled <- "true"
  large_config$spark.dynamicAllocation.maxExecutors <- 5
  large_config$spark.sql.shuffle.partitions <- 200
  large_config$spark.shuffle.service.enabled <- "true"

  sc <- sparklyr::spark_connect(
    master = "yarn-client",
    app_name = "large-session",
    config = large_config)
  
}

### Read in data

# Tidy fit/model
final_tidy_fit <- read.csv(paste(lookup_directory, final_fit_tidy_file_name, sep = "/"),
                           stringsAsFactors = FALSE)

# bootstrapped coefficients
if (bootstrap_CIs == TRUE) {
  
  bootstrapped_coefficients <- read.csv(paste(lookup_directory, bootstrapped_coefficients_file_name, sep = "/"),
                                        stringsAsFactors = FALSE)
  
}

# Region lookup
oa_to_region_lookup <- spark_read_csv(sc, path = oa_to_region_lookup_path)

# OA and LSOA Lookup
lsoa11_to_oa21 <- spark_read_csv(sc, path =  lsoa11_to_oa21_path)

# IMD and WIMD
imd <- spark_read_csv(sc, path = imd_path)

# Census 21 Output Area
census21_OA <- sdf_sql(sc, paste0("SELECT * FROM ", census21_OA_path))

# Census 21 resident table
census21_res <- sdf_sql(sc, paste0("SELECT * FROM ", census21_res_path)) %>%
  select(any_of(census_vars))

# Census 21 spine PDS linkage table
cen21_spine_pds <- sdf_sql(sc, paste0("SELECT census_id, nhs_number FROM ", cen21_spine_pds_path))

# Census 21 household table
census21_hh <- sdf_sql(sc, paste0("SELECT * FROM ", census21_hh_path))

# Census 21 to PDS linkage IPWs
cen21_spine_pds_ipws <- sdf_sql(sc, paste0("SELECT * FROM ", cen21_pds_linkage_ipws_path))

# GPES flags
gpes_flags <- sdf_sql(sc, paste0("SELECT * FROM ", winter_risk_factor_gpes_flags_path))

# ESP
ESP <- spark_read_csv(sc, name = "ESP", path = ESP_path, header = TRUE, delimiter = ",")

# Poisson
poisson <- spark_read_csv(sc, name = "poisson", path = poisson_path, header = TRUE, delimiter = ",")

### Geography processing

# Add region code for filtering
final_oa <- census21_OA %>%
  select(response_id, census21_oa = output_area) %>%
  left_join(oa_to_region_lookup, by = c("census21_oa" = "oa21cd")) %>%
  left_join(lsoa11_to_oa21, by = c("census21_oa" = "OA21CD")) %>%
  left_join(imd, by = c("LSOA11CD", "LSOA11NM")) %>%
  # select necessary variables
  select(response_id,
         imd_decile = IMD_DECILE, imd_quintile = IMD_QUINTILE,
         census21_oa, rgn22cd, rgn22nm, utla22cd, utla22nm,
         ltla22cd, ltla22nm, msoa21cd, msoa21nm)

### Staging
final_oa <- sdf_stage_in_hdfs(
  data = final_oa,
  filename = "final_oa",
  staging_folder = staging_folder
  )

### Census 21 Spine PDS linkage processing

## Create linkage flag, dedpulicate and drop nhs_number as it's not needed
cen21_spine_pds_gpes <- cen21_spine_pds %>%
  left_join(gpes_flags, by = "nhs_number") %>%
  mutate(across(.cols = ends_with("_flag"),
                .fns = ~ case_when(.x == "1" ~ 1, TRUE ~ 0))) %>%
  mutate(cen21_pds_link_flag = case_when(
          !is.na(nhs_number) ~ 1,
          TRUE ~ 0)) %>%
  group_by(census_id) %>%
  summarise(across(.cols = ends_with("_flag"),
                   .fns = ~ max(.x, na.rm = TRUE)),
            .groups = "drop") %>%
  ungroup()

### Staging
cen21_spine_pds_gpes <- sdf_stage_in_hdfs(
  data = cen21_spine_pds_gpes,
  filename = "cen21_spine_pds_gpes",
  staging_folder = staging_folder
  )

## Join PRS link flag
census_person <- census21_res %>%
  dplyr::distinct(resident_id, .keep_all = TRUE) %>%
  # join the Census 21 spine linkage to PDS
  left_join(cen21_spine_pds_gpes, by = c("resident_id" = "census_id"))

## Join cenusus files
census_person <- census_person %>%
  # join the region and IMD lookup
  left_join(final_oa, by = "response_id") %>%
  # join cen21 pds linkage IPWs
  left_join(cen21_spine_pds_ipws, by = "resident_id")

census_person <- census21_hh %>%
  select(household_id, heating_type) %>%
  right_join(census_person, by = "household_id")

## filter to population of interest
census_person <- census_person %>%
  filter(
         # include only private households
         residence_type == 1,
         # remove houses with students away
         usual_resident_ind == 1,
         # remove wholly imputed records
         proxy_answer != -2,
         # those living in England
         substr(census21_oa, 1, 1) == "E"
         )

### Staging
census_person <- sdf_stage_in_hdfs(
  data = census_person,
  filename = "census_person",
  staging_folder = staging_folder
  )

### Get household level table for predicting poverty

## Resident table
census_person_prediction <- census21_res %>%
  # join the region and IMD lookup
  left_join(final_oa, by = "response_id") %>%
  # filter to population of interest
  filter(# include only private household
         residence_type == 1,
         # remove houses with students away
         usual_resident_ind == 1,
         # Filter to keep England only
         substr(census21_oa, 1, 1) == "E") %>%
  # derive variables for predicting poverty
  mutate(# nssec_agg
         nssec_agg = case_when(
           ns_sec >= 1 & ns_sec < 4 ~ 1,
           ns_sec >= 4 & ns_sec < 7 ~ 2,
           ns_sec >= 7 & ns_sec < 8 ~ 3,
           ns_sec >= 8 & ns_sec < 10 ~ 4,
           ns_sec >= 10 & ns_sec < 12 ~ 5,
           ns_sec >= 12 & ns_sec < 13 ~ 6,
           ns_sec >= 13 & ns_sec < 14 ~ 7,
           ns_sec >= 14 & ns_sec < 15 ~ 8,
           (ns_sec >= 15 & ns_sec <= 17) | ns_sec == -8 | is.na(ns_sec) ~ 99),
         # long term health condition
         long_term_health = case_when(
           disability %in% 1:3 ~ 1,
           TRUE ~ 0),
         # educational attainment
         edattain = case_when(
           highest_qualification == 5 ~ 1,
           highest_qualification %in% c(1, 2, 3, 4, 6) ~ 2,
           activity_last_week == 3 ~ 3,
           highest_qualification == 0 ~ 4,
           highest_qualification == -8 ~ 99),
         # region
         region = case_when(
           rgn22nm == "North East" ~ 1,
           rgn22nm == "North West" ~ 2,
           rgn22nm == "Yorkshire and The Humber" ~ 3,
           rgn22nm == "East Midlands" ~ 4,
           rgn22nm == "West Midlands" ~ 5,
           rgn22nm == "East of England" ~ 6,
           rgn22nm == "London" ~ 7,
           rgn22nm == "South East" ~ 8,
           rgn22nm == "South West" ~ 9)) %>%
  # derive per household personal characteristics
  group_by(household_id) %>%
  summarise(imd_decile = mean(imd_decile, na.rm = TRUE),
            nssec_agg = min(nssec_agg, na.rm = TRUE),
            hh_min_age = min(resident_age, na.rm = TRUE),
            hh_max_age = max(resident_age, na.rm = TRUE),
            hh_mean_age = mean(resident_age, na.rm = TRUE),
            hh_adult_lt_health_condition = sum(long_term_health, na.rm = TRUE),
            hh_highest_edu_attain = min(edattain, na.rm = TRUE),
            region = mean(region, na.rm = TRUE)) %>%
  # ungroup just to make sure nothing goes wrong later
  ungroup() %>%
  # temporarily fix problematic household ages
  mutate(hh_max_age = case_when(hh_max_age < 18 ~ 18, TRUE ~ hh_max_age)) %>%
  # derive per household personal characteristics groups
  transmute(household_id = household_id,
            ### imd_decile
            imd_decile = as.character(imd_decile),
            ### nssec
            nssec_agg = case_when(nssec_agg == 99 ~ "X", TRUE ~ as.character(nssec_agg)),
            ### hh_max_age_group
            hh_max_age_group = case_when(
              hh_max_age <= 34 ~ "34_or_less",
              between(hh_max_age, 35, 39) ~ "35_to_39",
              between(hh_max_age, 40, 44) ~ "40_to_44",
              between(hh_max_age, 45, 49) ~ "45_to_49",
              between(hh_max_age, 50, 54) ~ "50_to_54",
              between(hh_max_age, 55, 59) ~ "50_to_59",
              between(hh_max_age, 60, 64) ~ "60_to_64",
              between(hh_max_age, 65, 69) ~ "65_to_69",
              between(hh_max_age, 70, 74) ~ "70_to_74",
              between(hh_max_age, 75, 79) ~ "75_to_79",
              hh_max_age >= 80 ~ "80_plus"),
            ### region
            region = case_when(
              region == 1 ~ "north_east",
              region == 2 ~ "north_west",
              region == 3 ~ "yorks_and_the_humber",
              region == 4 ~ "east_midlands",
              region == 5 ~ "west_midlands",
              region == 6 ~ "east_of_england",
              region == 7 ~ "london",
              region == 8 ~ "south_east",
              region == 9 ~ "south_west"),
           ### highest educational attainment
           hh_highest_edu_attain = case_when(
              hh_highest_edu_attain == 1 ~ "degree_or_above",
              hh_highest_edu_attain == 2 ~ "qualification_below_degree_level",
              hh_highest_edu_attain > 2 ~ "student_or_no_qualifications"),
           ### long term health condition
           hh_adult_lt_health_condition_group = case_when(
              hh_adult_lt_health_condition == 0 ~ "0",
              hh_adult_lt_health_condition == 1 ~ "1",
              hh_adult_lt_health_condition >= 2 ~ "2_plus")) %>%
  mutate(imd_decile = case_when(
              imd_decile != "10.0" ~ substr(imd_decile, 1, 1),
              imd_decile == "10.0" ~ substr(imd_decile, 1, 2)),
         nssec_agg = substr(nssec_agg, 1, 1))

### Staging
census_person_prediction <- sdf_stage_in_hdfs(
  data = census_person_prediction,
  filename = "census_person_prediction",
  staging_folder = staging_folder
  )

## Household table
census_household_prediction <- census21_hh %>%
  # select necessary variables
  select(# ID variables
         household_id,
         # Household characteristic variables
         hh_tenure, hh_size, hh_disabled, hh_adults_employment,
         number_bedrooms, hh_families_type) %>%
  # derive variables for predicting poverty
  transmute(household_id = household_id,
            hh_tenure_group = case_when(
              hh_tenure %in% c(0, 9, -8) ~ "owned_outright",
              hh_tenure %in% c(1:2) ~ "buying_with_mortgage",
              hh_tenure %in% c(3:4) ~ "social_rented_sector_tenants",
              hh_tenure %in% c(5:8) ~ "all_rented_privately"),
            hh_size_group = case_when(
              hh_size == 1 ~ "1",
              hh_size == 2 ~ "2",
              hh_size == 3 ~ "3",
              hh_size >= 4 ~ "4_plus"),
            hh_bedrooms_group = case_when(
              number_bedrooms == 1 ~ "1",
              number_bedrooms == 2 ~ "2",
              number_bedrooms == 3 ~ "3",
              number_bedrooms >= 4 ~ "4_plus"),
            hh_disabled_group = case_when(
              hh_disabled == 0 ~ "0",
              hh_disabled == 1 ~ "1",
              hh_disabled >= 2 ~ "2_plus"),
            hh_adults_employment_group = case_when(
              hh_adults_employment == 0 ~ "0",
              hh_adults_employment == 1 ~ "1",
              hh_adults_employment >= 2 ~ "2_plus"),
            hh_marital_group = case_when(
           hh_families_type %in% c(2:5) ~ "couple",
           TRUE ~ "single"))

### Staging
census_household_prediction <- sdf_stage_in_hdfs(
  data = census_household_prediction,
  filename = "census_household_prediction",
  staging_folder = staging_folder
  )

### Merge Census resident and household and region characteristics
census_prediction <- census_person_prediction %>%
  left_join(census_household_prediction, by = "household_id")

### Post-derivation checks

## Check any missingness
census_prediction %>% check_missingness() %>% print()

### Create dummy variables to apply tidy fit to
### split into 2 as I think too many mutates
### are causing spark to get confused.

### Set 1
input_columns_1 <- c("hh_tenure_group",
                     "hh_size_group",
                     "hh_bedrooms_group",
                     "hh_disabled_group",
                     "hh_adults_employment_group",
                     "hh_marital_group")

### Set 2
input_columns_2 <- c("imd_decile",
                     "nssec_agg",
                     "hh_max_age_group",
                     "region",
                     "hh_highest_edu_attain",
                     "hh_adult_lt_health_condition_group")

### Create dummies for set 1
for (input_col in input_columns_1) {
  
  census_prediction <- sdf_create_dummies(data = census_prediction,
                                          input_col = input_col)
  
}

### Staging
census_prediction <- sdf_stage_in_hdfs(
  data = census_prediction,
  filename = "census_prediction",
  staging_folder = staging_folder
  )

### Create dummies for set 2
for (input_col in input_columns_2) {
  
  census_prediction <- sdf_create_dummies(data = census_prediction,
                                          input_col = input_col)
  
}

### Staging
census_prediction <- sdf_stage_in_hdfs(
  data = census_prediction,
  filename = "census_prediction",
  staging_folder = staging_folder
  )

### Get breakdown vars

## Census 21 characteristics
census_person <- sdf_derive_census_vars(data = census_person)

### Staging
census_person <- sdf_stage_in_hdfs(
  data = census_person,
  filename = "census_person",
  staging_folder = staging_folder
  )

### Check missingness
census_person %>% check_missingness() %>% print()

### Get outputs

## Point Estimates

### Predict houshold being in poverty (low60ahc) using the tidy fit
census_prediction_point <- census_prediction %>%
  sdf_predict_from_tidy_fit(new_col_name = outcome, tidy_fit = final_tidy_fit) %>%
  select(household_id, low60ahc_probability)

## Merge it to person level file
census_person_point <- left_join(census_person, census_prediction_point, by = "household_id")

## GPES conditions and their interactions with poverty

# Create the interaction with the outcome variable
census_person_point <- census_person_point %>%
  mutate(across(.cols = all_of(gpes_conditions),
                .fns = ~ .x * .data[[outcome]],
                .names = "{.col}_{outcome}"),
         conditions_sum = rowSums(.[gpes_conditions]),
         cardiovascular_sum = rowSums(.[cardiovascular_conditions]),
         respiratory_sum = rowSums(.[respiratory_conditions])) %>%
  ungroup() %>%
  mutate(any_condition = case_when(conditions_sum > 0 ~ 1, TRUE ~ 0),
         any_cardiovascular = case_when(cardiovascular_sum > 0 ~ 1, TRUE ~ 0),
         any_respiratory = case_when(respiratory_sum > 0 ~ 1, TRUE ~ 0),
         any_cardiovascular_and_respiratory = case_when((any_cardiovascular == 1 &
                                                         any_respiratory == 1) ~ 1, TRUE ~ 0),
         any_2_conditions = case_when(conditions_sum >= 2 ~ 1, TRUE ~ 0),
         any_3_conditions = case_when(conditions_sum >= 3 ~ 1, TRUE ~ 0)) %>%
  mutate(across(.cols = all_of(!!c("any_condition",
                                   "any_2_conditions",
                                   "any_3_conditions",
                                   "any_cardiovascular",
                                   "any_respiratory",
                                   "any_cardiovascular_and_respiratory")),
                .fns = ~ .x * .data[[outcome]],
                .names = "{.col}_{outcome}")) %>%
  select(-conditions_sum, -cardiovascular_sum, -respiratory_sum)

### Staging
census_person_point <- sdf_stage_in_hdfs(
  data = census_person_point,
  filename = "census_person_point",
  staging_folder = staging_folder
  )

## Create outcomes; concatenate outcome and gpes flags and their interactions
outcomes <- c(outcome,
              c(gpes_conditions,
                "any_condition", "any_2_conditions", "any_3_conditions",
                "any_cardiovascular", "any_respiratory", "any_cardiovascular_and_respiratory",
                paste0(c(gpes_conditions,
                         "any_condition", "any_2_conditions", "any_3_conditions",
                         "any_cardiovascular", "any_respiratory", "any_cardiovascular_and_respiratory"),
                       "_", outcome)))

## Any condition*poverty by MSOA
poverty_rates_any_cond_msoa_raw <- purrr::map_dfr(c(outcome,
                                                    "any_condition",
                                                    paste0("any_condition_", outcome)), function(e) {
  
  get_rates(data = filter(census_person_point, cen21_pds_link_flag == 1),
            outcome = e,
            exposure = "all",
            geography = "msoa21nm",
            weight = "cen21_pds_linkage_IPW_rescaled_raw_msoa")
  
})

## Any condition*poverty by LTLA
poverty_rates_any_cond_ltla_raw <- purrr::map_dfr(c(outcome,
                                                    "any_condition",
                                                    paste0("any_condition_", outcome)), function(e) {
  
  get_rates(data = filter(census_person_point, cen21_pds_link_flag == 1),
            outcome = e,
            exposure = "all",
            geography = "ltla22nm",
            weight = "cen21_pds_linkage_IPW_rescaled_raw_ltla")
  
})

## England level but broken down by personal characteristics

# First level of loop: outcome
poverty_rates_england_raw <- purrr::map_dfr(outcomes, function(i) {
  
  # Second level of loop: exposure
  purrr::map_dfr(breakdown_vars, function(e) {

    get_rates(data = filter(census_person_point, cen21_pds_link_flag == 1),
              outcome = i,
              exposure = e,
              geography = "country",
              weight = "cen21_pds_linkage_IPW_rescaled_raw")      

    })
  
  })

## Confidence Intervals

if (bootstrap_CIs == TRUE) {
  
  ### Read files back in
  
  # Read back in to break data lineage
  census_person <- spark_read_parquet(sc,
                                      name = "census_person",
                                      paste0("/user/", username, "/", staging_folder, "/census_person.parquet"))
  
  # Read back in to break data lineage
  census_prediction <- spark_read_parquet(sc,
                                          name = "census_prediction",
                                          path = paste0("/user/", username, "/", staging_folder, "/census_prediction.parquet"))
  
  cat(paste0("Predicting start: ", Sys.time()), sep = "\n")
  
  ### Aggregate predictions table/Dimension reduction
  census_prediction_aggregated <- census_prediction %>%
    group_by(across(all_of(!!c(input_columns_1, input_columns_2)))) %>%
    count() %>%
    sdf_coalesce(20)
  
  ### Model Matrix Format
  
  ### Create dummies for set 1
  for (input_col in input_columns_1) {

    census_prediction_aggregated <- sdf_create_dummies(data = census_prediction_aggregated,
                                                       input_col = input_col)

  }

  ### Staging
  census_prediction_aggregated <- sdf_stage_in_hdfs(
    data = census_prediction_aggregated,
    filename = "census_prediction_aggregated",
    staging_folder = staging_folder
    )

  ### Create dummies for set 2
  for (input_col in input_columns_2) {

    census_prediction_aggregated <- sdf_create_dummies(data = census_prediction_aggregated,
                                                       input_col = input_col)

  }

  ### Staging
  census_prediction_aggregated <- sdf_stage_in_hdfs(
    data = census_prediction_aggregated,
    filename = "census_prediction_aggregated",
    staging_folder = staging_folder
    )
  
  ### Loop over number of iterations repeating the point estimates calculation with every ith model
  for (i in 1:n_iterations) {
    
    ## Print progress counter
    if (i %% width_iterations == 0) {

      cat(paste0(" Iteration ", i, ": ", Sys.time(), "\n"))

    }

    ## Choose the ith model
    final_tidy_fit_i <- bootstrapped_coefficients %>%
      filter(iteration == i)
    
    if (i == 1) {
      
      census_prediction_aggregated_wide <- sdf_predict_from_tidy_fit(data = census_prediction_aggregated,
                                                                     new_col_name = paste0(outcome, "_", i),
                                                                     tidy_fit = final_tidy_fit_i)
      
    }
    
    census_prediction_aggregated_wide <- sdf_predict_from_tidy_fit(data = census_prediction_aggregated_wide,
                                                                   new_col_name = paste0(outcome, "_", i),
                                                                   tidy_fit = final_tidy_fit_i)
    
    if (i %% save_iterations == 0) {
      
      ### Staging
      census_prediction_aggregated_wide <- sdf_stage_in_hdfs(
        data = census_prediction_aggregated_wide,
        filename = "census_prediction_aggregated_wide",
        staging_folder = staging_folder
        )
      
    }
      
    }
    
  ### Staging
  census_prediction_aggregated_wide <- sdf_stage_in_hdfs(
    data = census_prediction_aggregated_wide,
    filename = "census_prediction_aggregated_wide",
    staging_folder = staging_folder
    )
    
  ### Merge back to Prediction table
  census_prediction_wide <- left_join(
    census_prediction,
    select(census_prediction_aggregated_wide,
           all_of(!!c(input_columns_1, input_columns_2)), starts_with(outcome)),
    by = c(input_columns_1, input_columns_2))
  
  cat(paste0("Predicting done: ", Sys.time()), sep = "\n")
  
  cat(paste0("Pivoting start: ", Sys.time()), sep = "\n")
  
  ## Join to person level file
  census_person_wide <- census_person %>%
    left_join(select(census_prediction_wide, household_id, contains(outcome)),
              by = "household_id")
  
  ### Staging
  census_person_wide <- sdf_stage_in_hdfs(
    data = census_person_wide,
    filename = "census_person_wide",
    staging_folder = staging_folder
    )
  
  ## Aggregate
  census_person_wide_aggregated <- census_person_wide %>%
    filter(cen21_pds_link_flag == 1) %>%
    mutate(across(.cols = starts_with(paste0(outcome, "_")),
                  .fns = ~ .x * .data[["cen21_pds_linkage_IPW_rescaled_raw"]])) %>%
    group_by(across(all_of(!!c(breakdown_vars#,
                               #"msoa21nm", "ltla22nm"
                               )))) %>%
    summarise(across(.cols = starts_with(paste0(outcome, "_")),
                     .fns = ~ sum(.x, na.rm = TRUE)),
              weighted_population = sum(.data[["cen21_pds_linkage_IPW_rescaled_raw"]], na.rm = TRUE),
              .groups = "drop") %>%
    sdf_coalesce(20)

  ## Pivot long
  census_person_long_aggregated <- census_person_wide_aggregated %>%
    pivot_longer(cols = starts_with(paste0(outcome, "_")),
                 names_to = "iteration",
                 values_to = outcome) %>%
    mutate(iteration = regexp_replace(iteration, paste0(outcome, "_"), ""),
           iteration = as.integer(iteration))
  
  ### Staging
  census_person_long_aggregated <- sdf_stage_in_hdfs(
    data = census_person_long_aggregated,
    filename = "census_person_long_aggregated",
    staging_folder = staging_folder
    )

  cat(paste0("Pivoting done: ", Sys.time()), sep = "\n")
  
  cat(paste0("Estimates start: ", Sys.time()), sep = "\n")
  
  cat(paste0(" Estimates MSOA: ", Sys.time()), sep = "\n")

#  ## Any condition*poverty by MSOA
#  poverty_ci_any_cond_msoa_raw <- get_ci(
#    data = census_person_long_aggregated,
#    outcome = outcome,
#    exposure = "msoa21nm",
#    geography = "msoa21nm",
#    iteration = "iteration")
#  
#  cat(paste0(" Estimates LTLA: ", Sys.time()), sep = "\n")
#  
#  ## Any condition*poverty by LTLA
#  poverty_ci_any_cond_ltla_raw <- get_ci(
#    data = census_person_long_aggregated,
#    outcome = outcome,
#    exposure = "ltla22nm",
#    geography = "ltla22nm",
#    iteration = "iteration")

  ## England
  
  cat(paste0(" Estimates person level: ", Sys.time()), sep = "\n")
  
  # Read back in to break data lineage
  census_person_long_aggregated <- spark_read_parquet(sc,
                                                      name = "census_person_long_aggregated",
                                                      paste0("/user/", username, staging_folder, filename))

  # First level of loop: outcome
  poverty_ci_england_raw <- purrr::map_dfr(breakdown_vars, function(i) {

    get_ci(data = census_person_long_aggregated,
           outcome = outcome,
           exposure = i,
           geography = "country",
           iteration = "iteration")

    })
  
  cat(paste0("Estimates done: ", Sys.time()), sep = "\n")
  
  cat(paste0("Final edits: ", Sys.time()), sep = "\n")
  
  ### Merge the CIs to the point estimate files
#  poverty_rates_any_cond_msoa_raw <- left_join(
#    poverty_rates_any_cond_msoa_raw,
#    poverty_ci_any_cond_msoa_raw,
#    by = c("outcome" ="outcome", "geography" = "domain", "area" = "group")) %>%
#    relocate(total_lci, .after = total_est) %>%
#    relocate(total_uci, .after = total_lci) %>%
#    relocate(prop_lci, .after = prop_est) %>%
#    relocate(prop_uci, .after = prop_lci)
#  
#  poverty_rates_any_cond_ltla_raw <- left_join(
#    poverty_rates_any_cond_ltla_raw,
#    poverty_ci_any_cond_ltla_raw,
#    by = c("outcome" ="outcome", "geography" = "domain", "area" = "group")) %>%
#    relocate(total_lci, .after = total_est) %>%
#    relocate(total_uci, .after = total_lci) %>%
#    relocate(prop_lci, .after = prop_est) %>%
#    relocate(prop_uci, .after = prop_lci)

  poverty_rates_england_raw <- left_join(
    poverty_rates_england_raw,
    poverty_ci_england_raw,
    by = c("outcome", "domain", "group")) %>%
    relocate(total_lci, .after = total_est) %>%
    relocate(total_uci, .after = total_lci) %>%
    relocate(prop_lci, .after = prop_est) %>%
    relocate(prop_uci, .after = prop_lci)
  
  cat(paste0("Finished: ", Sys.time()), sep = "\n")
  
  }
  
### Get MSOA code and name lookup
msoa_lookup <- census_person %>%
  dplyr::distinct(msoa21nm, msoa21cd) %>%
  sdf_coalesce(1) %>%
  collect()

## Merge it to the files
poverty_rates_any_cond_msoa_raw <- left_join(poverty_rates_any_cond_msoa_raw,
                                             msoa_lookup, by = c("area" = "msoa21nm")) %>%
  relocate(msoa21cd, .after = area)

### Get LTLA code and name lookup
ltla_lookup <- census_person %>%
  dplyr::distinct(ltla22nm, ltla22cd) %>%
  sdf_coalesce(1) %>%
  collect()

## Merge it to the files
poverty_rates_any_cond_ltla_raw <- left_join(poverty_rates_any_cond_ltla_raw,
                                             ltla_lookup, by = c("area" = "ltla22nm")) %>%
  relocate(ltla22cd, .after = area)

### Apply Census Statistical Disclosure Control logic
poverty_rates_any_cond_msoa_sdc <- apply_census_sdc(data = poverty_rates_any_cond_msoa_raw)

poverty_rates_any_cond_ltla_sdc <- apply_census_sdc(data = poverty_rates_any_cond_ltla_raw)

poverty_rates_england_sdc <- apply_census_sdc(data = poverty_rates_england_raw)

### Save
write.csv(poverty_rates_any_cond_msoa_raw,
          paste0(project_directory, "/poverty_rates_any_cond_msoa_raw.csv"),
          row.names = FALSE)

write.csv(poverty_rates_any_cond_msoa_sdc,
          paste0(project_directory, "/poverty_rates_any_cond_msoa_sdc.csv"),
          row.names = FALSE)

write.csv(poverty_rates_any_cond_ltla_raw,
          paste0(project_directory, "/poverty_rates_any_cond_ltla_raw.csv"),
          row.names = FALSE)

write.csv(poverty_rates_any_cond_ltla_sdc,
          paste0(project_directory, "/poverty_rates_any_cond_ltla_sdc.csv"),
          row.names = FALSE)

write.csv(poverty_rates_england_raw,
          paste0(project_directory, "/poverty_rates_england_raw.csv"),
          row.names = FALSE)

write.csv(poverty_rates_england_sdc,
          paste0(project_directory, "/poverty_rates_england_sdc.csv"),
          row.names = FALSE)

### Check stability of bootstrapped CIs
if (check_stability == TRUE) {
    
    ### Calc CIs per iteration
    stability_check <- purrr::map_dfr(seq(width_iterations, n_iterations, by = width_iterations),
                                      function(e) {
      
      stability_check_e <- census_person_long_aggregated %>%
        filter(iteration <= e) %>%
        group_by(iteration, all) %>%
        summarise(weighted_population = sum(weighted_population, na.rm = TRUE),
                  total_est = sum(.data[[outcome]], na.rm = TRUE),
                  .groups = 'drop') %>%
        mutate(prop_est = total_est / weighted_population) %>%
        summarise(total_est_se = sd(total_est),
                  prop_est_se = sd(prop_est),
                  .groups = 'drop') %>%
        sdf_coalesce(1) %>%
        collect() %>%
        mutate(iteration = e)
      
    })
  
    ### Plot

    ## Totals
    pdf(paste0(project_directory, "/bootstrapped_poverty_totals_props_all_SE_plot.pdf"),
          onefile = TRUE)

      for (estimate in c("total_est_se", "prop_est_se")) {

          p <- stability_check %>%
            filter(outcome == outcome, !is.na(.data[[estimate]])) %>%
            ggplot(aes(x = .data[["iteration"]], y = .data[[estimate]])) +
            geom_line() +
            labs(title = estimate)

          print(p)

        }

    dev.off()
  
  }

### Run 2ndary disclosure tool if specified
if (run_2ndary_disclosure_tool == TRUE) {
  
  ### Secodary disclosure checking tool

  ### Pivot data wide
  poverty_rates_england_sdc_wide <- poverty_rates_england_sdc %>%
    select(all_of(c("domain",
                    "group",
                    "outcome",
                    "total_est"))) %>%
    pivot_wider(id_cols = all_of(c("domain",
                                   "group")),
                names_from = "outcome",
                values_from = "total_est") %>%
    mutate(group = if_else(domain == "ethnicity_agg" &
                           group == "Other", "Other Ethnic Group", group)) %>%
    select(-all_of(c("domain")))

  ### Save to it can be used in the tool
  write.csv(poverty_rates_england_sdc_wide,
            paste0(project_directory, "/poverty_rates_england_sdc_wide.csv"),
            row.names = FALSE)

  ### Use the tool
  sct(
    directory = project_directory,
    data.file = "poverty_rates_england_sdc_wide",
    spec.file = "poverty_rates_england_sdc_specs",
    out.file = "poverty_rates_england_sdc_out",
    report.file = "poverty_rates_england_sdc_report",
    pri.mark = "[c]",
    sec.mark = "S"
    )
  
}

## ESP Editing

# Collect to local memory
ESP <- ESP %>%
  sdf_coalesce(1) %>%
  collect()

# Calculate total population size
total_esp <- sum(ESP$ESP, na.rm = TRUE)

## Poisson Editing
poisson <- poisson %>%
  sdf_coalesce(1) %>%
  collect() %>%
  rename(count = Deaths)

### Get ASRs

## MSOA
poverty_asr_msoa <- purrr::map_dfr(c(outcome,
                                     "any_condition",
                                     paste0("any_condition_", outcome)), function(e) {
  
  get_age_standardised_rates(data = filter(census_person_point, cen21_pds_link_flag == 1),
                               ESP_data = ESP,
                               poisson_data = poisson, 
                               total_esp = total_esp,
                               outcome = e,
                               type_of_outcome = "count",
                               age_grp = "age_group_esp",
                               group_vars = "msoa21nm",
                               weight = "cen21_pds_linkage_IPW_rescaled_raw_msoa") %>%
      mutate(outcome = e) %>%
      relocate(outcome, .before = msoa21nm)
  
}) %>%
  left_join(msoa_lookup, by = "msoa21nm") %>%
  relocate(msoa21cd, .after = msoa21nm)

# SDC
poverty_asr_msoa_sdc <- apply_census_sdc(
  data = poverty_asr_msoa, rate_type = "asr"
  )

# Save
write.csv(poverty_asr_msoa,
          paste0(project_directory, "/poverty_asr_msoa_raw.csv"),
          row.names = FALSE)

write.csv(poverty_asr_msoa_sdc,
          paste0(project_directory, "/poverty_asr_msoa_sdc.csv"),
          row.names = FALSE)

## LTLA
poverty_asr_ltla <- purrr::map_dfr(c(outcome,
                                     "any_condition",
                                     paste0("any_condition_", outcome)), function(e) {
  
  get_age_standardised_rates(data = filter(census_person_point, cen21_pds_link_flag == 1),
                               ESP_data = ESP,
                               poisson_data = poisson, 
                               total_esp = total_esp,
                               outcome = e,
                               type_of_outcome = "count",
                               age_grp = "age_group_esp",
                               group_vars = "ltla22nm",
                               weight = "cen21_pds_linkage_IPW_rescaled_raw_ltla") %>%
      mutate(outcome = e) %>%
      relocate(outcome, .before = ltla22nm)
  
}) %>%
  left_join(ltla_lookup, by = "ltla22nm") %>%
  relocate(ltla22cd, .after = ltla22nm)

# SDC
poverty_asr_ltla_sdc <- apply_census_sdc(
  data = poverty_asr_ltla, rate_type = "asr"
  )

# Save
write.csv(poverty_asr_ltla,
          paste0(project_directory, "/poverty_asr_ltla_raw.csv"),
          row.names = FALSE)

write.csv(poverty_asr_ltla_sdc,
          paste0(project_directory, "/poverty_asr_ltla_sdc.csv"),
          row.names = FALSE)

## Engand
poverty_asr_england <- purrr::map_dfr(outcomes, function(e) {
  
  print(paste0(e, ": ", Sys.time()))
  
  purrr::map_dfr(breakdown_vars, function(i) {
    
    print(paste0(" ", i, ": ", Sys.time()))
    
    if (i %in% c("all", "age_group")) {
      
      return(NULL)
      
    }
    
    weight <- case_when(i == "ltla22nm" ~ "cen21_pds_linkage_IPW_rescaled_raw_ltla",
                        i == "msoa21nm" ~ "cen21_pds_linkage_IPW_rescaled_raw_msoa",
                        TRUE ~ "cen21_pds_linkage_IPW_rescaled_raw")
    
    get_age_standardised_rates(data = filter(census_person_point, cen21_pds_link_flag == 1),
                               ESP_data = ESP,
                               poisson_data = poisson, 
                               total_esp = total_esp,
                               outcome = e,
                               type_of_outcome = "count",
                               age_grp = "age_group_esp",
                               group_vars = i,
                               weight = weight) %>%
      mutate(outcome = e,
             domain = i) %>%
      rename(group = all_of(i)) %>%
      mutate(group = as.character(group)) %>%
      relocate(domain, .before = group) %>%
      relocate(outcome, .before = domain)
    
  })
  
})

# SDC
poverty_asr_england_sdc <- apply_census_sdc(
  data = poverty_asr_england, rate_type = "asr"
  )

# Save
write.csv(poverty_asr_england,
          paste0(project_directory, "/poverty_asr_england_raw.csv"),
          row.names = FALSE)

write.csv(poverty_asr_england_sdc,
          paste0(project_directory, "/poverty_asr_england_sdc.csv"),
          row.names = FALSE)

### Clean up HDFS after staging
cmd <- paste0("hdfs dfs -rm -r -skipTrash /user/", username, "/", staging_folder)
system(cmd, ignore.stdout = TRUE, ignore.stderr = TRUE)

### Disconnect
spark_disconnect(sc)

### End