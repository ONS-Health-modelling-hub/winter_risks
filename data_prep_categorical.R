# Read Data ----

## FRS: Household ----
frs_household <- paste0(data_directory,
                        "\\8948spss_FRS_2020_2021\\UKDA-8948-spss\\spss\\spss25\\househol.sav") %>%
  haven::read_spss(col_select = all_of(c("SERNUM",
                                         "BEDROOM6",
                                         "ADULTH",
                                         "DEPCHLDH",
                                         "DISCHHA1",
                                         "DISCHHC1",
                                         "EMP",
                                         "EMPHRP",
                                         "TENURE",
                                         "TENTYP2",
                                         "PTENTYP2",
                                         "GVTREGN"))) %>%
  ### Drop SPSS labels as they cause computation errors down the line
  haven::zap_labels() %>%
  ### Clean names to lower case
  janitor::clean_names() %>%
  ### Filter for England
  filter(gvtregn < (2 * (10 ^ 8))) %>%
  ### create region variable
  mutate(region = case_when(
    gvtregn == 112000001 ~ "north_east",
    gvtregn == 112000002 ~ "north_west",
    gvtregn == 112000003 ~ "yorks_and_the_humber",
    gvtregn == 112000004 ~ "east_midlands",
    gvtregn == 112000005 ~ "west_midlands",
    gvtregn == 112000006 ~ "east_of_england",
    gvtregn == 112000007 ~ "london",
    gvtregn == 112000008 ~ "south_east",
    gvtregn == 112000009 ~ "south_west",
  ))

## FRS: Adult ----
frs_adult <- paste0(data_directory,
                    "\\8948spss_FRS_2020_2021\\UKDA-8948-spss\\spss\\spss25\\adult.sav") %>%
  haven::read_spss(col_select = all_of(c("SERNUM",
                                         "HRPID",
                                         "EMPSTATC",
                                         "NSSEC",
                                         "HEALTH1",
                                         "GVTREGNO"))) %>%
  ### Drop SPSS labels as they cause computation errors down the line
  haven::zap_labels() %>%
  ### Clean names to lower case
  janitor::clean_names() %>%
  ### Filter for England (and optionally Wales)
  filter(gvtregno < 11) %>% # filter(gvtregno < 12) %>%
  ### derive variables
  mutate(### in employment binary flag
    in_employment_flag = if_else(empstatc %in% 1:3, 1, 0),
    ### nssec_agg; analytical aggregated version
    nssec = as.numeric(nssec),
    nssec_agg = case_when(
      nssec >= 1 & nssec < 4 ~ 1,
      nssec >= 4 & nssec < 7 ~ 2,
      nssec >= 7 & nssec < 8 ~ 3,
      nssec >= 8 & nssec < 10 ~ 4,
      nssec >= 10 & nssec < 12 ~ 5,
      nssec >= 12 & nssec < 13 ~ 6,
      nssec >= 13 & nssec < 14 ~ 7,
      nssec >= 14 & nssec < 15 ~ 8,
      between(nssec, 15, 17) | is.na(nssec) ~ 99),
    ### long term health condition
    health1 = if_else(health1 == 1, 1, 0)) %>%
  ### derive per household variables
  group_by(sernum) %>%
  summarise(hh_adults_employment = sum(in_employment_flag, na.rm = TRUE),
            nssec_agg = min(nssec_agg, na.rm = TRUE),
            hh_adult_lt_health_condition = sum(health1)) %>%
  ### ungroup just to make sure nothing is messed up down the line
  ungroup()

### Check number of rows
nrow(frs_household) == nrow(frs_adult)

## HBAI ----
hbai <- paste0(data_directory,
               "\\5828spss_HBAI_2018_2021\\UKDA-5828-spss\\spss\\spss25\\i1821e_2021prices.sav") %>%
  haven::read_spss(col_select = all_of(c("sernum",
                                         "HRPID",
                                         "LOW60AHC",
                                         "IMDE",
                                         "IMDW",
                                         "AGEHD",
                                         "AGE",
                                         "ETH",
                                         "MARITAL",
                                         "EDATTAIN",
                                         "TENHBAI",
                                         "YEAR",
                                         "GVTREGN")))  %>%
  ### Drop SPSS labels as they cause computation errors down the line
  haven::zap_labels() %>%
  ### Clean names to lower case
  janitor::clean_names() %>%
  ### Filter to keep FYE 21 only and England
  filter(year == 27, gvtregn < 11) %>%
  ### remove unnecessary variables
  select(-year) %>%
  ### derive variables
  mutate(### coalesce imd decile for England and Wales into one variable
    imd_decile = if_else(is.na(imde), imdw, imde),
    ### educational attainment
    edattain = if_else(edattain == 0, 99, edattain))

## Keep HBAI person level for later ----
hbai_pers <- hbai

### derive per household variables
hbai <- hbai %>%
  group_by(sernum) %>%
  summarise(low60ahc = max(low60ahc, na.rm = TRUE),
            imd_decile = mean(imd_decile, na.rm = TRUE),
            hh_highest_edu_attain = min(edattain, na.rm = TRUE),
            hh_tenure = min(tenhbai, na.rm = TRUE),
            hh_min_age = min(age, na.rm = TRUE),
            hh_max_age = max(age, na.rm = TRUE),
            hh_mean_age = mean(age, na.rm = TRUE),
            hh_marital = max(marital, na.rm = TRUE)) %>%
  ### ungroup just to make sure nothing is messed up down the line
  ungroup()

### check number of rows
nrow(frs_household) == nrow(hbai)

### This difference is because some FRS cases are removed from HBAI due to missing spouses,
### this is documented on page 10 (point 23) in:
### "\Data\5828spss_HBAI_2018_2021\UKDA-5828-spss\mrdoc\pdf\5828_hbai_2021_harmonised_dataset_user_guide.pdf.

# create frs ----

### Given that there are less cases in HBAI, left joing FRS data sets to HBAI
frs <- hbai %>%
  left_join(frs_household, by = "sernum") %>%
  left_join(frs_adult, by = "sernum")

### Check number of rows
nrow(hbai) == nrow(frs)

## clean up ----
rm(frs_household, frs_adult)

## check missingness ----
sapply(frs, FUN = function(x) {sum(is.na(x))})

# create census equivalent variables ----
### that means to create character/string variables with 
### specific reference levels.
frs <- frs %>%
  ### derive variables for modelling, to be replicated using census
  mutate(### poverty flag/outcome variable
    low60ahc = as.character(low60ahc),
    low60ahc = as.factor(low60ahc),
    low60ahc = relevel(low60ahc, ref = "0"),
    ### region
    region = as.factor(region),
    region = relevel(region, ref = "london"),
    ### imd decile
    imd_decile = as.character(imd_decile),
    imd_decile = as.factor(imd_decile),
    imd_decile = relevel(imd_decile, ref = "1"),
    ### nssec
    nssec_agg = if_else(nssec_agg == 99, "X", as.character(nssec_agg)),
    nssec_agg = as.factor(nssec_agg),
    nssec_agg = relevel(nssec_agg, ref = "1"),
    ### create number of people in the household
    hh_size = adulth + depchldh,
    ### create household size category
    hh_size_group = case_when(
      hh_size == 1 ~ "1",
      hh_size == 2 ~ "2",
      hh_size == 3 ~ "3",
      hh_size >= 4 ~ "4_plus"),
    hh_size_group = as.factor(hh_size_group),
    hh_size_group = relevel(hh_size_group, ref = "1"),
    ### number of bedrooms
    hh_bedrooms_group = case_when(
      bedroom6 == 1 ~ "1",
      bedroom6 == 2 ~ "2",
      bedroom6 == 3 ~ "3",
      bedroom6 >= 4 ~ "4_plus"),
    hh_bedrooms_group = as.factor(hh_bedrooms_group),
    hh_bedrooms_group = relevel(hh_bedrooms_group, ref = "3"),
    ### create number of disabled people in the household
    hh_disabled = dischha1 + dischhc1,
    ### create number of disabled people category
    hh_disabled_group = case_when(
      hh_disabled == 0 ~ "0",
      hh_disabled == 1 ~ "1",
      hh_disabled >= 2 ~ "2_plus"),
    hh_disabled_group = as.factor(hh_disabled_group),
    hh_disabled_group = relevel(hh_disabled_group, ref = "0"),
    ### create number of adults in employment category
    hh_adults_employment_group = case_when(
      hh_adults_employment == 0 ~ "0",
      hh_adults_employment == 1 ~ "1",
      hh_adults_employment >= 2 ~ "2_plus"),
    hh_adults_employment_group = as.factor(hh_adults_employment_group),
    hh_adults_employment_group = relevel(hh_adults_employment_group, ref = "0"),
    ### create aggregated version of educational attainment
    hh_highest_edu_attain = case_when(
      hh_highest_edu_attain == 1 ~ "degree_or_above",
      hh_highest_edu_attain == 2 ~ "qualification_below_degree_level",
      hh_highest_edu_attain > 2 ~ "student_or_no_qualifications"),
    hh_highest_edu_attain = as.factor(hh_highest_edu_attain),
    hh_highest_edu_attain = relevel(hh_highest_edu_attain, ref = "degree_or_above"),
    ### create age groups for the household head
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
    hh_max_age_group = as.factor(hh_max_age_group),
    hh_max_age_group = relevel(hh_max_age_group, ref = "50_to_54"),
    ### tenure group of the household
    hh_tenure_group = case_when(
      hh_tenure == 1 ~ "owned_outright",
      hh_tenure == 2 ~ "buying_with_mortgage",
      hh_tenure == 3 ~ "social_rented_sector_tenants",
      hh_tenure == 4 ~ "all_rented_privately"),
    hh_tenure_group = as.factor(hh_tenure_group),
    hh_tenure_group = relevel(hh_tenure_group, ref = "owned_outright"),
    ### marital status of the head of the household
    hh_marital_group = case_when(
      hh_marital == 1 ~ "single",
      hh_marital == 2 ~ "couple"),
    hh_marital_group = as.factor(hh_marital_group),
    hh_marital_group = relevel(hh_marital_group, ref = "single"),
    ### long term illness of head of household flag
    hh_adult_lt_health_condition_group = case_when(
      hh_adult_lt_health_condition == 0 ~ "0",
      hh_adult_lt_health_condition == 1 ~ "1",
      hh_adult_lt_health_condition >= 2 ~ "2_plus"),
    hh_adult_lt_health_condition_group = as.factor(hh_adult_lt_health_condition_group),
    hh_adult_lt_health_condition_group = relevel(hh_adult_lt_health_condition_group, ref = "0"))

# end ----