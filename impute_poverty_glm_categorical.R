# Libraries ----
library(tidyverse)
library(tidymodels)

# set working directory ----
setwd(".../WinterRiskFactors")

## create data and output directories ----
data_directory <- paste0(getwd(), "/Data")
output_directory <- paste0(getwd(), "/Outputs")

# User Inputs ----

## seed for reproducibility ----
seed <- 1234

## specify id, outcome and covariate variables ----
id_var <- "sernum"

outcome <- "low60ahc"

covariates <- c(
  "nssec_agg",
  "region",
  "imd_decile",
  "hh_size_group",
  "hh_tenure_group",
  "hh_bedrooms_group",
  "hh_max_age_group",
  "hh_marital_group",
  "hh_highest_edu_attain",
  "hh_adult_lt_health_condition_group",
  "hh_disabled_group",
  "hh_adults_employment_group"
)

# data prep ----
source(paste0(getwd(), "/Scripts/data_prep_categorical.R"))

# post prep checks ----

## keep variables for modelling only ----
frs <- frs %>%
  select(all_of(c(id_var, outcome, covariates)))

## check cross tabs ----
for (variable in c(outcome, covariates)) {
  
  print(variable)
  print(table(frs[[variable]], useNA = "ifany"))
  print(prop.table(table(frs[[variable]], useNA = "ifany")))
  
}

## filter to non-missing cases ----

## initial split ----
set.seed(seed)

frs_split <- initial_split(frs, strata = low60ahc)

## training sample ----
frs_train <- training(frs_split)

mean_low60ahc_frs_train <- frs_train %>%
  mutate(low60ahc = if_else(low60ahc == 1, 1, 0)) %>%
  summarise(mean_low60ahc = mean(low60ahc, na.rm = TRUE)) %>%
  pull(mean_low60ahc)

## testing sample ----
frs_test <- testing(frs_split)

## validation set ----
set.seed(seed)

val_set <- validation_split(frs_train, 
                            strata = low60ahc, 
                            prop = 0.80)
val_set

## define model formula ----
model_formula <- as.formula(paste0(outcome,
                                   " ~ ",
                                   paste(covariates, collapse = " + ")))
model_formula

# logistic regression ----

## define a generalized linear model for binary outcomes ----
lr_mod <- logistic_reg() %>% 
  set_engine("glm", family = "binomial")

## recipes ----
lr_recipe <- recipe(model_formula, data = frs_train) %>%
  step_dummy(all_nominal_predictors()) %>%
  step_zv(all_predictors())

## workflow ----
lr_workflow <- workflow() %>% 
  add_model(lr_mod) %>% 
  add_recipe(lr_recipe)

## grid for tuning ----
lr_reg_grid <- as.data.frame(NULL)

## train and tune the model ----
lr_res <- lr_workflow %>% 
  tune_grid(val_set,
            grid = lr_reg_grid,
            control = control_grid(save_pred = TRUE),
            metrics = metric_set(roc_auc, pr_auc))

## top model metrics ----

### ROC AUC
lr_best_roc_auc <- lr_res %>% 
  show_best("roc_auc", n = 1)

### RP AUC
lr_best_pr_auc <- lr_res %>% 
  show_best("pr_auc", n = 1)

## ROC AUC for logit ----

### Data for ROC plot ----
lr_roc <- lr_res %>% 
  collect_predictions(parameters = lr_best_roc_auc) %>% 
  roc_curve(truth = low60ahc, .pred_1, event_level = "second") %>% 
  mutate(model = "LR")

### ROC AUC value ----
lr_roc_auc <- lr_res %>% 
  collect_predictions(parameters = lr_best_roc_auc) %>% 
  roc_auc(truth = low60ahc, .pred_1, event_level = "second")

## Precision-Recall for logit ----

### Data for PR plot ----
lr_pr <- lr_res %>% 
  collect_predictions(parameters = lr_best_pr_auc) %>% 
  pr_curve(truth = low60ahc, .pred_1, event_level = "second") %>% 
  mutate(model = "LR")

#### PR AUC value ----
lr_pr_auc <- lr_res %>% 
  collect_predictions(parameters = lr_best_pr_auc) %>% 
  pr_auc(truth = low60ahc, .pred_1, event_level = "second")

# random forest ----

## get number of cores for parallel processing ----
cores <- parallel::detectCores()
cores

## define a random forst model for binary outcomes ----
rf_mod <- rand_forest(mtry = tune(), min_n = tune(), trees = 1000) %>% 
  set_engine("ranger", num.threads = cores) %>% 
  set_mode("classification")

## recipes ----
rf_recipe <- recipe(model_formula, data = frs_train) %>%
  step_dummy(all_nominal_predictors()) %>%
  step_zv(all_predictors()) #%>%
  #update_role(sernum, new_role = "ID")

## workflow ----
rf_workflow <- workflow() %>% 
  add_model(rf_mod) %>% 
  add_recipe(rf_recipe)

## train and tune the model ----
rf_mod

## show what will be tuned
extract_parameter_set_dials(rf_mod)

## a space-filling design to tune, with 25 candidate models ----
set.seed(seed)

rf_res <- rf_workflow %>% 
  tune_grid(val_set,
            grid = 25,
            control = control_grid(save_pred = TRUE),
            metrics = metric_set(roc_auc, pr_auc))

## the best model according to the ROC AUC metric ----
rf_best_roc_auc <- rf_res %>% 
  select_best(metric = "roc_auc")

## the best model according to the PR AUC metric ----
rf_best_pr_auc <- rf_res %>% 
  select_best(metric = "pr_auc")

## ROC AUC for random forest with household and household head characteristics ----
rf_roc <- rf_res %>% 
  collect_predictions(parameters = rf_best_roc_auc) %>% 
  roc_curve(truth = low60ahc, .pred_1, event_level = "second") %>%
  mutate(model = "RF")

rf_roc_auc <- rf_res %>% 
  collect_predictions(parameters = rf_best_roc_auc) %>% 
  roc_auc(truth = low60ahc, .pred_1, event_level = "second")

## Precision-Recall for random forest with household and household head characteristics ----
rf_pr <- rf_res %>% 
  collect_predictions(parameters = rf_best_pr_auc) %>% 
  pr_curve(truth = low60ahc, .pred_1, event_level = "second") %>% 
  mutate(model = "RF")

rf_pr_auc <- rf_res %>% 
  collect_predictions(parameters = rf_best_pr_auc) %>% 
  pr_auc(truth = low60ahc, .pred_1, event_level = "second")

# compare the logit and random forest models in a ROC AUC plot ----
roc <- bind_rows(rf_roc, lr_roc) %>% 
  mutate(model = as.factor(model)) %>%
  ggplot(aes(x = 1 - specificity, y = sensitivity, color = model)) + 
  geom_path(lwd = 1.5, alpha = 1) +
  geom_abline(lty = 3) + 
  coord_equal() + 
  scale_color_viridis_d(option = "plasma", end = .6)+
  theme_bw()

roc

ggsave("Outputs/ROC_curve_rf_lr.png", width = 6*4/3, height= 6)

# compare the logit and random forest models in ROC AUCs  ----
lr_rf_roc_auc <- tibble(label = c("lr_roc_auc",
                                  "rf_roc_auc"),
                        value = c(lr_roc_auc$.estimate,
                                  rf_roc_auc$.estimate))

lr_rf_roc_auc

# compare the logit and random forest models in a PR Curve plot ----
pr <- bind_rows(rf_pr, lr_pr) %>% 
  mutate(model = as.factor(model)) %>%
  ggplot(aes(x = recall, y = precision, color = model)) + 
  geom_path(lwd = 1.5, alpha = 1) +
  geom_hline(yintercept = mean_low60ahc_frs_train, lty = 3) + 
  coord_equal() + 
  scale_color_viridis_d(option = "plasma", end = .6)+
  theme_bw()

pr

ggsave("Outputs/PR_curve_rf_lr.png", width = 6*4/3, height= 6)

# compare the logit and random forest models in a PR AUC ----
lr_rf_pr_auc <- tibble(label = c("lr_pr_auc",
                                 "rf_pr_auc"),
                       value = c(lr_pr_auc$.estimate,
                                 rf_pr_auc$.estimate))

lr_rf_pr_auc

### Random forest doesn't outperform logistic regression but

# final model: logistic regression ----

## the last model ----
last_lr_mod <- logistic_reg() %>% 
  set_engine("glm", family = "binomial")

## the last workflow ----
last_lr_workflow <- 
  lr_workflow %>% 
  update_model(last_lr_mod) %>%
  update_recipe(lr_recipe)

## the last fit ----

## set seed for reproducibility
set.seed(seed)

last_lr_fit <- 
  last_lr_workflow %>% 
  last_fit(frs_split)

last_lr_fit

## check performance ----
last_lr_fit %>% 
  collect_metrics()

## variable importance scores ----
last_lr_fit %>% 
  extract_fit_parsnip() %>% 
  vip::vip(num_features = 20)

## ROC AUC ----
roc_auc <- last_lr_fit %>%
  collect_predictions() %>%
  roc_auc(truth = low60ahc, .pred_1, event_level = "second")

## ROC AUC  plot ----
roc_final <- last_lr_fit %>% 
  collect_predictions() %>% 
  roc_curve(truth = low60ahc, .pred_1, event_level = "second") %>%
  autoplot() +
  labs(y = "Sensitivity",
       title = paste0("AUC ROC = ",  round(roc_auc$.estimate, 3)))+
  theme_bw()

roc_final

## PR AUC  ----
pr_auc <- last_lr_fit %>% 
  collect_predictions() %>% 
  pr_auc(truth = low60ahc, .pred_1, event_level = "second")

## PR AUC  plot ----
pr_final <- last_lr_fit %>% 
  collect_predictions() %>% 
  pr_curve(truth = low60ahc, .pred_1, event_level = "second") %>%
  autoplot()+
  geom_hline(yintercept = mean_low60ahc_frs_train, lty = 3) + 
  labs(y = "Precision",
       x = "Recall",
       title = paste0("AUC PR = ",  round(pr_auc$.estimate, 3)))+
  theme_bw()

pr_final

ggpubr::ggarrange(roc_final, pr_final, nrow =2)
ggsave(paste0(output_directory, "/ROC_PR_final.png"), width = 7, height= 7)

## sensitivity ----
## positive predictions out of all true positives
last_lr_fit %>%
  collect_predictions() %>%
  sens(truth = low60ahc,
       estimate = .pred_class)

## specificity ----
## negative predictions out of all true negatives
last_lr_fit %>%
  collect_predictions() %>%
  spec(truth = low60ahc,
       estimate = .pred_class)

## all accuracy measures ----
caret::confusionMatrix(last_lr_fit %>%
                         collect_predictions() %>%
                         pull(.pred_class),
                       last_lr_fit %>%
                         collect_predictions() %>%
                         pull(low60ahc),
                       mode = "everything",
                       positive = "1")

## Hosmer-Lemeshow Goodness of Fit Test ----

### Raw predictions ----

glm_fit <- last_lr_fit %>% 
  collect_predictions() %>%
  mutate(low60ahc = if_else(low60ahc == 1, 1, 0))

hl_test <- ResourceSelection::hoslem.test(glm_fit$low60ahc, glm_fit$.pred_1)

hl_test

## Plot
hl_test$observed %>%
  bind_cols(hl_test$expected) %>%
  ggplot(aes(x = y1, y = yhat1)) +
  geom_point() +
  geom_abline() + 
  coord_equal()+
  labs(x = "Proportion in poverty (%)",
       y = "Predicted proportion in poverty (%)",
       title = paste0("Hosmer and Lemeshow test p-value: ", round(hl_test$p.value, 3))) + 
  theme_bw()

ggsave("Outputs/HL_test.png", width = 6*4/3, height= 6)

# final model ----

## final fit ----
final_fit <- last_lr_mod %>% 
  fit(model_formula, data = frs)
  
## extract final tidy model ----
final_glm_categorical_tidy <- final_fit %>%
  broom::tidy()

## save the final tidy fit ----
write.csv(final_glm_categorical_tidy,
          paste0(output_directory, "/final_glm_categorical_tidy.csv"),
          row.names = FALSE)

# end ----