### function to manually apply coefficients from the tidy fit to the data
sdf_predict_from_tidy_fit <- function(data, new_col_name, tidy_fit) {
  
  ### Check that the terms in tidy_fit are columns in data
  if (sum(tidy_fit[tidy_fit[["term"]] != "(Intercept)",][["term"]] %in% colnames(data)) != nrow(tidy_fit[tidy_fit[["term"]] != "(Intercept)",])) {
    
    ### print error to console if not, need dummy versions of variables in the data
    stop('"data" does not contain columns that are terms in "tidy_fit", "data" must be in model matrix form')
    
  }
  
  ### Start with the intercept
  linear_prediction_sql_part_1 <- paste0(tidy_fit[tidy_fit[["term"]] == "(Intercept)",][["estimate"]])
  
  ### Filter out the intercept to make the iterations less complicated below
  tidy_fit_no_intercept <- filter(tidy_fit, term != "(Intercept)")
  
  ### Iterate through terms
  linear_prediction_sql_part_2 <- purrr::map_chr(1:nrow(tidy_fit_no_intercept), function(e) {
    
    paste0("(",
           tidy_fit_no_intercept[e,][["term"]],
           " * ",
           tidy_fit_no_intercept[e,][["estimate"]],
           ")")
    
  })
  
  ### Paste all into one long string to parse as an expression inside mutate
  linear_prediction_sql <- paste0(linear_prediction_sql_part_1,
                                  " + ",
                                  paste(linear_prediction_sql_part_2, collapse = " + "))
  
  ### Calculate the prediction
  data <- data %>%
    mutate(temp_linear_pred = sql(linear_prediction_sql)) %>%
    mutate(temp_linear_pred = as.numeric(temp_linear_pred),
           temp_1 = 1,
           temp_1 = as.numeric(temp_1),
           "{new_col_name}" := temp_1 / (temp_1 + exp((-temp_1) * temp_linear_pred))) %>%
    select(-temp_linear_pred, -temp_1)
  
}