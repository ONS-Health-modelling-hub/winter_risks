### function to create dummy variables of factors
sdf_create_dummies <- function(data, input_col, ref = NULL, keep_orig = TRUE) {
  
  ### Get unique levels of a variable
  unique_levels <- data %>%
    sdf_distinct(input_col) %>%
    sdf_coalesce(1) %>%
    collect() %>%
    pull(all_of(input_col))
  
  ### Remove the reference level if it's supplied
  if (!missing(ref)) {
    
    unique_levels <- grep(x = unique_levels,
                          pattern = ref,
                          value = TRUE,
                          invert = TRUE)
    
  }
  
  ### Create a one hot encoded version of input_col per unique level
  for (unique_level in unique_levels) {
    
    data <- data %>%
      mutate(temp_to_rename = case_when(
        .data[[input_col]] == unique_level ~ 1,
        TRUE ~ 0)) %>%
      rename_with(function(e) {
        
        stringr::str_replace(string = e,
                             pattern = "temp_to_rename",
                             replacement = paste0(input_col, unique_level))
        
      })
    
  }
  
  ### drop the original column if instructed to
  if (keep_orig == FALSE) {
    
    data <- data %>%
      select(-all_of(input_col))
    
  }
  
  return(data)
  
}