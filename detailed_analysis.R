require("tidyr")
require("dplyr")

#Tidy up environment first.
rm(list = ls())

#Import the source file of interest and put all of the data into a single data frame subset by sensors
import_data <- function(data_file, sensors) {
  #Set column names explicitly as the column headings in the source are not what we need
  column_names <- c("sensor", "value_type", "time_stamp", "value")
  #Import data, skipping header rows and without factors for now, so that the data types can be more easily converted
  source_data <- read.delim(paste("data/", data_file, sep = ""), sep="|", skip = 2, col.names = column_names, header = FALSE, stringsAsFactors = FALSE, strip.white = TRUE) 
  #Delete last row if the values are null.
  nrow_source_data <- nrow(source_data)
  if (source_data[nrow_source_data, 2] == "") {
    source_data <- source_data[-nrow_source_data,]
  }
  #Remove spaces from meter/sensor names
  source_data$sensor <- gsub(" ", "_", source_data$sensor)

  #Subset to specific sensors
  source_data <- source_data[source_data$sensor %in% sensors,]

  #Convert time_stamp milliseconds to a POSIXct  
  source_data$time_stamp <- as.POSIXct(source_data$time_stamp, tz = "UTC", format = "%Y-%m-%d %H:%M:%OS", usetz = TRUE)
  #Sort source_data by time_stamp
  source_data <- source_data[order(source_data$time_stamp),]
  
  return (source_data)
}

get_value_types <- function(source_data) {
  value_types <- unique(source_data$value_type)
}

#Subset the data to a range of observations for a date range that includes
#complete observations for all variables
subset_to_common_data_range <- function(source_data, value_types) {
  low_time_stamp <- -Inf
  high_time_stamp <- Inf
  for (value_type in value_types) {
    #A subset of data for that value type
    value_type_time_stamp_df <- source_data[source_data$value_type == value_type, "time_stamp"]
    if (value_type_time_stamp_df[1] > low_time_stamp) {
      low_time_stamp <- value_type_time_stamp_df[1]
    }
    if (value_type_time_stamp_df[length(value_type_time_stamp_df)] < high_time_stamp) {
      high_time_stamp <- value_type_time_stamp_df[length(value_type_time_stamp_df)]
    }
  }
  source_data <- source_data[source_data$time_stamp >= low_time_stamp & source_data$time_stamp <= high_time_stamp,]
  
  return(source_data)
}

#Convert the data from a long format to a wide format, and put the time_stamp first
long_to_wide <- function(source_data) {
  wide_source_data <- spread(source_data, value_type, value)
  #Put the time_stamp column first
  col_idx <- grep("time_stamp", names(wide_source_data))
  wide_source_data <- wide_source_data[, c(col_idx, (1:ncol(wide_source_data))[-col_idx])]
  
  return(wide_source_data)
}

#Check that, in the conversion from long to wide, we have not lost anything
validate_long_to_wide <- function(long_sensor_data, wide_sensor_data) {
  sensors <- unique(long_sensor_data$sensor)
  value_types <- unique(long_sensor_data$value_type)
  long_sums_vctr <- numeric(length(sensors) * length(value_types))
  wide_sums_vctr <- numeric(0)
  val_col <- 1
  for (sensor_name in sensors) {
    for (value_type_name in value_types) {
      long_sums_vctr[val_col] <- sum(long_sensor_data[long_sensor_data$sensor == sensor_name & long_sensor_data$value_type == value_type_name, "value"])
      val_col <- val_col + 1
    }
    wide_sums_vctr <- append(wide_sums_vctr,
                             summarise_each(wide_sensor_data[wide_sensor_data$sensor == sensor_name, 3:ncol(wide_sensor_data)], funs(sum(., na.rm = TRUE))))
  }
  stopifnot(long_sums_vctr == wide_sums_vctr)
}

aggregate_time_periods <- function(wide_source_data, sensors, value_types, time_periods) {
  #Create a matrix to hold sets of observations for different time buckets, with a column for each sensor and for the summary
  all_observations_mtrx <- matrix(list(), nrow = length(time_periods), ncol = length(sensors) * 2)
  #Create a set of time periods, in minutes, into which we want to gather the data.
  #Counting rows in the matrix of all observations for all time periods
  all_obs_row_counter <- 1
  col_names <- c("sensor", value_types)
  #Get the lowest and highest time_stamp values from the data (not assuming that the data is sorted)
  low_time_stamp <- min(wide_source_data$time_stamp)
  high_time_stamp <- max(wide_source_data$time_stamp)
  for (time_period in time_periods) {
    start_time <- Sys.time()
    #Create a data frame for all observations for all value types for a given time period, this form creates the required number of columns
    time_period_observations_df <- read.table(text="", col.names = col_names, stringsAsFactors = FALSE)
    #Make the value type columns numeric
    time_period_observations_df[, 2:(length(value_types)+1)] <- sapply(time_period_observations_df[, 2:(length(value_types)+1)], as.numeric)
    time_period_secs = time_period * 60
    #Record number of rows not included in aggregated data, as a sanity check.
    rows_not_incuded <- data.frame(sensor = character(0), skipped = numeric(0))
    for (sensor_name in sensors) {
      rows_not_incuded <- rbind(rows_not_incuded, data.frame(sensor = sensor_name, 
                                                             skipped = nrow(wide_source_data[wide_source_data$sensor == sensor_name,])))
      #Process all rows, getting subsets for the time period for each sensor
      #Go through the data selecting observations that fit into the time period 
      #Set the period to start at the low date
      period_start <- low_time_stamp
      while (period_start <= high_time_stamp) {
        #Subset to observations in time_period, note that the upper range is less than but not equal
        time_period_df <- subset(wide_source_data, wide_source_data$time_stamp >= period_start &
                                   wide_source_data$time_stamp < (period_start + time_period_secs) &
                                   sensor == sensor_name)
        #Remove the time_stamp and sensor columns so that we can calculate means
        time_period_df <- time_period_df[, 3:(length(value_types)+2)]
        #If there are values for the sensor in the time period, bind a row of the means of the columns
        if (nrow(time_period_df) > 0) {
          values_col_means <- colMeans(time_period_df, na.rm = TRUE)
          means_df <- data.frame(sensor = sensor_name, t(values_col_means), stringsAsFactors = FALSE)
          time_period_observations_df <- rbind(time_period_observations_df, means_df, stringsAsFactors = FALSE)
          #if we have processed rows, then subtract them from the rows_not_included
          rows_not_incuded[rows_not_incuded$sensor == sensor_name, 2] <- (rows_not_incuded[rows_not_incuded$sensor == sensor_name, 2])-nrow(time_period_df)
        }
        #Increment the bucket by the amount of seconds
        period_start <- period_start + time_period_secs
        }
      }
    #Store results for each sensor separately.
    #Each sensor has two columns in all_observations_mtrx: one for the results and one for the summary
    sensor_col <- 1
    for (sensor_name in sensors) {
      #Capture the summary of the results in a data frame
      summary_df <- data.frame(sensor = as.character(),  
                               value_type = as.character(), 
                               period = as.numeric(), 
                               processing_time = as.numeric(), 
                               observations = as.numeric(), 
                               mean = as.numeric(),
                               skipped = as.numeric())
      sensor_subset_df <- subset(time_period_observations_df, sensor == sensor_name)
      #Put the results for a given sensor in the results matrix
      all_observations_mtrx[[all_obs_row_counter, sensor_col]] <- sensor_subset_df
      #Record results with a separate row for the mean of each value type
      #Note that the means will not all be the same, as the number of observations that fit into the time periods will vary depending on the period length.
      col_means <- colMeans(sensor_subset_df[2:(length(value_types)+1)], na.rm = TRUE)
      for (column in 1:length(col_means)) {
        summary_df <- rbind(summary_df, 
                                      data.frame(sensor = sensor_name,
                                                 value_type = names(col_means[column]), 
                                                 period = as.numeric(time_period), 
                                                 processing_time = as.numeric(Sys.time()-start_time, units="secs"), 
                                                 observations = as.numeric(nrow(sensor_subset_df)), 
                                                 mean = col_means[column], 
                                                 skipped = rows_not_incuded[rows_not_incuded$sensor == sensor_name, 2],
                                                 row.names = 2, stringsAsFactors = FALSE))
      }
        #Put the summary of the results in the results matrix
        all_observations_mtrx[[all_obs_row_counter, sensor_col+1]] <- summary_df
        sensor_col <- sensor_col + 2
    }
    all_obs_row_counter <- all_obs_row_counter +1
  }
  #Return the matrix of results
  return (all_observations_mtrx)
}

#Validate that the aggregated data has not lost anything.
validate_aggregate_time_periods <- function(wide_sensor_data, aggregated_time_period_observations_mtrx, time_periods) {
  #The end periods of the aggregated data are dependent on the whether a time_stamp fell within the time period,
  #so we need to check sums for the data for a period that will be consistent for all aggregated data sets.
  #We can do that by checking up to a period that is the maximum time period less than the maximum date
  max_time_period <- max(time_periods)
  #First we validate that the start time period is the same for all data frames in the results
  for (row in nrow(aggregated_time_period_observations_mtrx)) {
    col <- 1
    while (col < ncol(aggregated_time_period_observations_mtrx)) {
      observations_df <- aggregated_time_period_observations_mtrx[[row, col]] 
      low_dates <- 
      col <- col + 1
    }
  }
}

#Select the best linear models for the data sets in the matrix supplied
evaluate_linear_models <- function(all_observations_mtrx) {
  #Loop through the data frames in all_observations_mtrx to examine possible correlations
  models_results_df <- data.frame(sensor = as.character(), period = as.numeric(), model = as.character(), AR2 = as.numeric())
  for (obs_row in 1:nrow(all_observations_mtrx)) {
    sensor_col <- 1
    while(sensor_col < ncol(all_observations_mtrx)) {
      browser()
      observations_df <- all_observations_mtrx[[obs_row, sensor_col]]
      summary_df <- all_observations_mtrx[[obs_row, sensor_col+1]]
      sensor <- observations_df[1,1]
      #Remove sensor column
      observations_df <- observations_df[,2:ncol(observations_df)]
      best_model <- step(lm(data = observations_df, CO2 ~ .), trace=0)
      summary_best <- summary(best_model)
      models_results_df <- rbind(models_results_df, 
                                 data.frame(sensor = sensor, 
                                            period = as.numeric(summary_df[1, "period"]), 
                                            model = as.character(summary_best$call[2]), 
                                            AR2 = as.numeric(summary_best$adj.r.squared)))
      sensor_col <- sensor_col + 2
      }
    browser()
  }
  return(models_results_df)
}
# 
# #Use the same time buckets for the adjusted CO2 time_stamps so that we can compare like with like
# adjusted_co2_times <- time_buckets
# value_types <- colnames(model_df_mtrx)
# col_names <- c("time_stamp", value_types)
# #DEBUG
# #high_time_stamp <- low_time_stamp+(24*360)
# #Matrix to store the data frames for each time bucket, and the summary results for that data frame
# adjusted_co2_mtrx <- matrix(list(), nrow = length(adjusted_co2_times), ncol = 2)
# adjusted_co2_mtrx_row <- 1
# for (time in adjusted_co2_times) {
#   all_observations_co2_adjusted_df <- read.table(text="", col.names = col_names) 
#   col_counter <- 1
#   for (row in 1:nrow(model_df_mtrx)) {
#     while (col_counter <= length(value_types)) {
#       is_co2 <- (value_types[col_counter] == "CO2")
#       #A given type of observations is in a column
#       #A row represents a given sensor
#       observations <- model_df_mtrx[[row, col_counter]]
#       observations <- subset(observations, observations$time_stamp >= low_time_stamp & observations$time_stamp <= high_time_stamp)
#       #Add the observations to the data frame
#       num_obs <- nrow(all_observations_co2_adjusted_df)
#       obs_row <- 1
#       while (obs_row <= nrow(observations)) {
#         #Add time_stamp first
#         #Row index is calculated from the number of rows in the gathered observations plus one
#         time_stamp <- observations[obs_row, 1]
#         if (is_co2) {
#           time_stamp <- time_stamp - (as.numeric(time)*60)
#         }
#         value <- observations[obs_row, 2]
#         #If there is already a row with this time stamp, then add the value into that column for that row
#         time_stamp_row <- which(all_observations_co2_adjusted_df$time_stamp == time_stamp)[1]
#         if (!is.na(time_stamp_row)) {
#           all_observations_co2_adjusted_df[time_stamp_row, col_counter+1] <- value
#         }
#         #Else, create a new row
#         else {
#           all_observations_co2_adjusted_df[(num_obs <- num_obs + 1), 1] <- time_stamp
#           #Then add the value into the appropriate column
#           all_observations_co2_adjusted_df[num_obs, col_counter+1] <- value
#         }
#         obs_row <- obs_row + 1
#       }
#       col_counter <- col_counter +1
#     }
#   }
#   #Order by time_stamp
#   all_observations_co2_adjusted_df <- all_observations_co2_adjusted_df[order(all_observations_co2_adjusted_df$time_stamp),]
#   #Check colMeans and colSums as a reference point, skipping time_stamp
#   all_observations_co2_adjusted_results_df <- data.frame(col_means = colMeans(all_observations_co2_adjusted_df[, 2:ncol(all_observations_co2_adjusted_df)], na.rm = TRUE), 
#                                                          col_sums = colSums(all_observations_co2_adjusted_df[, 2:ncol(all_observations_co2_adjusted_df)], na.rm = TRUE))
#   #Add to the results matrix
#   adjusted_co2_mtrx[[adjusted_co2_mtrx_row, 1]] <- all_observations_co2_adjusted_df
#   adjusted_co2_mtrx[[adjusted_co2_mtrx_row, 2]] <- all_observations_co2_adjusted_results_df
#   adjusted_co2_mtrx_row <- adjusted_co2_mtrx_row + 1
# }
# 
# #Capture the results in a data frame
# bucket_processing_adjusted_co2_df <- data.frame(value_type = as.character(), 
#                                                 bucket = as.numeric(), 
#                                                 processing_time = as.numeric(), 
#                                                 observations = as.numeric(), 
#                                                 mean = as.numeric())
# #Create a matrix to hold sets of observations for different time buckets
# #The row is for time that the CO2 time frame has been adjusted by, the column is for the aggregated bucket the data is being organised into
# all_observations_adjusted_co2_mtrx <- matrix(list(), nrow = length(time_buckets), ncol = length(time_buckets))
# adjusted_co2_mtrx_row <- 1
# #Counting rows in the matrix of all observations for all time buckets
# all_obs_col_counter <- 1
# #The first time_bucket loop is to get each adjusted data set
# for (row_time_bucket in time_buckets) {
#   all_observations_co2_adjusted_df <- adjusted_co2_mtrx[[adjusted_co2_mtrx_row, 1]]
#   #The inner loop is for the aggregation of means
#   for (col_time_bucket in time_buckets) {
#     start_time <- Sys.time()
#     #Create a data frame for all observations for all value types for a given time bucket, this form creates the required number of columns
#     time_bucket_observations_df <- read.table(text="", col.names = value_types)    
#     time_bucket_secs = col_time_bucket * 60
#     #Set the bucket to start at the low date
#     bucket_start <- low_time_stamp
#     for (row in 1:nrow(all_observations_co2_adjusted_df)) {
#       #Go through the data selecting observations that fit into the time bucket 
#       while (bucket_start <= high_time_stamp) {
#         #Subset to observations in time_bucket, note that the upper range is less than but not equal
#         #Since we have moved CO2 back in time, we need to adjust the time window backwards also.
#         time_bucket_vt_df <- subset(all_observations_co2_adjusted_df, 
#                                     all_observations_co2_adjusted_df$time_stamp >= (bucket_start-time_bucket_secs) & 
#                                       all_observations_co2_adjusted_df$time_stamp < bucket_start)
#         #Remove the time_stamp column
#         time_bucket_vt_df <- time_bucket_vt_df[, 1:length(value_types)+1]
#         #If there are values in the time bucket, bind a row of the means of the columns
#         if (nrow(time_bucket_vt_df) > 0) {
#           time_bucket_observations_df <- rbind(time_bucket_observations_df, colMeans(time_bucket_vt_df, na.rm = TRUE))
#         }
#         #Increment the bucket by the amount of seconds
#         bucket_start <- bucket_start + time_bucket_secs
#       }
#     }
#     #Reset the column names which are changed with the rbind
#     colnames(time_bucket_observations_df) <- value_types
#     all_observations_adjusted_co2_mtrx[[adjusted_co2_mtrx_row, all_obs_col_counter]] <- time_bucket_observations_df
#     #Record results with a separate row for the mean of each value type
#     #Note that the means will not all be the same, as the number of observations that fit into the time buckets will vary depending on the bucket size.
#     col_means <- colMeans(time_bucket_observations_df, na.rm = TRUE)
#     for (column in 1:length(col_means)) {
#       bucket_processing_adjusted_co2_df <- rbind(bucket_processing_adjusted_co2_df, 
#                                                  data.frame(value_type = names(col_means[column]), 
#                                                             bucket = as.numeric(time_bucket), 
#                                                             processing_time = as.numeric(Sys.time()-start_time, units="secs"), 
#                                                             observations = as.numeric(nrow(time_bucket_observations_df)), 
#                                                             mean = col_means[column], row.names = 1))
#     }
#     all_obs_col_counter <- all_obs_col_counter + 1
#   }
#   all_obs_col_counter <- 1
#   adjusted_co2_mtrx_row <- adjusted_co2_mtrx_row + 1
# }
# 
# #Loop through the data frames in all_observations_mtrx to examine possible correlations
# correlations_results_adjusted_co2_df <- data.frame(adjust = as.numeric(), bucket = as.numeric(), model = as.character(), AR2 = as.numeric())
# for (obs_row in 1:nrow(all_observations_adjusted_co2_mtrx)) {
#   for (obs_col in 1:ncol(all_observations_adjusted_co2_mtrx)) {
#     observations_df <- all_observations_adjusted_co2_mtrx[[obs_row, obs_col]]
#     #Remove incomplete observations
#     observations_df <- na.omit(observations_df)
#     #Check that there are enough complete observations for the model to make sense
#     if (sum(complete.cases(observations_df)) > 100) {
#       best_model <- step(lm(data = observations_df, 
#                             CO2 ~ ., na.action = na.omit), trace=0)
#       summary_best <- summary(best_model)
#       correlations_results_adjusted_co2_df <- rbind(correlations_results_adjusted_co2_df,  
#                                                     data.frame(adjust = as.numeric(time_buckets[obs_col]), 
#                                                                bucket = as.numeric(time_buckets[obs_row]),  
#                                                                model = as.character(summary_best$call[2]), 
#                                                                AR2 = as.numeric(summary_best$adj.r.squared)))
#     }
#   }
# }
# #Sort results by AR2 to see the best fit
# correlations_results_adjusted_co2_df <- correlations_results_adjusted_co2_df[order(correlations_results_adjusted_co2_df$AR2),]
# ```