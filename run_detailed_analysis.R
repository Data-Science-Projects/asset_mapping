debugSource("./detailed_analysis.R")

sensors <- c("London_70bf78cf", "London_9c850e0a")
data_file <- "2_sensors_sensor_type_1.sql.gz"
sensor_data <- import_data(data_file, sensors)
value_types <- get_value_types(sensor_data)
sensor_data <- subset_to_common_data_range(sensor_data, value_types)
wide_sensor_data <- long_to_wide(sensor_data)

validate_long_to_wide(sensor_data, wide_sensor_data) 

#Temporary hack
#wide_sensor_data[is.na(wide_sensor_data)] <- 0

#With a range of time periods we can analyse the sensitivity of the predictive capability of the model for different time buckets, in minutes
time_periods <- c(1, 3, 5, 10, 20, 40, 60, 120, 180, 240) 
#time_periods <- c(1)
aggregated_time_period_observations_mtrx <- aggregate_time_periods(wide_sensor_data, sensors, value_types, time_periods)

validate_aggregate_time_periods(wide_sensor_data, aggregated_time_period_observations_mtrx, time_periods)
  
model_results <- evaluate_linear_models(aggregated_time_period_observations_mtrx)
