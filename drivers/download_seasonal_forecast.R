
download_seasonal_forecast <- function(){

  s3 <- arrow::s3_bucket(bucket = "bio230121-bucket01/flare",
                         endpoint_override = "amnh1.osn.mghpcc.org",
                         access_key = Sys.getenv("OSN_KEY"),
                         secret_key = Sys.getenv("OSN_SECRET"))

  s3$CreateDir("drivers/met/ensemble_forecast")

  # s3 <- arrow::s3_bucket("bio230121-bucket01/flare/drivers/met/seasonal_forecast",
  #                        endpoint_override = "amnh1.osn.mghpcc.org",
  #                        access_key = Sys.getenv("OSN_KEY"),
  #                        secret_key = Sys.getenv("OSN_SECRET"))

  duckdbfs::duckdb_secrets(endpoint = "amnh1.osn.mghpcc.org", key = Sys.getenv("OSN_KEY"), secret = Sys.getenv("OSN_SECRET"), bucket = "bio230121-bucket01")

  site_list <- readr::read_csv("https://raw.githubusercontent.com/FLARE-forecast/aws_noaa/master/site_list_v2.csv", show_col_types = FALSE)

  site_list <- site_list |>
    dplyr::filter(site_id %in% c("BARC", "CRAM", "LIRO", "PRLA", "PRPO", "SUGG", "TOOK", "fcre", "bvre", "ccre", "sunp", "ALEX"))

   for(i in 1:nrow(site_list)){

    print(site_list$site_id[i])

    ropenmeteo::get_seasonal_forecast(
      latitude = site_list$latitude[i],
      longitude = site_list$longitude[i],
      site_id = site_list$site_id[i],
      forecast_days = 274,
      past_days = 92,
      variables = ropenmeteo::glm_variables(product = "seasonal_forecast",
                                            time_step = "6hourly")) |>
      dplyr::mutate(reference_date = lubridate::as_date(reference_datetime)) |>
      duckdbfs::write_dataset(path = 's3://bio230121-bucket01/flare/drivers/met/seasonal_forecast',
                              format = 'parquet',
                              partitioning = c("model_id", "reference_date", "site_id"))
      # arrow::write_dataset(s3, format = 'parquet',
      #                      partitioning = c("model_id", "reference_date", "site_id"))
    Sys.sleep(10)

  }
}
