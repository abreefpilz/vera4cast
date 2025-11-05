target_generation_ThermistorTemp_C_daily <- function(current_file, historic_file){
  source('targets/target_functions/find_depths.R')
  ## read in current data file
  # Github, Googlesheet, etc.
  current_df <- readr::read_csv(current_file, show_col_types = F) |>
    dplyr::filter(Site == 50) |>
    dplyr::select(Reservoir, DateTime,
                  dplyr::starts_with('ThermistorTemp'))

  if (current_df$Reservoir[1] == 'BVR') {
    bvr_depths <- find_depths(data_file = current_file,
                                  depth_offset = "https://raw.githubusercontent.com/FLARE-forecast/BVRE-data/bvre-platform-data-qaqc/BVR_Depth_offsets.csv",
                                  output <- NULL) |>
      dplyr::filter(variable == 'ThermistorTemp') |>
      dplyr::select(Reservoir, DateTime, variable, depth_bin, Position)

    current_df_1 <- current_df  |>
      tidyr::pivot_longer(cols = starts_with('ThermistorTemp'),
                          names_to = c('variable','Position'),
                          names_sep = '_C_',
                          values_to = 'observation') |>
      dplyr::mutate(date = lubridate::as_date(DateTime),
                    Position = as.numeric(Position)) |>
      na.omit() |>
      dplyr::left_join(bvr_depths,
                       by = c('Position', 'DateTime', 'Reservoir', 'variable')) |>
      dplyr::group_by(date, Reservoir, depth_bin) |>
      dplyr::summarise(observation = mean(observation, na.rm = T),
                       n = dplyr::n(),
                       .groups = 'drop') |>
      dplyr::mutate(observation = ifelse(n < 144/3, NA, observation), # 144 = 24(hrs) * 6(10 minute intervals/hr)
                    Reservoir = 'bvre') |>

      dplyr::rename(site_id = Reservoir,
                    datetime = date,
                    depth = depth_bin) |>
      dplyr::select(-n)
  }

  # read in differently for FCR
  if (current_df$Reservoir[1] == 'FCR') {
    current_df_1 <- current_df |>
      tidyr::pivot_longer(cols = starts_with('ThermistorTemp'),
                          names_to = 'depth',
                          names_prefix = 'ThermistorTemp_C_',
                          values_to = 'observation') |>
      dplyr::mutate(Reservoir = ifelse(Reservoir == 'FCR',
                                       'fcre',
                                       ifelse(Reservoir == 'BVR',
                                              'bvre', NA)),
                    date = lubridate::as_date(DateTime)) |>
      na.omit() |>
      dplyr::group_by(date, Reservoir, depth) |>
      dplyr::summarise(observation = mean(observation, na.rm = T),
                       n = dplyr::n(),
                       .groups = 'drop') |>
      dplyr::mutate(observation = ifelse(n < 144/2, NA, observation)) |> # 144 = 24(hrs) * 6(10 minute intervals/hr)
      dplyr::rename(site_id = Reservoir,
                    datetime = date) |>
      dplyr::select(-n)
  }
  message('Current file ready')
  print(nrow(current_df_1))

  # read in historical data file
  # EDI
  infile <- tempfile()
  try(download.file(historic_file, infile, method="curl"))
  if (is.na(file.size(infile))) download.file(historic_file,infile,method="auto")

  historic_df <- readr::read_csv(infile, show_col_types = F) |>
    dplyr::filter(Site == 50) |>
    dplyr::select(Reservoir, DateTime,
                  dplyr::starts_with('ThermistorTemp'))

  # Extract depths for BVR
  if (historic_df$Reservoir[1] == 'BVR') {
    bvr_depths <- find_depths(data_file = historic_file,
                                  depth_offset = "https://raw.githubusercontent.com/FLARE-forecast/BVRE-data/bvre-platform-data-qaqc/BVR_Depth_offsets.csv",
                                  output <- NULL) |>
      dplyr::filter(variable == 'ThermistorTemp') |>
      dplyr::select(Reservoir, DateTime, variable, depth_bin, Position)

    historic_df_1 <- historic_df |>
      tidyr::pivot_longer(cols = starts_with('ThermistorTemp'),
                          names_to = c('variable','Position'),
                          names_sep = '_C_',
                          values_to = 'observation') |>
      dplyr::mutate(date = lubridate::as_date(DateTime),
                    Position = as.numeric(Position)) |>
      na.omit() |>
      dplyr::left_join(bvr_depths,
                       by = c('Position', 'DateTime', 'Reservoir', 'variable')) |>
      dplyr::group_by(date, Reservoir, depth_bin) |>
      dplyr::summarise(observation = mean(observation, na.rm = T),
                       n = dplyr::n(),
                       .groups = 'drop') |>
      dplyr::mutate(observation = ifelse(n < 144/3, NA, observation), # 144 = 24(hrs) * 6(10 minute intervals/hr)
                    Reservoir = 'bvre') |>

      dplyr::rename(site_id = Reservoir,
                    datetime = date,
                    depth = depth_bin) |>
      dplyr::select(-n)
  }

  if (historic_df$Reservoir[1] == 'FCR') {
    historic_df_1 <- historic_df |>
      tidyr::pivot_longer(cols = starts_with('ThermistorTemp'),
                          names_to = 'depth',
                          names_prefix = 'ThermistorTemp_C_',
                          values_to = 'observation') |>
      dplyr::mutate(Reservoir = ifelse(Reservoir == 'FCR',
                                       'fcre',
                                       ifelse(Reservoir == 'BVR',
                                              'bvre', NA)),
                    date = lubridate::as_date(DateTime)) |>
      dplyr::group_by(date, Reservoir, depth)  |>
      dplyr::summarise(observation = mean(observation, na.rm = T),
                       n = dplyr::n(),
                       .groups = 'drop') |>
      dplyr::mutate(observation = ifelse(n < 6/2, NA, observation)) |> # 6 = 6(10 minute intervals/hr)
      dplyr::rename(site_id = Reservoir,
                    datetime = date)|>
      dplyr::select(-n)
  }

  message('EDI file ready')

  ## manipulate the data files to match each other


  ## bind the two files using row.bind()
  final_df <- dplyr::bind_rows(historic_df_1, current_df_1) |>
    dplyr::mutate(variable = 'Temp_C_mean',
                  depth = as.numeric(ifelse(depth == "surface", 0.1, depth))) |>
    rename(depth_m = depth)

  final_df <- final_df |>
    mutate(observation = ifelse(is.nan(observation), NA, observation)) |>
    drop_na(depth_m)
  ## Match data to flare targets file
  # Use pivot_longer to create a long-format table
  # for time specific - use midnight UTC values for daily
  # for hourly

  ## return dataframe formatted to match FLARE targets
  return(final_df)
}

#a <- target_generation_ThermistorTemp_C_daily(current_file = latest, historic_file = edi)
