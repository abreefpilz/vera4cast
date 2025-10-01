#devtools::install_version("duckdb", "1.2.2")
remotes::install_github('cboettig/duckdbfs', upgrade = 'never')

score4cast::ignore_sigpipe()

library(dplyr)
library(duckdbfs)
library(progress)
library(bench)

library(DBI)
con <- duckdbfs::cached_connection(tempfile())
DBI::dbExecute(con, "SET THREADS=64;")

#library(minioclient)

#install_mc()
#mc_alias_set("osn", "sdsc.osn.xsede.org", Sys.getenv("OSN_KEY"), Sys.getenv("OSN_SECRET"))
#fs::dir_create("new_scores")

project <- "vera4cast"

duckdbfs::duckdb_secrets(endpoint = "amnh1.osn.mghpcc.org",
                         key = "",
                         secret = "",
                         bucket = "bio230121-bucket01")


# Omit scoring of daily forecasts that have a horizon > 35
forecasts <-
  open_dataset("s3://bio230121-bucket01/vera4cast/forecasts/bundled-parquet/",
               s3_endpoint = "amnh1.osn.mghpcc.org",
               anonymous=TRUE) |>
  mutate(depth_m = as.numeric(depth_m)) |>
  filter(project_id == {project},
         !is.na(model_id),
         !is.na(parameter),
         !is.na(prediction))



## INSTEAD, we pull our subset to local disk first.
## This looks silly but is much better for RAM and speed!!
bench::bench_time({ # ~ 5.4m (w/ 6mo cutoff)
  forecasts |> group_by(variable) |> write_dataset("tmp/forecasts")
})


bench::bench_time({
  forecasts <- open_dataset("tmp/forecasts/**")
})

groups <- forecasts |>
    distinct(project_id, duration, variable, model_id) |> collect()

for (i in seq_along(row_number(groups))) {

    curr_forecast <- forecasts |>
      dplyr::inner_join(groups[i,], copy=TRUE, by = dplyr::join_by(project_id, duration, variable, model_id)) |>
      dplyr::collect()

    df <- curr_forecast |>
    dplyr::summarise(prediction = mean(prediction), .by = dplyr::any_of(c("site_id", "datetime", "reference_datetime",
                                                                          "family", "depth_m", "duration", "model_id",
                                                                          "parameter", "pub_datetime", "reference_date",
                                                                          "variable", "project_id"))) |>
    score4cast::summarize_forecast(extra_groups = c("duration", "project_id", "depth_m")) |>
    duckdbfs::write_dataset("s3://bio230121-bucket01/vera4cast/forecasts/bundled-summaries", format = 'parquet',
                            partitioning = c("project_id",
                                             "duration",
                                             "variable",
                                             "model_id"),
                            options = list("PER_THREAD_OUTPUT false"))

}
