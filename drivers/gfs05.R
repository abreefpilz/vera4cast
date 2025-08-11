library(arrow)
source("drivers/download_ensemble_forecast.R")

download_ensemble_forecast("gfs05")

## Call healthcheck
RCurl::url.exists("https://hc-ping.com/ebd60b2d-dda4-430f-8e93-f999bdd08695", timeout = 5)
