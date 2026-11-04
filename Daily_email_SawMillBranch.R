# Title: Read in json files from S3 and make a plot for daily email
# Desciption: This file takes the files off the S3 bucket, unnests them to create a data frame and does this for all files on the S3 bucket. Then it creates a pdf with plots of the data. There is still more work to add other QAQC steps. 
# Authors: A. Breef-Pilz 
# Created: 4 Nov 2026
# AI Statement: I used VT ARC LLMS to create the functions to unnest the json file and get the paths for the files on S3 and make a data table

# read in the data packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(arrow, lubridate, tidyverse,furrr,purrr, data.table)

# AWS access keys

#Sys.setenv("AWS_ACCESS_KEY_ID" = Sys.getenv('MW_ACCESS_KEY_ID'),
#           "AWS_SECRET_ACCESS_KEY" = Sys.getenv('MW_ACCESS_SECRET'))

duckdbfs::duckdb_secrets(
  key = Sys.getenv("AWS_ACCESS_KEY_ID"),
  secret = Sys.getenv("AWS_SECRET_ACCESS_KEY"))

# Name the bucket, region, and column headers for the file
BUCKET <- "cef-carvins-streams"
REGION <- "us-east-2"
EXPECTED <- c("BattV", "PTemp_C", "AirTemp_C", "Lvl_psi", "PTwtr_C")

# function to unnest the json file

parse_one <- function(path) {
  tab <- arrow::read_json_arrow(path) |> collect()
  
  stopifnot(nrow(tab) == 1L)                     # one document per file
  
  flds <- tab$head$fields[[1]]$name
  dat  <- tab$data[[1]]
  
  if (nrow(dat) == 0L) stop("no data records")
  
  if (any(lengths(dat$vals) != length(flds))) {
    stop("vals/fields length mismatch: ",
         paste(unique(lengths(dat$vals)), collapse = "/"),
         " values vs ", length(flds), " fields")
  }
  if (!identical(flds, EXPECTED)) {
    warning("field set changed: ", paste(flds, collapse = ", "), call. = FALSE)
  }
 #if (!inherits(dat$time, "POSIXct")) {
  dat$time <- as.POSIXct(dat$time, format = "%Y-%m-%dT%H:%M:%S", tz = "EST") + lubridate::hours(5) # time is 5 hours behind when files are converted from json in r
 #}
  
  out <- dat |>
    mutate(vals = map(vals, ~ set_names(.x, flds))) |>
    unnest_longer(vals, values_to = "value") |>
    rename(variable = value_id)
  
  # env <- tab$head$environment[[1]]
  # out$station_name <- env
  # out$table_name   <- env$table_name
  # out$prog_name    <- tab$head$environment$prog_name
  # out$serial_no    <- tab$head$environment$serial_no
  # out$source_key   <- label
  
  # out[, c("time", "variable", "value", "station_name",
  #         "table_name", "prog_name", "serial_no", "source_key")]
}

fetch_parse <- function(key) {
  bucket <- arrow::s3_bucket(BUCKET, region = REGION)   # per worker
  path   <- bucket$path(key)
  tryCatch(
    parse_one(path),
    error = function(e) {
      warning("skipped ", key, ": ", conditionMessage(e), call. = FALSE)
      NULL
    }
  )
}

# now read in the files from the S3 bucket

key <- arrow::s3_bucket("cef-carvins-streams", region = "us-east-2")$ls("/CCR_301_SawMill/WaterLevel/", recursive = T)|>
   str_subset("PUT") 

plan(multisession, workers = 8)

# use the function and future_map to read in the json file and then deconstruct it so they can be combined together. 
parts <- future_map(
  key,
  fetch_parse,
  .progress = TRUE,
  .options  = furrr_options(seed = TRUE, chunk_size = 5)
)

plan(sequential)

# bind together to make a data frame
dt <- rbindlist(compact(parts), use.names = TRUE, fill = TRUE)

# add labels such as Reservoir and Site

final_df <- dt|>
  pivot_wider(names_from = variable, values_from = value)|>
  mutate(Reservoir = "CCR",
         Site = "301") |>
  dplyr:: rename(DateTime = time)|>
  select(Reservoir, Site, everything())

# save as a csv for now


# Add in the discharge. Figure out how to get that from the rating curve

# Write to a parquet file

# now figure out how to read in the parquet and add the new json files. 

# for now just using the file to make a pdf of the data for the daily email. This will be a seperate script later that just reads in the data to make the plots. 

# Taken from the FCR weir plots. 

end.time2 <- with_tz(as.POSIXct(strptime(Sys.time(), format = "%Y-%m-%d %H")), tzone = "EST") #gives us current time with rounded minutes in EDT
start.time2 <- end.time2 - days(7) #to give us seven days of data for looking at changes
full_time2 <- as.data.frame(seq(start.time2, end.time2, by = "15 min")) #create sequence of dates from past 7 days to fill in data and make it a dataframe
colnames(full_time2)=c("DateTime") #make it a data frame to merge to make obs1 later


#final_df$DateTime <- as.POSIXct(strptime(final_df$DateTime, "%Y-%m-%d %H:%M"), tz = "EST") #get dates aligned

if (length(na.omit(weirdata$TIMESTAMP[weirdata$TIMESTAMP>start.time2]))<2) { #if there is no data after start time, then a pdf will be made explaining this
  pdf(paste0("SawMillBranchDataFigures_", Sys.Date(), ".pdf"), width=8.5, height=11) #call PDF file
  plot(NA, xlim=c(0,5), ylim=c(0,5), bty='n',xaxt='n', yaxt='n', xlab='', ylab='') #creates empty plot
  mtext(paste("No data found between", start.time2, "and", end.time2, sep = " ")) #fills in text in top margin of plot
  dev.off() #file made!
  print("FCR weir file made with no data")
} else { #else, do normal data wrangling and plotting
 
  obs2 <- merge(full_time2,final_df, all.x=TRUE)#merge the data frame to get the last 7 days
  
  
  pdf(paste0("SawMillBranchDataFigures_", Sys.Date(), ".pdf"), width=8.5, height=11) #call PDF file
  par(mfrow=c(3,2))
  plot(obs2$DateTime,obs2$BattV, main="Battery", xlab="Time", ylab="Volts", type='l')
  if(min(tail(na.omit(obs2$BattV)))<11.5){
    mtext("Battery Charge Low", side = 3, col="red")
  }
  plot(obs2$DateTime,obs2$AirTemp_C, main="Air Temp", xlab="Time", ylab="degrees C", type='l')
  plot(obs2$DateTime,obs2$Lvl_psi, main="Water Level", xlab="Time", ylab="psi", type='l')
  plot(obs2$DateTime,obs2$PTwtr_C, main="Water Temp", xlab="Time", ylab="degrees C", type='l')
 # plot(obs2$DateTime,obs2$flowcms, main="Flow Rate", xlab="Time", ylab="cms", type='l')
  dev.off() #file made!
  print("Saw Mill Branch file made with data")
}
