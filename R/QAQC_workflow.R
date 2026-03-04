#install.packages("EDIutils")
#install.packages("xml2")
#install.packages("here")
#install.packages("hms")
library(tidyverse)
library(EDIutils)
library(xml2)
library(lubridate)
library(hms)
library(here)


home_directory <- here::here()
setwd(home_directory)


## pull in QAQC function from script stored on Github -- use for now, but want to use EDI pull eventually
source('https://raw.githubusercontent.com/CareyLabVT/Reservoirs/master/Scripts/L1_functions/eddy_flux_create.R')

## Comment out until we have an EDI file
## identify latest date for data on EDI (need to add one (+1) to both dates because we want to exclude all possible start_day data and include all possible data for end_day)
# package_ID <- 'edi.1061.5'
# eml <- read_metadata(package_ID)
# date_attribute <- xml_find_all(eml, xpath = ".//temporalCoverage/rangeOfDates/endDate/calendarDate")
# last_edi_date <- as.Date(xml_text(date_attribute)) + days(1)

last_edi_date <- as.Date("2026-01-01")

day_of_run <- Sys.Date() + days(1)

## assign data files 
dir <- home_directory
gshared <- NULL
outfile <-'CCRE_EddyFlux_streaming_L1.csv'

## run QAQC on the data within github
eddypro_cleaning_function(directory = dir, text_file = T, gdrive=F, gshared_drive = gshared, output_file = outfile, start_date = last_edi_date,  end_date = day_of_run)

#wq_qaqc <- read_csv('fcre-waterquality_L1.csv')

## convert all flag columns from numeric to factor data type -- this is also done inside of the function. Needs to be called at FLARE run time in the future, unless the flag columns are changed
# wq_qaqc <- wq_qaqc %>%
#   mutate(across(starts_with("Flag"),
#                 ~ as.factor(as.character(.))))
