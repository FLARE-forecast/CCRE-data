#install.packages("EDIutils")
#install.packages("xml2")
#install.packages("here")
library(tidyverse)
library(EDIutils)
library(xml2)
library(lubridate)

#install.packages('suncalc')
library(suncalc)


home_directory <- here::here()
setwd(home_directory)

### pull in QAQC function directly from EDI -- keep for now, but will need to update the function
#source('https://portal-s.edirepository.org/nis/dataviewer?packageid=edi.157.25&entityid=0531eb02833855f84d0b3b54c41ec61e')

## pull in QAQC function from script stored on Github -- use for now, but want to use EDI pull eventually
source('R/met_qaqc_function.R')

## identify latest date for data on EDI (need to add one (+1) to both dates because we want to exclude all possible start_day data and include all possible data for end_day)
package_ID <- 'edi.1105.4' # published id
#package_ID <- 'edi.1105.1'
eml <- read_metadata(package_ID) # no longer in staging
#eml <- read_metadata(package_ID, env = 'staging') ## change once out of staging
date_attribute <- xml_find_all(eml, xpath = ".//temporalCoverage/rangeOfDates/endDate/calendarDate")
last_edi_date <- as.Date(xml_text(date_attribute)) + lubridate::days(1)

day_of_run <- Sys.Date() + lubridate::days(1)

## assign data files 
met_data <- c('https://raw.githubusercontent.com/FLARE-forecast/CCRE-data/ccre-dam-data/ccre-met.csv', 
              'https://raw.githubusercontent.com/CareyLabVT/ManualDownloadsSCCData/master/current_files/CCRMetstation_L1.csv')
maintenance_file <- 'CCRM_Maintenancelog_new.csv'
outfile <-'ccre_met_L1.csv'

## run QAQC on the data within github
qaqc_ccrmet(data_file = met_data, 
            maintenance_file = maintenance_file, 
            output_file = outfile, 
            start_date = last_edi_date,  
            end_date = day_of_run, 
            notes = FALSE)

#wq_qaqc <- read_csv('ccre-waterquality_L1.csv')

## convert all flag columns from numeric to factor data type -- this is also done inside of the function. Needs to be called at FLARE run time in the future, unless the flag columns are changed
# wq_qaqc <- wq_qaqc %>%
#   mutate(across(starts_with("Flag"),
#                 ~ as.factor(as.character(.))))
