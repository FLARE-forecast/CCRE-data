

qaqc_ccr <- function(data_file, maintenance_file, output_file, start_date, end_date)
{
  
  CATPRES_COL_NAMES = c("DateTime", "RECORD", "CR3000Battery_V", "CR3000Panel_Temp_C", 
                        "ThermistorTemp_C_1", "ThermistorTemp_C_2", "ThermistorTemp_C_3", "ThermistorTemp_C_4",
                        "ThermistorTemp_C_5", "ThermistorTemp_C_6", "ThermistorTemp_C_7", "ThermistorTemp_C_8",
                        "ThermistorTemp_C_9","ThermistorTemp_C_10","ThermistorTemp_C_11", "ThermistorTemp_C_12",
                        "ThermistorTemp_C_13","EXO_Date_1", "EXO_Time_1", "EXOTemp_C_1", "EXOCond_uScm_1",
                        "EXOSpCond_uScm_1", "EXOTDS_mgL_1", "EXODOsat_percent_1", "EXODO_mgL_1", "EXOChla_RFU_1",
                        "EXOChla_ugL_1", "EXOBGAPC_RFU_1", "EXOBGAPC_ugL_1", "EXOfDOM_RFU_1", "EXOfDOM_QSU_1",
                        "EXOPressure_psi_1", "EXODepth_m_1", "EXOBattery_V_1", "EXOCablepower_V_1", "EXOWiper_V_1",
                        "EXO_Date_9", "EXO_Time_9", "EXOTemp_C_9", "EXOCond_uScm_9",
                        "EXOSpCond_uScm_9", "EXOTDS_mgL_9", "EXODOsat_percent_9", "EXODO_mgL_9", 
                        "EXOfDOM_RFU_9", "EXOfDOM_QSU_9","EXOPressure_psi_9", "EXODepth_m_9", "EXOBattery_V_9",
                        "EXOCablepower_V_9", "EXOWiper_V_9","LvlPressure_psi_13", "LvlTemp_C_13")
  
  #Adjustment period of time to stabilization after cleaning in seconds
  ADJ_PERIOD = 2*60*60 
  
  # read catwalk data and maintenance log
  # NOTE: date-times throughout this script are processed as UTC
  ccrwater <- read_csv(data_file, skip = 1, col_names = CATPRES_COL_NAMES,
                       col_types = cols(.default = col_double(), DateTime = col_datetime()))
  
  log_read <- read_csv(maintenance_file, col_types = cols(
    .default = col_character(),
    TIMESTAMP_start = col_datetime("%Y-%m-%d %H:%M:%S%*"),
    TIMESTAMP_end = col_datetime("%Y-%m-%d %H:%M:%S%*"),
    flag = col_integer()
  ))
  
  log <- log_read
  
  ### identify the date subsetting for the data
  if (!is.null(start_date)){
    ccrwater <- ccrwater %>% 
      filter(DateTime >= start_date)
    log <- log %>% 
      filter(TIMESTAMP_start >= start_date)
  }
  
  if(!is.null(end_date)){
    ccrwater <- ccrwater %>% 
      filter(DateTime <= end_date)
    log <- log %>% 
      filter(TIMESTAMP_end <= end_date)
  }
  
  if (nrow(log) == 0){
    log <- log_read
  }
  
  # Drop NAs 
  ccrwater<-ccrwater%>%drop_na(DateTime)
  
  # Change NaN into NA
  ccrwater[sapply(ccrwater, is.nan)] <- NA
  
  
  #############Missing Data Check ###################################################################################################
  #check for gaps and missing data and see if you can fill them with manual downloads. If not make a note
  # of the missing dates and times in the methods. 
  #order data by timestamp
  ccr2=ccrwater
  ccr2=ccr2[order(ccr2$DateTime),]
  ccr2$DOY=yday(ccr2$DateTime)
  
  
  #check record for gaps
  #daily record gaps by day of year
  for(i in 2:nrow(ccr2)){ #this identifies if there are any data gaps in the long-term record, and where they are by record number
    if(ccr2$DOY[i]-ccr2$DOY[i-1]>1){
      print(c(ccr2$DateTime[i-1],ccr2$DateTime[i]))
    }
  }
  # #sub-daily record gaps by record number
  for(i in 2:length(ccr2$RECORD)){ #this identifies if there are any data gaps in the long-term record, and where they are by record number
    if(abs(ccr2$RECORD[i]-ccr2$RECORD[i-1])>1){
      print(c(ccr2$DateTime[i-1],ccr2$DateTime[i]))
    }
  }
  
  ##########QAQC ############################################ 
  
  # Begin QAQC steps 
  
  # remove NaN data at beginning when data when no sensors were connected to the data logger
  ccrwater <- ccrwater %>% filter(DateTime >= ymd_hms("2021-04-09 15:20:00"))
  
  # for loop to create flag columns
  for(j in c(5:17,20:36,39:53)) { #for loop to create new columns in data frame
    ccrwater[,paste0("Flag_",colnames(ccrwater[j]))] <- 0 #creates flag column + name of variable
    ccrwater[c(which(is.na(ccrwater[,j]))),paste0("Flag_",colnames(ccrwater[j]))] <-7 #puts in flag 7 if value not collected
  }
  
  # When values besides the water temp, and pressure are negative replace with 0 and flag
  # These are for values on the EXO
  for(k in c(21:31,40:46)) { #for loop to create new columns in data frame
    ccrwater[c(which((ccrwater[,k]<0))),paste0("Flag_",colnames(ccrwater[k]))] <- 3
    ccrwater[c(which((ccrwater[,k]<0))),k] <- 0 #replaces value with 0
  }
  
  
  # Take out duplicates if there are any duplicated DateTime stamp
  ccrwater=ccrwater[!duplicated(ccrwater$DateTime), ]
  
  
  ####### Take out values in Maintenance Log #########  
  #Read in the maintenance log 
  
  # log <- read_csv(maintenance_file, col_types = cols(
  #   .default = col_character(),
  #   TIMESTAMP_start = col_datetime("%Y-%m-%d %H:%M:%S%*"),
  #   TIMESTAMP_end = col_datetime("%Y-%m-%d %H:%M:%S%*"),
  #   flag = col_integer()
  # ))
  
  # modify ccrwater based on the information in the log
  for(i in 1:nrow(log))
    #for(i in 37)  
  {
    # get start and end time of one maintenance event
    start <- log$TIMESTAMP_start[i]
    end <- log$TIMESTAMP_end[i]
    
    
    # get indices of columns affected by maintenance
    if(grepl("^\\d+$", log$colnumber[i])) # single num
    {
      maintenance_cols <- intersect(c(5:53), as.integer(log$colnumber[i]))
    }
    else if(grepl("^c\\(\\s*\\d+\\s*(;\\s*\\d+\\s*)*\\)$", log$colnumber[i])) # c(x;y;...)
    {
      maintenance_cols <- intersect(c(5:53), as.integer(unlist(regmatches(log$colnumber[i],
                                                                          gregexpr("\\d+", log$colnumber[i])))))
    }
    else if(grepl("^c\\(\\s*\\d+\\s*:\\s*\\d+\\s*\\)$", log$colnumber[i])) # c(x:y)
    {
      bounds <- as.integer(unlist(regmatches(log$colnumber[i], gregexpr("\\d+", log$colnumber[i]))))
      maintenance_cols <- intersect(c(5:53), c(bounds[1]:bounds[2]))
    }
    else
    {
      warning(paste("Could not parse column colnumber in row", i, "of the maintenance log. Skipping maintenance for",
                    "that row. The value of colnumber should be in one of three formats: a single number (\"47\"), a",
                    "semicolon-separated list of numbers in c() (\"c(47;48;49)\"), or a range of numbers in c() (\"c(47:74)\").",
                    "Other values (even valid calls to c()) will not be parsed properly."))
      next
    }
    
    # remove EXO_Date and EXO_Time columns from the list of maintenance columns, because they will be deleted later
    maintenance_cols <- setdiff(maintenance_cols, c(18, 19))
    
    if(length(maintenance_cols) == 0)
    {
      warning(paste("Did not parse any valid data columns in row", i, "of the maintenance log. Valid columns have",
                    "indices 2 through 39, excluding 21 and 22, which are deleted by this script. Skipping maintenance for that row."))
      next
    }
    #index the Flag columns
    if(grepl("^\\d+$", log$flagcol[i])) # single num
    {
      flag_cols <- intersect(c(54:98), as.integer(log$flagcol[i]))
      
    }
    else if(grepl("^c\\(\\s*\\d+\\s*(;\\s*\\d+\\s*)*\\)$", log$flagcol[i])) # c(x;y;...)
    {
      flag_cols <- intersect(c(54:98), as.integer(unlist(regmatches(log$flagcol[i],
                                                                    gregexpr("\\d+", log$flagcol[i])))))
    }
    
    else if(grepl("^c\\(\\s*\\d+\\s*:\\s*\\d+\\s*\\)$", log$flagcol[i])) # c(x:y)
    {
      bounds_flag <- as.integer(unlist(regmatches(log$flagcol[i], gregexpr("\\d+", log$flagcol[i]))))
      flag_cols <- intersect(c(54:98), c(bounds_flag[1]:bounds_flag[2]))
    }
    else
    {
      warning(paste("Could not parse column flagcol in row", i, "of the maintenance log. Skipping maintenance for",
                    "that row. The value of colnumber should be in one of three formats: a single number (\"47\"), a",
                    "semicolon-separated list of numbers in c() (\"c(47;48;49)\"), or a range of numbers in c() (\"c(47:74)\").",
                    "Other values (even valid calls to c()) will not be parsed properly."))
      next
    }
    
    #Get the Maintenance Flag 
    
    flag <- log$flag[i]
    
    # replace relevant data with NAs and set flags while maintenance was in effect
    if(flag==5){ #Flag 5 is a questionable value so values are just flagged and not removed
      ccrwater[ccrwater$DateTime >= start & ccrwater$DateTime <= end, flag_cols] <- flag
    }else {
      ccrwater[ccrwater$DateTime >= start & ccrwater$DateTime <= end, maintenance_cols] <- NA
      ccrwater[ccrwater$DateTime >= start & ccrwater$DateTime <= end, flag_cols] <- flag
    }
    #Add the 2 hour adjustment after DO sensor is out of the water
    if (log$colnumber[i]=="c(5:53)" && flag==1){
      DO_col=c("EXODOsat_percent_1", "EXODO_mgL_1","EXODOsat_percent_9", "EXODO_mgL_9")
      DO_flag_col=c("Flag_EXODOsat_percent_1", "Flag_EXODO_mgL_1","Flag_EXODOsat_percent_9", "Flag_EXODO_mgL_9")
      ccrwater[ccrwater$DateTime>start&ccrwater$DateTime<end+ADJ_PERIOD,DO_col] <- NA
      ccrwater[ccrwater$DateTime>start&ccrwater$DateTime<end+ADJ_PERIOD,DO_flag_col] <- flag
    }
    else if(log$colnumber[i] %in% c("c(20:36)","24","25") && flag==1){
      DO_col=c("EXODOsat_percent_1", "EXODO_mgL_1")
      DO_flag_col=c("Flag_EXODOsat_percent_1", "Flag_EXODO_mgL_1")
      ccrwater[ccrwater$DateTime>start&ccrwater$DateTime<end+ADJ_PERIOD,DO_col] <- NA
      ccrwater[ccrwater$DateTime>start&ccrwater$DateTime<end+ADJ_PERIOD,DO_flag_col] <- flag
    }
    else if(log$colnumber[i] %in% c("c(39:51)","43","44") && flag==1){
      DO_col=c("EXODOsat_percent_9", "EXODO_mgL_9")
      DO_flag_col=c("Flag_EXODOsat_percent_9", "Flag_EXODO_mgL_9")
      ccrwater[ccrwater$DateTime>start&ccrwater$DateTime<end+ADJ_PERIOD,DO_col] <- NA
      ccrwater[ccrwater$DateTime>start&ccrwater$DateTime<end+ADJ_PERIOD,DO_flag_col] <- flag
    }
    else if (log$colnumber[i] == "c(20:51)" && flag==1){
      DO_col=c("EXODOsat_percent_1", "EXODO_mgL_1","EXODOsat_percent_9", "EXODO_mgL_9")
      DO_flag_col=c("Flag_EXODOsat_percent_1", "Flag_EXODO_mgL_1","Flag_EXODOsat_percent_9", "Flag_EXODO_mgL_9")
      ccrwater[ccrwater$DateTime>start&ccrwater$DateTime<end+ADJ_PERIOD,DO_col] <- NA
      ccrwater[ccrwater$DateTime>start&ccrwater$DateTime<end+ADJ_PERIOD,DO_flag_col] <-flag
    }
    else{
      warning(paste("No DO time to adjust in row",i,"."))
    }
  }
  
  ############## Remove and Flag when sensor is out of position ####################
  
  #change EXO_1 at 1.5m values to NA if EXO depth is less than 0.3m and Flag as 2
  
  #index only the colummns with EXO at the beginning
  exo_idx <-grep("^EXO.*_1$",colnames(ccrwater))
  
  #create list of the Flag columns that need to be changed to 2
  exo_flag <- grep("^Flag_EXO.*_1$",colnames(ccrwater))
  
  #Change the EXO data to NAs when the EXO is above 0.5m and not due to maintenance
  ccrwater[which(ccrwater$EXODepth_m_1 < 1), exo_idx] <- NA
  #Flag the data that was removed with 2 for outliers
  ccrwater[which(ccrwater$EXODepth_m_1< 1),exo_flag]<- 2
  
  #index only the colummns with EXO at the beginning
  exo_idx9 <-grep("^EXO.*_9$",colnames(ccrwater))
  
  exo_flag9 <-grep("^Flag_EXO.*_9$",colnames(ccrwater))
  
  #create list of the Flag columns that need to be changed to 2
  #exo_flag9 <- c("Flag_EXOTemp_9", "Flag_EXOCond_9","Flag_EXOSpCond_9","Flag_EXOTDS_9",'Flag_EXODO_obs_9',
  #               "Flag_EXODO_sat_9",'Flag_EXOfDOM_9',"Flag_EXOPres_9",
  #               "Flag_EXObat_9","Flag_EXOcab_9","Flag_EXOwip_9")
  
  
  #Change the EXO data to NAs when the EXO is above 6m and not due to maintenance
  ccrwater[which(ccrwater$EXODepth_m_9 < 7), exo_idx9] <- NA
  #Flag the data that was removed with 2 for outliers
  ccrwater[which(ccrwater$EXODepth_m_9 < 7),exo_flag9]<- 2
  
  ############## Leading and Lagging QAQC ##########################
  # This finds the point that is way out of range from the leading and lagging point 
  
  # loops through all of the columns to catch values that are above 2 or 4 sd above or below
  # the leading or lagging point 
  
  # need to make it a data frame before
  
  ccrwater=data.frame(ccrwater)
  
  for (a in c(5:17,20:36,39:53)){
    Var_mean <- mean(ccrwater[,a], na.rm = TRUE)
    
    # For Algae sensors we use 4 sd as a threshold but for the others we use 2
    if (colnames(ccrwater[a]) %in% c("EXOChla_RFU_1","EXOChla_ugL_1","EXOBGAPC_RFU_1","EXOBGAPC_ugL_1")){
      Var_threshold <- 4 * sd(ccrwater[,a], na.rm = TRUE)
    }else{ # all other variables we use 2 sd as a threshold
      Var_threshold <- 2 * sd(ccrwater[,a], na.rm = TRUE)
    }
    # Create the observation column, the lagging column and the leading column
    ccrwater$Var <- lag(ccrwater[,a], 0)
    ccrwater$Var_lag = lag(ccrwater[,a], 1)
    ccrwater$Var_lead = lead(ccrwater[,a], 1)
    
    # Replace the observations that are above the threshold with NA and then put a flag in the flag column
    
    ccrwater[c(which((abs(ccrwater$Var_lag - ccrwater$Var) > Var_threshold) &
                       (abs(ccrwater$Var_lead - ccrwater$Var) > Var_threshold)&!is.na(ccrwater$Var))) ,a] <-NA
    
    ccrwater[c(which((abs(ccrwater$Var_lag - ccrwater$Var) > Var_threshold) &
                       (abs(ccrwater$Var_lead - ccrwater$Var) > Var_threshold)&!is.na(ccrwater$Var))) ,paste0("Flag_",colnames(ccrwater[a]))]<-2
  }
  
  
  # Remove the leading and lagging columns
  
  ccrwater<-ccrwater%>%select(-c(Var, Var_lag, Var_lead))
  
  
  ####Add a depth column and take out observations when sensor is in the air#######################################   
  #Convert the PSI of the pressure sensor to meters. 1psi=2.31ft, 1ft=0.305m
  ccrwater=ccrwater%>%mutate(LvlDepth_m_13=(LvlPressure_psi_13*0.70455) )
  
  # The distance of the sesnor away from the pressure sensor
  ccrwater=ccrwater%>%
    mutate(depth_1=LvlDepth_m_13-19.08)%>%
    mutate(depth_2=LvlDepth_m_13-18.125)%>%
    mutate(depth_3=LvlDepth_m_13-17.07)
  
  
  # For loop to set the values to NA when the thermistor is out of the water
  for(b in c(100:102)){
    if (colnames(ccrwater[b])=="depth_1"){
      d="ThermistorTemp_C_1"
    } else if (colnames(ccrwater[b])=="depth_2"){
      d="ThermistorTemp_C_2"
    }else if (colnames(ccrwater[b])=="depth_3"){
      d="ThermistorTemp_C_3"
    }
    
    ccrwater[c(which(!is.na(ccrwater[,b])& ccrwater[,b]<0)),paste0("Flag_",d)]<-2
    ccrwater[c(which(!is.na(ccrwater[,b])& ccrwater[,b]<0)),d]<-NA
    
  }
  
  
  #############################################################################################################################  
  # delete EXO_Date and EXO_Time columns and depth column used above
  ccrwater <- ccrwater %>% select(-EXO_Date_1,-EXO_Date_9,-EXO_Time_1,-EXO_Time_9,-depth_1,-depth_2, -depth_3)
  
  # add Reservoir and Site columns
  ccrwater$Reservoir <- "CCR"
  ccrwater$Site <- "51"
  
  # reorder columns
  ccrwater <- ccrwater %>% select(Reservoir, Site, DateTime,  
                                  ThermistorTemp_C_1, ThermistorTemp_C_2, ThermistorTemp_C_3, ThermistorTemp_C_4,
                                  ThermistorTemp_C_5, ThermistorTemp_C_6, ThermistorTemp_C_7, ThermistorTemp_C_8,
                                  ThermistorTemp_C_9,ThermistorTemp_C_10,ThermistorTemp_C_11, ThermistorTemp_C_12,
                                  ThermistorTemp_C_13, EXOTemp_C_1, EXOCond_uScm_1,
                                  EXOSpCond_uScm_1, EXOTDS_mgL_1, EXODOsat_percent_1, EXODO_mgL_1, EXOChla_RFU_1,
                                  EXOChla_ugL_1, EXOBGAPC_RFU_1, EXOBGAPC_ugL_1, EXOfDOM_RFU_1, EXOfDOM_QSU_1,
                                  EXOPressure_psi_1, EXODepth_m_1, EXOBattery_V_1, EXOCablepower_V_1, EXOWiper_V_1,
                                  EXOTemp_C_9, EXOCond_uScm_9,
                                  EXOSpCond_uScm_9, EXOTDS_mgL_9, EXODOsat_percent_9, EXODO_mgL_9, 
                                  EXOfDOM_RFU_9, EXOfDOM_QSU_9,EXOPressure_psi_9, EXODepth_m_9, EXOBattery_V_9,
                                  EXOCablepower_V_9, EXOWiper_V_9,LvlPressure_psi_13,LvlDepth_m_13, LvlTemp_C_13, 
                                  RECORD, CR3000Battery_V, CR3000Panel_Temp_C,everything())
  
  
  # convert datetimes to characters so that they are properly formatted in the output file
  ccrwater$DateTime <- as.character(ccrwater$DateTime)
  
  # write to output file
  write_csv(ccrwater, output_file)
}

# example usage
#qaqc("https://raw.githubusercontent.com/CareyLabVT/SCCData/mia-data/Catwalk.csv",
#     'https://raw.githubusercontent.com/FLARE-forecast/FCRE-data/fcre-catwalk-data/FCRWaterLevel.csv',
#     "https://raw.githubusercontent.com/CareyLabVT/SCCData/mia-data/CAT_MaintenanceLog.txt",
#    "Catwalk.csv")


