qaqc_ccrmet <- function(data_file = 'https://raw.githubusercontent.com/FLARE-forecast/CCRE-data/ccre-dam-data/ccre-met.csv',
                        data2_file = 'https://raw.githubusercontent.com/CareyLabVT/ManualDownloadsSCCData/master/current_files/CCRMetstation_L1.csv',
                        maintenance_file = 'https://raw.githubusercontent.com/FLARE-forecast/CCRE-data/ccre-dam-data-qaqc/CCRM_Maintenancelog_new.csv', 
                        output_file, 
                        start_date = NULL, 
                        end_date = NULL, 
                        notes = NULL){
  
  #### Name the data file #### 
  # This section either uses the compiled data from FCR_MET_QAQC_Plots_2015_2022.Rmd which is already labeled Met
  # or it reads in and formats the current file off the data logger
  
  if (is.character(data_file)==T) {
    Met<-read_csv(data_file, skip = 4, col_names=F, show_col_types = F)
    Met[,17]<-NULL #remove column
    names(Met) = c("DateTime","Record", "CR3000Battery_V", "CR3000Panel_Temp_C", 
                   "PAR_umolm2s_Average", "PAR_Total_mmol_m2", "BP_Average_kPa", "AirTemp_C_Average", 
                   "RH_percent", "Rain_Total_mm", "WindSpeed_Average_m_s", "WindDir_degrees", "ShortwaveRadiationUp_Average_W_m2",
                   "ShortwaveRadiationDown_Average_W_m2", "InfraredRadiationUp_Average_W_m2",
                   "InfraredRadiationDown_Average_W_m2", "Albedo_Average_W_m2")
  }else {
    Met=data_file
  }
  

  #read in manual data from the data logger to fill in missing gaps
  
  if(is.null(data2_file)){
    
    # If there is no manual files then set data2_file to NULL
    data2_file <- NULL
    
  } else{
    
    Met2<-read_csv(data2_file, skip = 1, col_names=F, show_col_types = F)
    Met2[,17]<-NULL #remove column
    names(Met2) = c("DateTime","Record", "CR3000Battery_V", "CR3000Panel_Temp_C", 
                    "PAR_umolm2s_Average", "PAR_Total_mmol_m2", "BP_Average_kPa", "AirTemp_C_Average", 
                    "RH_percent", "Rain_Total_mm", "WindSpeed_Average_m_s", "WindDir_degrees", "ShortwaveRadiationUp_Average_W_m2",
                    "ShortwaveRadiationDown_Average_W_m2", "InfraredRadiationUp_Average_W_m2",
                    "InfraredRadiationDown_Average_W_m2", "Albedo_Average_W_m2")
  }
  
  # Bind the streaming data and the manual downloads together so we can get any missing observations 
  Met <-bind_rows(Met,Met2)%>%
    drop_na(DateTime)
  
  # Set timezone as EST. Streaming sensors don't observe daylight savings
  Met$DateTime <- force_tz(as.POSIXct(Met$DateTime), tzone = "EST")
  
  # There are going to be lots of duplicates so get rid of them
  Met <- Met[!duplicated(Met$DateTime), ]
  
  #reorder 
  Met <- Met[order(Met$DateTime),]
  
  
  # convert NaN to NAs in the dataframe
  Met[sapply(Met, is.nan)] <- NA
  
  
  Met$Reservoir <- 'CCR'
  Met$Site <- 51
  
  ## read in maintenance file 
  log_read <- read_csv2(maintenance_file, col_types = cols(
    #read_csv("./Data/DataAlreadyUploadedToEDI/EDIProductionFiles/MakeEML_CCRMetData/2022/misc_data_files/CCRM_Met_Maintenance_2021_2022.txt", col_types = cols(
    .default = col_character(),
    TIMESTAMP_start = col_datetime("%Y-%m-%d %H:%M:%S%*"),
    TIMESTAMP_end = col_datetime("%Y-%m-%d %H:%M:%S%*"),
    flag = col_integer()
  )) 
  
  log <- log_read
  
  # Set timezone as EST. Streaming sensors don't observe daylight savings
  log$TIMESTAMP_start <- force_tz(as.POSIXct(log$TIMESTAMP_start), tzone = "EST")
  log$TIMESTAMP_end <- force_tz(as.POSIXct(log$TIMESTAMP_end), tzone = "EST")
  
  ### identify the date subsetting for the data
  if (!is.null(start_date)){
    Met <- Met %>% 
      filter(DateTime >= start_date)
    log <- log %>%
      filter(TIMESTAMP_start <= end_date)
  }
  
  if(!is.null(end_date)){
    Met <- Met %>% 
      filter(DateTime <= end_date)
    log <- log %>%
      filter(TIMESTAMP_end >= start_date)
  }
  
  if (nrow(log) == 0){
    log <- log_read
  }
  
  
  ####Create data flags for publishing ####
  #get rid of NaNs
  #create flag + notes columns for data columns c(5:17)
  #set flag 2
  for(i in colnames(Met%>%select(PAR_umolm2s_Average:Albedo_Average_W_m2))) { #for loop to create new columns in data frame
    Met[,paste0("Flag_",colnames(Met[i]))] <- 0 #creates flag column + name of variable
    Met[,paste0("Note_",colnames(Met[i]))] <- NA #creates note column + names of variable
    
    ## flag missing data (may be overwritten by maint log flag assignment)
    # Met[which(is.na(Met[,i])),i] <- NA
    
    Met[c(which(is.na(Met[,i]))),paste0("Flag_",colnames(Met[i]))] <-2 #puts in flag 2
    Met[c(which(is.na(Met[,i]))),paste0("Note_",colnames(Met[i]))] <- "Sample not collected" #note for flag 2
  }
  
 
  for(i in 1:nrow(log)){
    ### Assign variables based on lines in the maintenance log.
    
    ### get start and end time of one maintenance event
    start <- force_tz(as.POSIXct(log$TIMESTAMP_start[i]), tzone = "America/New_York")
    end <- force_tz(as.POSIXct(log$TIMESTAMP_end[i]), tzone = "America/New_York")
    
    ### Get the Reservoir Name
    Reservoir <- log$Reservoir[i]
    
    ### Get the Site Number
    Site <- as.numeric(log$Site[i])
    
    ### Get the depth value
    #Depth <- as.numeric(log$Depth[i]) ## IS THERE SUPPOSED TO BE A COLUMN ADDED TO MAINT LOG CALLED NEW_VALUE?
    
    ### Get the Maintenance Flag
    flag <- log$flag[i]
    
    ### Get the new value for a column
    update_value <- as.numeric(log$updated_value[i])
    
    ## Get the adjustment code from column
    maint_adjustment_code <- log$adjustment_code[i]
    
    ### Get the names of the columns affected by maintenance
    colname_start <- log$start_parameter[i]
    colname_end <- log$end_parameter[i]
    
    ### if it is only one parameter parameter then only one column will be selected
    
    if(is.na(colname_start)){
      
      maintenance_cols <- colnames(Met%>%select((colname_end)))
      
    }else if(is.na(colname_end)){
      
      maintenance_cols <- colnames(Met%>%select((colname_start)))
      
    }else{
      maint_col_df <- Met |> 
        select(-contains(c('Flag','Note', 'CR3000'))) |>
        select("DateTime","Record", 
               "PAR_umolm2s_Average", "PAR_Total_mmol_m2", "BP_Average_kPa", "AirTemp_C_Average", 
               "RH_percent", "Rain_Total_mm", "WindSpeed_Average_m_s", "WindDir_degrees", "ShortwaveRadiationUp_Average_W_m2",
               "ShortwaveRadiationDown_Average_W_m2", "InfraredRadiationUp_Average_W_m2",
               "InfraredRadiationDown_Average_W_m2", "Albedo_Average_W_m2")
      
      maintenance_cols <- colnames(maint_col_df%>%select((colname_start:colname_end)))
      
    }
    
    if(is.na(end)){
      # If there the maintenance is on going then the columns will be removed until
      # and end date is added
      Time <- Met |> filter(DateTime >= start) |> select(DateTime)
      
    }else if (is.na(start)){
      # If there is only an end date change columns from beginning of data frame until end date
      Time <- Met |> filter(DateTime <= end) |> select(DateTime)
      
    }else {
      Time <- Met |> filter(DateTime >= start & DateTime <= end) |> select(DateTime)
    }
    
    ### This is where information in the maintenance log gets updated
    
    if(flag %in% c(1,2)){
      # The observations are changed to NA for maintenance or other issues found in the maintenance log
      Met[Met$DateTime %in% Time$DateTime, maintenance_cols] <- NA
      Met[Met$DateTime %in% Time$DateTime, paste0("Flag_",maintenance_cols)] <- flag
      
    }else if (flag %in% c(3)){
      ## change negative values but exclude air temperature
      
      if('AirTemp_C_Average' %in% maintenance_cols){
        maintenance_cols[!maintenance_cols == 'AirTemp_C_Average']
      }
      
      Met[c(which(Met[,'Site'] == Site & Met$DateTime %in% Time$DateTime)),maintenance_cols] <- 0
      
    }else if (flag %in% c(4)){
      
      if(is.na(adjustment_code)){ ## Just use updated value if no adjustment code is provided
        Met[c(which(Met[,'Site'] == Site & Met$DateTime %in% Time$DateTime)),paste0("Flag_",maintenance_cols)] <- as.numeric(flag)
        Met[c(which(Met[,'Site'] == Site & Met$DateTime %in% Time$DateTime)),maintenance_cols] <- as.numeric(update_value)
        
      } else if(!is.na(adjustment_code)){ ## Use adjustment code if it's non-NA
        original_values <- Met[c(which(Met[,'Site'] == Site & Met$DateTime %in% Time$DateTime)),maintenance_cols]
        
        Met[c(which(Met[,'Site'] == Site & Met$DateTime %in% Time$DateTime)),paste0("Flag_",maintenance_cols)] <- as.numeric(flag)
        Met[c(which(Met[,'Site'] == Site & Met$DateTime %in% Time$DateTime)),maintenance_cols] <- eval(parse(text = maint_adjustment_code))
      }
      
    }else if(flag %in% c(5)){
      # UPDATE THE MANUAL ISSUE FLAGS (BAD SAMPLE / USER ERROR) BUT KEEP ORIGINAL VALUE
      Met[c(which(Met[,'Site'] == Site & Met$DateTime %in% Time$DateTime)),paste0("Flag_",maintenance_cols)] <- as.numeric(flag)
      
    }else{
      warning("Flag not coded in the L1 script. See Austin or Adrienne")
    }
  }
  #### END NEW MAINTENANCE LOG CODE
  
  #### Rain totals QAQC######
  
  # Take out Rain Totals above 5mm in 1 minute
  Rain<-c(which(!is.na(Met$Rain_Total_mm) & Met$Rain_Total_mm>5))
  Met[Rain, "Note_Rain_Total_mm"]<-"Rain total over 5mm in one minute so removed as outlier"
  Met[Rain, "Flag_Rain_Total_mm"]<-4
  Met[Rain, "Rain_Total_mm"] <- NA
  
  
  ####Air temperature data cleaning ####
  # This is how to find the linear regression but don't need it now so can comment it out. 
  #No pre and post filter but run all through the QAQC 
  #MetAir_2021=Met[Met$DateTime<"2021-12-31 19:00:00",c(1,4,8)]
  #lm_Panel2021=lm(MetAir_2021$AirTemp_C_Average ~ MetAir_2021$CR3000Panel_Temp_C)
  #summary(lm_Panel2021)#gives data on linear model parameters
  
  
  # substitute the calculated panel temp if air temp is missing
  Air<-which(Met$Flag_AirTemp_C_Average==2)
  
  Met[Air, "AirTemp_C_Average"]<- (-3.5604+(0.9289*(Met[Air, "CR3000Panel_Temp_C"])))
  Met[Air, "Note_AirTemp_C_Average"]<- "Substituted value calculated from Panel Temp and linear model"
  Met[Air, "Flag_AirTemp_C_Average"]<-4
  
  # Now substitute calculated panel temp if air temp greater than 4 sd
  # sd(lm_Panel2021$residuals)= 1.039299
  
  Air<-c(which((Met$AirTemp_C_Average - (-3.5604+(0.9289*Met$CR3000Panel_Temp_C)))>(4*1.039299) & !is.na(Met$AirTemp_C_Average)))
  
  Met[Air, "Note_AirTemp_C_Average"]<-"Substituted value calculated from Panel Temp and linear model"
  Met[Air, "Flag_AirTemp_C_Average"]<-4
  Met[Air, "AirTemp_C_Average"]<-(-3.5604+(0.9289*(Met[Air, "CR3000Panel_Temp_C"])))
  
  #Air temp maximum set
  
  b=c(which(!is.na(Met$AirTemp_C_Average) & Met$AirTemp_C_Average>40.6))
  
  Met[b,"Note_AirTemp_C_Average"]<-"Outlier set to NA. Value over 40.6"
  Met[b, "Flag_AirTemp_C_Average"]<-4
  Met[b, "AirTemp_C_Average"]<-NA
  
  ###Infared radiation cleaning####
  #fix infrared radiation voltage reading after airtemp correction
  #only need to do this for data from 2015 to  2016-07-25 10:12:00
  
  # name of which argument that is repeated below 
  # Dn<-c(which(Met$DateTime<ymd_hms("2016-07-25 10:12:00") & Met$InfraredRadiationDown_Average_W_m2<250 & 
  #               !is.na(Met$InfraredRadiationDown_Average_W_m2)))
  # 
  # Met[Dn, "Note_InfraredRadiationDown_Average_W_m2"] <- "Value corrected with InfRadDn equation as described in metadata"
  # Met[Dn, "Flag_InfraredRadiationDown_Average_W_m2"] <- 4
  # Met[Dn, "InfraredRadiationDown_Average_W_m2"] <- 5.67*10^-8*(Met[Dn, "AirTemp_C_Average"]+273.15)^4
  # 
  # # name of which argument that is repeated below
  # Up<-c(which(Met$DateTime<ymd_hms("2016-07-25 10:12:00") & Met$InfraredRadiationUp_Average_W_m2<100 & 
  #               !is.na(Met$InfraredRadiationUp_Average_W_m2)))
  # 
  # Met[Up, "Note_InfraredRadiationUp_Average_W_m2"] <- "Value corrected with InfRadUp equation as described in metadata"
  # Met[Up, "Flag_InfraredRadiationUp_Average_W_m2"] <- 4
  # Met[Up, "InfraredRadiationDown_Average_W_m2"] <- 5.67*10^-8*(Met[Up, "AirTemp_C_Average"]+273.15)^4
  # 
  # #Mean correction for InfRadDown (needs to be after voltage correction)
  # #Using 2018 data, taking the mean and sd of values on DOY to correct to
  # Met$DOY=yday(Met$DateTime)
  # 
  # # Read in the infrared file from Github for QAQC. This is the DOY averages of infrared and sd for 2018 
  # infrad <- read_csv(met_infrad)
  # #read_csv("./Data/DataAlreadyUploadedToEDI/EDIProductionFiles/MakeEML_FCRMetData/2022/misc_data_files/FCR_Met_Infrad_DOY_Avg_2018.csv")
  # 
  # #putting in columns for infrared mean and sd by DOY into main data set
  # Met=merge(Met, infrad, by = "DOY") 
  # Met=Met[order(Met$DateTime),] #ordering table after merging and removing unnecessary columns
  # 
  # #If the IR Down is greater than 3SD by DOY and replace with the equation from the manual(5.67*10-8(AirTemp_C_Average+273.15)^4)
  # 
  # # name of which argument used below
  # IR_Dn=c(which(abs(Met$InfraredRadiationDown_Average_W_m2-Met$infradavg)>(2*Met$infradsd) & !is.na(Met$InfraredRadiationDown_Average_W_m2)))
  # 
  # Met[IR_Dn, "Flag_InfraredRadiationDown_Average_W_m2"] <- 4
  # Met[IR_Dn, "Note_InfraredRadiationDown_Average_W_m2"] <- "Greater than 2SD Value corrected with InfRadDn equation as described in metadata"
  # Met[IR_Dn, "InfraredRadiationDown_Average_W_m2"] <- 5.67*10^-8*(Met[IR_Dn,"AirTemp_C_Average"]+273.15)^4
  # 
  # # Get rid of columns we don't need
  # Met<-Met%>%
  #   select(-c("DOY","infradavg","infradsd"))
  
  #Inf outliers, must go after corrections
  
  # name of which argument used below
  IR_Up=c(which(Met$InfraredRadiationUp_Average_W_m2<150 & !is.na(Met$InfraredRadiationUp_Average_W_m2)))
  
  Met[IR_Up,"Flag_InfraredRadiationUp_Average_W_m2"]<-4
  Met[IR_Up,"Note_InfraredRadiationUp_Average_W_m2"]<-"Outlier set to NA. Value below 150 W/m2"
  Met[IR_Up,"InfraredRadiationUp_Average_W_m2"]<-NA
  
  
  IR_Dn<-c(which(Met$InfraredRadiationDown_Average_W_m2>540 & !is.na(Met$InfraredRadiationDown_Average_W_m2)|Met$InfraredRadiationDown_Average_W_m2<200 & !is.na(Met$InfraredRadiationDown_Average_W_m2)))
  
  Met[IR_Dn,"Flag_InfraredRadiationDown_Average_W_m2"]<-4
  Met[IR_Dn,"Note_InfraredRadiationDown_Average_W_m2"]<-"Outlier set to NA due to probable sensor failure. Value above 540 W/m2"
  Met[IR_Dn,"InfraredRadiationDown_Average_W_m2"]<-NA
  
  #Replace missing values with estimations from the equation
  
  # Name of which argument to use below
  # IR_Dn<-which(Met$Flag_InfraredRadiationDown_Average_W_m2==2)    
  # 
  # Met[IR_Dn,"InfraredRadiationDown_Average_W_m2"]<- 5.67*10^-8*(Met[IR_Dn,"AirTemp_C_Average"]+273.15)^4 
  # Met[IR_Dn,"Note_InfraredRadiationDown_Average_W_m2"]<-"Value corrected with InfRadDn equation as described in metadata" 
  # Met[IR_Dn,"Flag_InfraredRadiationDown_Average_W_m2"]<-4
  # 
  # # Name of which argument to use below
  # IR_Up<-which(Met$Flag_InfraredRadiationUp_Average_W_m2==2)
  # 
  # Met[IR_Up,"InfraredRadiationUp_Average_W_m2"]<- 5.67*10^-8*(Met[IR_Up,"AirTemp_C_Average"]+273.15)^4 
  # Met[IR_Up,"Note_InfraredRadiationUp_Average_W_m2"]<-"Value corrected with InfRadUp equation as described in metadata" 
  # Met[IR_Up,"Flag_InfraredRadiationUp_Average_W_m2"]<-4
  
  # if(is.null(start_date)){
  #   # Read in the FCR met file 
  #   inUrl1  <- "https://pasta-s.lternet.edu/package/data/eml/edi/143/8/a5524c686e2154ec0fd0459d46a7d1eb" 
  #   infile1 <- paste0(getwd(),"/Data/Met_final_2015_2022.csv")
  #   download.file(inUrl1,infile1,method="curl")
  #   
  #   
  #   fc <- read_csv("./Data/Met_final_2015_2022.csv", header=T) %>%
  #     #fc <-read_csv("./misc_data_files/FCR_Met_final_2015_2022.csv")%>%  
  #     mutate(DateTime = as.POSIXct(strptime(DateTime, "%Y-%m-%d %H:%M:%S", tz="EST"))) %>% 
  #     filter(DateTime>as.POSIXct("2021-03-29 00:00:00"))%>%
  #     select(DateTime,Rain_Total_mm)
  #   
  #   Met<-merge(Met, fc, by="DateTime")
  #   
  #   # Name of which argument to use below
  #   FCR_rain<-which(Met$DateTime>"2022-07-19 00:00:00" & Met$DateTime<"2022-09-12 12:09:00")  
  #   
  #   Met[FCR_rain,"Rain_Total_mm.x"]<- 0.001494+(0.110725*Met[FCR_rain,"Rain_Total_mm.y"]) 
  #   Met[FCR_rain,"Note_Rain_Total_mm"]<-"Substituted value calculated from FCR rain guage and linear_model" 
  #   Met[FCR_rain,"Flag_Rain_Total_mm"]<-4
  #   
  #   # Change the values that should be 0 but are 0.001494 because of the linear model 
  #   Met[which(Met$Rain_Total_mm.x==0.001494),"Rain_Total_mm.x"]<-0
  #   
  #   # Take out FCR rain 
  #   Met$Rain_Total_mm.y<-NULL
  #   
  #   # Change back to Rain_Total_mm
  #   Met<-Met%>%dplyr::rename("Rain_Total_mm"="Rain_Total_mm.x")
  # }
  
  
  ###Impossible Outliers####
  #take out impossible outliers and Infinite values before the other outliers are removed
  #set flag 3 (see metadata: this corrects for impossible outliers)
  for(i in colnames(Met%>%select(PAR_umolm2s_Average:Albedo_Average_W_m2))) { #for loop to create new columns in data frame
    
    Met[c(which(is.infinite(Met[,i][[1]]))),paste0("Flag_", i)] <-3 #puts in flag 3
    Met[c(which(is.infinite(Met[,i][[1]]))),paste0("Note_", i)] <- "Infinite value set to NA" #note for flag 3
    Met[c(which(is.infinite(Met[,i][[1]]))),i] <- NA #set infinite vals to NA
    
    if(i!="AirTemp_C_Average") { #flag 3 for negative values for everything except air temp
      Met[c(which((Met[,i]<0))),paste0("Flag_",i)] <- 3
      Met[c(which((Met[,i]<0))),paste0("Note_",colnames(Met[i]))] <- "Negative value set to 0"
      Met[c(which((Met[,i]<0))),i] <- 0 #replaces value with 0
    }
    if(i=="RH_percent") { #flag for RH over 100
      Met[c(which((Met[,i]>100))),paste0("Flag_",i)] <- 3
      Met[c(which((Met[,i]>100))),paste0("Note_",i)] <- "Value set to 100"
      Met[c(which((Met[,i]>100))),i] <- 100 #replaces value with 100
    }
  }
  
  ###Wind Outliers####
  # Take out if wind direction is not between 0 and 360 and if wind speed is above 50 m/s
  
  # Name of which argument to use below
  Wi<-c(which(Met$WindDir_degrees<0 & !is.na(Met$WindDir_degrees) | Met$WindDir_degrees>360 & !is.na(Met$WindDir_degrees)))
  
  Met[Wi,"Note_WindDir_degrees"]<-"Wind direction is below 0 or above 360 degrees"
  Met[Wi,"Flag_WindDir_degrees"]<-4
  Met[Wi,"WindDir_degrees"]<-NA
  
  # Set wind speed to NA if above 50 m/s. Update as needed. Haven't seen any real windspreed that high
  # Name of which argument used below
  Ws<- c(which(Met$WindSpeed_Average_m_s>50 & !is.na(Met$WindSpeed_Average_m_s)))
  
  Met[Ws, "Note_WindSpeed_Average_m_s"]<-"Wind Speed is above 50 m/s"
  Met[Ws, "Flag_WindSpeed_Average_m_s"]<-4
  Met[Ws, "WindSpeed_Average_m_s"]<-NA
  
  ####Remove barometric pressure outliers####
  # Name of which argument used below and then flag and replace when BP is less than 98.5
  BP<-c(which(Met$BP_Average_kPa<95.5 & !is.na(Met$BP_Average_kPa)))
  
  Met[BP,"Note_BP_Average_kPa"]<-"Outlier set to NA. Value below 98.5"
  Met[BP,"Flag_BP_Average_kPa"]<-4
  Met[BP,"BP_Average_kPa"]<-NA
  
  
  #####remove high PAR values at night ######
  #get sunrise and sunset times
  suntimes=getSunlightTimes(date = seq.Date(as.Date("2021-03-29"), Sys.Date(), by = 1),
                            keep = c("sunrise",  "sunset"),
                            lat = 37.37, lon = -79.96, tz = "UTC")
  
  #create date column
  Met$date <- as.Date(Met$DateTime)
  
  #create subset to join
  
  
  #now merge the datasets to get daylight time
  Met <- left_join(Met, suntimes, by = "date") %>%
    mutate(daylight_intvl = interval(sunrise, sunset)) %>%
    mutate(during_day = DateTime %within% daylight_intvl)
  
  #Remove PAR Tot
  # Take out the PAR values when it is above 1 mmol/m2 during the night
  # Name the which argument used below
  Par_Tot<-c(which(Met$during_day==FALSE & Met$PAR_Total_mmol_m2>1 & !is.na(Met$PAR_Total_mmol_m2)))
  
  Met[Par_Tot, "Flag_PAR_Total_mmol_m2"]<-4
  Met[Par_Tot, "Note_PAR_Total_mmol_m2"]<-"Outlier set to NA. Above 1 mmol_m2 during the night"
  Met[Par_Tot, "PAR_Total_mmol_m2"]<-NA
  
  # Name of which argument used below
  Par_Avg<-c(which(Met$during_day==FALSE & Met$PAR_umolm2s_Average> 12 & !is.na(Met$PAR_umolm2s_Average)))
  
  Met[Par_Avg,"Flag_PAR_umolm2s_Average"]<-4
  Met[Par_Avg,"Note_PAR_umolm2s_Average"]<-"Outlier set to NA. Above 5 umolm2s during the night"
  Met[Par_Avg,"PAR_umolm2s_Average"]<-NA
  
  ####Remove total PAR (PAR_Tot) outliers ####
  # Remove and set to NA for values over 200
  # Name of which argument to use below
  Par_Tot<-c(which(Met$PAR_Total_mmol_m2>200& !is.na(Met$PAR_Total_mmol_m2)))
  
  Met[Par_Tot, "Flag_PAR_Total_mmol_m2"]<-4
  Met[Par_Tot, "Note_PAR_Total_mmol_m2"]<-"Outlier set to NA. Above 200 mmol_m2"
  Met[Par_Tot, "PAR_Total_mmol_m2"]<-NA
  
  # Remove and set to NA for values over 3000
  # Name of which argument to use below
  Par_Avg<-c(which(Met$PAR_umolm2s_Average>3000 & !is.na(Met$PAR_umolm2s_Average)))
  
  Met[Par_Avg,"Flag_PAR_umolm2s_Average"]<-4
  Met[Par_Avg,"Note_PAR_umolm2s_Average"]<-"Outlier set to NA. Above 3000 umolm2s"
  Met[Par_Avg,"PAR_umolm2s_Average"]<-NA
  
  
  #Remove shortwave radiation outliers
  #first shortwave upwelling
  # Name of which argument used below
  Sr_Avg<-c(which(Met$ShortwaveRadiationUp_Average_W_m2>1500 & !is.na(Met$ShortwaveRadiationUp_Average_W_m2)))
  
  Met[Sr_Avg,"Flag_ShortwaveRadiationUp_Average_W_m2"]<-4
  Met[Sr_Avg,"Note_ShortwaveRadiationUp_Average_W_m2"]<-"Outlier set to NA. Value above 1500 W/m2"
  Met[Sr_Avg,"ShortwaveRadiationUp_Average_W_m2"]<-NA
  
  #add a flag for suspect values from 1499 to 1300
  #above 1500 already set to NA
  # Name of which argument used below
  Sr_Avg<-c(which(Met$ShortwaveRadiationUp_Average_W_m2>1300 & !is.na(Met$ShortwaveRadiationUp_Average_W_m2)))
  
  Met[Sr_Avg,"Flag_ShortwaveRadiationUp_Average_W_m2"]<-5
  Met[Sr_Avg,"Note_ShortwaveRadiationUp_Average_W_m2"]<-"Questionable value left in. Value between 1500-1300 W/m2"
  
  
  #and then shortwave downwelling (what goes up must come down)
  
  # Take out if value above 300 and set to NA
  # Name of which argument used below
  Sr_Dn<-c(which(Met$ShortwaveRadiationDown_Average_W_m2>300 & !is.na(Met$ShortwaveRadiationDown_Average_W_m2)))
  # Take out if value above 300 and set to NA
  Met[Sr_Dn,"Flag_ShortwaveRadiationDown_Average_W_m2"]<-4
  Met[Sr_Dn,"Note_ShortwaveRadiationDown_Average_W_m2"]<-"Outlier set to NA. Value above 300 W/m2"
  Met[Sr_Dn,"ShortwaveRadiationDown_Average_W_m2"]<-NA
  
  #Remove albedo outliers
  # Remove and set to NA if over 1000
  Alb<-c(which(Met$Albedo_Average_W_m2>1000 & !is.na(Met$Albedo_Average_W_m2)))
  
  Met[Alb, "Flag_Albedo_Average_W_m2"]<-4
  Met[Alb, "Note_Albedo_Average_W_m2"]<-"Outlier_set_to_NA. Value above 1000 W/m2"
  Met[Alb, "Albedo_Average_W_m2"]<-NA
  
  #set to NA when shortwave radiation up is equal to NA
  
  Alb<-c(which(is.na(Met$ShortwaveRadiationUp_Average_W_m2)|is.na(Met$ShortwaveRadiationDown_Average_W_m2)))
  
  Met[Alb, "Flag_Albedo_Average_W_m2"]<-4
  Met[Alb, "Note_Albedo_Average_W_m2"]<-"Set to NA because Shortwave equals NA"
  Met[Alb, "Albedo_Average_W_m2"]<-NA
  
  # Take out the columns we don't need any more and add Reservoir and Site columns
  Met=Met%>%
    select(-c(date, lat, lon, sunrise, sunset,daylight_intvl,during_day))%>%
    mutate(Reservoir="CCR",
           Site=51)
  
  
  ###7) Write file with final cleaned dataset! ###
  Met_final=Met%>%
    select(c("Reservoir","Site","DateTime","Record","CR3000Battery_V","CR3000Panel_Temp_C","PAR_umolm2s_Average","PAR_Total_mmol_m2","BP_Average_kPa",                          
             "AirTemp_C_Average","RH_percent","Rain_Total_mm","WindSpeed_Average_m_s","WindDir_degrees","ShortwaveRadiationUp_Average_W_m2",       
             "ShortwaveRadiationDown_Average_W_m2","InfraredRadiationUp_Average_W_m2","InfraredRadiationDown_Average_W_m2","Albedo_Average_W_m2",
             "Flag_PAR_umolm2s_Average","Note_PAR_umolm2s_Average","Flag_PAR_Total_mmol_m2","Note_PAR_Total_mmol_m2","Flag_BP_Average_kPa",
             "Note_BP_Average_kPa","Flag_AirTemp_C_Average","Note_AirTemp_C_Average","Flag_RH_percent","Note_RH_percent","Flag_Rain_Total_mm",
             "Note_Rain_Total_mm","Flag_WindSpeed_Average_m_s","Note_WindSpeed_Average_m_s",
             "Flag_WindDir_degrees","Note_WindDir_degrees","Flag_ShortwaveRadiationUp_Average_W_m2","Note_ShortwaveRadiationUp_Average_W_m2",
             "Flag_ShortwaveRadiationDown_Average_W_m2","Note_ShortwaveRadiationDown_Average_W_m2",
             "Flag_InfraredRadiationUp_Average_W_m2","Note_InfraredRadiationUp_Average_W_m2",
             "Flag_InfraredRadiationDown_Average_W_m2","Note_InfraredRadiationDown_Average_W_m2","Flag_Albedo_Average_W_m2","Note_Albedo_Average_W_m2"))
  
  
  ## Should we include notes columns? Remove if notes is false
  if (notes == FALSE){
    Met_final <- Met_final |> select(-contains('Note'))
  }
  
  #### Write to CSV ####
  # convert datetimes to characters so that they are properly formatted in the output file
  Met_final$DateTime <- as.character(Met_final$DateTime)
  
  # write_csv was giving the wrong times. Let's see if this is better. 
  # If the output file is NULL then we are using it in a function and want the file returned and not saved. 
  if (is.null(output_file)){
    return(Met_final)
  }else{
    write_csv(Met_final, output_file)
  }
  
}

# # Example Use
# qaqc_ccrmet(data_file = 'https://raw.githubusercontent.com/FLARE-forecast/CCRE-data/ccre-dam-data/ccre-met.csv',
# data2_file = 'https://raw.githubusercontent.com/CareyLabVT/ManualDownloadsSCCData/master/current_files/CCRMetstation_L1.csv',
# maintenance_file = 'https://raw.githubusercontent.com/FLARE-forecast/CCRE-data/ccre-dam-data-qaqc/CCRM_Maintenancelog_new.csv', 
# output_file = "CCRMet_L1.csv", 
# start_date = as.Date("2023-01-01"), 
# end_date = Sys.Date(), 
# notes = FALSE)
