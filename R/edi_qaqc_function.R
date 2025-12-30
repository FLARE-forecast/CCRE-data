qaqc_ccr <- function(data_file = "https://raw.githubusercontent.com/FLARE-forecast/CCRE-data/ccre-dam-data/ccre-waterquality.csv",
                     EXO2_manual_file = "https://raw.githubusercontent.com/CareyLabVT/ManualDownloadsSCCData/master/current_files/CCR_1_5_EXO_L1.csv", 
                     maintenance_file = "https://raw.githubusercontent.com/FLARE-forecast/CCRE-data/ccre-dam-data-qaqc/CCRW_MaintenanceLog.csv", 
                     output_file, 
                     start_date = NULL, 
                     end_date = NULL)
{
  # Call the source function to get the depths
  
  source("https://raw.githubusercontent.com/LTREB-reservoirs/vera4cast/main/targets/target_functions/find_depths.R")
  
  
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
  
  
  # Add the turbidity columns to the header list
  
  CATPRES_COL_NAMES2 <- append(CATPRES_COL_NAMES, c("EXOTurbidity_FNU_1", "EXOTSS_mgL_1"), after = 31)
  
  
  #Adjustment period of time to stabilization after cleaning in seconds
  ADJ_PERIOD_DO = 2*60*60 
  ADJ_PERIOD_Temp = 30*60
  
  # use a for loop to loop over the different data files
  
  # create a blank data frame  
  df_ccr <- NULL  
  
  for(i in 1:length(data_file)){
    
    # if statement to read in the data file
    if(is.character(data_file[i])){
      
      # read in the first row to see how many columns there are
      first <- read_csv(data_file[i], skip = 1, n_max=1)
      
      
      if(ncol(first)%in% 53){
        
        # read catwalk data and maintenance log
        # NOTE: date-times throughout this script are processed as UTC
        ccrwater <- read_csv(data_file[i], skip = 1, col_names = CATPRES_COL_NAMES,
                             col_types = cols(.default = col_double(), DateTime = col_datetime()))
        
        ## clean up the data frame after adding turbidity sensor. Will make an if statement after fixing the df on the data logger
        
        # this is when we added the sensor
        if(ymd_hms("2025-04-03 09:30:00") %in% ccrwater$DateTime){
          
          
          # Filter out before adding the sensor
          pturb <- ccrwater|>
            filter(DateTime < ymd_hms("2025-04-03 09:30:00"))
          
          
          # Filter out 9:40 and 9:50 because they are 53 columns
          
          pturb2 <- ccrwater|>
            filter(grepl("2025-04-03 09:40:00|2025-04-03 09:50:00", DateTime))%>%
            bind_rows(pturb, .)|>
            # add in the new columns
            mutate(EXOTurbidity_FNU_1 = NA, 
                   EXOTSS_mgL_1 = NA)
          
          options(timeout=500)
          # read in the file without headers 
          aturb <- read.csv(data_file[i])
          
          # count the number of rows to use in the index below
          fg <- nrow(aturb)
          
          # get the df when added turbidity sensor
          def3 <-aturb[209844:fg,]
          
          # Filter out so just the rows that have the date in the correct column. 
          def4 <- dplyr::filter(def3, grepl('^202', TIMESTAMP))
          
          # take out 9:40 and 9:50 because they are are 53 columns and not 55
          def6 <- def4|>
            dplyr::filter(!grepl("2025-04-03 09:40:00|2025-04-03 09:50:00", TIMESTAMP))
          
          # Filter out just the pressure transducer readings
          def5 <- def3|>
            dplyr::filter(!grepl('^202', TIMESTAMP))|>
            dplyr::rename("LvlPressure_psi_13" = TIMESTAMP,
                          "LvlTemp_C_13" = RECORD)|>
            select(LvlPressure_psi_13, LvlTemp_C_13)
          
          # combine columns
          
          def7 <- cbind(def6, def5)
          
          # rename the columns with turbidity
          names(def7) <- CATPRES_COL_NAMES2
          
          def8 <- def7 |>
            mutate(DateTime = ymd_hms(DateTime))|>
            mutate_if(is.character, as.numeric)
          
          # Combine the two data frames with turbidity
          ccrwater <- bind_rows(def8, pturb2)|>
            arrange(DateTime)
        }
        
      }else if(ncol(first)%in% 55){
        ccrwater <- read_csv(data_file[i], skip = 1, col_names = CATPRES_COL_NAMES2,
                             col_types = cols(.default = col_double(), DateTime = col_datetime()))
        
      } else{
        warning("The data file has a different number of columns than usual and you need to check the column names in the CCR_qaqc function.")
      } 
      
    } else {
      
      # rename the data file if it is already a data frame
      ccrwater <- data_file[i]
    }
    
    # bind the files
    df_ccr <- bind_rows(df_ccr, ccrwater)
  }  
  
  # There are going to be lots of duplicates so get rid of them
  ccrwater <- df_ccr[!duplicated(df_ccr$DateTime), ]
  
  #reorder 
  ccrwater <- ccrwater[order(ccrwater$DateTime),]
  
  # Read in EXO file
  
  EXO <- read_csv(EXO2_manual_file, col_names=T,
                  col_types = cols(.default = col_double(), DateTime = col_datetime()), show_col_types = T)|>
    # add in the new columns
    mutate(EXOTurbidity_FNU_1 = NA, 
           EXOTSS_mgL_1 = NA)
  
  
  ### Add in the EXO here ####
  
  # Use the code you have in Add EXO to Gateway.R
  
  #Join the CCR-waterquality data and the EXO data
  CCR<- rquery::natural_join(ccrwater, EXO, 
                             by = "DateTime",
                             jointype = "LEFT")
  
  
  # #rearrange the column headers based on the original file since they get jumbled during the join 
  ccrwater <- CCR%>%
    select(all_of(CATPRES_COL_NAMES2))|>
    filter(!is.na(DateTime))
  
  # Set timezone as EST. Streaming sensors don't observe daylight savings
  ccrwater$DateTime <- force_tz(as.POSIXct(ccrwater$DateTime), tzone = "EST")
  
  # Take out the EXO_Date and EXO_Time column because we don't publish them 
  
  if("EXO_Date_1" %in% colnames(ccrwater)){
    ccrwater <- ccrwater%>%select(-any_of(c(starts_with("EXO_Date"), starts_with("EXO_Time"))))
  }
  
  # Duplicates were getting through so run it again after merging EXO files
  ccrwater <- ccrwater[!duplicated(ccrwater$DateTime), ]
  
  # convert NaN to NAs in the dataframe
  ccrwater[sapply(ccrwater, is.nan)] <- NA
  
  ## read in maintenance file 
  log <- read_csv2(maintenance_file, col_types = cols(
    .default = col_character(),
    TIMESTAMP_start = col_datetime("%Y-%m-%d %H:%M:%S%*"),
    TIMESTAMP_end = col_datetime("%Y-%m-%d %H:%M:%S%*"),
    flag = col_integer()
  ))
  
  # Set timezone as EST. Streaming sensors don't observe daylight savings
  log$TIMESTAMP_start <- force_tz(as.POSIXct(log$TIMESTAMP_start), tzone = "EST")
  log$TIMESTAMP_end <- force_tz(as.POSIXct(log$TIMESTAMP_end), tzone = "EST")
  
  ### identify the date subsetting for the data
  if (!is.null(start_date)){
    ccrwater <- ccrwater %>% 
      filter(DateTime >= start_date)
    log <- log %>% 
      filter(TIMESTAMP_start <= end_date)
  }
  
  if(!is.null(end_date)){
    ccrwater <- ccrwater %>% 
      filter(DateTime <= end_date)
    log <- log %>% 
      filter(TIMESTAMP_end >= start_date)
    
  }
  
  
  ### add Reservoir and Site columns
  ccrwater$Reservoir="CCR"
  ccrwater$Site=51
  
  
  #####Create Flag columns#####
  
  
  # for loop to create flag columns
  for(j in colnames(ccrwater%>%select(ThermistorTemp_C_1:LvlTemp_C_13))) { #for loop to create new columns in data frame
    ccrwater[,paste0("Flag_",j)] <- 0 #creates flag column + name of variable
    ccrwater[c(which(is.na(ccrwater[,j]))),paste0("Flag_",j)] <-7 #puts in flag 7 if value not collected
  }
  
  
  ### Convert RFU to ugL for Algae sensor ### 
  
  # Linear Relationship for chla when ugL not calculated
  # slope = 4.00 , int= -0.63
  
  ccrwater[c(which(is.na(ccrwater$EXOChla_ugL_1) & !is.na(ccrwater$EXOChla_RFU_1))), "Flag_EXOChla_ugL_1"]<- 6
  ccrwater[c(which(is.na(ccrwater$EXOChla_ugL_1) & !is.na(ccrwater$EXOChla_RFU_1))), "EXOChla_ugL_1"]<- (ccrwater[c(which(is.na(ccrwater$EXOChla_ugL_1) & !is.na(ccrwater$EXOChla_RFU_1))), "EXOChla_RFU_1"]*4.00)-0.63
  
  
  # Linear Relationship for phyco when ugL not calculated
  # slope = 1.00 , int = -0.59
  
  ccrwater[c(which(is.na(ccrwater$EXOBGAPC_ugL_1) & !is.na(ccrwater$EXOBGAPC_RFU_1))), "Flag_EXOBGAPC_ugL_1"]<- 6
  ccrwater[c(which(is.na(ccrwater$EXOBGAPC_ugL_1) & !is.na(ccrwater$EXOBGAPC_RFU_1))), "EXOBGAPC_ugL_1"]<- (ccrwater[c(which(is.na(ccrwater$EXOBGAPC_ugL_1) & !is.na(ccrwater$EXOBGAPC_RFU_1))), "EXOBGAPC_RFU_1"]*1.00)-0.59
  
  #update 
  for(k in colnames(ccrwater%>%select(EXOCond_uScm_1:EXOfDOM_QSU_1,EXOCond_uScm_9:EXOfDOM_QSU_9 ))) { #for loop to create new columns in data frame
    ccrwater[c(which((ccrwater[,k]<0))),paste0("Flag_",k)] <- 3
    ccrwater[c(which((ccrwater[,k]<0))),k] <- 0 #replaces value with 0
  }
  
  # Convert Pressure to Depth
  # Convert pressure to depth only if the pressure readin is above 16 psi the lower values get removed
  
  #create depth column
  ccrwater <- ccrwater%>%
    mutate(LvlDepth_m_13=ifelse(LvlPressure_psi_13>16, LvlPressure_psi_13*0.70455, NA))#1psi=2.31ft, 1ft=0.305m
  
  
  #####Maintenance Log QAQC############ 
  
  if(nrow(log)==0){
    print('No Maintenance Events Found...')
    
  } else {
    # modify ccrwater based on the information in the log   
    
    for(i in 1:nrow(log))
    {
      ### get start and end time of one maintenance event
      start <- log$TIMESTAMP_start[i]
      end <- log$TIMESTAMP_end[i]
      
      
      ### Get the Reservoir
      
      Reservoir <- log$Reservoir[i]
      
      ### Get the Site
      
      Site <- log$Site[i]
      
      ### Get the Maintenance Flag 
      
      flag <- log$flag[i]
      
      ### Get the update_value that an observation will be changed to 
      
      update_value <- as.numeric(log$update_value[i])
      
      ### Get the adjustment_code for a column to fix a value. If it is not an NA
      
      # These update_values are expressions so they should not be set to numeric
      adjustment_code <- log$adjustment_code[i]
      
      
      ### Get the names of the columns affected by maintenance
      
      colname_start <- log$start_parameter[i]
      colname_end <- log$end_parameter[i]
      
      ### if it is only one parameter parameter then only one column will be selected
      
      if(is.na(colname_start)){
        
        maintenance_cols <- colnames(ccrwater%>%select(all_of(colname_end))) 
        
      }else if(is.na(colname_end)){
        
        maintenance_cols <- colnames(ccrwater%>%select(all_of(colname_start)))
        
      }else{
        maintenance_cols <- colnames(ccrwater%>%select(c(colname_start:colname_end)))
      }
      
      
      ### Get the name of the flag column
      
      flag_cols <- paste0("Flag_", maintenance_cols)
      
      
      # Flag the pressure transducer values when fixing the LvlDepth column
      
      if("Flag_LvlDepth_m_13" %in% flag_cols){
        flag_cols <- "Flag_LvlPressure_psi_13"
      }
      
      ### Getting the start and end time vector to fix. If the end time is NA then it will put NAs 
      # until the maintenance log is updated
      
      if(is.na(end)){
        # If there the maintenance is on going then the columns will be removed until
        # and end date is added
        Time <- ccrwater$DateTime >= start
        
      }else if (is.na(start)){
        # If there is only an end date change columns from beginning of data frame until end date
        Time <- ccrwater$DateTime <= end
        
      }else {
        
        Time <- ccrwater$DateTime >= start & ccrwater$DateTime <= end
        
      }
      
      # replace relevant data with NAs and set flags while maintenance was in effect
      if (flag==1){
        # The observations are changed to NA for maintenance or other issues found in the maintenance log
        ccrwater[Time, maintenance_cols] <- NA
        ccrwater[Time, flag_cols] <- flag
        
      } else if (flag==2){
        if(!is.na(adjustment_code)){
          
          # finish the code chunk using paste and then evaluate it
          
          # Add a flag based on the conditions in the maintenance log
          eval(parse(text=paste0(adjustment_code, "flag_cols] <- flag")))
          # Change to NA based on the conditions in the maintenance log
          eval(parse(text=paste0(adjustment_code, "maintenance_cols] <- NA")))
          
        } else{  
          # The observations are changed to NA for maintenance or other issues found in the maintenance log
          ccrwater[Time, maintenance_cols] <- NA
          ccrwater[Time, flag_cols] <- flag
        }
        
        ## Flag 3 is removed in the for loop before the maintenance log where negative values are changed to 0
        
      } else if (flag==4){
        
        # Set the values to NA and flag
        ccrwater[Time, maintenance_cols] <- NA
        ccrwater[Time, flag_cols] <- flag
        
      } else if (flag==5){
        
        # Values are flagged but left in the dataset
        ccrwater[Time, flag_cols] <- flag
        
      } else if (flag==6){ #adjusting the conductivity based on the equation in the maintenance log 
        
        if (!is.na(update_value)){
          
          # change a vlue to something different
          
          ccrwater[Time, maintenance_cols]<- update_value
          
        }else{
          
          # use code in the maintenance log to adjust a value
          
          ccrwater[Time, maintenance_cols] <- eval(parse(text=adjustment_code))
          
          ccrwater[Time, flag_cols] <- flag
        }
        
      }else if (flag==7){
        # Data was not collected and already flagged as NA above 
        
      }else{
        # Flag is not in Maintenance Log
        warning(paste0("Flag ", flag, " used not defined in the L1 script. 
                     Talk to Austin and Adrienne if you get this message"))
      }
      
      # Add the 2 hour adjustment for DO. This means values less than 2 hours after the DO sensor is out of the water are changed to NA and flagged
      # In 2023 added a 30 minute adjustment for Temp sensors on the temp string  
      
      # Make a vector of the DO columns
      
      DO <- colnames(ccrwater%>%select(grep("DO_mgL|DOsat", colnames(ccrwater))))
      
      # Vector of thermistors on the temp string 
      Temp <- colnames(ccrwater%>%select(grep("Thermistor|RDOTemp|LvlTemp", colnames(ccrwater))))
      
      # make a vector of the adjusted time
      Time_adj_DO <- ccrwater$DateTime>start&ccrwater$DateTime<end+ADJ_PERIOD_DO
      
      Time_adj_Temp <- ccrwater$DateTime>start&ccrwater$DateTime<end+ADJ_PERIOD_Temp
      
      # Change values to NA after any maintenance for up to 2 hours for DO sensors
      
      if (flag ==1){
        
        # This is for DO add a 2 hour buffer after the DO sensor was out of the water
        ccrwater[Time_adj_DO,  maintenance_cols[maintenance_cols%in%DO]] <- NA
        ccrwater[Time_adj_DO, flag_cols[flag_cols%in%DO]] <- flag
        
        # Add a 30 minute buffer for when the temp string was out of the water
        ccrwater[Time_adj_Temp,  maintenance_cols[maintenance_cols%in%Temp]] <- NA
        ccrwater[Time_adj_Temp, flag_cols[flag_cols%in%Temp]] <- flag
      }
    }
    
  }
  ############## Remove and Flag when sensor is out of position ####################
  
  #change EXO_1 at 1.5m values to NA if EXO depth is less than 0.3m and Flag as 2
  
  #index only the colummns with EXO at the beginning
  exo_idx <-grep("^EXO.*_1$",colnames(ccrwater))
  
  #create list of the Flag columns that need to be changed to 2
  exo_flag <- grep("^Flag_EXO.*_1$",colnames(ccrwater))
  
  #Flag the data that was removed with 2 for outliers
  ccrwater[which(ccrwater$EXODepth_m_1< 0.1),exo_flag]<- 2
  #Change the EXO data to NAs when the EXO is above 0.75m and not already flagged as maintenance. Change to 0.4 right now to have data
  ccrwater[which(ccrwater$EXODepth_m_1 < 0.1), exo_idx] <- NA

  # Flag data but leave in if depth between 0.1 and 0.75
  ccrwater[which(ccrwater$EXODepth_m_1> 0.1 & ccrwater$EXODepth_m_1<0.75),exo_flag]<- 5
  
  #index only the colummns with EXO at the beginning
  exo_idx9 <-grep("^EXO.*_9$",colnames(ccrwater))
  
  exo_flag9 <-grep("^Flag_EXO.*_9$",colnames(ccrwater))
  
  #create list of the Flag columns that need to be changed to 2
  #exo_flag9 <- c("Flag_EXOTemp_9", "Flag_EXOCond_9","Flag_EXOSpCond_9","Flag_EXOTDS_9",'Flag_EXODO_obs_9',
  #               "Flag_EXODO_sat_9",'Flag_EXOfDOM_9',"Flag_EXOPres_9",
  #               "Flag_EXObat_9","Flag_EXOcab_9","Flag_EXOwip_9")
  
  #Flag the data that was removed with 2 for outliers
  ccrwater[which(ccrwater$EXODepth_m_9 < 6),exo_flag9]<- 2
  #Change the EXO data to NAs when the EXO is above 6m and not due to maintenance
  ccrwater[which(ccrwater$EXODepth_m_9 < 6), exo_idx9] <- NA
  
  
  # Flag the EXO data when the wiper isn't parked in the right position because it could be on the sensor when taking a reading
  #Flag the data that was removed with 2 for outliers
  ccrwater[which(ccrwater$EXOWiper_V_1 !=0 & ccrwater$EXOWiper_V_1 < 0.7 & ccrwater$EXOWiper_V_1 > 1.6),exo_flag]<- 2
  ccrwater[which(ccrwater$EXOWiper_V_1 !=0 & ccrwater$EXOWiper_V_1 < 0.7 & ccrwater$EXOWiper_V_1 > 1.6), exo_idx] <- NA
  
  ccrwater[which(ccrwater$EXOWiper_V_9 !=0 & ccrwater$EXOWiper_V_9 < 0.7 & ccrwater$EXOWiper_V_9 > 1.6),exo_flag9]<- 2
  ccrwater[which(ccrwater$EXOWiper_V_9 !=0 & ccrwater$EXOWiper_V_9 < 0.7 & ccrwater$EXOWiper_V_9 > 1.6), exo_idx9] <- NA
  
  
  
  
  ############## Leading and Lagging QAQC ##########################
  # This finds the point that is way out of range from the leading and lagging point 
  
  # loops through all of the columns to catch values that are above 2 or 4 sd above or below
  # the leading or lagging point 
  
  # need to make it a data frame because I was having issues with calculating the mean
  
  ccrwater=data.frame(ccrwater)
  
  for (a in colnames(ccrwater%>%select(ThermistorTemp_C_1:EXOfDOM_QSU_1, EXOTemp_C_9:EXOfDOM_QSU_9, LvlPressure_psi_13:LvlTemp_C_13))){
    Var_mean <- mean(ccrwater[,a], na.rm = TRUE)
    
    # For Algae sensors we use 4 sd as a threshold but for the others we use 2
    if (colnames(ccrwater[a]) %in% c("EXOChla_RFU_1.5","EXOChla_ugL_1.5","EXOBGAPC_RFU_1.5","EXOBGAPC_ugL_1.5")){
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
  
  print("leading and lagging worked")
  
  ### Remove observations when sensors are out of the water ###
  
  # Using the find_depths function
  
  ccrwater2 <- find_depths (data_file = ccrwater, # data_file = the file of most recent data either from EDI or GitHub. Currently reads in the L1 file
                            depth_offset = "https://raw.githubusercontent.com/FLARE-forecast/CCRE-data/ccre-dam-data-qaqc/CCR_Depth_offsets.csv",  # depth_offset = the file of depth of each sensor relative to each other. This file for BVR is on GitHub
                            output = NULL, # output = the path where you would like the data save
                            round_digits = 2, #round_digits = number of digits you would like to round to
                            bin_width = 0.25, # bin width in m
                            wide_data = T)  
  
  print("depth function worked")
  # Flag observations that were removed but don't have a flag yet
  
  for(j in colnames(ccrwater2%>%select(ThermistorTemp_C_1:ThermistorTemp_C_13))) { #for loop to create new columns in data frame
    ccrwater2[c(which(is.na(ccrwater2[,j]) & ccrwater2[,paste0("Flag_",j)]==0)),paste0("Flag_",j)] <-2 #put a flag of 2 for observations out of the water
  }
  
  
  
  
  #############################################################################################################################  
  
  
  # reorder columns
  ccrwater2 <- ccrwater2 %>% select(Reservoir, Site, DateTime,  
                                    ThermistorTemp_C_1, ThermistorTemp_C_2, ThermistorTemp_C_3, ThermistorTemp_C_4,
                                    ThermistorTemp_C_5, ThermistorTemp_C_6, ThermistorTemp_C_7, ThermistorTemp_C_8,
                                    ThermistorTemp_C_9,ThermistorTemp_C_10,ThermistorTemp_C_11, ThermistorTemp_C_12,
                                    ThermistorTemp_C_13, EXOTemp_C_1, EXOCond_uScm_1,
                                    EXOSpCond_uScm_1, EXOTDS_mgL_1, EXODOsat_percent_1, EXODO_mgL_1, EXOChla_RFU_1,
                                    EXOChla_ugL_1, EXOBGAPC_RFU_1, EXOBGAPC_ugL_1, EXOfDOM_RFU_1, EXOfDOM_QSU_1, EXOTurbidity_FNU_1,
                                    EXOPressure_psi_1, EXODepth_m_1, EXOBattery_V_1, EXOCablepower_V_1, EXOWiper_V_1,
                                    EXOTemp_C_9, EXOCond_uScm_9,
                                    EXOSpCond_uScm_9, EXOTDS_mgL_9, EXODOsat_percent_9, EXODO_mgL_9, 
                                    EXOfDOM_RFU_9, EXOfDOM_QSU_9,EXOPressure_psi_9, EXODepth_m_9, EXOBattery_V_9,
                                    EXOCablepower_V_9, EXOWiper_V_9,LvlPressure_psi_13,LvlDepth_m_13, LvlTemp_C_13, 
                                    RECORD, CR3000Battery_V, CR3000Panel_Temp_C,everything())
  
  
  # write_csv was giving the wrong times. Let's see if this is better. 
  # If the output file is NULL then we are using it in a function and want the file returned and not saved. 
  if (is.null(output_file)){
    return(ccrwater2)
  }else{
    # convert datetimes to characters so that they are properly formatted in the output file
    ccrwater2$DateTime <- as.character(format(ccrwater2$DateTime))
    write_csv(ccrwater2, output_file)
  }
  print("CCR WQ file qaqced")
}

# # Example usage
#  qaqc_ccr(output_file = "CCRCatwalk_L1.csv", start_date = as.Date("2023-01-01"), end_date = Sys.Date())


