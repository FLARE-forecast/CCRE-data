#script written to create daily figures that are sent to lucky SCC team members :)
#written by CCC, BB, Vahid-Dan, ABP

# Edits:
# 20 May 2025- duplicated from FCRE script to create CCRE script, remove FCR/BVR related code
# 18 March 2024- add section for eddyflux fluxes
# 29 Sept. 2024- added in the water level to CCR
# 09 Dec. 2024 - added in an if statment to read in BVR file because added back in the data logger header. Add print statment when plots printed. 
# 06 Nov. 2025 - added a plot of the surface EXO depth to watch it especially as the water level changes
# 04 Mar. 2026 - added in EddyFlux plots 

continue_on_error <- function()
{
  print("ERROR! CONTINUING WITH THE REST OF THE SCRIPT ...")
}

options(error=continue_on_error)

#loading packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(Rcpp, generics, lubridate, tidyverse,gridExtra,openair, dplyr, magrittr, ggplot2)

# Set the timeout option to 500 seconds instead of 60
options(timeout=500)


download.file('https://raw.githubusercontent.com/FLARE-forecast/CCRE-data/ccre-dam-data/ccre-waterquality2.csv', 'ccre-waterquality.csv')
download.file('https://raw.githubusercontent.com/FLARE-forecast/CCRE-data/ccre-dam-data/ccre-met.csv', 'ccre-met.csv')
download.file('https://raw.githubusercontent.com/FLARE-forecast/CCRE-data/ccre-eddyflux-data-qaqc/CCRE_EddyFlux_streaming_L1.csv', 'Eddyflux.csv')

#CCR met data

ccrmetheader<-read.csv("ccre-met.csv", skip=1, as.is=T) #get header minus wonky Campbell rows
ccrmetdata<-read.csv("ccre-met.csv", skip=4, header=F) #get data minus wonky Campbell rows
names(ccrmetdata)<-names(ccrmetheader) #combine the names to deal with Campbell logger formatting

ccrmetdata=ccrmetdata%>%
  filter(grepl("^20", TIMESTAMP))%>% #keep only right time
  distinct(TIMESTAMP, .keep_all= TRUE) #taking out duplicates

#for the time sequence we can use the same as from the FCR met staion
end.time <- with_tz(as.POSIXct(strptime(Sys.time(), format = "%Y-%m-%d %H:%M")), tzone = "Etc/GMT+5") #gives us current time with rounded minutes in EDT
start.time <- end.time - days(7) #to give us seven days of data for looking at changes
full_time <- seq(start.time, end.time, by = "min") #create sequence of dates from past 5 days to fill in data

obs4 <- array(NA,dim=c(length(full_time),13)) #create array that will be filled in with 10 columns
#commented all lines that are irrelevant for 2020 data, per change in data downloads
#met_timechange=max(which(ccrmetdata$TIMESTAMP=="2019-04-15 10:19:00")) #shows time point when met station was switched from GMT -4 to GMT -5
ccrmetdata$TIMESTAMP<-as.POSIXct(strptime(ccrmetdata$TIMESTAMP, "%Y-%m-%d %H:%M"), tz = "Etc/GMT+5") #get dates aligned
#ccrmetdata$TIMESTAMP[c(1:met_timechange-1)]<-with_tz(force_tz(ccrmetdata$TIMESTAMP[c(1:met_timechange-1)],"Etc/GMT+4"), "Etc/GMT+5") #pre time change data gets assigned proper timezone then corrected to GMT -5 to match the rest of the data set
#ccrmetdata=ccrmetdata[-c(met_timechange-1),]

if (length(na.omit(ccrmetdata$TIMESTAMP[ccrmetdata$TIMESTAMP>start.time]))<2) { #if there is no data after start time, then a pdf will be made explaining this
  pdf(paste0("CCRMetDataFigures_", Sys.Date(), ".pdf"), width=8.5, height=11) #call PDF file
  plot(NA, xlim=c(0,5), ylim=c(0,5), bty='n',xaxt='n', yaxt='n', xlab='', ylab='') #creates empty plot
  mtext(paste("No data found between", start.time, "and", end.time, sep = " ")) #fills in text in top margin of plot
  dev.off() #file made!
  print("CCR met file made with no data")
} else {
  #merge instead of a for loop
  for(i in 1:length(full_time)){ #this loop looks for matching dates and extracts data from ccrmetdata file to obs4 array
    index = which(ccrmetdata$TIMESTAMP==full_time[i])
    if(length(index)>0){
      obs4[i,] <- unlist(ccrmetdata[index,c(1,2,3,5,8,9,10,11,12,13,14,15,16)])
    }
  }
  #obs4=merge(full_time,ccrmetdata, all.x=TRUE)#merge the data frame to get the last 7 days
  obs4<-as.data.frame(obs4) #make into DF
  colnames(obs4)<-names(ccrmetdata[index,c(1,2,3,5,8,9,10,11,12,13,14,15,16)]) #get column names
  obs4$TIMESTAMP<-full_time #now have your array with a proper timedate stamp!
  
  pdf(paste0("CCRMetDataFigures_", Sys.Date(), ".pdf"), width=8.5, height=11) #call PDF file
  par(mfrow=c(3,2))
  plot(obs4$TIMESTAMP,obs4$RECORD, main="RECORD", xlab="Time", ylab="Number", type='l')
  plot(obs4$TIMESTAMP,obs4$BattV, main="Battery", xlab="Time", ylab="Volts", type='l')
  plot(obs4$TIMESTAMP,obs4$AirTC_Avg, main="Air Temp", xlab="Time", ylab="degrees C", type='l')
  plot(obs4$TIMESTAMP,obs4$RH, main="Rel Hum", xlab="Time", ylab="%", type='l')
  plot(obs4$TIMESTAMP,obs4$Rain_mm_Tot, main="Rain", xlab="Time", ylab="mm", type='l')
  plot(obs4$TIMESTAMP,obs4$WS_ms_Avg, main="Wind speed", xlab="Time", ylab="m/s",type='l')
  plot(obs4$TIMESTAMP,obs4$WindDir, main="Wind direction", xlab="Time", ylab="degrees", type='l')
  plot(obs4$TIMESTAMP,obs4$SR01Up_Avg, main="Shortwave Up", xlab="Time", ylab="W/m2",type='l')
  plot(obs4$TIMESTAMP,obs4$SR01Dn_Avg, main="Shortwave Down", xlab="Time", ylab="W/m2",type='l')
  plot(obs4$TIMESTAMP,obs4$IR01UpCo_Avg, main="Longwave Up", xlab="Time", ylab="W/m2",type='l')
  plot(obs4$TIMESTAMP,obs4$IR01DnCo_Avg, main="Longwave Down", xlab="Time", ylab="W/m2",type='l')
  plot(obs4$TIMESTAMP,obs4$PAR_Den_Avg, main="PAR", xlab="Time", ylab="umol/s/m^2",type='l')
  dev.off() #file made!
  print("CCR met file made with data")
}

#CCR water sensors. Going to have to edit when the EXOs are added

ccrwaterheader<-read.csv("ccre-waterquality.csv", skip=1, as.is=T) #get header minus wonky Campbell rows
ccrwaterdata<-read.csv("ccre-waterquality.csv", skip=4,header=F) #get data minus wonky Campbell rows
names(ccrwaterdata)<-names(ccrwaterheader) #combine the names to deal with Campbell logger formatting

ccrwaterdata=ccrwaterdata%>%
  filter(grepl("^20", TIMESTAMP))%>% #keep only right time
  distinct(TIMESTAMP, .keep_all= TRUE) #taking out duplicates

#For the time sequence we can use the same as the FCR catwalk
end.time1 <- with_tz(as.POSIXct(strptime(Sys.time(), format = "%Y-%m-%d %H")), tzone = "Etc/GMT+5") #gives us current time with rounded hours in EDT
start.time1 <- end.time1 - days(7) #to give us seven days of data for looking at changes
full_time1 <- as.data.frame(seq(start.time1, end.time1, by = "10 min")) #create sequence of dates from past 5 days to fill in data
colnames(full_time1)=c("TIMESTAMP") #make it a data frame to merge to make obs5 later

#obs5 <- array(NA,dim=c(length(full_time1),41)) #create array that will be filled in with 41 columns (the entire size of the array)
#cat_timechange=max(which(ccrwaterdata$TIMESTAMP=="2019-04-15 10:00:00"))
ccrwaterdata$TIMESTAMP<-as.POSIXct(strptime(ccrwaterdata$TIMESTAMP, "%Y-%m-%d %H:%M"), tz = "Etc/GMT+5") #get dates aligned
#ccrwaterdata$TIMESTAMP[c(1:cat_timechange-1)]<-with_tz(force_tz(ccrwaterdata$TIMESTAMP[c(1:cat_timechange-1)],"Etc/GMT+4"), "Etc/GMT+5") #pre time change data gets assigned proper timezone then corrected to GMT -5 to match the rest of the data set

if (length(na.omit(ccrwaterdata$TIMESTAMP[ccrwaterdata$TIMESTAMP>start.time1]))<2) { #if there is no data after start time, then a pdf will be made explaining this
  pdf(paste0("CCRWaterQualityDataFigures_", Sys.Date(), ".pdf"), width=8.5, height=11) #call PDF file
  plot(NA, xlim=c(0,5), ylim=c(0,5), bty='n',xaxt='n', yaxt='n', xlab='', ylab='') #creates empty plot
  mtext(paste("No data found between", start.time1, "and", end.time1, sep = " ")) #fills in text in top margin of plot
  dev.off() #file made!
  print("CCR WQ file made with no data")
} else {
  
  obs5 <- merge(full_time1,ccrwaterdata, all.x = TRUE)#merge the data frame to get the last 7 days
  
  obs5$Depth_m=obs5$Lvl_psi*0.70455 #converts pressure to depth
  
  pdf(paste0("CCRWaterQualityDataFigures_", Sys.Date(), ".pdf"), width=8.5, height=11) #call PDF file
  par(mfrow=c(3,2))
  
  plot(obs5$TIMESTAMP,obs5$BattV, main="Campbell Logger Battery", xlab="Time", ylab="Volts", type='l')
  #Going to add these back in when the EXos are in
  #added y limits so the axises would show up when the are no data
  plot(obs5$TIMESTAMP,obs5$EXO_battery_9, main="EXO Battery", xlab="Time", ylab="Volts", type='l',lwd=1.5,  ylim=c(2,8))
  points(obs5$TIMESTAMP, obs5$EXO_battery_1, col="red", type='l', lwd=1.5 )
  legend("topleft", c("1.5m EXO", "9m EXO"), text.col=c("red","black"), x.intersp=0.001)
  
  plot(obs5$TIMESTAMP,obs5$EXO_cablepower_9, main="EXO Cable Power", xlab="Time", ylab="Volts", type='l',lwd=1.5, ylim=c(10,15))
  points(obs5$TIMESTAMP, obs5$EXO_cablepower_1, col="red", type='l', lwd=1.5 )
  legend("topleft", c("1.5m EXO", "9m EXO"), text.col=c("red","black"), x.intersp=0.001)
  
  plot(obs5$TIMESTAMP,obs5$EXO_depth_9, main="EXO Depth", xlab="Time", ylab="Meters", type='l', ylim=c(0,11))
  points(obs5$TIMESTAMP, obs5$EXO_depth_1, col="red", type="l", lwd=1.5)
  legend("topleft", c("1.5m EXO", "9m EXO"), text.col=c("red","black"), x.intersp=0.001)
  
  plot(obs5$TIMESTAMP, obs5$Depth_m, main="Depth of Bottom sensor
       (Does not reflect actual water level)", xlab="Time", ylab="Meters",type='l')
  
  plot(obs5$TIMESTAMP, obs5$EXO_wiper_9, main= "Wiper Voltage", xlab="Time", ylab="Volts", type='l', ylim=c(0,3))
  points(obs5$TIMESTAMP, obs5$EXO_wiper_1, col="red", type="l", lwd=1.5)
  legend("topleft", c("1.5m EXO", "9m EXO"), text.col=c("red","black"), x.intersp=0.001)

  # add in the depth of the EXO. Good thing to check as the water level drops
  plot(obs5$TIMESTAMP, obs5$EXO_depth_1, main= "Surface EXO depth", xlab="Time", ylab="Meters", type='l')
  
  plot(obs5$TIMESTAMP,obs5$EXO_pressure_9, main="Sonde Pressure", xlab="Time", ylab="psi", type='l', ylim=c(-1,37))
  points(obs5$TIMESTAMP, obs5$EXO_pressure_1, col="purple", type="l", lwd=1.5)
  points(obs5$TIMESTAMP, obs5$Lvl_psi, col="blue4", type='l', lwd=1.5)
  legend("topleft", c("1.5m EXO", "9m EXO", "19m PT"), text.col=c("purple","black", "blue4"), x.intersp=0.001)
  
  
  plot(obs5$TIMESTAMP,obs5$doobs_9, main="DO", xlab="Time", ylab="mg/L", type='l', col="medium sea green", lwd=1.5, ylim=c(-0.5,15))
  points(obs5$TIMESTAMP,obs5$doobs_1, main="DO", xlab="Time", ylab="mg/L", type='l', col="magenta", lwd=1.5)
  legend("topleft", c("1.5m EXO", "9m EXO"), text.col=c("magenta", "medium sea green"), x.intersp=0.001)
  
  plot(obs5$TIMESTAMP,obs5$dosat_9, main="DO % saturation", xlab="Time", ylab="% saturation", type='l', col="medium sea green", lwd=1.5, ylim=c(-0.5,170))
  points(obs5$TIMESTAMP, obs5$dosat_1, col="magenta", type='l', lwd=1.5)
  legend("topleft", c("1.5m EXO", "9m EXO"), text.col=c("magenta", "medium sea green"), x.intersp=0.001)
  
  plot(obs5$TIMESTAMP,obs5$Cond_9, main="Cond, SpCond, TDS @ 1.5m and 9m", xlab="Time", ylab="uS/cm or mg/L", type='l', col="red", lwd=1.5, ylim=c(20,130))
  points(obs5$TIMESTAMP, obs5$Cond_1, col="magenta", type="l", lwd=1.5)
  points(obs5$TIMESTAMP, obs5$SpCond_9, col="black", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$SpCond_1, col="gray", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$TDS_9, col="orange", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$TDS_1, col="DarkOrange1", type="l", lwd=1.5)
  legend("topleft", c("TDS 1m", "TDS 9m", "SPCond 1m","SpCond 9m", "Cond 1m","Cond 9m"), text.col=c("DarkOrange1", "orange", "gray", "black", "magenta","red"), x.intersp=0.001)
  
  #
  plot(obs5$TIMESTAMP,obs5$Chla_1, main="Chla, Phyco, fDOM", xlab="Time", ylab="ug/L or QSU", type='l', col="green", lwd=1.5, ylim=c(-0.5,40))
  points(obs5$TIMESTAMP, obs5$BGAPC_1, col="blue", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$fDOM_QSU_1, col="firebrick4", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$fDOM_QSU_9, col="DarkOrange3", type="l", lwd=1.5)
  points(obs5$TIMESTAMP, obs5$Turbidity_FNU_1, col="brown", type='l', lwd=1.5)
  legend("topleft", c("Chla 1.5m", "Phyco 1.5m", "fDOM 1.5m", "fDOM 9m", "Turbidity 1.5m"), text.col=c("green", "blue", "firebrick4", "DarkOrange3", "brown"), x.intersp=0.001)
  
  par(mfrow=c(1,1))
  par(oma=c(1,1,1,4))
  plot(obs5$TIMESTAMP,obs5$wtr_1, main="Water Temp", xlab="Time", ylab="degrees C", type='l', col="firebrick4", lwd=1.5, ylim=c(0, 35))
  points(obs5$TIMESTAMP, obs5$wtr_2, col="firebrick1", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$wtr_3, col="DarkOrange1", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$wtr_4, col="gold", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$wtr_5, col="greenyellow", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$wtr_6, col="medium sea green", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$wtr_7, col="sea green", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$wtr_8, col="DeepSkyBlue4", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$wtr_9, col="blue2", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$EXO_wtr_9, col="blue4", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$wtr_10, col="darkslateblue", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$wtr_11, col="magenta2", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$wtr_12, col="darkmagenta", type='l', lwd=1.5)
  points(obs5$TIMESTAMP, obs5$wtr_13, col="black", type='l', lwd=1.5)
  par(fig=c(0,1,0,1), oma=c(0,0,0,0), mar=c(0,0,0,0), new=T)
  plot(0, 0, type = "n", bty = "n", xaxt = "n", yaxt = "n")
  legend("right",c("0.1m","1m", "2m", "3m", "4m", "5m", "6m", "7m", "8m","EXO_9m","10m","11m","15m","19m"),
         text.col=c("firebrick4", "firebrick1", "DarkOrange1", "gold", "greenyellow", "medium sea green", "sea green",
                    "DeepSkyBlue4", "blue2", "blue4", "darkslateblue", "magenta2", "darkmagenta", "black"),
         cex=1, y.intersp=1, x.intersp=0.001, inset=c(0,0), xpd=T, bty='n')
  
  dev.off() #file made!
  print("CCR WQ file made with data")
}

### EddyFlux plots
# Let's make the QAQC plots for streaming EddyFlux files

# Read in the data file
eflux<-read.csv("Eddyflux.csv") 

# Make the datetime column
eflux$datetime <- ymd_hms(paste0(eflux$date," ",eflux$time), tz="America/New_York")


eflux <- eflux%>%
  filter(grepl("^20", datetime))%>% #keep only the right TIMESTAMP rows 
  distinct(datetime, .keep_all= TRUE) #taking out the duplicate values 

end.time3 <- with_tz(as.POSIXct(strptime(Sys.time(), format = "%Y-%m-%d %H")), tzone = "America/New_York") #gives us current time with rounded minutes in EDT
start.time3 <- end.time3 - days(7) #to give us seven days of data for looking at changes
full_time3 <- as.data.frame(seq(start.time3, end.time3, by = "30 min")) #create sequence of dates from past 7 days to fill in data and make it a dataframe
colnames(full_time3)=c("datetime") #make it a data frame to merge to make obs1 later


if (length(na.omit(eflux$datetime[eflux$datetime>start.time3]))==0) { #if there is no data after start time, then a pdf will be made explaining this
  pdf(paste0("CCRE_EddyFluxDataFigures_", Sys.Date(), ".pdf"), width=8.5, height=11) #call PDF file
  plot(NA, xlim=c(0,5), ylim=c(0,5), bty='n',xaxt='n', yaxt='n', xlab='', ylab='') #creates empty plot
  mtext(paste("No data found between", start.time2, "and", end.time2, sep = " ")) #fills in text in top margin of plot
  dev.off() #file made!
} else { #else, do normal data wrangling and plotting
  
  # Make the data frame for the last 7 days
  obs6 <- merge(full_time3,eflux, all.x = TRUE)#merge the data frame to get the last 7 days
  
  
  # Plots
  pdf(paste0("CCRE_EddyFluxDataFigures_", Sys.Date(), ".pdf"), width=8.5, height=11) #call PDF file
  
  #Create the first page
  plot(0:10, type = "n", xaxt="n", yaxt="n", bty="n", xlab = "", ylab = "")
  
  text(5, 8, "EC Data Fluxes in Carvins Cove Reservoir, Roanoke, VA")
  text(5, 7, paste0("Start: ",obs6[1,'date']))
  
  
  # Now the plots for the first page
  p1=obs6 %>% 
    ggplot(aes(datetime, ch4_flux_umolm2s)) + geom_point() + geom_line() + theme_bw()
  
  p2=obs6 %>% 
    ggplot(aes(datetime, co2_flux_umolm2s)) + geom_point() + geom_line() + theme_bw()
  
  # CO2 and CH4 signal strength
  
  p3=obs6 %>% 
    ggplot(aes(datetime, rssi_77_mean)) + geom_point() + geom_line() +
    ylab("CH4 signal strength (%)")+
    theme_bw()
  
  #CO2 signal changes when took out the 7200
  if("co2_signal_strength_7200_mean" %in% names(obs6)) {
    p4=obs6 %>% 
      ggplot(aes(datetime, co2_signal_strength_7200_mean)) + geom_point() + geom_line() +
      ylab("CO2 signal strength (%)")+
      theme_bw()
  } else {
    p4=obs6 %>% 
      ggplot(aes(datetime, co2_signal_strength_7500_mean)) + geom_point() + geom_line() +
      ylab("CO2 signal strength (%)")+
      theme_bw()
  }
  
  #Smartflux voltage in 
  p5=obs6 %>% 
    ggplot(aes(datetime, vin_sf_mean)) + geom_point() +
    theme_bw() +
    geom_line() +
    xlab(" ") +
    ylab('Smartflux voltage (V)') +
    scale_x_datetime(date_breaks = "1 days", date_labels = "%m-%d") 
  theme(axis.text.x = element_text(size = 14, color = "black", angle = 60, hjust = 1),
        axis.text.y = element_text(size = 14, color = "black"),
        axis.title = element_text(size = 16, color = "black"))
  
  grid.arrange(p1,p2,p3,nrow=3)
  
  # plot ustar distribution, wind speed, windrose and temperature
  # Create a histogram of u*
  p6=obs6 %>% 
    ggplot(aes(x=u_star_ms)) + geom_histogram()+
    geom_histogram(color="black", fill="white")+
    xlab('u star (m s^-1)')+
    theme_bw()
  
  # Wind Speed U,V,W
  p7=obs6 %>% 
    ggplot(aes(datetime, u_var_ms)) + geom_point() + geom_line() +
    ylab("U (m/s)")+
    theme_bw()
  p8=obs6 %>% 
    ggplot(aes(datetime, v_var_ms)) + geom_point() + geom_line() +
    ylab("V (m/s)")+
    theme_bw()
  p9=obs6 %>% 
    ggplot(aes(datetime, w_var_ms)) + geom_point() + geom_line() +
    ylab("W (m/s)")+
    theme_bw()
  
  grid.arrange(p6,p7,p8,p9,nrow=4)
  
  # Visualize wind directions that 
  chicago_wind=obs6%>%
    select(datetime,wind_speed_ms,wind_dir)%>%
    dplyr::rename(date = datetime, ws = wind_speed_ms, wd = wind_dir)
  pollutionRose(chicago_wind, pollutant="ws")
  
  # Sonic Temperature
  p10=obs6 %>% mutate(sonicC=sonic_temperature_k-273.15)%>%
    ggplot(aes(datetime, sonicC)) + geom_point() + geom_line() +
    ylab("Sonic Temperature (Degrees C)")+
    theme_bw()
  
  # H and LE
  p11=obs6 %>% 
    ggplot(aes(datetime, H_wm2)) + geom_point() + 
    ylab("Sensible heat flux (W m^(-2))")+
    theme_bw()
  
  p12=obs6 %>% 
    ggplot(aes(datetime, LE_wm2)) + geom_point() +
    ylab("Latent heat flux (W m^(-2))")+
    theme_bw()
  
  grid.arrange(p11,p12,nrow=2)
  
  # Flow Rate
  if("flowrate_mean" %in% names(obs6)) {
    p13=obs6 %>%
      ggplot(aes(datetime, flowrate_mean*60000)) + geom_point() +
      theme_bw() +
      geom_line() +
      xlab(" ") +
      ylab('Flowrate (L min-1)') 
  } else {
    p13=obs6%>%
      ggplot(aes(datetime,60000))+geom_blank()+theme_bw()+
      labs(title="CO2 sensor is out for service and there is no flowrate")
  }
  grid.arrange(p10,p13,nrow=2)
  
  dev.off()
  print("EddyFlux file made")
}

