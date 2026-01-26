# New CCR viz for the managers
# Author: A.Breef-Pilz
# Written: 21 Oct. 2025
# Edit:
# 11 Jan 2026 - added a filter to the L1 data for only the current year. 
# 26 Jan 2026 - updated the EDI link with 2025 data. 

# These are plots made by A. Breef-Pilz with the help of Eric Powers from the WVWA. This script uses the L1 files and the EDI published files. The link to the EDI data needs to be updated each year after the data are published. 

# 1. Last figure have the temp profiles but remove sensors when they are out of the water and change the legend to elevation

# 2. Relate the water level sensor to elevation based on a relationship between the WVWA sensor and our pressure sensor

# 3. Create yearly plots for 9m DO, 9m fDOm, and 1.5m chla with the daily average over the historic daily max and the historic daily min

# Continue even if there was an error
continue_on_error <- function()
{
  print("ERROR! CONTINUING WITH THE REST OF THE SCRIPT ...")
}

options(error=continue_on_error)



# load packages#loading packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse, plotly, forcats, gridExtra, ggpubr)


# Read in the L1 file

ccr_L1 <- read_csv("https://raw.githubusercontent.com/FLARE-forecast/CCRE-data/refs/heads/ccre-dam-data-qaqc/ccre-waterquality_L1.csv")|>
  filter(year(DateTime) %in% year(Sys.Date())) # filter out for the current year before EDI data are published. 



# historical EDI data- have to update each year with the new link. 

EDI_data <- read_csv("https://pasta.lternet.edu/package/data/eml/edi/1069/4/42e6d8bb3d379d40a4a4fb566d4ff36e")

# internet slow downloaded and read in data
#EDI_data <- read_csv("ccre-waterquality_2021_2024.csv")


# Make the data frame of the current observations

DOY_current <- ccr_L1|>
  select(DateTime, EXOChla_ugL_1, EXOTurbidity_FNU_1, EXOSpCond_uScm_9, EXODO_mgL_9, EXOfDOM_QSU_9, LvlPressure_psi_13)|>
  filter(EXOSpCond_uScm_9<5000)|>
  mutate(Date = as.Date(DateTime))|>
  group_by(Date)|>
  summarise(across(EXOChla_ugL_1:LvlPressure_psi_13, \(x) mean(x, na.rm = TRUE),.names = "mean_{.col}"))|>
  ungroup()|>
  mutate(DOY = yday(Date),
         # the relationship is is Elevation = (LvlPressure *2.34) + 1104.85. Did the calculations but not putting it here.       
         Elevation_ft = (mean_LvlPressure_psi_13 * 2.34)+1104.85,
         ddate = as.Date(DOY-1)) # make the DOY into a date but it is off by 1 so subtract and then convert it into a date format. This way they all have the same year and will stack on top of one another)


# relate pressure to historical water level observations 

# Make the plot. Clean up aesthetics later
Press_plot <- ggplot(DOY_current, aes(x = Date, y = Elevation_ft))+
  geom_line(linewidth = 1.5, lineend = "round")+
  scale_y_continuous(limits = c(1120, 1180),
                     n.breaks = 6) + 
  scale_x_date(date_breaks = "month", date_labels = "%b") +
  ggtitle("Reservoir Level")+
  labs(x = "")+
  theme_bw(base_size = 18) +
  theme(plot.title = element_text(hjust = 0.5))

Press_plot

# Make the plots of historical min and max over current observation

# clean up the data frame and take only the varaibles we want and get daily average

hist_ccr <- EDI_data|>
  select(any_of (c("DateTime", "EXOChla_ugL_1", "EXOTurbidity_FNU_1", "EXOSpCond_uScm_9" ,"EXODO_mgL_9", "EXOfDOM_QSU_9")))|>
  mutate(Date = as.Date(DateTime))|>
  group_by(Date)|>
  summarise(across(EXOChla_ugL_1:EXOfDOM_QSU_9, \(x) mean(x, na.rm = TRUE),.names = "mean_{.col}"))|>
  ungroup()|>
  mutate(DOY = yday(Date))|>
  group_by(DOY)|>
  summarise(across(starts_with("mean"), list(max = ~max(.x, na.rm = TRUE), min = ~min(.x, na.rm = TRUE)), .names = "{.col}_{.fn}"))|>
  ungroup()|>
  mutate(across(everything(), ~ replace(., is.infinite(.), NA)))
  # pivot_longer(-DOY, 
  #              #names_to = "sensor",
  #              names_to = c("mean", "sensor", "units", "depth", "tendency"),
  #              names_sep = "_",
  #              values_to = "obs")



# Make the Plots

# Function to make plots with the gray ribbon of historical min and max and current years daily observation. 
max_min_plot <- function(
    current_data = DOY_current,
    historical_data = hist_ccr,
    variable,
    col,
    title,
    ylabel
    
  ){
  
# check if there are historical observations for the variable if not then don't have the historical max and min ribbon 
     
  if(paste0(variable,"_max") %in% colnames(historical_data)){
    Hist = T
  } else {
    Hist = F
  }
  
 # Check to make sure there are observations for the label if not take the previous observation
  
  label_data <- tail(current_data, n = 1)
  
  if(label_data[[variable]] == "NaN"){
    label_data <- current_data[nrow(current_data)-1,]
  }
  
  # make the plot  
  plot <- ggplot()+
    geom_label( 
      data=label_data, # data from above and just the most recent non NA values
      aes(x = as.Date(DOY-1), y = .data[[variable]], label=round(.data[[variable]], digits = 1)), nudge_x = 17
    ) +
    geom_point(data =current_data, aes(x = as.Date(DOY-1), y = .data[[variable]]), color = col, size = 2) +
    {if(Hist)geom_ribbon(data = historical_data, aes(x = as.Date(DOY-1), ymax = .data[[paste0(variable,"_max")]] , ymin = .data[[paste0(variable,"_min")]]), alpha = 0.3, linejoin = "round")} +
    scale_x_date(date_breaks = "month", date_labels = "%b", expand = c(0, 0), limits = c(as.Date("1970-01-01"), as.Date("1970-12-31"))) +
    ylab(ylabel)+ 
    xlab("")+
    ggtitle(title)+
    theme_bw(base_size = 15) +
    theme(plot.title = element_text(hjust = 0.5)) 
  
  return(plot)
}

# chla plot made
chla <- max_min_plot(
  variable = "mean_EXOChla_ugL_1", 
  col = "green", 
  title = "Average daily Chlorophyll a (ug/L) at 
  3 ft. below the surface", 
  ylabel = "micorgram/liter")

# DO plot made
do <- max_min_plot(
  variable = "mean_EXODO_mgL_9",
  col = "blue",
  title = "Average daily dissolved oxygen (mg/L) at 9 m",
  ylabel = "milligrams/liter")
    
# fdom

fdom <- max_min_plot(
  variable = "mean_EXOfDOM_QSU_9",
  col = "brown",
  title = "Average daily organic matter at 9 m",
  ylabel = "parts per a billion")

# turbidity

turb <- max_min_plot(
  variable = "mean_EXOTurbidity_FNU_1",
  col = "darkorange",
  title = "Average daily turbidity (FNU) at 9 m",
  ylabel = "Formazin Nephelometric Units")

# specific conductivity

spcond <- max_min_plot(
  variable = "mean_EXOSpCond_uScm_9",
  col = "purple",
  title = "Average daily specific conductivity (uS/cm) at 9 m",
  ylabel = "microsiemens/centimeter")


# Make the Temp plots with ggplot

# make sure they are all numeric columns

# make data frame that is just the last 7 days

# Get the last 7 days of temp

All_temp<-ccr_L1|>
  #filter(DateTime > Sys.Date()- days(15))|>
  select(DateTime, starts_with("Ther"), starts_with("EXOTemp"))|>
  #mutate(across(wtr_1:EXO_wtr_9, as.numeric))|>
  pivot_longer(-c(DateTime), names_to="Sensor", values_to="Reading", values_drop_na=TRUE)

# Rename the legends to be intakes

# 1170ft is full pond
# 1166.7 is Thermistor 2
# 1163.4 is Thermistor 3
# 1160.1 is Thermistor 4
# 1156.8 is Thermistor 5
# 1153.5 is Thermistor 6
# 1150.2 is Thermistor 7
# 1146.9 is Thermistor 8
# 1143.6 is Thermistor 9 
# 1140 ft is 9m EXO 
# 1137 is Thermistor 10
# 1133.7 is Thermistor 11
# 1120.5 is Thermistor 12
# 1110.6 is Thermistor 13

# make a data frame of thermistor and elevation

elevation <- data.frame(
  Sensor = c("ThermistorTemp_C_1", "ThermistorTemp_C_2", "EXOTemp_C_1", "ThermistorTemp_C_3", "ThermistorTemp_C_4","ThermistorTemp_C_5","ThermistorTemp_C_6","ThermistorTemp_C_7","ThermistorTemp_C_8","ThermistorTemp_C_9","EXOTemp_C_9","ThermistorTemp_C_10","ThermistorTemp_C_11","ThermistorTemp_C_12","ThermistorTemp_C_13"),
  Elevation_ft = c(1170,1166.7, 1165.1, 1163.4, 1160.1, 1156.8, 1153.5, 1150.2, 1146.9, 1143.6, 1140, 1137, 1133.7, 1120.5, 1110.6),
  stringsAsFactors = FALSE
)

# Merge the data frame to add elevation

All_temp_elv <- merge(All_temp, elevation, by = "Sensor")


 temp_15 <- All_temp_elv|>
  filter(DateTime > Sys.Date()- days(15))|>
  ggplot(aes(x=DateTime, y = Reading, color = as.factor(Elevation_ft)))+
  geom_line(linewidth = 0.75)+
  guides(color = guide_legend(reverse = TRUE, ncol = 2)) +
  scale_y_continuous(limits = c(0, 40)) + 
  scale_x_datetime(date_breaks = "3 day", date_labels = "%b %d", date_minor_breaks = "1 day") +
  labs(color='Elevation') +
  ylab("Degrees Celsius")+ # probably want fahrenheit
  xlab("")+
  ggtitle("Water Temperature Profile for the last 15 days")+
  theme_bw(base_size = 14) + #size of all the text in the figure.
  theme(plot.title = element_text(hjust = 0.5))

 temp_15
 
 # Yearly temp plots. Figure out the legends
 
 temp <- All_temp_elv|>
   #filter(DateTime > Sys.Date()- days(15))|>
   filter(Reading !=0)|>
   # make daily average
   mutate(Date = as.Date(DateTime))|>
   group_by(Date, Sensor, Elevation_ft)|>
   summarise(mean_temp = mean(Reading, na.rm =T))|>
   ungroup()|>
   ggplot(aes(x=Date, y = mean_temp, color = as.factor(Elevation_ft)))+
   geom_line(linewidth = 0.75)+
   guides(color = guide_legend(reverse = TRUE, ncol = 2))+
   scale_y_continuous(limits = c(0, 40)) + 
   scale_x_date(date_breaks = "month", date_labels = "%b") +
   labs(color='Elevation') +
   ylab("Degrees Celsius")+ # probably want fahrenheit
   xlab("")+
   ggtitle("Water Temperature Profile")+
   theme_bw(base_size = 14) + #size of all the text in the figure.
   theme(plot.title = element_text(hjust = 0.5)) 
 
 temp
 
# Arrange the plots and save the pdf on multiple pages
 
 multi.page <- ggarrange(Press_plot, chla, do, fdom, turb, spcond, temp_15, temp,
                         nrow = 2, ncol = 1)
 
# temp2 <- list(ggarrange(temp_15,temp, nrow = 2, ncol = 1))
 
 # list all the plots
# multi.page2  <- append(multi.page, temp2) # Appends 4 to the end
 
 #save the pdf
 ggexport(multi.page, filename = paste0("CCRWQ_Managers_DataFigures_", Sys.Date(), ".pdf"))
 
print("CCR Managers WQ files made with L1 files and EDI files")
# Need to fix the find_depths function and hopefully take out when the sensor is in the air. 
 
