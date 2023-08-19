#Combine the manual EXO data into the streamed file and push to Annie
#by ABP 18 NOV 21

pacman::p_load("tidyverse", "lubridate","rqdatatable")

#Get up to date collated EXO file
download.file("https://raw.githubusercontent.com/CareyLabVT/ManualDownloadsSCCData/master/CCR_manual_downloads/CCR_1_5_EXO_downloads/Collated_CCR_1_5_EXO.csv","CCR_EXO.csv")

ccrwaterdata <- read_csv("ccre-waterquality.csv", col_names = T, 
                          col_types = cols(.default = col_double(), TIMESTAMP = col_character()), show_col_types = T)
EXO= read_csv("CCR_EXO.csv", col_names=T,
              col_types = cols(.default = col_double(), TIMESTAMP = col_character()), show_col_types = T)
#Take out the columns that don't match
EXO=EXO%>%
  select(-c(nLF.Cond.uS.cm, Vertical.Position.m))

#Join the CCR-waterquality data and the EXO data
CCR<- natural_join(ccrwaterdata, EXO, 
                          by = "TIMESTAMP",
                          jointype = "LEFT")

#name of the columns to get in the right order
wQnames=colnames(ccrwaterdata)

#rearrange the column headers based on the original file since they get jumbled during the join 
CCR=CCR%>%
  select(wQnames)

#Always remember to go and change the NAN to "NAN". I do this is notepad

write.csv(CCR, "ccre-waterquality.csv",na="NAN", row.names = FALSE)
