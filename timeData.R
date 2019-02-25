library(lubridate)
library(xlsx)
library(chron) 


dateFirst = 11  # JUST CHANGE THE FIRST DATE (FOR 5 DAYS)

if (dateFirst < 10) {
  FirstDay = paste0('0', as.character(dateFirst))
} else { FirstDay = as.character(dateFirst) }


cpuCount1 = function(file){
  ## read data ##
  cpu = read.csv(file)
  data = cpu[,c(1,2)]
  
  data[,1] = as.character(data[,1])
  data[,1] = as.POSIXct(strptime(data[,1], "%Y-%m-%d-%H:%M:%S"))
  
  
  ## preprocessing ##
  data[,2] = as.numeric(gsub("us", "", data[,2]))
  data[which(is.na(data[,2]) ==T),2] = 100
  
  h1 = hour(data[,1][1])
  h2 = hour(data[,1][dim(data)[1]])
  
  ## Max & Avg ##
  usageMax = c()
  usageAvg = c()
  
  for(j in h1:h2){
    for (i in 0:59){
      usageMax = append(usageMax, max( data[which(hour(data[,1]) == j & minute(data[,1]) == i),2] ) )
      usageAvg = append(usageAvg, mean( data[which(hour(data[,1]) == j & minute(data[,1]) == i),2] ) )
    }
  }
  
  which(is.na(usageMax) ==T)
  usageMax = usageMax[usageMax != -Inf]
  which(is.na(usageAvg) !=T)
  usageAvg = usageAvg[which(is.na(usageAvg) !=T)]
  
  start = as.POSIXct(strptime(paste0(date(data[,1][1]),' ',hour(data[,1][1]),':',minute(data[,1][1])), "%Y-%m-%d %H:%M"))
  
  diff = as.difftime("00:01:00","%H:%M:%S",units="hour")
  tt = start
  
  c1 = c()

  for (i in 1:length(usageMax)){
    tt = tt + diff
    start = append(start,tt)
    c1 = append(c1,times(strftime(format(tt),"%H:%M:%S")))
  }
  
  outputFormat = data.frame(c1,usageMax,round(usageAvg,2))
  colnames(outputFormat) = c('time','usageMax','usageAvg')
  outputFormat[,1] = as.character(outputFormat[,1])
  
  dat = date(data[,1][1])
  filename = paste0('C:/Users/Jacky/Desktop/work/cpu/usage.xlsx')
  
  write.xlsx(outputFormat, file=filename, sheetName=as.character(dat), row.names=FALSE)
}
cpuCount2 = function(file){
  ## read data ##
  cpu = read.csv(file)
  data = cpu[,c(1,2)]
  
  data[,1] = as.character(data[,1])
  data[,1] = as.POSIXct(strptime(data[,1], "%Y-%m-%d-%H:%M:%S"))
  
  
  ## preprocessing ##
  data[,2] = as.numeric(gsub("us", "", data[,2]))
  data[which(is.na(data[,2]) ==T),2] = 100
  
  h1 = hour(data[,1][1])
  h2 = hour(data[,1][dim(data)[1]])
  
  ## Max & Avg ##
  usageMax = c()
  usageAvg = c()
  
  for(j in h1:h2){
    for (i in 0:59){
      usageMax = append(usageMax, max( data[which(hour(data[,1]) == j & minute(data[,1]) == i),2] ) )
      usageAvg = append(usageAvg, mean( data[which(hour(data[,1]) == j & minute(data[,1]) == i),2] ) )
    }
  }
  
  which(is.na(usageMax) ==T)
  usageMax = usageMax[usageMax != -Inf]
  which(is.na(usageAvg) !=T)
  usageAvg = usageAvg[which(is.na(usageAvg) !=T)]
  
  start = as.POSIXct(strptime(paste0(date(data[,1][1]),' ',hour(data[,1][1]),':',minute(data[,1][1])), "%Y-%m-%d %H:%M"))
  
  diff = as.difftime("00:01:00","%H:%M:%S",units="hour")
  tt = start
  
  c1 = c()
  
  for (i in 1:length(usageMax)){
    tt = tt + diff
    start = append(start,tt)
    c1 = append(c1,times(strftime(format(tt),"%H:%M:%S")))
  }
  
  outputFormat = data.frame(c1,usageMax,round(usageAvg,2))
  colnames(outputFormat) = c('time','usageMax','usageAvg')
  outputFormat[,1] = as.character(outputFormat[,1])
  
  dat = date(data[,1][1])
  filename = paste0('C:/Users/Jacky/Desktop/work/cpu/usage.xlsx')
  
  write.xlsx(outputFormat, file=filename, append=TRUE, sheetName=as.character(dat), row.names=FALSE)
}

file1 = paste0('C:/Users/Jacky/Desktop/work/cpu/loaddata/cpu-2019-02-',FirstDay,'.csv')
cpuCount1(file1)


for (i in 1:4){
  dateFirst = dateFirst + 1
  if (dateFirst < 10) {
    FirstDay = paste0('0', as.character(dateFirst))
  } else { FirstDay = as.character(dateFirst) }
  
  file2 = paste0('C:/Users/Jacky/Desktop/work/cpu/loaddata/cpu-2019-02-',FirstDay,'.csv')
  cpuCount2(file2)
}
