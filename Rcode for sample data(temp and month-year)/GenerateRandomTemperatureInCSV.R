k <- 1
Months <- c("Jan","Feb","March","April","May","June","July","Aug","Sept","Oct","Nov","Dec")

for (k in 1:length(Months)){

  data <- read.csv ("/home/puneet/Documents/Hadoop-CensusData/finalData/data.csv", sep=",", header=T)
  colNames <- names(data)
  colNames[8:9] <- c("Min Temp","Max Temp")
  
  
  # Generating random Min and Max temperature
  minTemp <- round(runif (1000, 1.0, 25.0), 2)
  maxTemp <- round(runif (1000, 25.1, 50.0), 2)
  
  data[,8] <- minTemp
  data[,9] <- maxTemp
  
  colnames(data) <- colNames
  
  #i = 1 
  #j = 1000
  
  
  fileName <- paste("/home/puneet/Documents/Hadoop-CensusData/finalData/NewFiles/","2014_",Months[k],sep="")
  write.csv(data,file=fileName)
 # k <- k+1
#  i <- i+1000
#  j <- j+1000
}