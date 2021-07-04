data<-read.csv("/Users/jroallanos/Desktop/AZURE/MDP-Project/countries.csv",header = TRUE)

## ordenar por conteo 
data<-data[order(-data$count),]
row.names(data)<-NULL
data<-data[-c(8),]

# graficar numero significativo de respuestas por pais
nrow(data[(data$count>200),])
# hay 100 paises con mas de 200 rptas.

library(ggplot2)
data50<-data[(data$count>1405),]
data50$Country <- factor(data50$Country, levels = data50$Country[order(-data50$count)])
data50$Country  # notice the changed order of factor levels
ggplot(data=data50, aes(x=Country,y=count))+geom_bar(stat='identity')+theme_bw()
