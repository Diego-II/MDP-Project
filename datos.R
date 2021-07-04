data<-read.csv("/Users/jroallanos/Desktop/AZURE/MDP-Project/countries.csv",header = TRUE)

## ordenar por conteo 
data<-data[order(-data$count),]
row.names(data)<-NULL
data<-data[-c(8),]

# graficar numero significativo de respuestas por pais
nrow(data[(data$count>200),])
# hay 100 paises con mas de 200 rptas.
data100<-data[(data$count>=198),]


library(ggplot2)
data50<-data[(data$count>=1340),]
data50$Country <- factor(data50$Country, levels = data50$Country[order(-data50$count)])
data50$Country  # notice the changed order of factor levels
ggplot(data=data50, aes(x=Country,y=count))+geom_bar(stat='identity')+theme_bw()+
  ggtitle("N° of responses by country")+
  ylab("Responses")+
  scale_y_continuous(breaks = seq(0, 550000, 50000))+
  theme(plot.title = element_text(hjust = 0.5,size=20),
        axis.text=element_text(size=10),
        axis.title=element_text(size=14),
#        legend.title = element_text(size = 12),
#        legend.text = element_text(size=12)
  )
  
