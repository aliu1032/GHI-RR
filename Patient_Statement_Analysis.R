
library(rmarkdown)
library(dplyr)

setwd("C:/Users/aliu/Documents/R_workspace")


QDX_filepath = "C:/Users/aliu/Box Sync/aliu Cloud Drive/Analytics/Payor Analytics/QDX USD-Jul12/"

txStmn_file = "txStmntInfo.txt"
stmn_file = 'stmntInfo.txt'


#####https://www.stat.berkeley.edu/classes/s133/factors.html


## read the statement data
'''{r data}
stmn_data = read.csv(paste(QDX_filepath,stmn_file, sep=''), header=TRUE, sep="|")
summary(stmn_data)
subset(stmn_data,StmntAcctNum=c("PT000922668","PT000927684"))

stmnt_DF <- data.frame(stmn_data)
stmnt_DF()

txStmnt_data = read.csv(paste(QDX_filepath,txStmn_file, sep=''), header=TRUE, sep="|")
'''