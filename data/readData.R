# setup odbc connections

#------------ Method 1 ------------- using DSN
# https://sqlserverrider.wordpress.com/2015/09/25/connect-to-sql-server-in-r-without-odbc-dns-connection/
# https://sqlserverrider.wordpress.com/2015/09/20/connect-to-sql-server-in-r/
## need to create the DNS in the Control Panel - ODBC
library(RODBC)
con <- odbcConnect("GHI_EDWStage")
data <- sqlQuery(con, 'select * from StagingDB.ODS.stgQDXAppealsMaster')
odbcClose(con)

con <- odbcConnect("ODSProd01_Quadax")
data <- sqlQuery(con, "select * from dbo.appealSuccess")
odbcClose(con)



#------------ Method 2 ------------- using RODBC: odbcDriverConnect
library(RODBC)

con <-odbcDriverConnect(connection = "Driver={ODBC Driver 13 for SQL Server};trusted_connection=yes;
                       server=EDWStage;
                       database=StagingDB")
data <- sqlQuery(con, 'select * from ODS.stgQDXAppealsMaster')
odbcClose(con)

con <-odbcDriverConnect(connection = "Driver={ODBC Driver 13 for SQL Server};trusted_connection=yes;
                       server=EDWStage;
                       database=StagingDB")
data <- sqlQuery(con, 'select * from dbo.dimCaseType')
odbcClose(con)


#------------ Method 3 ------------- using DBI: dbConnect
# DBI works with the connection pane in Rstudio
library(DBI)
con <- dbConnect(odbc::odbc(),.connection_string="Driver={ODBC Driver 13 for SQL Server};trusted_connection=yes;
                server=EDWStage;
                database=EDWDB")
data <- dbGetQuery(con, 'select * from EDWDB.dbo.dimCaseType')




## read data from SQL Server
test_sql <- "SELECT * FROM Enum.tblFinancialCategoryEnum
where LocalEnumID <10"


test_sql1 <- {"select *
  from Analytics.mvwRevenue
  where OrderLineItemID in
  (SELECT OrderLineItemID
  from Analytics.stgOrderDetail
  where OrderstartDate >= '2015-1-1'
  and OrderstartDate <= '2015-1-31'
  )
  "}

mydata <- sqlQuery(con, test_sql)



# put the sql in a file
# read into memory
sql <- paste(readr::read_lines("./data/StagingBD_CaseStatus.sql"),collapse="\n")
mydata <- sqlQuery(con, sql)

library(RODBC)
con <-odbcDriverConnect(connection = "Driver={ODBC Driver 13 for SQL Server};trusted_connection=yes;
                       server=EDWStage;
                       database=EDWDB")
a <- trimws(readr::read_lines("./data/EDWDB_fctRevenue.sql"))
sql <- paste(trimws(readr::read_lines("./data/EDWDB_fctRevenue.sql")), collapse="\n")
mydata <- sqlQuery(con, sql)



## read a csv file from the web
getData <- function(url) {
  data <- read.csv(url,header=TRUE, sep=",")
}


