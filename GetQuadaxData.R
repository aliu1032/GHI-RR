## ------------------------------------------------------------------------

con <-odbcDriverConnect(connection = "Driver={ODBC Driver 13 for SQL Server};trusted_connection=yes;
                       server=EDWStage;
                       database=EDWDB")
a <- trimws(readr::read_lines("./data/EDWDB_fctRevenue.sql"))
sql <- paste(trimws(readr::read_lines("./data/EDWDB_fctRevenue.sql")), collapse="\n")
Revenue <- sqlQuery(con, sql)
odbcClose(con)

## ------------------------------------------------------------------------
# read the raw data from ODSProd01
stdClaim <- function (usage, folder, refresh = 0) {
    
    target <- "QDX_Claim.txt"
    
    print(paste('stdClaim : reading Claim Data ',Sys.time(), sep = " "))
    if (refresh == 1) {
        con <-odbcDriverConnect(connection = "Driver={ODBC Driver 13 for SQL Server};trusted_connection=yes;
                               server=ODSProd01;
                               database=Quadax")
        #a <- trimws(readr::read_lines("./data/QDX_stdClaim.sql"))
        sql <- paste(trimws(readr::read_lines("./data/QDX_stdClaim.sql")), collapse="\n")

        Claim <- sqlQuery(con, sql, stringsAsFactors = FALSE)
        write.table(Claim,paste(folder,target,sep=""), sep="|",row.names=FALSE)
        odbcClose(con)
    } else {
        Claim <- read.table(paste(folder,target,sep=""), sep="|", header=TRUE, stringsAsFactors = FALSE)
    }

    print(paste('stdClaim : cleaning Claim Data ',Sys.time(), sep = " "))
    
    prep_file_name <- "QDX_ClaimDataPrep.xlsx"
    claim_note <- read.xlsx(paste(prep_file_path, prep_file_name, sep=""), sheetName ="QDXClaim", startRow=2,
                            colIndex=c(2,3:6),stringsAsFactors=FALSE)

    # rename columns
    rename_col <- match(claim_note$QDX_stdClaimFile,names(Claim))
    names(Claim)[na.omit(rename_col)] <- claim_note$Synonyms[which(!is.na(rename_col))]

    
    # Translate QDX test code to GHI
    QDX_GHI_Test_Code <- data.table(QDX_Code = c('GL','GLD','GLC','MMR','GLP','LIQSEL','UNK'),
                                    GHI_Code = c('IBC','DCIS','Colon','MMR','Prostate','LIQSEL','Unknown'))
    
    for (i in seq_along(QDX_GHI_Test_Code$QDX_Code)) {
        #print (QDX_GHI_Test_Code$QDX_Code[i])
        a <- which(Claim$Test == QDX_GHI_Test_Code$QDX_Code[i])
        Claim[a,'Test'] <- QDX_GHI_Test_Code$GHI_Code[i]
    }
    # table(Claim$Test)
    #a <- Claim %>%
    #       filter(Test=='GLP') %>%
    #       select(ReRoutedPrimaryInsPlan_GHICode, PrimaryInsPlanName)
    #       View(a)
    
    # Convert the dates
    Claim$TXNDate <- ymd(Claim$TXNDate)
    Claim$OLIDOS <- ymd(Claim$OLIDOS)
    
    # set the TXNLineNumber and TXNType for TXNDetail Report
    Claim <- Claim %>%
             mutate(TXNLineNumber = 1, TXNType='CL')
    
    
    # find the Payor/plan that the Claim Ticket is issued to
    # the value should come from the ReReoutedPayor. If ReReoutedPayor is blank, take from the PrimaryPayor
    QDX_Tick_Payor_map <- data.table(Ticket_payor = c('TicketInsComp_QDXCode',
                                                       'TicketInsPlan_QDXCode',
                                                       'TicketInsCompName',
                                                       'TicketInsPlanName',
                                                       'TicketInsFC',
                                                       'TicketInsComp_GHICode',
                                                       'TicketInsPlan_GHICode'),
                                     ReRoutePrimary = c('ReRoutedPrimaryInsComp_QDXCode',
                                                        'ReRoutedPrimaryInsPlan_QDXCode',
                                                        'ReRoutedPrimaryInsCompName',
                                                        'ReRoutedPrimaryInsPlanName',
                                                        'ReRoutedPrimaryInsFC',
                                                        'ReRoutedPrimaryInsComp_GHICode',
                                                        'ReRoutedPrimaryInsPlan_GHICode'),
                                        PrimaryIns = c('PrimaryInsComp_QDXCode',
                                                       'PrimaryInsPlan_QDXCode',
                                                       'PrimaryInsCompName',
                                                       'PrimaryInsPlanName',
                                                       'PrimaryInsFC',
                                                       'PrimaryInsComp_GHICode',
                                                       'PrimaryInsPlan_GHICode')
                                    )
    
    a <- which(is.na(Claim$ReRoutedPrimaryInsPlan_QDXCode))
    b <- which(!is.na(Claim$ReRoutedPrimaryInsPlan_QDXCode))
    for (i in seq_along(QDX_Tick_Payor_map$Ticket_payor)) {
        #print (paste(i, " : ", QDX_Tick_Payor_Source$Ticket_payor[i],
        #             " : ",QDX_Tick_Payor_Source$PrimaryIns[i],
        #             " : ",QDX_Tick_Payor_Source$ReRoutePrimary[i], sep=""))
        Claim[a,QDX_Tick_Payor_map$Ticket_payor[i]] <- Claim[a,QDX_Tick_Payor_map$PrimaryIns[i]]
        Claim[b,QDX_Tick_Payor_map$Ticket_payor[i]] <- Claim[b,QDX_Tick_Payor_map$ReRoutePrimary[i]]
    }
    
    print(paste('stdClaim : output Claim Data based on usage ', usage ,Sys.time(), sep = " "))
    
    if (usage == 'Claim2Rev') {
        select_columns <- claim_note[which(claim_note$Claim2Rev==1),'Synonyms']
    } else if (usage == 'ClaimTicket'){
        select_columns <- claim_note[which(claim_note$ClaimTicket==1),'Synonyms']
    }
    Claim <- select(Claim, select_columns)
    return(Claim)
}

## ------------------------------------------------------------------------
stdPayment <- function (usage, folder, refresh = 0){
    
    target <- "QDX_Payment.txt"
    
    print(paste('stdPayment : reading Payment Data ',Sys.time(), sep = " "))
    if (refresh == 1) {
        con <-odbcDriverConnect(connection = "Driver={ODBC Driver 13 for SQL Server};trusted_connection=yes;
                               server=ODSProd01;
                               database=Quadax")
        #a <- trimws(readr::read_lines("./data/QDX_stdClaim.sql"))
        sql <- paste(trimws(readr::read_lines("./data/QDX_stdPayment.sql")), collapse="\n")

        Pymnt <- sqlQuery(con, sql, stringsAsFactors = FALSE)
        write.table(Pymnt,paste(folder,target,sep=""), sep="|",row.names=FALSE)
        odbcClose(con)
    } else {
        Pymnt <- read.table(paste(folder,target,sep=""), sep="|", header=TRUE, stringsAsFactors = FALSE)
    }

    prep_file_name <- "QDX_ClaimDataPrep.xlsx"
    pymnt_note <- read.xlsx(paste(prep_file_path, prep_file_name, sep=""), sheetName ="QDXPayment", startRow=2,
                            colIndex=c(2,3:8),stringsAsFactors=FALSE)

    # rename columns
    rename_col <- match(pymnt_note$QDX_stdPaymentFile,names(Pymnt))
    names(Pymnt)[na.omit(rename_col)] <- pymnt_note$Synonyms[which(!is.na(rename_col))]

    print(paste('stdPayment : cleaning and transforming Payment Data ',Sys.time(), sep = " "))
    
    # Convert the dates
    Pymnt$TXNDate <- ymd(Pymnt$TXNDate)
    Pymnt$OLIDOS <- ymd(Pymnt$OLIDOS)

    # Translate QDX test code to GHI
    QDX_GHI_Test_Code <- data.table(QDX_Code = c('GL','GLD','GLC','MMR','GLP','LIQSEL','UNK'),
                                    GHI_Code = c('IBC','DCIS','Colon','MMR','Prostate','LIQSEL','Unknown'))
    
    for (i in seq_along(QDX_GHI_Test_Code$QDX_Code)) {
        #print (QDX_GHI_Test_Code$QDX_Code[i])
        a <- which(Pymnt$Test == QDX_GHI_Test_Code$QDX_Code[i])
        Pymnt[a,'Test'] <- QDX_GHI_Test_Code$GHI_Code[i]
    }
    
    # Remove Allowable, Deductible and Coinsurance from Adjustment Lines
    a <- which(Pymnt$TXNType %in% c('AC','AD'))
    Pymnt[a,c('stdPymntAllowedAmt','stdPymntDeductibleAmt','stdPymntCoinsAmt')]<-NA
    
    # Populate the PymntInsPlan_QDXCode field with QDXAdjustmentCode field for Payment Transaction (ie TXNType = RP, RI)
    a <- which(Pymnt$TXNType %in% c('RI','RP'))
    Pymnt[a,'PymntInsPlan_QDXCode'] <- Pymnt[a,'QDXAdjustmentCode']
    Pymnt[a,'QDXAdjustmentCode'] <- NA
    Pymnt[a,'GHIAdjustmentCode'] <- NA
   
    QDX_Tick_Payor_map <- data.table(Ticket_payor = c('TicketInsComp_QDXCode',
                                                      # 'TicketInsPlan_QDXCode',
                                                      # 'TicketInsCompName',
                                                      # 'TicketInsPlanName',
                                                      # 'TicketInsFC',
                                                       'TicketInsComp_GHICode',
                                                       'TicketInsPlan_GHICode'),
                                         PrimaryIns = c('PrimaryInsComp_QDXCode',
                                                       #'PrimaryInsPlan_QDXCode',
                                                       #'PrimaryInsCompName',
                                                       #'PrimaryInsPlanName',
                                                       #'PrimaryInsFC',
                                                       'PrimaryInsComp_GHICode',
                                                       'PrimaryInsPlan_GHICode')
                                    )
    a <- which(is.na(Pymnt$TicketInsPlan_QDXCode))
    for (i in seq_along(QDX_Tick_Payor_map$Ticket_payor)) {
        Pymnt[a,QDX_Tick_Payor_map$Ticket_payor[i]] <- Pymnt[a,QDX_Tick_Payor_map$PrimaryIns[i]]
    }
    
    # update an old GHI adjustment code for PRAC to GH01, Financial Assistance Adj for Financial Assistance
    a <- which(Pymnt$QDXAdjustmentCode=='PRAC' & Pymnt$GHIAdjustmentCode != 'GH07')
    Pymnt[a,'GHIAdjustmentCode'] <- 'GH07'
 
    # Resolve the adjusment code
    ## ************************
    AdjustmentCode <- read.xlsx(paste(prep_file_path, prep_file_name, sep=""), sheetName ="AdjustmentCode", header=TRUE, startRow=1,
                            colIndex=c(1,3,5,7),stringsAsFactors=FALSE)
    #temp <- merge(Pymnt, AdjustmentCode, all.x=TRUE, by.x="QDXAdjustmentCode", by.y = "Code")  ## merge not working
    Pymnt <- left_join(Pymnt, AdjustmentCode, by = c('QDXAdjustmentCode' = 'Code'))
    
    
    print(paste('stdPayment : select & return Payment Data ',Sys.time(), sep = " "))

    if (usage == 'Claim2Rev') {
        select_columns <- pymnt_note[which(pymnt_note$Claim2Rev==1),'Synonyms']
    } else if (usage == 'ClaimTicket'){
        select_columns <- pymnt_note[which(pymnt_note$ClaimTicket==1),'Synonyms']
    }
    Pymnt <- select(Pymnt, select_columns)
    return(Pymnt)
}

