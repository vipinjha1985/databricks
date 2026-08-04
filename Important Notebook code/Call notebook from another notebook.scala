// Databricks notebook source
// MAGIC %run /Unilever/BDL/SC/SharedLib/MarketFileSystem

// COMMAND ----------

// DBTITLE 1,Automation PDL History Load - Not to Run
/*
val Regionid     = List("A","R","I","E")
val regionFolder = Map("A"->"CordilleraECC","R"->"U2K2ECC","E"->"SiriusECC","I"->"FusionECC")
val start_Date   = "2020-01-01"
val end_Date     = "2020-03-31"

dbutils.fs.rm("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/PurchaseDocumentLine/", true)  
for (r <- Regionid) {
  //Market Data Delete
  display(marketDataDelete("PurchaseDocumentLine",r,"TransactionObject"))
  if(r =="A"){
    dbutils.notebook.run("/Unilever/BDL/SC/Transaction/PurchaseDocumentLine_Multi_Transaction_Nb_History",12000,
                        Map("inParam_targetPath"-> "/BusinessDataLake/SC/TransactionObject/PurchaseDocumentLine/Processed", 
                          "inParam_ekpo"-> "/UniversalDataLake/InternalSources/"+regionFolder{r}+"/Table/EKPO/Processed/",
                          "inParam_pdh"->"/BusinessDataLake/SC/TransactionObject/PurchaseDocumentHeader/Processed_Parquet/",
                          "inParam_regionID"->r,
                          "Load_Start_Date"->start_Date,
                          "Load_End_Date"->end_Date))
  }
  else{
    dbutils.notebook.run("/Unilever/BDL/SC/Transaction/PurchaseDocumentLine_Multi_Transaction_Nb_History",12000,
                        Map("inParam_targetPath"-> "/BusinessDataLake/SC/TransactionObject/PurchaseDocumentLine/Processed", 
                          "inParam_ekpo"-> "/UniversalDataLake/InternalSources/SiriusECC/Table/EKPO/Processed_Parquet/",
                          "inParam_pdh"->"/BusinessDataLake/SC/TransactionObject/PurchaseDocumentHeader/Processed_Parquet/",
                          "inParam_regionID"->r,
                          "Load_Start_Date"->start_Date,
                          "Load_End_Date"->end_Date))
  }
}
*/

// COMMAND ----------

// DBTITLE 1,Automation PDL  Delta Load - Not to Run
/*
val Regionid     = List("A","R","I","E")
val regionFolder = Map("A"->"CordilleraECC","R"->"U2K2ECC","E"->"SiriusECC","I"->"FusionECC")
val start_Date   = "2020-01-08"
val end_Date     = "2020-01-08"

for (r <- Regionid) {
  display(marketDataDelete("PurchaseDocumentLine",r,"TransactionObject"))
  if(r =="A"){
    //EKPO Source CSV Format 
    dbutils.notebook.run("/Unilever/BDL/SC/Transaction/PurchaseDocumentLine_Multi_Transaction_Nb_Delta",12000,
                        Map("inParam_targetPath"-> "/BusinessDataLake/SC/TransactionObject/PurchaseDocumentLine/Processed", 
                          "inParam_ekpo"-> "/UniversalDataLake/InternalSources/"+regionFolder{r}+"/Table/EKPO/Processed/",
                          "inParam_pdh"->"/BusinessDataLake/SC/TransactionObject/PurchaseDocumentHeader/Processed_Parquet/",
                          "inParam_regionID"->r,
                          "Load_Start_Date"->start_Date,
                          "Load_End_Date"->end_Date))
  }
  else{
    //EKPO Source Parquet Format 
    dbutils.notebook.run("/Unilever/BDL/SC/Transaction/PurchaseDocumentLine_Multi_Transaction_Nb_Delta",12000,
                        Map("inParam_targetPath"-> "/BusinessDataLake/SC/TransactionObject/PurchaseDocumentLine/Processed", 
                          "inParam_ekpo"-> "/UniversalDataLake/InternalSources/SiriusECC/Table/EKPO/Processed_Parquet/",
                          "inParam_pdh"->"/BusinessDataLake/SC/TransactionObject/PurchaseDocumentHeader/Processed_Parquet/",
                          "inParam_regionID"->r,
                          "Load_Start_Date"->start_Date,
                          "Load_End_Date"->end_Date))
  }
}
*/

// COMMAND ----------

// DBTITLE 1,Remove PurchaseMaterialVolumeForecast  Object 
//dbutils.fs.rm("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/PurchaseMaterialVolumeForecast/", true) 

// COMMAND ----------

// DBTITLE 1,Remove PDL Object 
//dbutils.fs.rm("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/PurchaseDocumentLine/", true) 

// COMMAND ----------

// DBTITLE 1,Market Data Delete
val Regionid     = List("A","R","I","E")
for (r <- Regionid) {
  marketDataDelete("PurchaseDocumentLine",r,"TransactionObject")
}

// COMMAND ----------

// DBTITLE 1,PDL U2K2 History Load
dbutils.notebook.run("/Unilever/BDL/SC/Transaction/PurchaseDocumentLine_Multi_Transaction_Nb_History",12000,
                        Map("inParam_targetPath"-> "/BusinessDataLake/SC/TransactionObject/PurchaseDocumentLine/Processed", 
                          "inParam_ekpo"-> "/UniversalDataLake/InternalSources/U2K2ECC/Table/EKPO/Processed_Parquet/",
                          "inParam_pdh"->"/BusinessDataLake/SC/TransactionObject/PurchaseDocumentHeader/Processed_Parquet/",
                          "inParam_regionID"->"R",
                          "inParam_startDate"->"2018-01-01",
                          "inParam_endDate"->"2020-04-25"))

// COMMAND ----------

// MAGIC %sql select Regionid,count(*) from PurchaseDocumentLinemainTable group by 1 
