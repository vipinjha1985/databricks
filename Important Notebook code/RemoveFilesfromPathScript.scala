// Databricks notebook source
// DataBricks notebook code
// This script removes all files and folder from spefic folder in ADLS.
// Intentionally commented to avoid mistakenly run by people
/*
val filePaths = Vector(
"/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/DeliveryHeader/",
"/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/DeliveryLine/",
"/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/SalesOrderHeader/",
"/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/SalesOrderLine/",
"/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/SalesOrderLossAudit/",
"/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/SalesOrderLossDetail/")

for (file <- filePaths) { 
  val filelist=dbutils.fs.ls(file)
  
  for (file <- filelist) { 
    println(file.path)
    dbutils.fs.rm(file.path,true)
  }
}
*/
