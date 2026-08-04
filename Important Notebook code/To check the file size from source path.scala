// Databricks notebook source
import com.databricks.backend.daemon.dbutils.FileInfo

 def recursiveDirSize(location: String): Long = {
  
   def go(items: List[FileInfo], results: Long): Long = items match {
     case head :: tail =>
       val files = dbutils.fs.ls(head.path)
       val directories = files.filter(_.isDir)
       val updated = files.map(_.size).foldLeft(results)(_ + _)
       go(tail ++ directories, updated)
     case _ => results
   }
  
   go(dbutils.fs.ls(location).toList, 0)
 }
 
//   recursiveDirSize("abfss://container@account.dfs.core.windows.net") 

// COMMAND ----------

recursiveDirSize("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/DaysBeforeNextRunProductionLine/Processed_Parquet")

// COMMAND ----------

// MAGIC %sql 
// MAGIC Select cast(14718223 as long)/cast(1000000 as long) as BytesToMbConversion

// COMMAND ----------

// MAGIC %sql 
// MAGIC Select cast(493.795007 as long)/cast(1024 as long) as MbToGbConversion

// COMMAND ----------

// MAGIC %fs
// MAGIC ls "/mnt/adls/centrallake/BusinessDataLake/SC/ReferenceObject/Plant/Processed"
