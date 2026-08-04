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

recursiveDirSize("/mnt/adls/centrallake/UniversalDataLake/InternalSources/U2K2ECC/Table/LIPS/MBLanding/YYYY=2020/MM=09")

// COMMAND ----------

// MAGIC %sql 
// MAGIC Select cast(446662986 as long)/cast(1000000 as long) as BytesToMbConversion

// COMMAND ----------

// MAGIC %sql 
// MAGIC Select cast(1370.756635 as long)/cast(1024 as long) as MbToGbConversion

// COMMAND ----------

// MAGIC %fs
// MAGIC ls "/mnt/adls/centrallake/BusinessDataLake/SC/ReferenceObject/Plant/Processed"
