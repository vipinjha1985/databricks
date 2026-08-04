// Databricks notebook source
// Add the Storage Account, Container, and SAS Token
val STORAGE_ACCOUNT = "azurestorageaccount2021"
val CONTAINER = "zipfile"
val SASTOKEN = "?sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-02-18T19:37:09Z&st=2021-02-18T11:37:09Z&spr=https&sig=3rXlANOruBYIe%2Fl65XUytUNUKqCdW6dZ2V32LnyLzrg%3D"

//val SASTOKEN = dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>") 

// Do not change these values
val SOURCE = s"wasbs://$CONTAINER@$STORAGE_ACCOUNT.blob.core.windows.net/"
val URI = s"fs.azure.sas.$CONTAINER.$STORAGE_ACCOUNT.blob.core.windows.net"
val MOUNTPOINT = "/mnt/zipfile/"

try {
  dbutils.fs.mount(
    source = SOURCE,
    mountPoint = MOUNTPOINT,
    extraConfigs = Map(URI -> SASTOKEN))
  println("Success.")
} catch {
  case e: Exception => println(e.getCause().getMessage())
}

// COMMAND ----------

display(dbutils.fs.ls("/mnt/zipfile/"))

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC dbutils.widgets.text("container_path", "","")
// MAGIC container_path=dbutils.widgets.get("container_path")
// MAGIC 
// MAGIC 
// MAGIC dbutils.widgets.text("file_name", "","")
// MAGIC file_name=dbutils.widgets.get("file_name")
// MAGIC 
// MAGIC fullfilepath = container_path+file_name
// MAGIC print(fullfilepath)

// COMMAND ----------

// MAGIC %python 
// MAGIC a = "file:"+fullfilepath
// MAGIC print(a)

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.cp("/mnt/zipfile/current_DataHub_DISCOVER.csv.bz2", "file:/tmp/current_DataHub_DISCOVER.csv.bz2")

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.cp(fullfilepath, "file:"+fullfilepath)

// COMMAND ----------

// MAGIC %python 
// MAGIC fpath = "/tmp/"+file_name
// MAGIC print(fpath)

// COMMAND ----------

// MAGIC %sh
// MAGIC bzip2 -d /tmp/current_DataHub_DISCOVER.csv.bz2

// COMMAND ----------

// MAGIC %sh
// MAGIC bzip2 -d $fpath

// COMMAND ----------

// MAGIC %sh
// MAGIC bzip2 -d $fpath

// COMMAND ----------

display(dbutils.fs.ls("file:/tmp/current_DataHub_DISCOVER.csv/"))

// COMMAND ----------

import org.apache.spark.sql.functions.lit //To add new column
import org.apache.spark.sql.functions.col
val myBadRecords = "/mnt/rootbpk/Output/badRecordsPath"
val readdataremduplcate =        spark.read
.format("csv").option("header",true)
//.option("mode", "FAILFAST/PERMISSIVE/DROPMALFORMED")  // To check the error data
.option("badRecordsPath", myBadRecords)
//.option("columnNameOfCorruptRecord", "_corrupt_record")
.load("file:/tmp/current_DataHub_DISCOVER.csv/")

//.filter($"Dept" === "IT")         // Filter condition
//readdataremduplcate.show()
readdataremduplcate.createOrReplaceTempView("sourcedata")

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(1) from sourcedata

// COMMAND ----------

dbutils.fs.rm("file:/tmp/current_DataHub_DISCOVER.csv",true)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC readdataremduplcate = spark.read.option('header','true').format("csv").load(fullfilepath)
// MAGIC readdataremduplcate.createOrReplaceTempView("sourcedata")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from sourcedata limit 10

// COMMAND ----------


