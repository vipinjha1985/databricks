// Databricks notebook source
// MAGIC %md Convert a flattened Dataframe to nested JSON by nesting a case class within another case class.

// COMMAND ----------

// MAGIC %md Step 1 - Define your custom nested schema using case classes.

// COMMAND ----------

case class link(mandatory:String,url:String)
case class details(entries:Array[link],table : String ,fullscanned : String ,delim : String ,skiph : String ,format : String ,compression : String)
case class file_record(details:details)

// COMMAND ----------

// MAGIC %md
// MAGIC Split  File name into Detail derive columns

// COMMAND ----------

import java.sql.Timestamp
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import spark.implicits._

val  df       = dbutils.fs.ls("/mnt/testdata/").toDF()  // To list all files and folder of container in data frame
val fileinfoDF= df.withColumn("_tmp", split($"name", "\\."))  // Create _tmp column with split .
                 .withColumn("timeStamp", regexp_replace($"_tmp".getItem(2), "_", "").as("TimeStamp"))  // create timestamp column
                 .withColumn("mandatory",when(($"size" > 0) ,"TRUE").otherwise("FALSE"))  // check the size 
                 .withColumn("url",lit("UNK"))  // add url column
.select(
  $"mandatory",
  $"url",
  $"_tmp".getItem(1).as("table"),
  $"_tmp".getItem(6).as("fullscanned"),
  $"_tmp".getItem(8).as("delim"),
  $"_tmp".getItem(10).as("skiph"),
  $"_tmp".getItem(11).as("format"),
  $"_tmp".getItem(12).as("compression"),
  $"_tmp".getItem(4).as("batchNo"),
  $"timeStamp".as("TimeStamp"),
//   concat(substring($"timeStamp",0,8),lit(" "),substring($"timeStamp",-8,8)).as("TimeStampSpace"),
//   unix_timestamp(concat(substring($"timeStamp",0,8),lit(" "),substring($"timeStamp",-8,8)), "yyyy/MM/dd HH:mm:ss").cast(TimestampType).as("TimeStamptest"),
  $"name"
).na.drop()  // To Drop the null records
display(fileinfoDF)
val firsttimestamp = fileinfoDF.select(col("TimeStamp")).orderBy($"TimeStamp" asc).first.getString(0)
val lasttimestamp  = fileinfoDF.select(col("TimeStamp")).orderBy($"TimeStamp" desc).first.getString(0)

// table.sap_adrc.2021_01_19_23_36_13_404.batch.16127881740871663.fullscanned.true.delim.comm.skiph.1.csv.gz

// COMMAND ----------

// MAGIC %md Step 2 - Convert the flattented DF to a nested structure using map to pass every row object to a case class.

// COMMAND ----------

val nestedDF= fileinfoDF.map(r=>{
  val link_l    = link(r.getString(0),r.getString(1))
  val detail_l  = details(Array(link_l),r.getString(2) ,r.getString(3),r.getString(4) ,r.getString(5) ,r.getString(6) ,r.getString(7))
  file_record(detail_l)
}
)

display(nestedDF)

// COMMAND ----------

display(dbutils.fs.mounts())

// COMMAND ----------

val targetPath = "/mnt/testdata/JsonOutput_vip/"
nestedDF.coalesce(1).write.format("json").save(targetPath)

// COMMAND ----------

// MAGIC %sh ls

// COMMAND ----------

def writeJsonfile(nestedDF:Dataframe,TargetPath:String,targetFileName:String):Unit={
  
  nestedDF.coalesce(1).write.format("json").save(TargetPath)
  val fs   = FileSystem.get(sc.hadoopConfiguration)
  val file = fs.globStatus(new Path(TargetPath+"part*"))(0).getPath().getName()

  fs.rename(new Path(TargetPath + file), new Path(targetFileName))
//   fs.delete(new Path(TargetPath), true)
  println("Complete Successfully")
}

// COMMAND ----------

val targetPath = "/tmp/JsonOutput/"
val targetfilename = "manifest-table.sap_adrc_poc.15155153526"+firsttimestamp+".batch.15153637172735"+lasttimestamp+".json"
writeJsonfile(nestedDF,targetPath,targetfilename)
