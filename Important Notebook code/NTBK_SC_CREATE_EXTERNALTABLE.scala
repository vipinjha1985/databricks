// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, md5, concat_ws,lit}
import scala.collection.mutable.ListBuffer
import spark.implicits._

// COMMAND ----------

// Master file name : /mnt/devopsfolder/IDQ/Input/MasterFileList.csv -- This file exists in Blob. Take backup of this file and insert the new entries in the actual file
//outputFilePath: String = /mnt/devopsfolder/IDQ/Output/resultlog.csv

// COMMAND ----------

dbutils.widgets.text("inputFileName","","")
dbutils.widgets.text("outputFileName","","")
dbutils.widgets.text("basePath","","")

//val basePath = "/mnt/devopsfolder/IDQ/Output/"
val adlsmountpoint = "/mnt/adls"
val basePath=dbutils.widgets.get("basePath")
val inputFileName=dbutils.widgets.get("inputFileName")
val outputFileName=dbutils.widgets.get("outputFileName")

val masterFilePath = basePath + "/Input/" + inputFileName
val outputFolder = basePath + "/Output/"
val outputFilePath = outputFolder + outputFileName

// COMMAND ----------

var inputDF = spark.read.format("csv")
        .option("header", "true")
        .option("delimiter",",")
        .option("inferSchema", "false")
        .load(masterFilePath)

// COMMAND ----------

def deleteLogFiles(path:String):Unit = {

val file_list = dbutils.fs.ls(path)

file_list.size match
  {
    case 0 => println("**************There is no file to remove")
    case _ =>
    {
      file_list.foreach {
        file => {
          val file_info=file.toString.split(",").map(_.trim).toList
          val file_name = file_info(1)
          val file_name_with_path = file_info(0).split("\\(").map(_.trim).toList(1)
      
          file_name  match
          {
            case s if s.startsWith("_") => dbutils.fs.rm(file_name_with_path,false)
            case _ => println("**************Skipping other files")
          }
        }
	  }
    }
  }
}

// COMMAND ----------

def renamePartFiles(path:String, newFileName:String):Unit = {
    val file_list = dbutils.fs.ls(path)  
    file_list.size match
    {
      case 0 => println("**************There is no file to rename")
      case _ =>
      {
        file_list.foreach {
        file => 
          {
            val file_info=file.toString.split(",").map(_.trim).toList
            val file_name = file_info(1)
            val file_name_with_path = file_info(0).split("\\(").map(_.trim).toList(1)
            file_name  match
            {
               case s if s.startsWith("part") => dbutils.fs.mv(file_name_with_path,newFileName)
                                             println("**************Renamed")
               case _ => println("**************Skipping other files")
            }
          }
        }
      }
    }
}

// COMMAND ----------

var sourcecount :Long  =0L
var targetcount :Long =0L
var Filepath = ""
var Tablename = ""
var delimiter = ""
var finalDF: DataFrame = null
var outputDF: DataFrame = null
inputDF.collect().foreach {row => 
  try
  {
  Filepath=adlsmountpoint + row(0).asInstanceOf[String]
  Tablename=row(1).asInstanceOf[String]
  delimiter=row(2).asInstanceOf[String]
  var sqlqry = s"""CREATE TABLE IF NOT EXISTS ${Tablename} USING CSV OPTIONS (header 'true', inferSchema 'true', delimiter '${delimiter}') LOCATION '${Filepath}'"""
  spark.sql(s"""${sqlqry}""") 
  
  val ADLSDF = spark.read.format("csv")
        .option("header", "true")
        .option("delimiter",delimiter)
        .option("inferSchema", "true")
        .load(Filepath)
  sourcecount = ADLSDF.count
  
  targetcount = spark.sql(s"""select * from ${Tablename}""").count()
  outputDF= Seq((Filepath,Tablename,sourcecount,targetcount)).toDF("ADLS file path","Table name","Count in File","Count in Table")
  if(finalDF == null){
         finalDF = outputDF
          } 
  else{
          finalDF=finalDF.union(outputDF) 
          }
  
}
  
   catch 
      {
        case unknown: Exception => println("Unknown exception: ADLS path Not Found : " + Filepath)                      
      }
}
finalDF.coalesce(1).write
        .mode(SaveMode.Overwrite)
        .format("csv")
        .option("header", "true")
        .option("delimiter",",")
        .option ("quoteAll","true")
        .save(outputFolder)
     deleteLogFiles(outputFolder)
     renamePartFiles(outputFolder,outputFilePath) 

  
