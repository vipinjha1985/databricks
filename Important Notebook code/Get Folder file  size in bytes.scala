// Databricks notebook source

import sqlContext.implicits._
import org.apache.spark.sql.functions._
val a = dbutils.fs.ls("/mnt/adls/centrallake/BusinessDataLake/SC/ReferenceObject/ChannelHierarchyText/Processed_Parquet/").toDF()
val b  = a.select('name,'size)
val c = b.agg(sum("size"))
display(c)

// COMMAND ----------

import sqlContext.implicits._
import org.apache.spark.sql.functions._
val a = dbutils.fs.ls("/mnt/adls/gen2UAT/UniversalDataLake/InternalSources/CordilleraECC/Table/Z0IMS0048_CHHIER/Processed_Parquet/").toDF()
val b  = a.select('name,'size)
val c = b.agg(sum("size"))
display(c)
