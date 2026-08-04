// Databricks notebook source
import org.apache.spark.sql.DataFrame

// COMMAND ----------

def createDeltaTableDef(df: DataFrame, deltaTableName: String, targetPath: String): String = {
  val columns = df.dtypes
  var createTableDef = "CREATE TABLE "+ deltaTableName + " ("
  var colDef = ""
  if(columns.map(col=> col._1).contains("Market")){
    colDef = (for (c <-columns) yield(c._1+" "+c._2.replace("Type",""))).mkString(", ")
    createTableDef += colDef + ") USING DELTA PARTITIONED BY (Market) LOCATION '" + targetPath +"'"
  }
  else{
    colDef = (for (c <-columns) yield(c._1+" "+c._2.replace("Type",""))).mkString(", ")
    createTableDef += colDef + ") USING DELTA LOCATION '" + targetPath +"'"
  }
  createTableDef
}

// COMMAND ----------

val df= spark.read.format("delta").load("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/OTMShipmentCost/Processed_Parquet/Cordillera")

// COMMAND ----------

val dff1 = df.drop("Market")
val tableDef = createDeltaTableDef(dff1, "OTMShipmentCostDeltaTab", "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/OTMShipmentCost/Processed_Parquet/Cordillera")

// COMMAND ----------

// val dff1 = df.drop("Market")
val tableDef1 = createDeltaTableDef(df, "OTMShipmentCostDeltaTab", "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/OTMShipmentCost/Processed_Parquet/Cordillera")
