// Databricks notebook source
// Earlier known as /BDL_OTC/DIMENSION_TABLES/CSLMInclusion

// COMMAND ----------

// MAGIC %run /Unilever/BDL/SC/SharedLib/Configuration

// COMMAND ----------

// MAGIC %run /AuditLogs/Audit_Logs

// COMMAND ----------

import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import java.util.concurrent.TimeUnit

val startTime=getCurrentTime()

// COMMAND ----------


val mntUDLRead =mountPointReadUDL()
//val mntUDLRead ="/mnt/adls/centrallake"
val mntBDLRead =mountPointReadBDL()
val mntBDLWrite =mountPointWriteBDL()
dbutils.widgets.text("inParamADLSFilePath", "")
val source_path=dbutils.widgets.get("inParamADLSFilePath")

//val source_path="/TechDebt/InternalSources/OnPremFileShare/FileFMT/InclusionExclusion/Processed"

val InclusionManualDF1=spark.read.option("inferSchema","false").option("delimiter","|").option("header","true")
                      .option("ignoreLeadingWhiteSpace", "false")
                      .option("ignoreTrailingWhiteSpace", "false")
                      .csv(mntUDLRead + source_path).select("RegionID" 
,"SQLFieldGroup"
,"SQLFieldValue"
,"IncludeDescription"
,"Scenario"
,"IncludeFlag"
,"Comments"
,"IncludeStartDate"
,"IncludeEndDate")

// COMMAND ----------

val InclusionManualDF = InclusionManualDF1.distinct()

// COMMAND ----------

InclusionManualDF.createOrReplaceTempView("CSLM_Inc")

// COMMAND ----------

val InclusionManualDF=spark.sql("""Select 
CSLM_Inc.RegionID AS RegionID,
CSLM_Inc.SQLFieldGroup AS SQLFieldGroup,
IF(CAST(CSLM_Inc.SQLFieldValue AS INTEGER) IS NULL,CSLM_Inc.SQLFieldValue, concat(Repeat('0',4-length(CSLM_Inc.SQLFieldValue)), CSLM_Inc.SQLFieldValue)) AS SQLFieldValue,
CSLM_Inc.IncludeDescription AS IncludeDescription,
CSLM_Inc.Scenario AS Scenario,
CSLM_Inc.IncludeFlag AS IncludeFlag,
CSLM_Inc.Comments AS Comments,
COALESCE(IncludeStartDate,'1900-01-01') AS  IncludeStartDate,
COALESCE(IncludeEndDate,'1900-01-01') AS IncludeEndDate 
FROM CSLM_Inc""");
InclusionManualDF.createOrReplaceTempView("CSLMINCLUSION_SRC")

// COMMAND ----------

val target_path1="/BusinessDataLake/SC/Hierarchies/CSLMInclusion"

// COMMAND ----------

// MAGIC %sql
// MAGIC select 'SRC',count(*) from CSLMINCLUSION_SRC
// MAGIC UNION ALL
// MAGIC select 'POST',count(*) from CSLMInclusion_POST 

// COMMAND ----------

// MAGIC 
// MAGIC 
// MAGIC %sql
// MAGIC select RegionID, TRIM(SQLFieldGroup), TRIM(SQLFieldValue), TRIM(IncludeDescription), TRIM(Scenario), TRIM(IncludeFlag), TRIM(Comments), TRIM(IncludeStartDate), TRIM(IncludeEndDate) from CSLMINCLUSION_SRC
// MAGIC MINUS
// MAGIC select RegionID, SQLFieldGroup, SQLFieldValue, IncludeDescription, Scenario, IncludeFlag, Comments, IncludeStartDate, IncludeEndDate from CSLMInclusion_POST 

// COMMAND ----------

// MAGIC %sql
// MAGIC select RegionID, SQLFieldGroup, SQLFieldValue, IncludeDescription, Scenario, IncludeFlag, Comments, IncludeStartDate, IncludeEndDate from CSLMInclusion_POST 
// MAGIC MINUS
// MAGIC select RegionID, TRIM(SQLFieldGroup), TRIM(SQLFieldValue), TRIM(IncludeDescription), TRIM(Scenario), TRIM(IncludeFlag), TRIM(Comments), TRIM(IncludeStartDate), TRIM(IncludeEndDate) from CSLMINCLUSION_SRC

// COMMAND ----------

// MAGIC %sql
// MAGIC select RegionID, SQLFieldGroup, SQLFieldValue, IncludeDescription, Scenario, IncludeFlag, Comments, IncludeStartDate, IncludeEndDate from CSLMInclusion 
// MAGIC MINUS
// MAGIC select RegionID, TRIM(SQLFieldGroup), TRIM(SQLFieldValue), TRIM(IncludeDescription), TRIM(Scenario), TRIM(IncludeFlag), TRIM(Comments), TRIM(IncludeStartDate), TRIM(IncludeEndDate) from CSLMINCLUSION_SRC

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from CSLMInclusion_Pre

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from CSLMInclusion

// COMMAND ----------

// DBTITLE 1,PRE-POST TESTING
val TargetDF = spark.read
                   .option("inferSchema","true")
                   .option("delimiter","|")
                   .option("header","true")
                .csv("/mnt/adls/centrallake/BusinessDataLake/SC/Hierarchies/CSLMInclusion")
 TargetDF.createOrReplaceTempView("CSLMInclusion_POST") 


// COMMAND ----------

val TargetDF1 = spark.read
                   .option("inferSchema","true")
                   .option("delimiter","|")
                   .option("header","true")
                .csv("/mnt/adls/centrallake/BusinessDataLake/SC/Hierarchies/CSLMInclusion_Pre")
 TargetDF1.createOrReplaceTempView("CSLMInclusion_PRE1") 


// COMMAND ----------

TargetDF.columns

// COMMAND ----------

// MAGIC %sql
// MAGIC select 'PRE',count(*) from CSLMInclusion_PRE1
// MAGIC UNION ALL
// MAGIC select 'POST',count(*) from CSLMInclusion_POST 

// COMMAND ----------

// MAGIC %sql
// MAGIC select 'PRE',count(*) from CSLMInclusion_PRE
// MAGIC UNION ALL
// MAGIC select 'POST',count(*) from CSLMInclusion

// COMMAND ----------

// MAGIC 
// MAGIC 
// MAGIC %sql
// MAGIC select RegionID, SQLFieldGroup, SQLFieldValue, IncludeDescription, Scenario, IncludeFlag, Comments, IncludeStartDate, IncludeEndDate from CSLMInclusion_PRE1
// MAGIC MINUS
// MAGIC select RegionID, SQLFieldGroup, SQLFieldValue, IncludeDescription, Scenario, IncludeFlag, Comments, IncludeStartDate, IncludeEndDate from CSLMInclusion_POST 

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC select RegionID, SQLFieldGroup, SQLFieldValue, IncludeDescription, Scenario, IncludeFlag, Comments, IncludeStartDate, IncludeEndDate from CSLMInclusion_POST
// MAGIC MINUS
// MAGIC select RegionID, SQLFieldGroup, SQLFieldValue, IncludeDescription, Scenario, IncludeFlag, Comments, IncludeStartDate, IncludeEndDate from CSLMInclusion_PRE1
// MAGIC  

// COMMAND ----------

// MAGIC 
// MAGIC 
// MAGIC %sql
// MAGIC select RegionID, SQLFieldGroup, SQLFieldValue, IncludeDescription, Scenario, IncludeFlag, Comments, IncludeStartDate, IncludeEndDate from CSLMInclusion_PRE
// MAGIC MINUS
// MAGIC select RegionID, SQLFieldGroup, SQLFieldValue, IncludeDescription, Scenario, IncludeFlag, Comments, IncludeStartDate, IncludeEndDate from CSLMInclusion

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from CSLMInclusion

// COMMAND ----------

// MAGIC 
// MAGIC 
// MAGIC %sql
// MAGIC select 'PRE',RegionID, SQLFieldGroup, SQLFieldValue, IncludeDescription, Scenario, IncludeFlag, Comments, IncludeStartDate, IncludeEndDate from CSLMInclusion_PRE 
// MAGIC WHERE SQLFieldGroup='SalesOrg' AND SQLFieldValue='5120'
// MAGIC UNION ALL
// MAGIC select 'POST',RegionID, SQLFieldGroup, SQLFieldValue, IncludeDescription, Scenario, IncludeFlag, Comments, IncludeStartDate, IncludeEndDate from CSLMInclusion_POST 
// MAGIC WHERE SQLFieldGroup LIKE '%SalesOrg%' AND SQLFieldValue LIKE '%5120%'

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT IncludeEndDate,RegionID,Scenario,SQLFieldGroup,SQLFieldValue,COUNT(*) FROM CSLMInclusion_POST
// MAGIC GROUP BY IncludeEndDate,RegionID,Scenario,SQLFieldGroup,SQLFieldValue
// MAGIC HAVING COUNT(*)>1

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM CSLMInclusion_POST
// MAGIC WHERE  IncludeEndDate IS NULL OR RegionID IS NULL OR Scenario IS NULL OR SQLFieldGroup IS NULL OR SQLFieldValue IS NULL OR 
// MAGIC IncludeEndDate ='' OR RegionID  ='' OR Scenario  ='' OR SQLFieldGroup  ='' OR SQLFieldValue  ='' OR 
// MAGIC IncludeEndDate =' ' OR RegionID  =' ' OR Scenario  =' ' OR SQLFieldGroup  =' ' OR SQLFieldValue  =' ' 

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC SQLFieldValue,* from CSLM_Inc  WHERE SQLFieldGroup='OrderType'  and SQLFieldValue is null-- and IncludeDescription='Preorder'--AND SQLFieldValue='5120'
