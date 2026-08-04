// Databricks notebook source
// MAGIC %python 
// MAGIC 
// MAGIC import re
// MAGIC path = "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/"
// MAGIC 
// MAGIC 
// MAGIC list_path = dbutils.fs.ls(path)
// MAGIC obj_name = "PurchaseAccountingDocument" # change this to object name else untouched
// MAGIC 
// MAGIC delta = "_Delta"
// MAGIC tc = 0
// MAGIC for l in list_path:
// MAGIC   ls = dbutils.fs.ls(l[0])
// MAGIC   for ele in ls:
// MAGIC     if re.search(obj_name, ele[1]):
// MAGIC       #
// MAGIC       try:
// MAGIC         #butils.fs.rm(ele[0],True)
// MAGIC         #spark.sql("""drop table """+ ele[0].split("/")[-3]+obj_name+delta)
// MAGIC         #spark.sql("""delete from """+ele[0].split("/")[-3]+obj_name+delta)
// MAGIC         #print(ele[0].split("/")[-3]+obj_name+delta)
// MAGIC         tc = tc + spark.table(ele[0].split("/")[-3]+obj_name+delta).count()#spark.sql("""select AccountingDocumentKeyCode,AccountingDocumentLineNumber,count(1) from """+ ele[0].split("/")[-3]+obj_name+delta + """ group by 1,2 having count(1) > 1""").count()
// MAGIC         #print("")
// MAGIC         #dbutils.fs.rm(ele[0],True)
// MAGIC         #if ele[0].split("/")[-3][0] == 'R':
// MAGIC           #spark.sql("""drop table """+ ele[0].split("/")[-3]+obj_name+delta)
// MAGIC           #dbutils.fs.rm(ele[0],True)
// MAGIC           #print(ele[0].split("/")[-3]+obj_name+delta + " " + str(spark.table(ele[0].split("/")[-3]+obj_name+delta).count()))
// MAGIC       except:
// MAGIC         print("Error For "+ ele[0].split("/")[-3]+obj_name+delta )
// MAGIC print(tc)

// COMMAND ----------

 spark.table("PurchaseAccountingDocument_delta").where($"RegionId".isNotNull).count()

// COMMAND ----------

spark.sql("""select AccountingDocumentKeyCode,AccountingDocumentLineNumber,count(1) from PurchaseAccountingDocument_delta group by 1,2 having count(1) > 1""").count()

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC import re
// MAGIC path = "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/"
// MAGIC 
// MAGIC 
// MAGIC list_path = dbutils.fs.ls(path)
// MAGIC obj_name = "PurchaseAccountingDocument" # change this to object name else untouched
// MAGIC 
// MAGIC delta = "_Delta"
// MAGIC 
// MAGIC for l in list_path:
// MAGIC   ls = dbutils.fs.ls(l[0])
// MAGIC   for ele in ls:
// MAGIC     if re.search(obj_name, ele[1]):
// MAGIC       #
// MAGIC       try:
// MAGIC         print(ele[0].split("/")[-3]+obj_name+delta)
// MAGIC         print(""" Duplicate Count : """+str(spark.sql("""select AccountingDocumentKeyCode,AccountingDocumentLineNumber,count(1) from """+ ele[0].split("/")[-3]+obj_name+delta + """ group by 1,2 having count(1) > 1""").count()))
// MAGIC         print("")
// MAGIC       except:
// MAGIC         print("Error For "+ ele[0].split("/")[-3]+obj_name+delta )

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from EEuropeWestPurchaseAccountingDocument_Delta 

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select regionId,count(*) from PurchaseAccountingDocument_delta group by 1 

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from AcordilleraPurchaseAccountingDocument_Delta
// MAGIC union all
// MAGIC select count(*) from ANorthAmericaPurchaseAccountingDocument_Delta
// MAGIC union all
// MAGIC select count(*) from ALatinAmericaPurchaseAccountingDocument_Delta

// COMMAND ----------

val A=spark.read.csv("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/ACordillera/PurchaseAccountingDocument/Processed").option("header")
A.count

// COMMAND ----------

val B=spark.read.csv("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/ALatinAmerica/PurchaseAccountingDocument/Processed")
B.count

// COMMAND ----------

// MAGIC %sql
// MAGIC s

// COMMAND ----------

marketDataDelete("PurchaseAcccountingDocument","A","")

// COMMAND ----------

// MAGIC %run /Unilever/BDL/SC/SharedLib/MarketFileSystem

// COMMAND ----------

// MAGIC %run /Unilever/BDL/SC/SharedLib/Utilities

// COMMAND ----------

val attributes = getAllAttributes("PurchaseAccountingDocument")
val mapppingFile=getAllAttributes("PurchaseAccountingDocument","A")
val pkList=mapppingFile("primaryKeys").asInstanceOf[List[String]]
val joinstring = pkList.map(x => "Target."+ x+" = Source."+ x ).mkString(" AND ")

// COMMAND ----------

val PADFINALDF = spark.sql(""" select * from RVietnamPurchaseAccountingDocument_Delta where  AccountingDocumentKeyCode ="520005982144182019" and AccountingDocumentLineNumber = "008" """)

val market_df = SP_GetBDLmarkets("PurchaseAccountingDocument",PADFINALDF.where($"RegionId"==="R"),"R")

market_df.createOrReplaceTempView("market")

val TargetDF = spark.sql(""" select * from RVietnamPurchaseAccountingDocument_Delta where  AccountingDocumentKeyCode ="520005982144182019" and AccountingDocumentLineNumber = "008" """)

// COMMAND ----------

display(PADFINALDF)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select AccountingDocumentKeyCode,AccountingDocumentLineNumber,count(1) from market group by 1,2 having count(1) > 1

// COMMAND ----------

println(joinstring)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC -- select Regionid,BDLLoadTimestamp,BDLUpdateTimestamp,count(1) from RNAMEAccountingDocumentHeader_Delta group by Regionid,BDLLoadTimestamp,BDLUpdateTimestamp 
// MAGIC 
// MAGIC -- having 
// MAGIC 
// MAGIC  delete from RPhilippinesAccountingDocumentHeader_Delta where
// MAGIC  Regionid="R"	 and BDLLoadTimestamp="2020-03-13T18:56:48.959+0000" and	BDLUpdateTimestamp="2020-03-13T18:56:48.959+0000"
// MAGIC 
// MAGIC --  order by 3 desc

// COMMAND ----------



// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select count(1) from RIndonesiaPurchaseAccountingDocument_Delta

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select AccountingDocumentKeyCode, AccountingDocumentLineNumber, count(1) from RIndonesiaPurchaseAccountingDocument_Delta group by 1,2 having count(1) >1 

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select companycode,regionid, count(1) from company group by 1,2 having count(1) > 1

// COMMAND ----------

LT A A
LT R A
LT E A

// COMMAND ----------

  var refMarketDF= refernceDF.join(marketDF, 
                                   refernceDF.col(referencekey) === marketDF.col(marketKey) && lit(rId) === marketDF.col("MregionId"),
                                 "leftouter")
.select("RegionId",sournceReferencekey,"BDLMarketFolder")
.withColumnRenamed(sournceReferencekey,"xsournceReferencekey")
.withColumnRenamed("RegionId","xregionId")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select CountryCode,regionid,"A"  from company where CountryCode = "NL"-- group by 1 having count(1) > 1 and 

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select companycode,count(1) from company 
// MAGIC left outer join market on company.CountryCode = market.ISO2digit and "A" = market.regionId  
// MAGIC --where company.regionid = "A" 
// MAGIC group by 1
// MAGIC having count(1) >1

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select companycode,company.RegionId,count(1) from company left outer join market on company.CountryCode = market.ISO2digit and "A" = market.regionId group by 1,2 having count(1) >1

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from tcurx

// COMMAND ----------

val df = spark.read.format("csv").option("header","true").option("sep","|").load("/mnt/adls/centrallake/BusinessDataLake/SC/ReferenceObject/MarketCountry/Processed")
val df2 = spark.read.format("delta").load("/mnt/adls/centrallake/BusinessDataLake/SC/ReferenceObject/Company/Processed_Parquet")

df.createOrReplaceTempView("market")
df2.createOrReplaceTempView("company")

// COMMAND ----------

// MAGIC %sql
// MAGIC select companycode,regionid,count(1) from (select companycode,company.regionid,bdlmarketfolder from company left join market on company.countrycode=market.iso2digit and "A"=market.regionid) group by 1,2 having count(*)>1

// COMMAND ----------

// MAGIC %run  /Unilever/BDL/SC/SharedLib/Utilities

// COMMAND ----------



// COMMAND ----------

def SP_GetBDLmarkets(ObjectName: String,Sourcedf:DataFrame,rId: String): DataFrame = {
              val mntPointUDL =mountPointUDL()
              val mntPoint =  mountPointBDL()
    val mapppingFile=getAllAttributes(ObjectName,rId)
    val Sourcekey=mapppingFile("objectKey").mkString
    val sournceReferencekey=mapppingFile("DimKey").mkString
    val referenceObjectPath=mapppingFile("DimPath").mkString
    val referencekey=mapppingFile("DimMarketKey").mkString
    val marketKey=mapppingFile("MarketDimKey").mkString

   import org.apache.spark.sql.functions.col
 
       var refernceDF: org.apache.spark.sql.DataFrame =null
       if(referenceObjectPath.contains("_Parquet"))
       {
         refernceDF = readDeltaParquetToDF(mntPointUDL+referenceObjectPath)
       }
        else{
          refernceDF = readCSVtoDF(List(mntPointUDL+referenceObjectPath), "false")
        }

  
  var marketDF =  readCSVtoDF(List(mntPoint+"/BusinessDataLake/SC/ReferenceObject/MarketCountry/Processed/"), "false") 
     marketDF=marketDF.withColumnRenamed("regionId","MregionId")
  var refMarketDF= refernceDF.join(marketDF, refernceDF.col(referencekey) === marketDF.col(marketKey) && lit(rId) === marketDF.col("MregionId"), "leftouter").select("RegionId",sournceReferencekey,"BDLMarketFolder").withColumnRenamed(sournceReferencekey,"xsournceReferencekey").withColumnRenamed("RegionId","xregionId")

  var mm=Sourcedf.join(refMarketDF,Sourcedf.col(Sourcekey)===refMarketDF.col("xsournceReferencekey") && lit(rId) === refMarketDF.col("xregionId") ,"left_outer").drop("xsournceReferencekey","xregionId")

     val nmm= mm.withColumn("BDLMarketFolder", when(col("BDLMarketFolder").isNull && col("RegionId") === "A","ACordillera")
                                    . when(col("BDLMarketFolder").isNull && col("RegionId") === "E","ESirius")
                                      .when(col("BDLMarketFolder").isNull && col("RegionId") === "R","RU2K2")
                                      .when(col("BDLMarketFolder").isNull && col("RegionId") === "I","IFusion")
                                      .when(col("BDLMarketFolder").isNull && col("RegionId") === "G","GGlobal")
                                      .otherwise(col("BDLMarketFolder")))
      nmm
 
}

// COMMAND ----------

CountryCode
ISO2digit

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select companycode, regionid, count(1) from company group by companycode, regionid having count(1) > 1

// COMMAND ----------

val df = spark.read.format("delta").load("/mnt/adls/centrallake/UniversalDataLake/InternalSources/CordilleraECC/Table/LIKP/Processed_Parquet")

df.createOrReplaceTempView("LIKP")

// COMMAND ----------

'AT_SC_BDL_DeliveryHeader' 
,'AT_ADB_BDL_DeliveryLine' 
,'AT_SC_D_CORDILLERA_BDL_BillingDocumentLine' 
,'AT_SC_BDL_SalesOrderHeader'  
,'AT_NA_OTC_GBDL_SalesOrderLine' 
,'AT_SC_D_Global_BDL_DemandPlanningDeliveriesAtCustomer' 

// COMMAND ----------


RVietnam

RPhilippines
RPhilippinesAccountingDocumentHeader_Delta

// COMMAND ----------

DemandPlanningDeliveriesAtCustomer
SalesDocumentPartner


// COMMAND ----------

// MAGIC %fs
// MAGIC 
// MAGIC ls /mnt/adls/centrallake//UniversalDataLake/InternalSources/CordilleraECC/Table/VBFA/Processed_Parquet/YYYY=2020/MM=02/DD=14

// COMMAND ----------

dbutils.fs.ls("/mnt/adls/centrallake/UniversalDataLake/InternalSources/SiriusAPO/Table/LOC/")

// COMMAND ----------

display(dbutils.fs.ls("/mnt/adls/centrallake/UniversalDataLake/InternalSources/CordilleraECC/Table/BSEG/"))

// COMMAND ----------

display(dbutils.fs.ls("/mnt/adls/centrallake/UniversalDataLake/InternalSources/SheEPR/Oracle/VW_MRD_SITE/Processed/"))

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from PurchaseContractLineDetail_Delta 
// MAGIC where
// MAGIC ContractNumber = 
// MAGIC ContractLineNumber = 
// MAGIC RegionId = 

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select ContractNumber,ContractLineNumber,RegionId,count(1) from PurchaseContractLineDetail_Delta group by 1,2,3 having count(1) > 1

// COMMAND ----------

val PurchaseContractLine_Df = spark.read
                  .option("inferSchema","false")
                  .option("delimiter","|")
                  .option("header","true")
                  .option("escape","\"")
                  .option("ignoreLeadingWhiteSpace", true)
                  .option("ignoreTrailingWhiteSpace", true)  
                  .option("quote", "\"")
                  .csv("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/PurchaseContractLine")

print(PurchaseContractLine_Df.dropDuplicates("ContractHeader", "ContractLine","RegionId").count())
print(PurchaseContractLine_Df.count())

// COMMAND ----------

val PurchaseContractHeader_Df = spark.read
                  .option("inferSchema","false")
                  .option("delimiter","|")
                  .option("header","true")
                  .option("escape","\"")
                  .option("ignoreLeadingWhiteSpace", true)
                  .option("ignoreTrailingWhiteSpace", true)  
                  .option("quote", "\"")
                  .csv("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/PurchaseContractHeader")

PurchaseContractHeader_Df.createOrReplaceTempView("PurchaseContractHeader_src")

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC import re
// MAGIC path = "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/"
// MAGIC 
// MAGIC 
// MAGIC list_path = dbutils.fs.ls(path)
// MAGIC obj_name = "PurchaseOrderDetail" # change this to object name else untouched
// MAGIC 
// MAGIC delta = "_Delta"
// MAGIC 
// MAGIC for l in list_path:
// MAGIC   ls = dbutils.fs.ls(l[0])
// MAGIC   for ele in ls:
// MAGIC     if re.search(obj_name, ele[1]):
// MAGIC       #
// MAGIC       try:
// MAGIC         print("Count For "+ele[0].split("/")[-3]+obj_name+": \t")
// MAGIC         print(spark.table(ele[0].split("/")[-3]+obj_name+"_Delta").count())
// MAGIC       except:
// MAGIC         print("Error For "+ ele[0].split("/")[-3]+obj_name+delta )
// MAGIC 
// MAGIC 
// MAGIC print(obj_name + " Total Count : ")
// MAGIC print(spark.read.format("delta").load(path+obj_name+"/Processed_Parquet/").count())

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC import re
// MAGIC path = "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/"
// MAGIC 
// MAGIC 
// MAGIC list_path = dbutils.fs.ls(path)
// MAGIC obj_name = "PurchaseContractLineCoverageDetail" # change this to object name else untouched
// MAGIC 
// MAGIC delta = "_Delta"
// MAGIC 
// MAGIC for l in list_path:
// MAGIC   ls = dbutils.fs.ls(l[0])
// MAGIC   for ele in ls:
// MAGIC     if re.search(obj_name, ele[1]):
// MAGIC       #
// MAGIC       try:
// MAGIC         print("Count For "+ele[0].split("/")[-3]+obj_name+": \t")
// MAGIC         print(spark.table(ele[0].split("/")[-3]+obj_name+"_Delta").count())
// MAGIC       except:
// MAGIC         print("Error For "+ ele[0].split("/")[-3]+obj_name+delta )
// MAGIC 
// MAGIC 
// MAGIC print(obj_name + " Total Count : ")
// MAGIC print(spark.read.format("delta").load(path+obj_name+"/Processed_Parquet/").count())

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC import re
// MAGIC path = "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/"
// MAGIC 
// MAGIC 
// MAGIC list_path = dbutils.fs.ls(path)
// MAGIC obj_name = "PurchaseContractLineDetail" # change this to object name else untouched
// MAGIC 
// MAGIC delta = "_Delta"
// MAGIC 
// MAGIC for l in list_path:
// MAGIC   ls = dbutils.fs.ls(l[0])
// MAGIC   for ele in ls:
// MAGIC     if re.search(obj_name, ele[1]):
// MAGIC       #
// MAGIC       try:
// MAGIC         print("Count For "+ele[0].split("/")[-3]+obj_name+": \t")
// MAGIC         print(spark.table(ele[0].split("/")[-3]+obj_name+"_Delta").count())
// MAGIC       except:
// MAGIC         print("Error For "+ ele[0].split("/")[-3]+obj_name+delta )
// MAGIC 
// MAGIC 
// MAGIC print(obj_name + " Total Count : ")
// MAGIC print(spark.read.format("delta").load(path+obj_name+"/Processed_Parquet/").count())

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/adls/centrallake/UniversalDataLake/InternalSources/OTM/Table/SHIPMENT_STATUS/Processed_Parquet

// COMMAND ----------

dbutils.fs.ls("/mnt/adls/centrallake/UniversalDataLake/InternalSources/OTM/Table/SHIPMENT_STATUS/Processed_Parquet")

// COMMAND ----------

dbutils.fs.rm("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/PurchaseOrderDetail/",true)

// COMMAND ----------



// COMMAND ----------

// MAGIC %run /Unilever/BDL/SC/SharedLib/Utilities

// COMMAND ----------

getPathsBetweenDates("2020-01-17","2020-01-19","/mnt/alds/centrallake/UniversalDataLake/InternalSources/U2K2ECC/Table/COEP/Processed_Parquet/").length

// COMMAND ----------

val coepPaths_List=getPathsBetweenDates(startDate,endDate,mntPointUDL + coepParquetPath)
      if(coepPaths_List.length == 0){
        val msg = String.format("No File Exists for date: %s",endDate)
        throw new Exception(msg)
      } 

// COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/adhtimetemp",true)

// COMMAND ----------

spark.sql("""DROP TABLE IF EXISTS ADHTimeTemp""")

spark.sql(s"""CREATE TABLE ADHTimeTemp 

AS 

SELECT

'A' as RegionID,

MAX(BDLLoadTimeStamp) AS ADHLastLoad,

Current_Timestamp AS ParameterTime

FROM AccDocHeaderDelta""")

// COMMAND ----------

val df = spark.read.format("csv").option("sep","|").option("header",true).load("/mnt/adls/centrallake/UniversalDataLake/InternalSources/CordilleraECC/Table/BSEG/Processed")

df.createOrReplaceTempView("df")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC 
// MAGIC select * from df where 
// MAGIC BUKRS = "2646" and
// MAGIC BELNR = "1270000012"
// MAGIC GJAHR

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from  AccountingDocumentLineDelta where AccountingDocumentNumber = 1270000012 and CompanyCode = 2646 and FiscalYear = 2017 

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select ContractEndDate from PurchaseContractLineDetail_Delta where ContractEndDate is not null and ContractEndDate >= current_date()

// COMMAND ----------

// MAGIC %sql
// MAGIC desc DeliveryHeader_Delta TotalSpotsQuantity  

// COMMAND ----------

// MAGIC %sql
// MAGIC with DLUNQ as(
// MAGIC select DeliveryLineCode
// MAGIC ,RegionID
// MAGIC ,SUM(DeliveryQuantity) as DeliveryQuantity
// MAGIC ,SUM(ActualQuantityDeliveredStockKeepingUnits) as ActualQuantityDeliveredStockKeepingUnits
// MAGIC ,SUM(GrossWeight) as GrossWeight
// MAGIC ,SUM(NetWeight) as NetWeight
// MAGIC ,max(PlantCode) as PlantCode
// MAGIC ,max(ReferenceDocumentCode) as ReferenceDocumentCode
// MAGIC ,SUM(DeliveredVolume) as DeliveredVolume
// MAGIC 
// MAGIC from DeliveryLine_Delta group by DeliveryLineCode,RegionID 
// MAGIC ) 
// MAGIC 
// MAGIC select count(1) from (Select 
// MAGIC case when DH.DeliveryType  = "EL" then PDH.SupplyingPlant end as SupplyPlantCode,
// MAGIC case when DH.DeliveryType != "EL" then PDH.SupplyingPlant end as SupplyVendorCode
// MAGIC  from DeliveryHeader_Delta DH left join 
// MAGIC DLUNQ DL on cast(DL.DeliveryLineCode as int) = cast(DH.DeliveryCode as int)
// MAGIC and DL.RegionId = DH.RegionId
// MAGIC left join PurchaseDocumentHeaderMainTable PDH on 
// MAGIC PDH.PurchaseDocumentCode = DL.ReferenceDocumentCode 
// MAGIC and PDH.RegionID = DL.RegionId
// MAGIC where DL.ReferenceDocumentCode is not null) a where SupplyPlantCode is not null

// COMMAND ----------

// MAGIC %sql
// MAGIC with DLUNQ as(
// MAGIC select DeliveryLineCode
// MAGIC ,RegionID
// MAGIC ,SUM(DeliveryQuantity) as DeliveryQuantity
// MAGIC ,SUM(ActualQuantityDeliveredStockKeepingUnits) as ActualQuantityDeliveredStockKeepingUnits
// MAGIC ,SUM(GrossWeight) as GrossWeight
// MAGIC ,SUM(NetWeight) as NetWeight
// MAGIC ,max(PlantCode) as PlantCode
// MAGIC ,max(ReferenceDocumentCode) as ReferenceDocumentCode
// MAGIC ,SUM(DeliveredVolume) as DeliveredVolume
// MAGIC 
// MAGIC from DeliveryLine_Delta group by DeliveryLineCode,RegionID 
// MAGIC ) 
// MAGIC 
// MAGIC Select count(1)
// MAGIC  from DeliveryHeader_Delta DH left join 
// MAGIC DLUNQ DL on DL.DeliveryLineCode = DH.DeliveryCode
// MAGIC and DL.RegionId = DH.RegionId
// MAGIC left join PurchaseDocumentHeaderMainTable PDH on 
// MAGIC PDH.PurchaseDocumentCode = DL.ReferenceDocumentCode
// MAGIC and PDH.RegionID = DL.RegionId
// MAGIC where DL.ReferenceDocumentCode is not null and DH.DeliveryType ="EL" and PDH.supplyingplant is not null

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC 
// MAGIC select count(1),BDLUpdateTimeStamp from DeliveryHeader_Delta group by 2  

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from DeliveryHeaderLogisticsLog 

// COMMAND ----------

spark.read.format("delta").load("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/DeliveryHeader/Processed_Parquet").count

// COMMAND ----------

display(dbutils.fs.mounts())

// COMMAND ----------

// MAGIC %python 
// MAGIC 
// MAGIC import re
// MAGIC path = "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/"
// MAGIC 
// MAGIC 
// MAGIC list_path = dbutils.fs.ls(path)
// MAGIC obj_name = "PurchaseOrderDetail" # change this to object name else untouched
// MAGIC 
// MAGIC delta = "_Delta"
// MAGIC 
// MAGIC for l in list_path:
// MAGIC   ls = dbutils.fs.ls(l[0])
// MAGIC   for ele in ls:
// MAGIC     if re.search(obj_name, ele[1]):
// MAGIC       #
// MAGIC       try:
// MAGIC 	print(ele[0].split("/")[-3]+obj_name+"\t")
// MAGIC         #println(spark.sql("""select count(1) from """+ele[0].split("/")[-3]+obj_name+delta))
// MAGIC         spark.sql("""drop table """+ ele[0].split("/")[-3]+obj_name+delta)
// MAGIC         dbutils.fs.rm(ele[0],True)
// MAGIC         print(ele[0].split("/")[-3]+obj_name+delta)
// MAGIC         #if ele[0].split("/")[-3][0] == 'R':
// MAGIC           #spark.sql("""drop table """+ ele[0].split("/")[-3]+obj_name+delta)
// MAGIC           #dbutils.fs.rm(ele[0],True)
// MAGIC           #print(ele[0].split("/")[-3]+obj_name+delta + " " + str(spark.table(ele[0].split("/")[-3]+obj_name+delta).count()))
// MAGIC       except:
// MAGIC         print("Error For "+ ele[0].split("/")[-3]+obj_name+delta )

// COMMAND ----------

// MAGIC %python
// MAGIC import re
// MAGIC path = "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/"
// MAGIC 
// MAGIC 
// MAGIC list_path = dbutils.fs.ls(path)
// MAGIC obj_name = "PurchaseOrderDetail" #change this to object name else untouched
// MAGIC 
// MAGIC for l in list_path:
// MAGIC   ls = dbutils.fs.ls(l[0])
// MAGIC   for ele in ls:
// MAGIC     if re.search(obj_name, ele[1]):
// MAGIC       dbutils.fs.rm(ele[0],True)
// MAGIC       print(ele[0])

// COMMAND ----------

display(dbutils.fs.ls("/mnt/adls/centrallake/UniversalDataLake/InternalSources/CordilleraECC/Table/COEP/Processed/YYYY=2016"))

// COMMAND ----------

display(spark.read.csv("/mnt/adls/centrallake//BusinessDataLake/SC/AuditLogs/Procurement//PurchaseOrderDetail"))

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select BDLLoadTimestamp ,BDLUpdateTimeStamp, count(1) from hierarchyDemandPlanDelta group by BDLLoadTimestamp,BDLUpdateTimeStamp

// COMMAND ----------

UniversalDataLake/InternalSources/CordilleraECC/Table/CAWN/
UniversalDataLake/InternalSources/CordilleraECC/Table/CAWNT/
UniversalDataLake/InternalSources/CordilleraECC/Table/VBUK/
UniversalDataLake/InternalSources/CordilleraECC/Table/VBRK/
UniversalDataLake/InternalSources/CordilleraECC/Table/T001/

// COMMAND ----------

display(dbutils.fs.ls("/mnt/adls/centrallake/UniversalDataLake/InternalSources/CordilleraECC/Table/CAWN/"))

// COMMAND ----------

val PurchaseContractLine_Df = spark.read
                  .option("inferSchema","false")
                  .option("delimiter","|")
                  .option("header","true")
                  .option("escape","\"")
                  .option("ignoreLeadingWhiteSpace", true)
                  .option("ignoreTrailingWhiteSpace", true)  
                  .option("quote", "\"")
                  .csv("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/DemandPlanningDeliveriesAtCustomer")

// COMMAND ----------

val PurchaseContractLine_Df = spark.read
                  .option("inferSchema","false")
                  .option("delimiter","|")
                  .option("header","true")
                  .option("escape","\"")
                  .option("ignoreLeadingWhiteSpace", true)
                  .option("ignoreTrailingWhiteSpace", true)  
                  .option("quote", "\"")
                  .csv("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/DemandPlanningDeliveriesAtCustomer")

PurchaseContractLine_Df.createOrReplaceTempView("PurchaseContractLine_src")

//Reading PurchaseContractHeader Data
val PurchaseContractHeader_Df = spark.read
                  .option("inferSchema","false")
                  .option("delimiter","|")
                  .option("header","true")
                  .option("escape","\"")
                  .option("ignoreLeadingWhiteSpace", true)
                  .option("ignoreTrailingWhiteSpace", true)  
                  .option("quote", "\"")
                  .csv("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/PurchaseContractHeader")

PurchaseContractHeader_Df.createOrReplaceTempView("PurchaseContractHeader_src")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select ContractHeader
// MAGIC ,ContractLine
// MAGIC ,RegionId from PurchaseContractLine_src

// COMMAND ----------

display(spark.read.option("header","true").option("sep","|").csv("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/APOPlannedProductionWithResource/Processed").where($"RegionId" === "A"))

// COMMAND ----------

display(spark.read.option("header","true").option("sep","|").csv("/mnt/adls/centrallake/BusinessDataLake/SC/ReferenceObject/DemandPlanningDeliveryType/Processed"))

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(1) , RegionId from SalesOrderLine_Delta
// MAGIC group by Regionid

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select BDLUpdateTimestamp,count(1) from BillingDocumentLineU2K2CSLM_Delta
// MAGIC group by BDLUpdateTimestamp

// COMMAND ----------

// MAGIC %sql
// MAGIC select BDLLoadTimestamp,count(1) 
// MAGIC from BillingDocumentLine_Delta
// MAGIC group by BDLLoadTimestamp 
// MAGIC order by 1 desc
