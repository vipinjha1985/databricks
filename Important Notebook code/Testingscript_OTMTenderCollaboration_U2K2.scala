// Databricks notebook source
// MAGIC %run /Unilever/BDL/SC/SharedLib/Utilities

// COMMAND ----------

// MAGIC %run /Unilever/BDL/SC/SharedLib/Configuration 

// COMMAND ----------

val mntPointUDL = mountPointReadUDL()
val mntPointRdBDL = mountPointReadBDL()
val mntPointBDL = mountPointWriteBDL()

// COMMAND ----------

dbutils.widgets.text("inParam_targetPath","/BusinessDataLake/SC/TransactionObject/OTMTenderCollaboration/Processed")
dbutils.widgets.text("inParam_tendercollaborationpath","/UniversalDataLake/InternalSources/OTM/BigDataAdapter/AAR/TENDER_COLLABORATION_T/Processed_Parquet")
dbutils.widgets.text("inParam_regionID", "R")
val tendercollaborationpath = dbutils.widgets.get("inParam_tendercollaborationpath")

//LATAM - /UniversalDataLake/InternalSources/OTM/BigDataAdapter/LA/TENDER_COLLABORATION/Processed_Parquet
//Cordillera - /UniversalDataLake/InternalSources/OTM/Table/TENDER_COLLABORATION/Processed_Parquet
//U2K2- /UniversalDataLake/InternalSources/OTM/BigDataAdapter/AAR/TENDER_COLLABORATION_T/Processed_Parquet
val targetPath = dbutils.widgets.get("inParam_targetPath")
val regionID = dbutils.widgets.get("inParam_regionID")
// val mntPointUDL = mountPointReadUDL()
// val mntPointBDL = mountPointWriteBDL()
dbutils.widgets.text("inParam_startDate","")
val startDate = dbutils.widgets.get("inParam_startDate")    
dbutils.widgets.text("inParam_endDate","")
val endDate = dbutils.widgets.get("inParam_endDate")
val parquet = "_Parquet"

// COMMAND ----------

dbutils.widgets.text("inParam_OTMShipmentRefnumpath","/BusinessDataLake/SC/ReferenceObject/OTMShipmentRefNum/Processed")
val OTMShipmentRefnumpath = dbutils.widgets.get("inParam_OTMShipmentRefnumpath")
dbutils.widgets.text("inParam_OTMLocationpath","/BusinessDataLake/SC/ReferenceObject/OTMLocation/Processed")
val OTMLocationpath = dbutils.widgets.get("inParam_OTMLocationpath")
dbutils.widgets.text("inParam_OTMShipmentpath","/BusinessDataLake/SC/TransactionObject/OTMShipment/Processed")
val OTMShipmentpath = dbutils.widgets.get("inParam_OTMShipmentpath")
dbutils.widgets.text("inParam_ShipmentHeaderpath","/BusinessDataLake/SC/TransactionObject/ShipmentHeader/Processed")
val ShipmentHeaderpath = dbutils.widgets.get("inParam_ShipmentHeaderpath")
dbutils.widgets.text("inParam_OTMTenderCollabServprovpath","/BusinessDataLake/SC/TransactionObject/OTMTenderCollabServprov/Processed")
val OTMTenderCollabServprovpath = dbutils.widgets.get("inParam_OTMTenderCollabServprovpath")
dbutils.widgets.text("inParam_OTMTenderCollaborationStatuspath","/BusinessDataLake/SC/TransactionObject/OTMTenderCollaborationStatus/Processed")
val OTMTenderCollaborationStatuspath = dbutils.widgets.get("inParam_OTMTenderCollaborationStatuspath")


// COMMAND ----------

// DBTITLE 1,Source Dataframe
val OTMShipmentRefnum_DF = readDeltaParquetToDF(mntPointBDL+OTMShipmentRefnumpath,regionID)
OTMShipmentRefnum_DF.createOrReplaceTempView("OTMShipmentRefnum_VW")

var OTMLocation_DF = readDeltaParquetToDF(mntPointRdBDL+OTMLocationpath,regionID)
OTMLocation_DF.createOrReplaceTempView("OTMLocation_VW")

val ShipmentHeader_DF = readDeltaParquetToDF(mntPointRdBDL+ShipmentHeaderpath)
ShipmentHeader_DF.createOrReplaceTempView("ShipmentHeader_VW")

val OTMTenderCollabServprov_DF = readDeltaParquetToDF(mntPointBDL+OTMTenderCollabServprovpath,regionID)
OTMTenderCollabServprov_DF.createOrReplaceTempView("OTMTenderCollabServprov_VW")

var OTMTenderCollaborationStatus_DF = readDeltaParquetToDF(mntPointRdBDL+OTMTenderCollaborationStatuspath,regionID)
OTMTenderCollaborationStatus_DF.createOrReplaceTempView("OTMTenderCollaborationStatus_temp")
val OTMTenderCollaborationStatus_DF1 = spark.sql(""" SELECT *, ROW_NUMBER() OVER (partition by ITransactionNumber,StatusTypeGID,RegionId ORDER BY InsertDate, UpdateDate DESC, StatusValueGID ) as rank from OTMTenderCollaborationStatus_temp""").filter($"rank"==="1").drop($"rank")
OTMTenderCollaborationStatus_DF1.createOrReplaceTempView("OTMTenderCollaborationStatus_VW")

val OTMShipment_DF = readDeltaParquetToDF(mntPointBDL+OTMShipmentpath,regionID)
OTMShipment_DF.createOrReplaceTempView("OTMShipment_VW")

// COMMAND ----------

val now = Calendar.getInstance();
val month = if ((now.get(Calendar.MONTH)+ 1)<10) "0"+(now.get(Calendar.MONTH)+ 1) else (now.get(Calendar.MONTH)+ 1)
val day = if (now.get(Calendar.DATE)<10) "0"+(now.get(Calendar.DATE)) else (now.get(Calendar.DATE)-1)
var start_year = (now.get(Calendar.YEAR) - 3) + "-" + month + "-" + day

// COMMAND ----------

// DBTITLE 1,Tendercollabation
val tendercollaboration_DF =readDeltaParquetToDF(mntPointUDL+tendercollaborationpath,startDate,endDate).withColumn("RegionID", lit(regionID))
  val  tendercollaborationFiltered_DF = tendercollaboration_DF.filter($"INSERT_DATE">="2017")
  
  tendercollaborationFiltered_DF.createOrReplaceTempView("tendercollabation")

// COMMAND ----------

tendercollaborationFiltered_DF.count()

// COMMAND ----------

var tendercollabDF = spark.sql(""" SELECT  DISTINCT
                          --CAST(I_TRANSACTION_NO AS double) AS I_TRANSACTION_NO,
                          I_TRANSACTION_NO,
                          RegionID,
                          right(SHIPMENT_GID,char_length(SHIPMENT_GID)-instr(SHIPMENT_GID,".")) as SHIPMENT_GID,
                          TENDER_TYPE,
                          PLANNED_COST_CURRENCY_GID,
                          right(DESTINATION_LOCATION, char_length(DESTINATION_LOCATION)-instr(DESTINATION_LOCATION,'.')) as 
                          DESTINATION_LOCATION,
                          right(ORIGIN_LOCATION, char_length(ORIGIN_LOCATION)-instr(ORIGIN_LOCATION,'.')) as ORIGIN_LOCATION,
                          COALESCE(INSERT_DATE,"1999-01-01 00:00:00") as INSERT_DATE,
                          left(SHIPMENT_GID,instr(SHIPMENT_GID,'.')-1) AS DomainName,
                          DELIVERY_TIME As  DeliveryTime,
                          EXPECTED_RESPONSE As  ExpectedResponseTime,
                          INSERT_USER As  InsertUserName,
                          ORIGINAL_PICKUP_TIME As  OriginalPickupTime,
                          PLANNED_COST As  PlannedCostAmount,
                          PLANNED_COST_BASE As  PlannedCostBaseAmount,
                          PROCESS_CONTROL_REQUEST_ID As  ProcessControlRequestId,
                          CASE WHEN RATE_GEO_GID='' or RATE_GEO_GID is null then  'UNK'  ELSE RATE_GEO_GID  END AS  RateGeoGid,
                          SHIPMENT_TIME As  ShipmentTime,
                          STEP_RESPONSE_TIME As  StepResponseTime,
                          STEP_RESPONSE_TIME_BASE As  StepResponseTimeBaseAmount,
                          STEP_RESPONSE_TIME_UOM_CODE As  StepResponseTimeUomCode,
                          IS_STEP_TENDER As  StepTenderIndicator,
                          STEP_TENDER_PCR_ID As  StepTenderPcrId,
                          IS_TENDER_COUNT As  TenderCountIndicator,
                          UPDATE_DATE As  UpdateDate,
                          UPDATE_USER As  UpdateUserName,
                          optype as BDLFlag
                          
                          from tendercollabation""")
tendercollabDF.createOrReplaceTempView("tendercollab_VW")

// COMMAND ----------

// OTMLocationTable:---Remmoving Prifix

val OLTD = spark.sql(""" SELECT DISTINCT
                         RegionId,
                          CONCAT(UPPER(LEFT(City,1)),LOWER(SUBSTRING(City,2,LENGTH(City)))) as City, --ITSDLD-312
                          ProvinceCode,
                          ProvinceName,
                         -- REGEXP_REPLACE(LocationXID,"[^0-9]", '') AS LocationXID
                         LocationXID --ITSDLD-312
                          from OTMLocation_VW""")

OLTD.createOrReplaceTempView("OLTD")

// COMMAND ----------

// OTMShipmentTable::--- Removing Prifix

val OTMShip = spark.sql(""" SELECT DISTINCT
                                 RegionId,
                                 right(SourceLocationGID,char_length(SourceLocationGID)-instr(SourceLocationGID,".")) as SourceLocationGID,
                                 DestLocationGID,
                                 SHIPMENTGID,
                                 ServprovGID,
                                 FirstEquipmentGroupGid
                                 
                                 from OTMShipment_VW""")

OTMShip.createOrReplaceTempView("OTMShip")

// COMMAND ----------

/* -- Load UDL & BDL data into Temporary Table, apply business logic for required records as mentioned in DMR */

val OTMTenderCollaboration_DF = spark.sql("""SELECT DISTINCT
                                     cast(cast(TC.I_TRANSACTION_NO as decimal(12,0)) as string)  as ITransactionNumber,
                                     
                                     TC.RegionId,
                                     TC.SHIPMENT_GID AS ShipmentGID,
                                     TC.TENDER_TYPE AS TenderTypeCode,
                                     TC.PLANNED_COST_CURRENCY_GID AS PlannedCostCurrencyGID,
                                     
                                     CASE WHEN OSR.ShipmentRefnumValue = 'SECONDARY' THEN 'Outbound'
                                     WHEN OSR.ShipmentRefnumValue = 'INBOUND' THEN 'Inbound'
                                     WHEN OSR.ShipmentRefnumValue = 'Outbound' THEN 'STO'
                                     END AS TransportationLegNumber,
--ITSDLD-312
                                     CASE WHEN (OL_Source.City is not null OR OL_Source.City <>'') AND (OL_Source.ProvinceCode is not null 
                                     OR OL_Source.ProvinceCode <>'') 
                                     THEN CONCAT((NVL(CONCAT(OL_Source.City,", "),'')),(NVL(OL_Source.ProvinceCode,''))) 
                                      WHEN (OL_Source.City is not null OR OL_Source.City <>'') AND (OL_Source.ProvinceCode is null 
                                     OR OL_Source.ProvinceCode = '')
                                     THEN OL_Source.City 
                                     ELSE OL_Source.ProvinceCode 
END AS OriginCityName,

                                     CASE WHEN (OL_Destination.City is not null OR OL_Destination.City <>'') AND 
                                     (OL_Destination.ProvinceCode  is not null OR OL_Destination.ProvinceCode <>'') 
                                     THEN CONCAT((NVL(CONCAT(OL_Destination.City,", "),'')),(NVL(OL_Destination.ProvinceCode,''))) 
                                      WHEN (OL_Destination.City is not null OR OL_Source.City <>'') AND 
                                     (OL_Destination.ProvinceCode is null OR OL_Destination.ProvinceCode = '')
                                     THEN OL_Destination.City 
                                     ELSE OL_Destination.ProvinceCode 
END AS DestinationCityName, 
--ITSDLD-312
  
                                    -- CONCAT((NVL(CONCAT(OL_Source.City,", "),'')),(NVL(OL_Source.ProvinceCode,''))) AS OriginCityName,
                                    -- CONCAT((NVL(CONCAT(OL_Destination.City,", "),'')),(NVL(OL_Destination.ProvinceCode,''))) AS DestinationCityName,

                                     CASE WHEN  OTCS.StatusValueGID LIKE '%ACCEPTED%' THEN 1
                                          ELSE 0
                                     END AS TenderAcceptedIndicator,

                                     CASE WHEN  OTCS.StatusValueGID LIKE '%WITHDRAWN%' THEN 1
                                          ELSE 0
                                     END AS TenderWithdrawnQuantity,

                                      CASE WHEN  OTCS.StatusValueGID LIKE '%DECLINED%' THEN 1
                                        ELSE 0
                                     END AS TenderDeclinedIndicator,
                                           
                                     CASE WHEN  OTCS.StatusValueGID LIKE '%TIMEDOUT%' THEN 1
                                           ELSE 0
                                      END AS TimeOutIndicator,


                                       COALESCE(SH.PlannedShipmentStartDate,"1999-09-09 00:00:00") AS PlannedShipmentStartDate,
                                       TC.DESTINATION_LOCATION AS DestinationLocationCode,
                                       TC.ORIGIN_LOCATION AS OriginLocationCode,
                                       OS.ServprovGID AS ActualCarrierCode,

                                        CASE WHEN 
                                        OTCS.StatusValueGID LIKE '%ACCEPTED%' THEN 'ACCEPTED' 
                                        ELSE
                                        CASE WHEN
                                        OTCS.StatusValueGID LIKE '%WITHDRAWN%' THEN 'WITHDRAWN'
                                        ELSE
                                        CASE WHEN
                                        OTCS.StatusValueGID LIKE '%TIMEDOUT%' THEN 'TIMEDOUT'
                                        ELSE
                                        CASE WHEN
                                        OTCS.StatusValueGID LIKE '%DECLINED%' THEN 'DECLINED'
                                        ELSE 'UNKNOWN' END
                                          END 
                                          END
                                          END
                                          AS BehaviourTypeCode,
  
                                        CASE WHEN OS.ServprovGID = OTCSP.ServprovGid THEN 'Final'
                                          ELSE 'Not Final'
                                        END AS FinalCarrierCode,

                                        TC.INSERT_DATE AS InsertDate,

                                        OS.FirstEquipmentGroupGid AS OTMEquipmentID,
                                        OTCS.StatusValueGID AS TenderStatusCode,

                                        CASE WHEN OTCS.StatusValueGID LIKE '%WITHDRAWN%' THEN 1
                                        ELSE 0
                                        END AS TenderWithdrawnIndicator,
                                        TC.DomainName,
                                        TC.DeliveryTime,
                                        TC.ExpectedResponseTime,
                                        TC.InsertUserName,
                                        TC.OriginalPickupTime,
                                        TC.PlannedCostAmount,
                                        TC.PlannedCostBaseAmount,
                                        TC.ProcessControlRequestId,
                                        TC.RateGeoGid,
                                        TC.ShipmentTime,
                                        TC.StepResponseTime,
                                        TC.StepResponseTimeBaseAmount,
                                        TC.StepResponseTimeUomCode,
                                        TC.StepTenderIndicator,
                                        TC.StepTenderPcrId,
                                        TC.TenderCountIndicator,
                                        TC.UpdateDate,
                                        TC.UpdateUserName,
                                        current_timestamp  AS  BDLLoadTimeStamp,
                                        current_timestamp AS BDLUpdateTimeStamp,
                                        COALESCE(OTCS.UpdateDate,"1999-01-01 00:00:00"),
                                        TC.BDLFlag as BDLFlag                                        
                                        FROM  tendercollab_VW TC 
                                           
                                           
              LEFT JOIN OTMTenderCollaborationStatus_VW OTCS  ON TC.I_TRANSACTION_NO = OTCS.ITransactionNumber AND TC.RegionId = OTCS.RegionId
              
              LEFT JOIN OTMShipmentRefnum_VW OSR ON TC.SHIPMENT_GID = OSR.SHIPMENTGID AND  TC.RegionId = OSR.RegionId
              
              LEFT JOIN OTMShip OS ON TC.SHIPMENT_GID = OS.SHIPMENTGID AND TC.RegionId = OS.RegionId              

              LEFT JOIN OLTD OL_Source ON OS.SourceLocationGID = OL_Source.LocationXID AND OL_Source.RegionId = OS.RegionId

             LEFT JOIN OTMTenderCollabServprov_VW OTCSP ON  TC.I_TRANSACTION_NO = OTCSP.ITransactionNumber AND  TC.RegionId = OTCSP.RegionId

             LEFT JOIN OLTD OL_Destination ON OS.DestLocationGID = OL_Destination.LocationXID AND  OS.RegionId = OL_Destination.RegionId 

             LEFT JOIN ShipmentHeader_VW SH ON TC.SHIPMENT_GID = SH.SHIPMENTCode AND TC.RegionId = SH.RegionId
             
            -- where  OSR.ShipmentRefnumValue in ('SECONDARY','INBOUND','Outbound')
             """)

 OTMTenderCollaboration_DF.createOrReplaceTempView("OTMTenderCollaborationfinal")

// COMMAND ----------

//OTMTenderCollaboration.OriginCityName & OTMTenderCollaboration.DestinatinonCityName
val lineDF = spark.sql("""select *, CASE WHEN (OriginCityName <>'' or OriginCityName is not null) and (DestinationCityName <>'' 
                                  or DestinationCityName is not null) THEN  CONCAT((NVL(OriginCityName,'')),"_", (NVL(DestinationCityName,''))) 
                                  WHEN (OriginCityName <>'' or OriginCityName is not null) and (DestinationCityName = '' 
                                  or DestinationCityName is null) THEN  OriginCityName
                                  WHEN (OriginCityName ='' or OriginCityName is null)  and (DestinationCityName <>'' 
                                  or DestinationCityName is not null) THEN  DestinationCityName
                                  WHEN (OriginCityName ='' or OriginCityName is null) and (DestinationCityName = '' 
                                  or DestinationCityName is null) THEN  ' '
                                  END AS LaneID from OTMTenderCollaborationfinal""")
                                  //ITSDLD-312
lineDF.createOrReplaceTempView("OTMTenderCollaborationfinal1")

// COMMAND ----------

val MaxDateDF = spark.sql(""" SELECT ITransactionNumber,
                                      RegionId,
                                      ShipmentGID,
                                      TenderTypeCode,
                                      PlannedCostCurrencyGID,
                                      TransportationLegNumber,
                                      OriginCityName,
                                      DestinationCityName,
                                      TenderAcceptedIndicator,
                                      TenderWithdrawnQuantity,
                                      TenderDeclinedIndicator,
                                      TimeOutIndicator,
                                      PlannedShipmentStartDate,
                                      DestinationLocationCode,
                                      OriginLocationCode,
                                      ActualCarrierCode,
                                      BehaviourTypeCode,
                                      FinalCarrierCode,
                                      InsertDate,
                                      LaneID,
                                      OTMEquipmentID,
                                      TenderStatusCode,
                                      TenderWithdrawnIndicator,
                                      DomainName,
                                      DeliveryTime,
                                      ExpectedResponseTime,
                                      InsertUserName,
                                      OriginalPickupTime,
                                      PlannedCostAmount,
                                      PlannedCostBaseAmount,
                                      ProcessControlRequestId,
                                      RateGeoGid,
                                      ShipmentTime,
                                      StepResponseTime,
                                      StepResponseTimeBaseAmount,
                                      StepResponseTimeUomCode,
                                      StepTenderIndicator,
                                      StepTenderPcrId,
                                      TenderCountIndicator,
                                      UpdateDate,
                                      UpdateUserName,
                                      BDLLoadTimeStamp,
                                      BDLUpdateTimeStamp,
                                      BDLFlag AS BDLFlag,
                                      ROW_NUMBER() OVER(PARTITION BY 
                                                                 COALESCE(ITransactionNumber,0)
                                                                ,COALESCE(RegionId,0)
                                                                ,COALESCE(ShipmentGID,0) 
                                                                ,COALESCE(DomainName,0)
                                                                 ORDER BY InsertDate,TransportationLegNumber DESC) AS ROW_NUM
                                      from OTMTenderCollaborationfinal1 
                                              """).filter($"ROW_NUM"==="1").drop($"ROW_NUM")

MaxDateDF.createOrReplaceTempView("OTMTenderCollaboration_Src")

// COMMAND ----------

// DBTITLE 1,Reading Target
spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")//.where($"RegionId"===regionID)
val df_OTMTenderCollaboration_p  = spark.read.parquet(mntPointBDL+targetPath+ "_Parquet").where($"RegionId"===regionID)
df_OTMTenderCollaboration_p.createOrReplaceTempView("OTMTenderCollaboration_pqt")

// COMMAND ----------

// DBTITLE 1,Reading Market
val df_OTMTenderCollaboration_mktd = readDeltaParquetToDF(mntPointBDL+"/BusinessDataLake/SC/TransactionObject/RU2K2/OTMTenderCollaboration/Processed_Parquet")
df_OTMTenderCollaboration_mktd.createOrReplaceTempView("OTMTenderCollaboration_mkt")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from OTMTenderCollaboration_pqt

// COMMAND ----------

// DBTITLE 1,Count Check
// MAGIC %sql
// MAGIC SELECT "Source", RegionId, COUNT(*) FROM OTMTenderCollaboration_Src group by RegionId
// MAGIC union all
// MAGIC SELECT "Target", RegionId, COUNT(*) FROM OTMTenderCollaboration_pqt group by RegionId 
// MAGIC union all
// MAGIC SELECT "Market", RegionId, COUNT(*) FROM OTMTenderCollaboration_mkt group by RegionId

// COMMAND ----------

// DBTITLE 1,Duplicate Check
// MAGIC %sql
// MAGIC Select ITransactionNumber,RegionID,ShipmentGID,DomainName,Count(*) from OTMTenderCollaboration_pqt
// MAGIC Group by ITransactionNumber,RegionID,ShipmentGID,DomainName
// MAGIC having count(*)>1
// MAGIC union all
// MAGIC Select ITransactionNumber,RegionID,ShipmentGID,DomainName,Count(*) from OTMTenderCollaboration_mkt
// MAGIC Group by ITransactionNumber,RegionID,ShipmentGID,DomainName
// MAGIC having count(*)>1 

// COMMAND ----------

// DBTITLE 1,Null/Blank/Nothing Check
// MAGIC %sql
// MAGIC Select ITransactionNumber,RegionID,ShipmentGID,DomainName from OTMTenderCollaboration_pqt where 
// MAGIC ITransactionNumber is null or ITransactionNumber='' or
// MAGIC RegionID is null or RegionID='' or
// MAGIC ShipmentGID is null or ShipmentGID='' or 
// MAGIC DomainName is null or DomainName=''
// MAGIC union all
// MAGIC Select ITransactionNumber,RegionID,ShipmentGID,DomainName from OTMTenderCollaboration_mkt where 
// MAGIC ITransactionNumber is null or ITransactionNumber='' or
// MAGIC RegionID is null or RegionID='' or
// MAGIC ShipmentGID is null or ShipmentGID='' or 
// MAGIC DomainName is null or DomainName=''

// COMMAND ----------

// DBTITLE 1,Source minus Target
// MAGIC %sql
// MAGIC select
// MAGIC ITransactionNumber, RegionId, ShipmentGID, TenderTypeCode, PlannedCostCurrencyGID, TransportationLegNumber, OriginCityName, DestinationCityName, TenderAcceptedIndicator, TenderWithdrawnQuantity, TenderDeclinedIndicator, TimeOutIndicator, cast(PlannedShipmentStartDate as date), DestinationLocationCode, OriginLocationCode, ActualCarrierCode, BehaviourTypeCode, FinalCarrierCode, cast(InsertDate as date), LaneID, OTMEquipmentID, TenderStatusCode, TenderWithdrawnIndicator, DomainName, cast(DeliveryTime as date), cast(ExpectedResponseTime as date), InsertUserName, cast(OriginalPickupTime as date), cast(PlannedCostAmount as double), cast(PlannedCostBaseAmount as double), ProcessControlRequestId, RateGeoGid, cast(ShipmentTime as date), cast(StepResponseTime as double), StepResponseTimeBaseAmount, StepResponseTimeUomCode, StepTenderIndicator, StepTenderPcrId, TenderCountIndicator, cast(UpdateDate as date), UpdateUserName from OTMTenderCollaboration_Src
// MAGIC minus
// MAGIC select ITransactionNumber, RegionId, ShipmentGID, TenderTypeCode, PlannedCostCurrencyGID, TransportationLegNumber, OriginCityName, DestinationCityName, TenderAcceptedIndicator, TenderWithdrawnQuantity, TenderDeclinedIndicator, TimeOutIndicator, cast(PlannedShipmentStartDate as date), DestinationLocationCode, OriginLocationCode, ActualCarrierCode, BehaviourTypeCode, FinalCarrierCode, cast(InsertDate as date), LaneID, OTMEquipmentID, TenderStatusCode, TenderWithdrawnIndicator, DomainName, cast(DeliveryTime as date), cast(ExpectedResponseTime as date), InsertUserName, cast(OriginalPickupTime as date), cast(PlannedCostAmount as double), cast(PlannedCostBaseAmount as double), ProcessControlRequestId, RateGeoGid, cast(ShipmentTime as date), cast(StepResponseTime as double), StepResponseTimeBaseAmount, StepResponseTimeUomCode, StepTenderIndicator, StepTenderPcrId, TenderCountIndicator, cast(UpdateDate as date), UpdateUserName from OTMTenderCollaboration_pqt 

// COMMAND ----------

// DBTITLE 1,Target minus Source
// MAGIC %sql
// MAGIC select ITransactionNumber, RegionId, ShipmentGID, TenderTypeCode, PlannedCostCurrencyGID, TransportationLegNumber, OriginCityName, DestinationCityName, TenderAcceptedIndicator, TenderWithdrawnQuantity, TenderDeclinedIndicator, TimeOutIndicator, cast(PlannedShipmentStartDate as date), DestinationLocationCode, OriginLocationCode, ActualCarrierCode, BehaviourTypeCode, FinalCarrierCode, cast(InsertDate as date), LaneID, OTMEquipmentID, TenderStatusCode, TenderWithdrawnIndicator, DomainName, cast(DeliveryTime as date), cast(ExpectedResponseTime as date), InsertUserName, cast(OriginalPickupTime as date), cast(PlannedCostAmount as double), cast(PlannedCostBaseAmount as double), ProcessControlRequestId, RateGeoGid, cast(ShipmentTime as date), cast(StepResponseTime as double), StepResponseTimeBaseAmount, StepResponseTimeUomCode, StepTenderIndicator, StepTenderPcrId, TenderCountIndicator, cast(UpdateDate as date), UpdateUserName from OTMTenderCollaboration_pqt
// MAGIC minus
// MAGIC select ITransactionNumber, RegionId, ShipmentGID, TenderTypeCode, PlannedCostCurrencyGID, TransportationLegNumber, OriginCityName, DestinationCityName, TenderAcceptedIndicator, TenderWithdrawnQuantity, TenderDeclinedIndicator, TimeOutIndicator, cast(PlannedShipmentStartDate as date), DestinationLocationCode, OriginLocationCode, ActualCarrierCode, BehaviourTypeCode, FinalCarrierCode, cast(InsertDate as date), LaneID, OTMEquipmentID, TenderStatusCode, TenderWithdrawnIndicator, DomainName, cast(DeliveryTime as date), cast(ExpectedResponseTime as date), InsertUserName, cast(OriginalPickupTime as date), cast(PlannedCostAmount as double), cast(PlannedCostBaseAmount as double), ProcessControlRequestId, RateGeoGid, cast(ShipmentTime as date), cast(StepResponseTime as double), StepResponseTimeBaseAmount, StepResponseTimeUomCode, StepTenderIndicator, StepTenderPcrId, TenderCountIndicator, cast(UpdateDate as date), UpdateUserName from OTMTenderCollaboration_Src

// COMMAND ----------

// DBTITLE 1,Source minus Market
// MAGIC %sql
// MAGIC select ITransactionNumber, RegionId, ShipmentGID, TenderTypeCode, PlannedCostCurrencyGID, TransportationLegNumber, OriginCityName, DestinationCityName, TenderAcceptedIndicator, TenderWithdrawnQuantity, TenderDeclinedIndicator, TimeOutIndicator, cast(PlannedShipmentStartDate as date), DestinationLocationCode, OriginLocationCode, ActualCarrierCode, BehaviourTypeCode, FinalCarrierCode, cast(InsertDate as date), LaneID, OTMEquipmentID, TenderStatusCode, TenderWithdrawnIndicator, DomainName, cast(DeliveryTime as date), cast(ExpectedResponseTime as date), InsertUserName, cast(OriginalPickupTime as date), cast(PlannedCostAmount as double), cast(PlannedCostBaseAmount as double), ProcessControlRequestId, RateGeoGid, cast(ShipmentTime as date), cast(StepResponseTime as double), StepResponseTimeBaseAmount, StepResponseTimeUomCode, StepTenderIndicator, StepTenderPcrId, TenderCountIndicator, cast(UpdateDate as date), UpdateUserName from OTMTenderCollaboration_Src
// MAGIC minus
// MAGIC select ITransactionNumber, RegionId, ShipmentGID, TenderTypeCode, PlannedCostCurrencyGID, TransportationLegNumber, OriginCityName, DestinationCityName, TenderAcceptedIndicator, TenderWithdrawnQuantity, TenderDeclinedIndicator, TimeOutIndicator, cast(PlannedShipmentStartDate as date), DestinationLocationCode, OriginLocationCode, ActualCarrierCode, BehaviourTypeCode, FinalCarrierCode, cast(InsertDate as date), LaneID, OTMEquipmentID, TenderStatusCode, TenderWithdrawnIndicator, DomainName, cast(DeliveryTime as date), cast(ExpectedResponseTime as date), InsertUserName, cast(OriginalPickupTime as date), cast(PlannedCostAmount as double), cast(PlannedCostBaseAmount as double), ProcessControlRequestId, RateGeoGid, cast(ShipmentTime as date), cast(StepResponseTime as double), StepResponseTimeBaseAmount, StepResponseTimeUomCode, StepTenderIndicator, StepTenderPcrId, TenderCountIndicator, cast(UpdateDate as date), UpdateUserName from OTMTenderCollaboration_mkt 

// COMMAND ----------

// DBTITLE 1,Market minus Source
// MAGIC %sql
// MAGIC select ITransactionNumber, RegionId, ShipmentGID, TenderTypeCode, PlannedCostCurrencyGID, TransportationLegNumber, OriginCityName, DestinationCityName, TenderAcceptedIndicator, TenderWithdrawnQuantity, TenderDeclinedIndicator, TimeOutIndicator, cast(PlannedShipmentStartDate as date), DestinationLocationCode, OriginLocationCode, ActualCarrierCode, BehaviourTypeCode, FinalCarrierCode, cast(InsertDate as date), LaneID, OTMEquipmentID, TenderStatusCode, TenderWithdrawnIndicator, DomainName, cast(DeliveryTime as date), cast(ExpectedResponseTime as date), InsertUserName, cast(OriginalPickupTime as date), cast(PlannedCostAmount as double), cast(PlannedCostBaseAmount as double), ProcessControlRequestId, RateGeoGid, cast(ShipmentTime as date), cast(StepResponseTime as double), StepResponseTimeBaseAmount, StepResponseTimeUomCode, StepTenderIndicator, StepTenderPcrId, TenderCountIndicator, cast(UpdateDate as date), UpdateUserName from OTMTenderCollaboration_mkt where Regionid='R'
// MAGIC minus
// MAGIC select ITransactionNumber, RegionId, ShipmentGID, TenderTypeCode, PlannedCostCurrencyGID, TransportationLegNumber, OriginCityName, DestinationCityName, TenderAcceptedIndicator, TenderWithdrawnQuantity, TenderDeclinedIndicator, TimeOutIndicator, cast(PlannedShipmentStartDate as date), DestinationLocationCode, OriginLocationCode, ActualCarrierCode, BehaviourTypeCode, FinalCarrierCode, cast(InsertDate as date), LaneID, OTMEquipmentID, TenderStatusCode, TenderWithdrawnIndicator, DomainName, cast(DeliveryTime as date), cast(ExpectedResponseTime as date), InsertUserName, cast(OriginalPickupTime as date),
// MAGIC cast(PlannedCostAmount as double), cast(PlannedCostBaseAmount as double), ProcessControlRequestId, RateGeoGid, cast(ShipmentTime as date), cast(StepResponseTime as double), StepResponseTimeBaseAmount, StepResponseTimeUomCode, StepTenderIndicator, StepTenderPcrId, TenderCountIndicator, cast(UpdateDate as date), UpdateUserName from OTMTenderCollaboration_Src

// COMMAND ----------

// MAGIC %sql
// MAGIC desc OTMTenderCollaboration_pqt

// COMMAND ----------

// MAGIC %run /Unilever/BDL/SC_GLOBAL_BDL_Test/Shared/Automation_Logs 

// COMMAND ----------

val Capability="Team #6"  //Stream name
val Layer="BDL"  //Target Layer
val ObjectName="OTMTenderCollaboration_U2K2"  // Object Name

   //Duplicate check 
val Dup_Test= "Duplicate"
val Dup_Status=  if ( duplicateCheck1.isEmpty) "PASS" else "FAIL"  
val checkMap1=Map("Capability"->Capability,"Layer"->Layer,"Object_name"->ObjectName,"Test_Type"->Dup_Test,"Test_Status"->Dup_Status)
val parameters1=checkMap1.mapValues(_.toString)
automation_logs(parameters1)


 // count check
val Cnt_Test= "Record Count"
val Cnt_Status= if ( countCheck.isEmpty ) "PASS" else "FAIL"   
val checkMap2=Map("Capability"->Capability,"Layer"->Layer,"Object_name"->ObjectName,"Test_Type"->Cnt_Test,"Test_Status"->Cnt_Status)
val parameters2=checkMap2.mapValues(_.toString)
automation_logs(parameters2)

// Minus check
val Minus_Check= "Business Rule"
val Minus_status= if ( minus4.isEmpty && minus3.isEmpty && minus2.isEmpty && minus1.isEmpty ) "PASS" else "FAIL"        
val checkMap3=Map("Capability"->Capability,"Layer"->Layer,"Object_name"->ObjectName,"Test_Type"->Minus_Check,"Test_Status"->Minus_status)
val parameters3=checkMap3.mapValues(_.toString)
automation_logs(parameters3)


// Blank and Null check  
val Blank_Null_Check= " Blank/Null" 
val Blank_Null_status=  if ( nullBlankCheck1.isEmpty) "PASS" else "FAIL"                         
val checkMap4=Map("Capability"->Capability,"Layer"->Layer,"Object_name"->ObjectName,"Test_Type"->Blank_Null_Check,"Test_Status"->Blank_Null_status)
val parameters4=checkMap4.mapValues(_.toString)
automation_logs(parameters4)
