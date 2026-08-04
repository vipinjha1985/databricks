// Databricks notebook source
// MAGIC %md
// MAGIC NoteBookName: FileInfo_Utility <br />
// MAGIC Author: Akbar Belif <br />
// MAGIC Version 1: Initial Draft/ 25th Sept 2020 <br />
// MAGIC Notebook Objective: Notebook Created to give Azure Data lake File Information in Aggregated Level support  Gen 1 and Gen2.

// COMMAND ----------

// MAGIC %run /AuditLogs/Audit_Logs

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try, Success, Failure}


case class FInfo(
  path  : String,
  parent: String,
  isDir : Boolean,
  size  : Long,
  modificationTime: Long,
  partitions: Map[String, String]) 
{
  
  // @todo encoding issues
  def hasExt(ext: String) = endsWith(ext)
  def endsWith(str: String) = path.endsWith(str)
}

def getPartitions(path: String): Map[String, String] =
  path.split('/')
    .filter(_.contains('='))
    .foldLeft(Map.empty[String, String]){
      case (memo, partition) =>
        val parts = partition.split('=')
        memo + (parts(0) -> parts(1))
    }

def collectFiles(paths: Seq[String]): Seq[FInfo] = {
  val remainingDirectories = new ArrayBuffer[FInfo]
  val allFiles             = new ArrayBuffer[FInfo]
  remainingDirectories ++= paths.map(new FInfo(_, "",isDir = true,0,0, Map.empty[String, String]))
  while (remainingDirectories.nonEmpty) {
    val newDirs = sc.parallelize(remainingDirectories.map(_.path))
    val currentBatch = newDirs.mapPartitions { iter =>
      val fs = FileSystem.get(new java.net.URI(paths.head), new Configuration())
      iter.flatMap{path =>
        try {
//           println(path)
          fs.listStatus(new Path(path))
            .map(s => new FInfo(
              s.getPath.toString, 
              path,
              s.isDir, 
              s.getLen, 
              s.getModificationTime, 
              getPartitions(s.getPath.toString)))
        } catch {
          case e: java.io.FileNotFoundException =>
            println(s"File $path not found.")
            Nil
        }
      }
    }.collect
    val (dirs, files) = currentBatch.partition(_.isDir)
    remainingDirectories.clear()
    remainingDirectories ++= dirs
    
    allFiles ++= files
  }
  allFiles
}

def collectFiles(path:String): Seq[FInfo] = 
  collectFiles(path::Nil)

val get_last = udf((xs: Seq[String]) => Try(xs.last).toOption)

/* path =  File Info Path,targetpath = WriteLocation, level = Aggregate Level */
def adb_fileInfo(path: String,targetpath : String, level : Int): Unit = {
  
  var files :  Seq[FInfo] = null
  var df    : DataFrame   = null
  /* Collecting File Information */
  files = collectFiles(path)
  
  /*Converting File Information into DataFrame*/
  
  val selectExprs = 0 until level map (i => $"temp".getItem(i).as(s"level$i"))
  val grpByExprs  = 0 until level map (i => col(s"level$i"))
  
  df = files.toDF()
    .withColumn("Master",lit(path))
    .withColumn("temp", split(split($"path", path).getItem(1), "/"))
    .withColumn("LastModDateTime_utc", expr("from_unixtime(modificationTime/1000)"))
    .withColumn("FileName" , get_last(split(col("path"), "/")))
    .select(
            //col("path")              +:
            col("Master")              +:
            col("FileName")            +:
            col("size").as("FileSize") +:
            col("LastModDateTime_utc") +:
            selectExprs: _*).sort("path")
          .groupBy( col("Master") +: grpByExprs: _*)
          .agg(sum("FileSize").as("FileSize"),count("FileName").as("FileCount"),max("LastModDateTime_utc").as("LastModDateTime_utc"))
  
  df.write.format("csv")
          .option("header", "true")
          .option("delimiter", "|")
          .option("emptyValue", "")
          .mode(SaveMode.Append)
          .save(targetpath)
}

// COMMAND ----------

// MAGIC %fs ls mnt/adls/centrallakePROD/BusinessDataLake/SC/TransactionObject/InventorySnapshot/

// COMMAND ----------

dbutils.fs.rm("/mnt/adls/centrallake/BusinessDataLake/SC/AuditLogs/SCBDL_FileProdCount/PROD_SC_Trans_Inv_20210507",true)

// COMMAND ----------

val masterList=List("/TransactionObject/InventorySnapshot/")

// COMMAND ----------

val masterList=List(
"/TransactionObject/606/",
"/TransactionObject/ACordillera/",
"/TransactionObject/ALatinAmerica/",
"/TransactionObject/ANorthAmerica/",
"/TransactionObject/APODemandPlanningBook/",
"/TransactionObject/APODemandPlanningBookU2K2PA1Weekly/",
"/TransactionObject/APOPlannedProductionWithResource/",
"/TransactionObject/APOSNPU2K2WeekLoad/",
"/TransactionObject/AccessorialCost/",
"/TransactionObject/AccountingDocumentHeader/",
"/TransactionObject/AccountingDocumentLine/",
"/TransactionObject/AccountingDocumentLineCORDILLERACSLM/",
"/TransactionObject/AccountingDocumentLineCSLM/",
"/TransactionObject/AccountingDocumentLineSIRIUSCSLM/",
"/TransactionObject/AccountingDocumentLineU2K2CSLM/",
"/TransactionObject/AccountingDocumentLine_R14/",
"/TransactionObject/ActualProduction/",
"/TransactionObject/Agreement/",
"/TransactionObject/BillOfMaterial/",
"/TransactionObject/BillOfMaterialExplosion/",
"/TransactionObject/BillingDocumentHeader/",
"/TransactionObject/BillingDocumentHeaderCordilleraCSLM/",
"/TransactionObject/BillingDocumentHeaderPartial/",
"/TransactionObject/BillingDocumentHeaderSIRIUSCSLM/",
"/TransactionObject/BillingDocumentHeaderU2K2CSLM/",
"/TransactionObject/BillingDocumentHeader_24102020/",
"/TransactionObject/BillingDocumentLine/",
"/TransactionObject/BillingDocumentLineCordilleraCSLM/",
"/TransactionObject/BillingDocumentLinePartial/",
"/TransactionObject/BillingDocumentLineSIRIUSCSLM/",
"/TransactionObject/BillingDocumentLineU2K2CSLM/",
"/TransactionObject/BillingDocumentLineUAT/",
"/TransactionObject/ByBrandPrimary/",
"/TransactionObject/ByBrandSecondary/",
"/TransactionObject/CASSClaims/",
"/TransactionObject/CCFOTDeliveryTracking/",
"/TransactionObject/CLONE_AccessorialCost/",
"/TransactionObject/ChangeDocumentHeader/",
"/TransactionObject/ChangeDocumentLine/",
"/TransactionObject/ChangeSalesOrder/",
"/TransactionObject/ChangeSalesOrderDeltaFusion_5Mar_bkp/",
"/TransactionObject/ChangeSalesOrderDeltaSirius_5Mar_bkp/",
"/TransactionObject/ChangeSalesOrderDeltaU2K2_5Mar_bkp/",
"/TransactionObject/ChangeSalesOrderDelta_5Mar_bkp/",
"/TransactionObject/ClearMetal/",
"/TransactionObject/ConsignmentWithdrawal/",
"/TransactionObject/ContractCoverageFirstReportedOn/",
"/TransactionObject/ContractCoverageSnapshotByDay/",
"/TransactionObject/CountryMaterialProductFormStockMargin/",
"/TransactionObject/CreditControlArea/",
"/TransactionObject/CustomerInvoicePaymentCheck/",
"/TransactionObject/CustomerMasterCreditManagement/",
"/TransactionObject/DCActualDispatch/",
"/TransactionObject/DCPlannedDispatch/",
"/TransactionObject/DaysBeforeNextRunEAN/",
"/TransactionObject/DaysBeforeNextRunForwardLooking/",
"/TransactionObject/DaysBeforeNextRunProductionLine/",
"/TransactionObject/DaysBeforeNextRunSKU/",
"/TransactionObject/DeliveryByMaterialPlant/",
"/TransactionObject/DeliveryHeader/",
"/TransactionObject/DeliveryHeaderAOTC/",
"/TransactionObject/DeliveryHeaderEnriched/",
"/TransactionObject/DeliveryHeaderLogistics/",
"/TransactionObject/DeliveryHeaderNAOTC/",
"/TransactionObject/DeliveryHeaderPartial/",
"/TransactionObject/DeliveryHeader_Hist_12Nov/",
"/TransactionObject/DeliveryLine/",
"/TransactionObject/DeliveryLineAOTC/",
"/TransactionObject/DeliveryLineLogistics/",
"/TransactionObject/DeliveryLineNAOTC/",
"/TransactionObject/DeliveryLinePartial/",
"/TransactionObject/DeliveryPrimary/",
"/TransactionObject/DeliverySecondary/",
"/TransactionObject/DemandPlanningBook/",
"/TransactionObject/DemandPlanningBookWeekly/",
"/TransactionObject/DemandPlanningDeliveriesAtCustomer/",
"/TransactionObject/DemandPlanningMonthlyLag/",
"/TransactionObject/DemandPlanningMonthlylag/",
"/TransactionObject/DemandPlanningWeeklyLag/",
"/TransactionObject/DemandPlanningWeeklylag/",
"/TransactionObject/DispatchCompliance/",
"/TransactionObject/E4ScoreInboundFile/",
"/TransactionObject/ECCBOMHeader/",
"/TransactionObject/ECCBOMItemSelection/",
"/TransactionObject/ECCBOMLineItem/",
"/TransactionObject/ECCCrossPlantBatchManagementBatch/",
"/TransactionObject/ECCMaterialPlantBatch/",
"/TransactionObject/ECCMaterialPlantStorageLocationBatch/",
"/TransactionObject/ECCMaterialToBOMLink/",
"/TransactionObject/ECCShipmentStatus/",
"/TransactionObject/EEuropeEast/",
"/TransactionObject/EEuropeMiddle/",
"/TransactionObject/EEuropeOther/",
"/TransactionObject/EEuropeUKI/",
"/TransactionObject/EEuropeWest/",
"/TransactionObject/ESirius/",
"/TransactionObject/FEUForecast/",
"/TransactionObject/FinancialDataForUIP/",
"/TransactionObject/GGlobal/",
"/TransactionObject/HierarchyAPOU2K2Product/",
"/TransactionObject/IFusion/",
"/TransactionObject/IOTFactorySpecialTag/",
"/TransactionObject/ISouthAsia/",
"/TransactionObject/ITSDLD_333/",
"/TransactionObject/Ifinance/",
"/TransactionObject/IfinanceSource/",
"/TransactionObject/InboundDelivery/",
"/TransactionObject/InventoryMaterialLossTreeSnapshot/",
"/TransactionObject/InventoryMaterialSnapshot/",
"/TransactionObject/InventoryMaterialSnapshotControl/",
"/TransactionObject/InventorySnapshot/",
"/TransactionObject/InventorySnapshotTarget/",
"/TransactionObject/InventorySnapshotTest/",
"/TransactionObject/InventorySnapshotTestNorms/",
"/TransactionObject/LossSiriusCSLM/",
"/TransactionObject/MaterialCountryPriceCurrent/",
"/TransactionObject/MaterialCountryPriceMonthEnd/",
"/TransactionObject/MaterialDocumentHeader/",
"/TransactionObject/MaterialMovement/",
"/TransactionObject/MaterialMovementHistory/",
"/TransactionObject/MaterialMovement_HLD/",
"/TransactionObject/MiscellaneousSpend/",
"/TransactionObject/NorthAmericaPlannedAccountMapping/",
"/TransactionObject/OTMAppointment/",
"/TransactionObject/OTMAuditTrail/",
"/TransactionObject/OTMIEShipmentStatus/",
"/TransactionObject/OTMOrderMovement/",
"/TransactionObject/OTMOrderRelease/",
"/TransactionObject/OTMRateGeoCost/",
"/TransactionObject/OTMSSStatusHistory/",
"/TransactionObject/OTMShipCommitAllocJoin/",
"/TransactionObject/OTMShipUnit/",
"/TransactionObject/OTMShipment/",
"/TransactionObject/OTMShipmentCost/",
"/TransactionObject/OTMShipmentStatus/",
"/TransactionObject/OTMShipmentStop/",
"/TransactionObject/OTMShipmentStopD/",
"/TransactionObject/OTMShipmentStopDUAT/",
"/TransactionObject/OTMShipmentStop_R15/",
"/TransactionObject/OTMShipmentStop_Test/",
"/TransactionObject/OTMShipment_R15/",
"/TransactionObject/OTMTenderCollabServprov/",
"/TransactionObject/OTMTenderCollaboration/",
"/TransactionObject/OTMTenderCollaborationStatus/",
"/TransactionObject/OutputReliability/",
"/TransactionObject/PODItem/",
"/TransactionObject/PlannedOrder/",
"/TransactionObject/PlannedProduction/",
"/TransactionObject/PlantInventoryPriceSnapshot/",
"/TransactionObject/PortfolioProducedWeekly/",
"/TransactionObject/PricingConditionHeader/",
"/TransactionObject/PricingConditionHeaderCSLM/",
"/TransactionObject/PricingConditionLine/",
"/TransactionObject/PricingConditionLineCSLM/",
"/TransactionObject/ProductCostingComponent/",
"/TransactionObject/ProductCostingHeader/",
"/TransactionObject/ProductionConsumptionDetail/",
"/TransactionObject/ProductionOrderHeader/",
"/TransactionObject/ProductionOrderLine/",
"/TransactionObject/ProductionOrderOperation/",
"/TransactionObject/ProductionOrderStatus/",
"/TransactionObject/ProductionSnapshot/",
"/TransactionObject/ProductionVolumeWithResource/",
"/TransactionObject/ProofOfDeliveryItemInformation/",
"/TransactionObject/PurchaseAccountingDocument/",
"/TransactionObject/PurchaseContractHeader/",
"/TransactionObject/PurchaseContractLine/",
"/TransactionObject/PurchaseContractLineCoverageDetail/",
"/TransactionObject/PurchaseContractLineCoverageDetailDUpsUAT/",
"/TransactionObject/PurchaseContractLineCoverageDetail_Test/",
"/TransactionObject/PurchaseContractLineDetail/",
"/TransactionObject/PurchaseDocumentDeliveryCosts/",
"/TransactionObject/PurchaseDocumentHeader/",
"/TransactionObject/PurchaseDocumentHistory/",
"/TransactionObject/PurchaseDocumentLine/",
"/TransactionObject/PurchaseDocumentLineSplit/",
"/TransactionObject/PurchaseDocumentScheduleLine/",
"/TransactionObject/PurchaseDocumentScheduleLine_1Parquet/",
"/TransactionObject/PurchaseDocumentScheduleLine_ToDelete/",
"/TransactionObject/PurchaseGoodsReceiptDetail/",
"/TransactionObject/PurchaseInfoRecord/",
"/TransactionObject/PurchaseInvoice/",
"/TransactionObject/PurchaseInvoiceDetail/",
"/TransactionObject/PurchaseInvoiceHeader/",
"/TransactionObject/PurchaseInvoiceLine/",
"/TransactionObject/PurchaseInvoiceLineSplit/",
"/TransactionObject/PurchaseInvoiceLine_Test/",
"/TransactionObject/PurchaseMaterialVolumeForecast/",
"/TransactionObject/PurchaseOrder/",
"/TransactionObject/PurchaseOrderDetail/",
"/TransactionObject/PurchasePlannedOrderCoverageDetail/",
"/TransactionObject/PurchaseQualityNotification/",
"/TransactionObject/PurchaseQualityNotificationDetail/",
"/TransactionObject/PurchaseRequisition/",
"/TransactionObject/PurchaseRequisitionTarget/",
"/TransactionObject/QualityNotificationHeader/",
"/TransactionObject/QualityNotificationLine/",
"/TransactionObject/QualityNotificationStatus/",
"/TransactionObject/RANZ/",
"/TransactionObject/RAfrica/",
"/TransactionObject/RIndonesia/",
"/TransactionObject/RIsrael/",
"/TransactionObject/RNAME/",
"/TransactionObject/RNorthAsia/",
"/TransactionObject/RPhilippines/",
"/TransactionObject/RRUB/",
"/TransactionObject/RSEAT/",
"/TransactionObject/RSouthAsia/",
"/TransactionObject/RTUI/",
"/TransactionObject/RU2K2/",
"/TransactionObject/RVietnam/",
"/TransactionObject/RapidResponseDemandPlanningBook1/",
"/TransactionObject/RapidResponseDemandPlanningBook2/",
"/TransactionObject/RapidResponseSupplyPlanningBook1/",
"/TransactionObject/Reservations/",
"/TransactionObject/SFISOrderOperationDataForWorkCentre/",
"/TransactionObject/SHEBoilerSOX/",
"/TransactionObject/SHECODDirectDischarge/",
"/TransactionObject/SHECODEnhanced/",
"/TransactionObject/SHECODInfluence/",
"/TransactionObject/SHECODMunicipalDischarge/",
"/TransactionObject/SHEEnergy/",
"/TransactionObject/SHEEnergyEnhanced/",
"/TransactionObject/SHEOccupancy/",
"/TransactionObject/SHEPaper/",
"/TransactionObject/SHEProduction/",
"/TransactionObject/SHESPSDTParticulates/",
"/TransactionObject/SHESPSOXSulphonation/",
"/TransactionObject/SHESitePeriodCompletionStatus/",
"/TransactionObject/SHEWaste/",
"/TransactionObject/SHEWasteEnhanced/",
"/TransactionObject/SHEWater/",
"/TransactionObject/SHEWaterEnhanced/",
"/TransactionObject/SNPSnapshotByDay/",
"/TransactionObject/SNPSnapshotByWeek/",
"/TransactionObject/SalesDocumentFlow/",
"/TransactionObject/SalesDocumentHeaderStatus/",
"/TransactionObject/SalesDocumentHeaderStatusPartial/",
"/TransactionObject/SalesDocumentIncompletionLog/",
"/TransactionObject/SalesDocumentIncompletionLogPartial/",
"/TransactionObject/SalesDocumentPartner/",
"/TransactionObject/SalesDocumentScheduleLine/",
"/TransactionObject/SalesDocumentScheduleLinePartial/",
"/TransactionObject/SalesOrderHeader/",
"/TransactionObject/SalesOrderHeaderAOTC/",
"/TransactionObject/SalesOrderHeaderAOTC_Bkp/",
"/TransactionObject/SalesOrderHeaderNAOTC/",
"/TransactionObject/SalesOrderHeaderPartial/",
"/TransactionObject/SalesOrderHeaderSIRIUSCSLM/",
"/TransactionObject/SalesOrderInitialDemand/",
"/TransactionObject/SalesOrderLine/",
"/TransactionObject/SalesOrderLineAOTC/",
"/TransactionObject/SalesOrderLineAOTC_Bkp/",
"/TransactionObject/SalesOrderLineCSLM/",
"/TransactionObject/SalesOrderLineLossU2K2Ccfot/",
"/TransactionObject/SalesOrderLineNAOTC/",
"/TransactionObject/SalesOrderLinePartial/",
"/TransactionObject/SalesOrderLineProcessingStatusArchived/",
"/TransactionObject/SalesOrderLineProcessingStatusCurrent/",
"/TransactionObject/SalesOrderLineProcessingStatusPrevious/",
"/TransactionObject/SalesOrderLineProdbackup/",
"/TransactionObject/SalesOrderLineSIRIUSCSLM/",
"/TransactionObject/SalesOrderLineStatus/",
"/TransactionObject/SalesOrderLineU2K2CSLMBW/",
"/TransactionObject/SalesOrderLineU2k2Ccfot/",
"/TransactionObject/SalesOrderLine_HLD/",
"/TransactionObject/SalesOrderLossAudit/",
"/TransactionObject/SalesOrderLossAuditAOTC/",
"/TransactionObject/SalesOrderLossAuditAOTC_Bkp/",
"/TransactionObject/SalesOrderLossAuditNAOTC/",
"/TransactionObject/SalesOrderLossDetail/",
"/TransactionObject/SalesOrderLossDetailAOTC/",
"/TransactionObject/SalesOrderLossDetailAOTC_Bkp/",
"/TransactionObject/SalesOrderLossDetailNAOTC/",
"/TransactionObject/SalesOrdersNowArchived/",
"/TransactionObject/ScbdlFinance/",
"/TransactionObject/ScbdlFinanceTest/",
"/TransactionObject/ScbdlProvDisp/",
"/TransactionObject/ShipmentCostAccountDetermination/",
"/TransactionObject/ShipmentCostHeader/",
"/TransactionObject/ShipmentCostLine/",
"/TransactionObject/ShipmentHeader/",
"/TransactionObject/ShipmentHeaderEnriched/",
"/TransactionObject/ShipmentHeaderLogistics/",
"/TransactionObject/ShipmentHeaderPartial/",
"/TransactionObject/ShipmentLine/",
"/TransactionObject/ShipmentLineDetails/",
"/TransactionObject/ShipmentLineLogistics/",
"/TransactionObject/ShipmentLineLogisticsTarget/",
"/TransactionObject/ShipmentLinePartial/",
"/TransactionObject/ShipmentLine_MS_POST/",
"/TransactionObject/ShipmentLine_MS_POST_Test/",
"/TransactionObject/ShipmentStage/",
"/TransactionObject/SmartSchedulingActivation/",
"/TransactionObject/SmartSchedulingJobDetail/",
"/TransactionObject/SmartSchedulingJobDetailPartial/",
"/TransactionObject/SmartSchedulingJobDetailPartialtest/",
"/TransactionObject/SmartSchedulingJobHeader/",
"/TransactionObject/SmartSchedulingJobHeaderPartial/",
"/TransactionObject/SpecialStockFromVendor/",
"/TransactionObject/SpecialStockWithVendor/",
"/TransactionObject/StorageLocationDataForMaterial/",
"/TransactionObject/SupplyPlanningBook/",
"/TransactionObject/TeaBlendOrderHeader/",
"/TransactionObject/TeaBlendOrderLine/",
"/TransactionObject/Test/",
"/TransactionObject/Test1/",
"/TransactionObject/TestProd/",
"/TransactionObject/Test_18_MaterialMovements_ToDelete/",
"/TransactionObject/Test_18_PurchaseDocumentLine_ToDelete/",
"/TransactionObject/Test_18_PurchaseDocumentScheduleLine_ToDelete/",
"/TransactionObject/Test_MaterialMovements_ToDelete/",
"/TransactionObject/Test_PurchaseDocumentScheduleLine_ToDelete/",
"/TransactionObject/TransDataPayment/",
"/TransactionObject/TransLinkPayment/",
"/TransactionObject/VendorConfirmation/",
"/TransactionObject/XPAFEDaily/",
"/TransactionObject/test123/",
"/TransactionObject/testsample/")

// COMMAND ----------

// /mnt/adls/centrallake/BusinessDataLake/SC/AuditLogs/SCBDL_FileProdCount/PROD_SC_20210208
// /mnt/adls/centrallake/BusinessDataLake/SC/AuditLogs/SCBDL_FileProdCount/PROD_Transaction_Part2_SC_20210208
// /mnt/adls/centrallake/BusinessDataLake/SC/AuditLogs/SCBDL_FileProdCount/PROD_Transaction_SC_20210208

// COMMAND ----------

/* Save csv file Location */
var target_path : String = "/mnt/adls/centrallake/BusinessDataLake/SC/AuditLogs/SCBDL_FileProdCount/PROD_SC_Trans_Inv_20210507"

/* Azure Data Lake File Path Detail 
Note : End the string input with '/' */
var filepath    : String = "/mnt/adls/centrallakePROD/BusinessDataLake/SC"
/* File Level Aggregation Integer type */
var level       : Int   = 1

// COMMAND ----------

// DBTITLE 1,Calling Function
// adb_fileInfo(filepath,target_path,level)
for(master <- masterList)
{
  println(filepath+master)
  adb_fileInfo(filepath+master,target_path,level)
}

// COMMAND ----------

var df_PROD_SC_Trans_Inv_20210507 = spark.read.format("csv")
                   .option("multiline", false)
                   .option("header", "true")
                   .option("sep", "|")
                   .option("ignoreLeadingWhiteSpace", "true")
                   .option("ignoreTrailingWhiteSpace", "true")
                   .load("/mnt/adls/centrallake/BusinessDataLake/SC/AuditLogs/SCBDL_FileProdCount/PROD_SC_Trans_Inv_20210507")

df_PROD_SC_Trans_Inv_20210507.createOrReplaceTempView("PROD_SC_Trans_Inv_20210507")
display(df_PROD_SC_Trans_Inv_20210507)

// COMMAND ----------

var df_PROD_TRAN_SC_20210402 = spark.read.format("csv")
                   .option("multiline", false)
                   .option("header", "true")
                   .option("sep", "|")
                   .option("ignoreLeadingWhiteSpace", "true")
                   .option("ignoreTrailingWhiteSpace", "true")
                   .load("/mnt/adls/centrallake/BusinessDataLake/SC/AuditLogs/SCBDL_FileProdCount/PROD_TRAN_SC_20210402")

df_PROD_TRAN_SC_20210402= df_PROD_TRAN_SC_20210402
    .withColumn("SubjectArea" , split($"Master", "/mnt/adls/centrallakePROD/BusinessDataLake/SC/TransactionObject/").getItem(1))
    .withColumn("Master" ,lit("/mnt/adls/centrallakePROD/BusinessDataLake/SC/TransactionObject/"))
      .select(col("Master"),regexp_replace(col("SubjectArea"), "/", "").as("SubjectArea"),col("level0"),col("level1"),col("level2"),col("FileSize"),col("FileCount"),col("LastModDateTime_utc"))
// df_PROD_SC_20210208.createOrReplaceTempView("PROD_SC_20210208")
df_PROD_TRAN_SC_20210402.createOrReplaceTempView("PROD_TRAN_SC_20210402")
// df_PROD_SC_20210208.createOrReplaceTempView("PROD_SC_20210208")
display(df_PROD_TRAN_SC_20210402)
// /mnt/adls/centrallake/BusinessDataLake/SC/AuditLogs/SCBDL_FileProdCount/PROD_SC_20210208 
// /mnt/adls/centrallake/BusinessDataLake/SC/AuditLogs/SCBDL_FileProdCount/PROD_Transaction_Part2_SC_20210208
// /mnt/adls/centrallake/BusinessDataLake/SC/AuditLogs/SCBDL_FileProdCount/PROD_Transaction_SC_20210208

// COMMAND ----------

// MAGIC %sql
// MAGIC select Master,level0,
// MAGIC -- case when level0 like "part-%" or level0 like "_%" then "OBJECT_LEVEL_FILE" else level0  End AS level0,
// MAGIC case when level1 like "part-%" or level1 like "_committed%" or  level1 like "%_started%" or level1 IN ("_delta_log","_SUCCESS") then "OBJECT_LEVEL_FILE" else NVL(level1,"")  End AS level1,
// MAGIC case when level2 like "part-%" or level2 like "_committed%" or  level2 like "%_started%" or level2 IN ("_delta_log","_SUCCESS") then "OBJECT_LEVEL_FILE" else NVL(level2,"")  End AS level2,
// MAGIC SUM(FileSize) as FileSize,SUM(FileCount) as FileCount,Max(LastModDateTime_utc) as LastModDateTime_utc
// MAGIC from PROD_SC_Ref_Hier_20210506
// MAGIC -- where SubjectArea  IN ("ACordillera","ALatinAmerica","ANorthAmerica","EEuropeEast","EEuropeMiddle","EEuropeOther","EEuropeUKI","EEuropeWest",
// MAGIC --                 "ESirius","GGlobal","IFusion","ISouthAsia","RANZ","RAfrica","RIndonesia","RIsrael","RNAME","RNorthAsia",
// MAGIC --                 "RPhilippines","RRUB","RSEAT","RSouthAsia","RTUI","RU2K2","RVitetnam") 
// MAGIC                 group by 1,2,3,4
// MAGIC -- select SUM(FileSize) as FileSize from  without_Market

// COMMAND ----------

// MAGIC %sql
// MAGIC select Master,SubjectArea,
// MAGIC case when level0 like "part-%" or level0 like "_%" then "OBJECT_LEVEL_FILE" else level0  End AS level0,
// MAGIC case when level1 like "part-%" or level1 like "/_%" then "OBJECT_LEVEL_FILE" else level1  End AS level1,
// MAGIC case when level2 like "part-%" or level2 like "/_%" then "OBJECT_LEVEL_FILE" else level2  End AS level2,
// MAGIC SUM(FileSize) as FileSize,SUM(FileCount) as FileCount,Max(LastModDateTime_utc) as LastModDateTime_utc
// MAGIC from PROD_TRAN_SC_20210402
// MAGIC where SubjectArea  IN ("ACordillera","ALatinAmerica","ANorthAmerica","EEuropeEast","EEuropeMiddle","EEuropeOther","EEuropeUKI","EEuropeWest",
// MAGIC                 "ESirius","GGlobal","IFusion","ISouthAsia","RANZ","RAfrica","RIndonesia","RIsrael","RNAME","RNorthAsia",
// MAGIC                 "RPhilippines","RRUB","RSEAT","RSouthAsia","RTUI","RU2K2","RVitetnam") 
// MAGIC                 group by 1,2,3,4,5
// MAGIC -- select SUM(FileSize) as FileSize from  without_Market

// COMMAND ----------

// MAGIC %sql
// MAGIC select "/mnt/adls/BusinessDataLake/SC/TransactionObject/" AS Master,
// MAGIC ObjectName as level0,
// MAGIC case when level0 like "part-%" then "OBJECT_LEVEL_FILE" else level0  End AS level1,
// MAGIC case when level1 like "part-%" then "OBJECT_LEVEL_FILE" else level1  End AS level2,
// MAGIC -- case when level2 like "part-%" then "OBJECT_LEVEL_FILE" else level2  End AS level2,
// MAGIC SUM(FileSize) as FileSize,SUM(FileCount) as FileCount,Max(LastModDateTime_utc) as LastModDateTime_utc
// MAGIC from PROD_TRAN_SC_20210402
// MAGIC where ObjectName  IN ("ACordillera/","ALatinAmerica/","ANorthAmerica/","EEuropeEast/","EEuropeMiddle/","EEuropeOther/","EEuropeUKI/","EEuropeWest/",
// MAGIC                 "ESirius/","GGlobal/","IFusion/","ISouthAsia/","RANZ/","RAfrica/","RIndonesia/","RIsrael/","RNAME/","RNorthAsia/",
// MAGIC                 "RPhilippines/","RRUB/","RSEAT/","RSouthAsia/","RTUI/","RU2K2/","RVitetnam/") 
// MAGIC                 group by 1,2,3,4
// MAGIC -- select SUM(FileSize) as FileSize from  without_Market

// COMMAND ----------

// MAGIC %sql
// MAGIC select "/mnt/adls/BusinessDataLake/SC/TransactionObject/" AS Master,
// MAGIC ObjectName as level0,
// MAGIC case when level0 like "part-%" then "OBJECT_LEVEL_FILE" else level0  End AS level1,
// MAGIC case when level1 like "part-%" then "OBJECT_LEVEL_FILE" else level1  End AS level2,
// MAGIC -- case when level2 like "part-%" then "OBJECT_LEVEL_FILE" else level2  End AS level2,
// MAGIC SUM(FileSize) as FileSize,SUM(FileCount) as FileCount,Max(LastModDateTime_utc) as LastModDateTime_utc
// MAGIC from PROD_TRAN_SC_20210402
// MAGIC where ObjectName  IN ("ACordillera/","ALatinAmerica/","ANorthAmerica/","EEuropeEast/","EEuropeMiddle/","EEuropeOther/","EEuropeUKI/","EEuropeWest/",
// MAGIC                 "ESirius/","GGlobal/","IFusion/","ISouthAsia/","RANZ/","RAfrica/","RIndonesia/","RIsrael/","RNAME/","RNorthAsia/",
// MAGIC                 "RPhilippines/","RRUB/","RSEAT/","RSouthAsia/","RTUI/","RU2K2/","RVitetnam/") 
// MAGIC                 group by 1,2,3,4
// MAGIC -- select SUM(FileSize) as FileSize from  without_Market

// COMMAND ----------

var df : org.apache.spark.sql.DataFrame = null
df = spark.sql(s"""
select Master,ObjectName,Folder from 
(select case when Master like "%/ReferenceObject/" then "ReferenceObject"
            when Master like "%/TransactionObject/%" then "TransactionObject"
            when Master like "%/Hierarchies/" then "Hierarchies" end Master,
            level0 as ObjectName,level1 as Folder
            from PROD_SC 
where level1 IN ("Processed","Processed_Parquet") 
union all 
select case when Master like "%/ReferenceObject/" then "ReferenceObject"
            when Master like "%/TransactionObject/%" then "TransactionObject"
            when Master like "%/Hierarchies/" then "Hierarchies" end Master,
            level0 as ObjectName,level2 as Folder
            from PROD_SC 
where level2 IN ("Processed","Processed_Parquet") ) group by 1,2,3""")

df.createOrReplaceTempView("PROD_SC_Processed_Parquet")

// COMMAND ----------


df.filter($"Folder" === "Processed").createOrReplaceTempView("PROD_SC_Processed")
df.filter($"Folder" === "Processed_Parquet").createOrReplaceTempView("PROD_SC_Parquet")

// COMMAND ----------

// MAGIC %sql 
// MAGIC select Case when Proc.Master is null then Parq.Master else Proc.Master end as Master,
// MAGIC         Proc.Folder,
// MAGIC         Parq.Folder,
// MAGIC         Case when Proc.Folder is null then 0 else 1 end as CSV_only,
// MAGIC         Case when Parq.Folder is null then 0 else 1 end as Delta_only,
// MAGIC         Case when Parq.Folder is not null and Proc.Folder is not null then 1 else 0 end as Both_Delta_CSV
// MAGIC       from PROD_SC_Processed Proc 
// MAGIC       full outer join PROD_SC_Parquet Parq
// MAGIC       on Parq.Master=Proc.Master and 
// MAGIC       Parq.ObjectName=Proc.ObjectName

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC select sum(CSV_only) as CSV_only,sum(Delta_only) as Delta_only,sum(Both_Delta_CSV) as Both_Delta_CSV from
// MAGIC (select Case when Proc.Master is null then Parq.Master else Proc.Master end as Master,
// MAGIC         Case when Proc.Folder is null then 0 else 1 end as CSV_only,
// MAGIC         Case when Parq.Folder is null then 0 else 1 end as Delta_only,
// MAGIC         Case when Parq.Folder is not null and Proc.Folder is not null then 1 else 0 end as Both_Delta_CSV
// MAGIC       from PROD_SC_Processed Proc 
// MAGIC       full outer join PROD_SC_Parquet Parq
// MAGIC       on Parq.Master=Proc.Master and 
// MAGIC       Parq.ObjectName=Proc.ObjectName)

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC select Master,sum(CSV_only) as CSV_only,sum(Delta_only) as Delta_only,sum(Both_Delta_CSV) as Both_Delta_CSV from
// MAGIC (select Case when Proc.Master is null then Parq.Master else Proc.Master end as Master,
// MAGIC         Case when Proc.Folder is null then 0 else 1 end as CSV_only,
// MAGIC         Case when Parq.Folder is null then 0 else 1 end as Delta_only,
// MAGIC         Case when Parq.Folder is not null and Proc.Folder is not null then 1 else 0 end as Both_Delta_CSV
// MAGIC       from PROD_SC_Processed Proc 
// MAGIC       full outer join PROD_SC_Parquet Parq
// MAGIC       on Parq.Master=Proc.Master and 
// MAGIC       Parq.ObjectName=Proc.ObjectName)group by 1

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC select Master,ObjectName,sum(CSV_only) as CSV_only,sum(Delta_only) as Delta_only,sum(Both_Delta_CSV) as Both_Delta_CSV from
// MAGIC (select Case when Proc.Master is null then Parq.Master else Proc.Master end as Master,
// MAGIC         Case when Proc.ObjectName is null then Parq.ObjectName else Proc.ObjectName end as ObjectName,
// MAGIC         Case when Proc.Folder is null then 0 else 1 end as CSV_only,
// MAGIC         Case when Parq.Folder is null then 0 else 1 end as Delta_only,
// MAGIC         Case when Parq.Folder is not null and Proc.Folder is not null then 1 else 0 end as Both_Delta_CSV
// MAGIC       from PROD_SC_Processed Proc 
// MAGIC       full outer join PROD_SC_Parquet Parq
// MAGIC       on Parq.Master=Proc.Master and 
// MAGIC       Parq.ObjectName=Proc.ObjectName)group by 1,2

// COMMAND ----------

var PROD_SC_DF_test = df_PROD_SC_20210208

PROD_SC_DF_test = PROD_SC_DF_test.withColumn("FileType",
                                             when($"level0".like("%.csv") ,"CSV")
                                            .when($"level1".like("%.csv") ,"CSV")
                                            .when($"level2".like("%.csv") ,"CSV")
                                            .when($"level0" === ("_delta_log") ,"Delta")
                                            .when($"level1" === ("_delta_log") ,"Delta")
                                            .when($"level2" === ("_delta_log") ,"Delta"))     
display(PROD_SC_DF_test)
// val Volume0fData_PerType = PROD_SC_DF_test.groupBy($"FileType").agg(sum($"FileSize")).show()
// val FileCount_PerType    = PROD_SC_DF_test.groupBy($"FileType").agg(sum($"FileCount")).show()

// COMMAND ----------

Volume0fData_PerType

// COMMAND ----------

var Result_SC_DF: org.apache.spark.sql.DataFrame = null

Result_SC_DF = PROD_SC_DF_test.withColumn("BDL File Format",when($"FileType" === "CSV",lit("Csv file Format"))
                                                      .when($"FileType" === "Parquet" ,lit("Parquet Format"))
                                                      .when($"FileType" === "Other"   ,lit("Any other format like json, Xml etc")))
                        .withColumn("BDL File Averge",when($"FileType" === "CSV"      ,
                                                           ((Volume0fData_PerType.filter($"FileType") === "CSV")/sum_TotalVolume0fData))
                                                     .when($"FileType" === "Parquet" ,
                                                           (Volume0fData_PerType.filter($"FileType") === "Parquet")/sum_TotalVolume0fData)
                                                     .when($"FileType" === "Other"   ,
                                                           (Volume0fData_PerType.filter($"FileType") === "Other")/sum_TotalVolume0fData))
                        .withColumn("Approximate volume of data",
                                                       when($"FileType"  === "CSV"     ,FileCount_PerType.filter($"FileType"==="CSV"    ))
                                                      .when($"FileType" === "Parquet" ,FileCount_PerType.filter($"FileType"==="Parquet"))
                                                      .when($"FileType" === "Other"   ,FileCount_PerType.filter($"FileType"==="Other"  )))

// COMMAND ----------

// MAGIC %sql 
// MAGIC with Prod_SC_BDL_20210208 AS (
// MAGIC select Master,level0,level1,"UNK" as level2,FileSize,FileCount,LastModDateTime_utc from PROD_SC_20210208
// MAGIC Union all
// MAGIC select Master,level0,level1,level2,FileSize,FileCount,LastModDateTime_utc  from PROD_Transaction_Part2_SC_20210208
// MAGIC Union all
// MAGIC select Master,level0,level1,level2,FileSize,FileCount,LastModDateTime_utc  from PROD_Transaction_SC_20210208
// MAGIC )
// MAGIC select BDL_File_Format,sum(BDL_FILE_averge) as Per_of_Processed_Files_In_BDL_ADLS ,sum(Volume0fData) as Approximate_volume_of_data from 
// MAGIC (
// MAGIC select "Csv file Format" as BDL_File_Format,
// MAGIC         (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC         Sum(FileSize) AS Volume0fData
// MAGIC         from Prod_SC_BDL_20210208  where level0 like "%.csv"
// MAGIC union all
// MAGIC select "Csv file Format" as BDL_File_Format,
// MAGIC         (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC         Sum(FileSize) AS Volume0fData
// MAGIC         from Prod_SC_BDL_20210208  where level1 like "%.csv"
// MAGIC union all
// MAGIC select "Csv file Format" as BDL_File_Format,
// MAGIC         (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC         Sum(FileSize) AS Volume0fData
// MAGIC         from Prod_SC_BDL_20210208  where level2 like "%.csv"
// MAGIC union all        
// MAGIC select "Parquet Format" as BDL_File_Format,
// MAGIC         (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC         Sum(FileSize) AS Volume0fData
// MAGIC         from Prod_SC_BDL_20210208  where level0 like "%.parquet"
// MAGIC union all
// MAGIC select "Parquet Format" as BDL_File_Format,
// MAGIC         (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC         Sum(FileSize) AS Volume0fData
// MAGIC         from Prod_SC_BDL_20210208  where level1 like "%.parquet"
// MAGIC union all
// MAGIC select "Parquet Format" as BDL_File_Format,
// MAGIC         (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC         Sum(FileSize) AS Volume0fData
// MAGIC         from Prod_SC_BDL_20210208  where level2 like "%.parquet"
// MAGIC union all
// MAGIC select "Any other format like json, Xml etc" as BDL_File_Format,
// MAGIC       (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC        SUM(FileSize) AS Volume0fData
// MAGIC         from Prod_SC_BDL_20210208  where level0 like "%.json" 
// MAGIC union all
// MAGIC select "Any other format like json, Xml etc" as BDL_File_Format,
// MAGIC       (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC        SUM(FileSize) AS Volume0fData
// MAGIC         from Prod_SC_BDL_20210208  where level1 like "%.json"
// MAGIC union all
// MAGIC select "Any other format like json, Xml etc" as BDL_File_Format,
// MAGIC       (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC        SUM(FileSize) AS Volume0fData
// MAGIC         from Prod_SC_BDL_20210208  where level2 like "%.json" 
// MAGIC union all
// MAGIC select "Any other format like json, Xml etc" as BDL_File_Format,
// MAGIC       (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC        SUM(FileSize) AS Volume0fData
// MAGIC         from Prod_SC_BDL_20210208  where level0  like "%.xml" 
// MAGIC union all
// MAGIC select "Any other format like json, Xml etc" as BDL_File_Format,
// MAGIC       (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC        SUM(FileSize) AS Volume0fData
// MAGIC         from Prod_SC_BDL_20210208  where level1  like "%.xml" 
// MAGIC union all
// MAGIC select "Any other format like json, Xml etc" as BDL_File_Format,
// MAGIC       (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC        SUM(FileSize) AS Volume0fData
// MAGIC         from Prod_SC_BDL_20210208  where level2  like "%.xml"         
// MAGIC -- union all
// MAGIC -- select "Any other format like json, Xml etc" as BDL_File_Format,
// MAGIC --       (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_FILE_averge,
// MAGIC --        SUM(FileSize) AS Volume0fData
// MAGIC --         from Prod_SC_BDL_20210208  where level2 not like "%_SUCCESS" and  level2 not like "%.parquet"
// MAGIC -- union all
// MAGIC -- select "Delta Table" as BDL_File_Format,
// MAGIC --         (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_CSV_averge,
// MAGIC --         SUM(FileSize) AS Volume0fData
// MAGIC --         from Prod_SC_BDL_20210208  where level0 = "_delta_log" 
// MAGIC -- union all
// MAGIC -- select "Delta Table" as BDL_File_Format,
// MAGIC --         (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_CSV_averge,
// MAGIC --         SUM(FileSize) AS Volume0fData
// MAGIC --         from Prod_SC_BDL_20210208  where level1 = "_delta_log" 
// MAGIC -- union all
// MAGIC -- select "Delta Table" as BDL_File_Format,
// MAGIC --         (SUM(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_CSV_averge,
// MAGIC --         SUM(FileSize) AS Volume0fData
// MAGIC --         from Prod_SC_BDL_20210208  where level2 = "_delta_log"
// MAGIC         ) group by 1

// COMMAND ----------

// MAGIC %sql 
// MAGIC with Prod_SC_BDL_20210208 AS (
// MAGIC select Master,level0,level1,"UNK" as level2,FileSize,FileCount,LastModDateTime_utc from PROD_SC_20210208
// MAGIC Union all
// MAGIC select Master,level0,level1,level2,FileSize,FileCount,LastModDateTime_utc  from PROD_Transaction_Part2_SC_20210208
// MAGIC Union all
// MAGIC select Master,level0,level1,level2,FileSize,FileCount,LastModDateTime_utc  from PROD_Transaction_SC_20210208
// MAGIC )
// MAGIC select "Parquet Format" as BDL_File_Format,sum(BDL_CSV_averge) as Per_of_Processed_Files_In_BDL_ADLS  from 
// MAGIC (select "level0",(sum(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_CSV_averge
// MAGIC         from Prod_SC_BDL_20210208  where level0 like "%.parquet"
// MAGIC union all
// MAGIC select "level1",(sum(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_CSV_averge
// MAGIC         from Prod_SC_BDL_20210208  where level1 like "%.parquet"
// MAGIC union all
// MAGIC select "level2",(sum(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_CSV_averge
// MAGIC         from Prod_SC_BDL_20210208  where level2 like "%.parquet")

// COMMAND ----------

// MAGIC %sql 
// MAGIC with Prod_SC_BDL_20210208 AS (
// MAGIC select Master,level0,level1,"UNK" as level2,FileSize,FileCount,LastModDateTime_utc from PROD_SC_20210208
// MAGIC Union all
// MAGIC select Master,level0,level1,level2,FileSize,FileCount,LastModDateTime_utc  from PROD_Transaction_Part2_SC_20210208
// MAGIC Union all
// MAGIC select Master,level0,level1,level2,FileSize,FileCount,LastModDateTime_utc  from PROD_Transaction_SC_20210208
// MAGIC )
// MAGIC       
// MAGIC         
// MAGIC select "Any other format like json, Xml etc" as BDL_File_Format,sum(BDL_CSV_averge) as Per_of_Processed_Files_In_BDL_ADLS  from 
// MAGIC (select "level0",(sum(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_CSV_averge
// MAGIC         from Prod_SC_BDL_20210208  where level0 not like "%.csv" and  level0 not like "%.parquet"
// MAGIC union all
// MAGIC select "level1",(sum(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_CSV_averge
// MAGIC         from Prod_SC_BDL_20210208  where level1 not like "%.csv" and  level1 not like "%.parquet"
// MAGIC union all
// MAGIC select "level2",(sum(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_CSV_averge
// MAGIC         from Prod_SC_BDL_20210208  where level2 not like "%.csv" and  level2 not like "%.parquet")

// COMMAND ----------

// MAGIC %sql 
// MAGIC with Prod_SC_BDL_20210208 AS (
// MAGIC select Master,level0,level1,"UNK" as level2,FileSize,FileCount,LastModDateTime_utc from PROD_SC_20210208
// MAGIC Union all
// MAGIC select Master,level0,level1,level2,FileSize,FileCount,LastModDateTime_utc  from PROD_Transaction_Part2_SC_20210208
// MAGIC Union all
// MAGIC select Master,level0,level1,level2,FileSize,FileCount,LastModDateTime_utc  from PROD_Transaction_SC_20210208
// MAGIC )
// MAGIC       
// MAGIC         
// MAGIC select "Delta Table" as BDL_File_Format,sum(BDL_CSV_averge) as Per_of_Processed_Files_In_BDL_ADLS  from 
// MAGIC (select "level0",(sum(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_CSV_averge
// MAGIC         from Prod_SC_BDL_20210208  where level0 = "_delta_log" 
// MAGIC union all
// MAGIC select "level1",(sum(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_CSV_averge
// MAGIC         from Prod_SC_BDL_20210208  where level1 = "_delta_log" 
// MAGIC union all
// MAGIC select "level2",(sum(FileCount)/(select sum(FileCount)  from  Prod_SC_BDL_20210208) * 100)AS BDL_CSV_averge
// MAGIC         from Prod_SC_BDL_20210208  where level2 = "_delta_log" )

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC select "Csv file Format" as "BDL File Format", (
// MAGIC select * from PROD_SC_20210208 where level0 like "%.csv"
// MAGIC )

// COMMAND ----------

display(df.filter($"level0".like("%.csv")))

// COMMAND ----------

val Transaction_df = df.withColumn("ObjectName", 
                                   get_last(split(col("Master"), "TransactionObject/"))).filter($"Master".like("%TransactionObject/%"))
Transaction_df.createOrReplaceTempView("Transaction_UATFileCount")

// COMMAND ----------

// MAGIC %sql
// MAGIC select Master,
// MAGIC case when level0 like "part-%" then "OBJECT_LEVEL_FILE" else level0  End AS level0,
// MAGIC case when level1 like "part-%" then "OBJECT_LEVEL_FILE" else level1  End AS level1,
// MAGIC -- case when level2 like "part-%" then "OBJECT_LEVEL_FILE" else level2  End AS level2,
// MAGIC SUM(FileSize) as FileSize,SUM(FileCount) as FileCount,Max(LastModDateTime_utc) as LastModDateTime_utc
// MAGIC from UATFileCount
// MAGIC where Master not IN ("/mnt/adls/centrallake/BusinessDataLake/SC/ReferenceObject/","/mnt/adls/centrallake/BusinessDataLake/SC/Hierarchies/")
// MAGIC group by 1,2,3

// COMMAND ----------

// MAGIC %sql
// MAGIC select "/mnt/adls/BusinessDataLake/SC/TransactionObject/" AS Master,
// MAGIC ObjectName as level0,
// MAGIC case when level0 like "part-%" then "OBJECT_LEVEL_FILE" else level0  End AS level1,
// MAGIC case when level1 like "part-%" then "OBJECT_LEVEL_FILE" else level1  End AS level2,
// MAGIC -- case when level2 like "part-%" then "OBJECT_LEVEL_FILE" else level2  End AS level2,
// MAGIC SUM(FileSize) as FileSize,SUM(FileCount) as FileCount,Max(LastModDateTime_utc) as LastModDateTime_utc
// MAGIC from Transaction_UATFileCount
// MAGIC where ObjectName  IN ("ACordillera/","ALatinAmerica/","ANorthAmerica/","EEuropeEast/","EEuropeMiddle/","EEuropeOther/","EEuropeUKI/","EEuropeWest/",
// MAGIC                 "ESirius/","GGlobal/","IFusion/","ISouthAsia/","RANZ/","RAfrica/","RIndonesia/","RIsrael/","RNAME/","RNorthAsia/",
// MAGIC                 "RPhilippines/","RRUB/","RSEAT/","RSouthAsia/","RTUI/","RU2K2/","RVitetnam/") 
// MAGIC                 group by 1,2,3,4
// MAGIC -- select SUM(FileSize) as FileSize from  without_Market

// COMMAND ----------

// MAGIC %sql
// MAGIC select "/mnt/adls/BusinessDataLake/SC/TransactionObject/" AS Master,
// MAGIC ObjectName as level0,
// MAGIC case when level0 like "part-%" then "OBJECT_LEVEL_FILE" else level0  End AS level1,
// MAGIC -- case when level1 like "part-%" then "OBJECT_LEVEL_FILE" else level1  End AS level2,
// MAGIC -- case when level2 like "part-%" then "OBJECT_LEVEL_FILE" else level2  End AS level2,
// MAGIC SUM(FileSize) as FileSize,SUM(FileCount) as FileCount,Max(LastModDateTime_utc) as LastModDateTime_utc
// MAGIC from Transaction_UATFileCount
// MAGIC where ObjectName NOT IN ("ACordillera/","ALatinAmerica/","ANorthAmerica/","EEuropeEast/","EEuropeMiddle/","EEuropeOther/","EEuropeUKI/","EEuropeWest/",
// MAGIC                 "ESirius/","GGlobal/","IFusion/","ISouthAsia/","RANZ/","RAfrica/","RIndonesia/","RIsrael/","RNAME/","RNorthAsia/",
// MAGIC                 "RPhilippines/","RRUB/","RSEAT/","RSouthAsia/","RTUI/","RU2K2/","RVitetnam/") 
// MAGIC                 group by 1,2,3
// MAGIC -- select SUM(FileSize) as FileSize from  without_Market

// COMMAND ----------


