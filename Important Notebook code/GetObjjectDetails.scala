// Databricks notebook source
// MAGIC %run /Unilever/BDL/SC/SharedLib/Utilities

// COMMAND ----------

/** Creates a dataframe after reading parquet files.
 *
 *  @param path: BDL Object path till Processed_Parquet
 *  @return a list of directories in path
 * 
 */
import org.apache.hadoop.conf.Configuration
def listDirectories(path:String) :ListBuffer[String] = {
  var directories = new ListBuffer[String]()
  val fs = FileSystem.get(new Configuration())
  val status = fs.listStatus(new Path(path))
  status.foreach(x => directories += x.getPath.toString() )
  directories
}

// COMMAND ----------

var objectList: Seq[(String,String)] = Nil
var colRow: Seq[(String,String)] = Nil
val objectTypes = List("Hierarchies", "ReferenceObject", "TransactionObject")
val baseDirectories = listDirectories("/mnt/adls/centrallake/BusinessDataLake/SC")
for(baseDirectory <- baseDirectories){
  if(objectTypes.exists(baseDirectory.contains)){
    val paths = listDirectories(baseDirectory)
    for(path <- paths){
      val object_name = path.split(":").apply(1).split("/").apply(7)
      colRow = Seq(( object_name,path))
      objectList = objectList ++ colRow
    }
  }
}
val objListDF = objectList.toDF("ObjectName","BDLPath")
objListDF.createOrReplaceTempView("BDL_OBJ_List_1")

// COMMAND ----------

val segregatedFolders = List("ACordillera", "ALatinAmerica", "ANorthAmerica", "EEuropeEast", "EEuropeMiddle", "EEuropeOther", "EEuropeUKI", "EEuropeWest", "ESirius", "GGlobal", "IFusion", "ISouthAsia", "RANZ", "RAfrica", "RIndonesia", "RIsrael", "RNAME", "RNorthAsia", "RPhilippines", "RRUB", "RSEAT", "RSouthAsia", "RTUI", "RU2K2", "RVietnam")

// COMMAND ----------

display(dbutils.fs.ls("/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/RSEAT"))

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from BDL_OBJ_List_1 where ObjectName not like '%_Prod' and ObjectName not like 'Test%' and ObjectName not like '%_PROD' and ObjectName not like '%.csv%' and ObjectName not like '%_UAT' and ObjectName not like '%_Parquet' and ObjectName not like 'test%' and ObjectName not like '%_Backup' and ObjectName not like '%_Test'  and ObjectName not like '%_Bkp%' and ObjectName not like '%Backup%' and ObjectName not like '%ToDelete'
// MAGIC 
// MAGIC --PurchaseDocumentScheduleLine_Bkp_02192019_ToDelete
