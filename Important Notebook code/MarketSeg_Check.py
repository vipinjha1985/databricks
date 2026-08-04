# Databricks notebook source
BDLOBJECT = Seq("ScbdlProvDisp","PurchaseInvoiceLineSplit","OTMTenderCollabServprov","OTMShipmentStatus")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc detail MaterialSalesOrganisationTable

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

import re
path = "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/"


list_path = dbutils.fs.ls(path)
obj_name = "ScbdlProvDisp" # change this to object name else untouched

delta = "_Delta"

for l in list_path:
  ls = dbutils.fs.ls(l[0])
  for ele in ls:
    if re.search(obj_name, ele[1]):
      #
      try:
        print("Count For "+ele[0].split("/")[-3]+obj_name+": \t")
        print(spark.table(ele[0].split("/")[-3]+obj_name+"_Delta").count())
      except:
        print("Error For "+ ele[0].split("/")[-3]+obj_name+delta )


print(obj_name + " Total Count : ")
print(spark.read.format("delta").load(path+obj_name+"/Processed_Parquet/").count())


# COMMAND ----------

import re
path = "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/"


list_path = dbutils.fs.ls(path)
obj_name = "PurchaseInvoiceLineSplit" # change this to object name else untouched

delta = "_Delta"

for l in list_path:
  ls = dbutils.fs.ls(l[0])
  for ele in ls:
    if re.search(obj_name, ele[1]):
      #
      try:
        print("Count For "+ele[0].split("/")[-3]+obj_name+": \t")
        print(spark.table(ele[0].split("/")[-3]+obj_name+"_Delta").count())
      except:
        print("Error For "+ ele[0].split("/")[-3]+obj_name+delta )


print(obj_name + " Total Count : ")
print(spark.read.format("delta").load(path+obj_name+"/Processed_Parquet/").count())


# COMMAND ----------

import re
path = "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/"


list_path = dbutils.fs.ls(path)
obj_name = "OTMTenderCollabServprov" # change this to object name else untouched

delta = "_Delta"

for l in list_path:
  ls = dbutils.fs.ls(l[0])
  for ele in ls:
    if re.search(obj_name, ele[1]):
      #
      try:
        print("Count For "+ele[0].split("/")[-3]+obj_name+": \t")
        print(spark.table(ele[0].split("/")[-3]+obj_name+"_Delta").count())
      except:
        print("Error For "+ ele[0].split("/")[-3]+obj_name+delta )


print(obj_name + " Total Count : ")
print(spark.read.format("delta").load(path+obj_name+"/Processed_Parquet/").count())


# COMMAND ----------

import re
path = "/mnt/adls/centrallake/BusinessDataLake/SC/TransactionObject/"


list_path = dbutils.fs.ls(path)
obj_name = "OTMShipmentStatus" # change this to object name else untouched

delta = "_Delta"

for l in list_path:
  ls = dbutils.fs.ls(l[0])
  for ele in ls:
    if re.search(obj_name, ele[1]):
      #
      try:
        print("Count For "+ele[0].split("/")[-3]+obj_name+": \t")
        print(spark.table(ele[0].split("/")[-3]+obj_name+"_Delta").count())
      except:
        print("Error For "+ ele[0].split("/")[-3]+obj_name+delta )


print(obj_name + " Total Count : ")
print(spark.read.format("delta").load(path+obj_name+"/Processed_Parquet/").count())

# Count For ACordilleraOTMShipmentStatus: 	
# 45232966
# OTMShipmentStatus Total Count : 
# 45232966


# COMMAND ----------


