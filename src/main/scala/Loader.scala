package stormshieldLogs

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.{call_udf, col, lit, when}

import java.nio.file.{Paths, Files}

class Loader(spark:SparkSession,storageDir:String) {
  def loadStormshieldLogs(logFilePath:String,logType:String,cacheResult:Boolean): Dataset[Row] = {
    //val logTypes: Array[String] = Array("alarm", "auth", "connections", "filterstat", "monitor", "plugin", "system", "web") //avaliable logs types
    val logDirPath = s"${logFilePath}${logType}/*.log"
    val logsDF = spark.read.text(logDirPath)
    if (cacheResult) {logsDF.write.mode("overwrite").parquet(s"${storageDir}{logType}.parquet")}
    logsDF
  }

  private def RawTextStoreParquet(loadPath: String, savePath: String): Dataset[Row] = {
    //load multiple text files, save to parquet format and return dataframe
    val dfLogs: Dataset[Row] = spark.read.text(loadPath)
    dfLogs.repartition(1).write.mode("overwrite").parquet(savePath)
    dfLogs
  }

}

