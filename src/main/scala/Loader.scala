package stormshieldLogs

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class Loader(spark:SparkSession,storageDir:String) {
  def loadStormshieldLogs(logFilePath:String,logType:String): Dataset[Row] = {
    val logDirPath:String   = s"$logFilePath$logType/*.log"
    val logsDF:Dataset[Row] = spark.read.text(logDirPath)
    logsDF
  }

}

