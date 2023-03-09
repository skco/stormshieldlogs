package stormshieldLogs

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

class Loader(spark:SparkSession,storageDir:String) {
  /**
   * load StormShield devices log files
   *
   * @param logFilePath   path to log files
   * @param logType       log file type directory
   * @return
   */
  def loadStormshieldLogs(logFilePath:String,logType:String): Dataset[Row] = {
    val logDirPath:String   = s"$logFilePath$logType/*.log"
     spark.read
          .text(logDirPath)
  }

  /**
  * Remove rows with anomaly rows length
  *
  * @param df          raw text DataFrame (lines as strings)
  * @return            DataFrame without rows longer 2x average line length
   */

  def filterAnomalyRows(df:Dataset[Row]):Dataset[Row] = {
    val dfWithLen:Dataset[Row] = df.withColumn("len",length(col("value")))
    val avgValue :Float        = dfWithLen.select(avg("len"))
                                          .collect()(0)(0)
                                          .toString
                                          .toFloat

    dfWithLen.where(dfWithLen("len")<avgValue*2)
             .drop("len")
  }

}

