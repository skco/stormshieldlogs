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
  * Remove rows with anomaly length
  *
  * @param DF          raw text DataFrame (lines as string)
  * @return            DataFrame without anomaly rows length > 2x average line length
   */

  def filterAnomalyRows(DF:Dataset[Row]):Dataset[Row] = {
    val dfWithLen:Dataset[Row] = DF.withColumn("len",length(col("value")))
    val avgValue :Float        = dfWithLen.select(avg("len"))
                                          .collect()(0)(0)
                                          .toString
                                          .toFloat
    dfWithLen.where(dfWithLen("len")<avgValue*2)
             .drop("len")
  }

}

