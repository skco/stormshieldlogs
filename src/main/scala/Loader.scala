package stormshieldLogs

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

class Loader(spark:SparkSession,storageDir:String) {
  def loadStormshieldLogs(logFilePath:String,logType:String): Dataset[Row] = {
    val logDirPath:String   = s"$logFilePath$logType/*.log"
    val logsDF:Dataset[Row] = spark.read.text(logDirPath)
    logsDF
  }

  def filterAnomalyRows(df:Dataset[Row]):Dataset[Row] = {
    val dfWithLen = df.withColumn("len",length(col("value")))
    val avgValue:Float  = dfWithLen.select(avg("len"))
                                   .collect()(0)(0).toString.toFloat
    //println(avgValue)
    val dfAboveAvg:Dataset[Row] = dfWithLen.where(s"len>${(avgValue*2).toString}")
    //dfAboveAvg.show(1000,false)
    dfWithLen.where(dfWithLen("len")<avgValue)

  }

}

