package stormshieldLogs

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object StormshieldLogsApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("StormShieldLogs")
      .master("local[*]")
      .getOrCreate()

      val logDir: String     = "E:/logsALL/"
      val storageDir: String = "E:/logStore/"
      val logType: String    = "monitor"     //"alarm", "auth", "connections", "filterstat", "plugin", "system", "web" available log types

      val rebuildColumns = false

      val loader : Loader  = new Loader (spark:SparkSession,storageDir:String)
      val cleaner: Cleaner = new Cleaner(spark:SparkSession,storageDir:String)

      val logsDF   : Dataset[Row] = loader .loadStormshieldLogs (logDir, logType)
      val cleanedDF: Dataset[Row] = cleaner.cleanStormshieldLogs(logsDF, logType, logDir,rebuildColumns)

      cleanedDF.show()

      //val usersCount: Int = cleanedDF.select("user").distinct().count().toInt
      //cleanedDF.select("user").distinct().sort("user").show(usersCount, false)
  }
}

