package stormshieldLogs

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object stormshieldLogs {
  val saveIntermediateResults: Boolean = false

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("StormShieldLogs")
      .master("local[*]")
      .getOrCreate()

    val logDir: String = "E:/logsALL/"
    val logType: String = "auth"
    val storageDir: String = "E:/logStore/"

    var t = Benchmark.time { // simple "benchmarking"

      val loader: Loader   = new Loader (spark:SparkSession,storageDir:String)
      val cleaner: Cleaner = new Cleaner(spark:SparkSession,storageDir:String)

      val logsDF: Dataset[Row]    = loader.loadStormshieldLogs  (logDir, logType, true)
      val cleanedDF: Dataset[Row] = cleaner.cleanStormshieldLogs(logsDF, logType, logDir)

      val usersCount: Int = cleanedDF.select("user").distinct().count().toInt
      //cleanedDF.select("user").distinct().sort("user").show(usersCount, false)
    }
  }
}

