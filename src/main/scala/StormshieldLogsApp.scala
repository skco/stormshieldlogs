import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object StormshieldLogsApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("StormShieldLogsApp")
                                          //.master("local[*]")
                                          .getOrCreate()

      val logType: String = args(0)

      val mapColUDF: UserDefinedFunction = udf(CustomUDFs.mapCols)
      spark.udf.register("mapColUDF", mapColUDF)

      val loader : Loader  = new Loader (spark:SparkSession,StormshieldLogsAppSettings.storageDir:String)
      val cleaner: Cleaner = new Cleaner(spark:SparkSession,StormshieldLogsAppSettings.storageDir:String)

      val logsDF           : Dataset[Row] = loader.loadStormshieldLogs(StormshieldLogsAppSettings.logsDir, logType)
      val withoutAnomalies : Dataset[Row] = loader.filterAnomalyRows(logsDF:Dataset[Row])

      val cleanedDF: Dataset[Row] = cleaner.cleanStormshieldLogs(withoutAnomalies,logType,StormshieldLogsAppSettings.logsDir,StormshieldLogsAppSettings.rebuildColumns)
      cleanedDF.show()

      cleanedDF.show(false)
      cleanedDF.write
               .mode("overwrite")
               .parquet(StormshieldLogsAppSettings.storageDir+logType+".parquet")
     // val usersCount: Int = cleanedDF.select("user").distinct().count().toInt
     // cleanedDF.select("user").distinct().sort("user").show(usersCount, false)
  }
}

