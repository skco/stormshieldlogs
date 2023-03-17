import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
 * main app settings object
 */
object StormshieldLogsAppSettings {
  val logDir:     String = "E:/logsALL/"
  val storageDir: String = "E:/logStore/"
  val logType:    String = "auth"     //"alarm", "auth", "connections", "filterstat", "plugin", "system", "web" available log types
  val rebuildColumns: Boolean = true
}


object CustomUDFs {
  val mapCols = (row: String, cols: Map[String, String]) => { //udf function
    val pattern: String = "[ =]+(?=(?:[^\"]*[\"][^\"]*[\"])*[^\"]*$)" // match ; and = outside double quotes ""
    val pairs: Iterator[Array[String]] = row.split(pattern)
      .grouped(2) // split and group to (key, value)

       cols ++ pairs.map { case Array(k, v) => k -> v } // insert values
      .toMap
  }: Map[String, String]
}

object StormshieldLogsApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("StormShieldLogs")
                                          .master("local[*]")
                                          .getOrCreate()

      val mapColUDF: UserDefinedFunction = udf(CustomUDFs.mapCols)
      spark.udf.register("mapColUDF", mapColUDF)

      val loader : Loader  = new Loader (spark:SparkSession,StormshieldLogsAppSettings.storageDir:String)
      val cleaner: Cleaner = new Cleaner(spark:SparkSession,StormshieldLogsAppSettings.storageDir:String)

      val logsDF           : Dataset[Row] = loader.loadStormshieldLogs(StormshieldLogsAppSettings.logDir, StormshieldLogsAppSettings.logType)
      val withoutAnomalies : Dataset[Row] = loader.filterAnomalyRows(logsDF)

      val cleanedDF: Dataset[Row] = cleaner.cleanStormshieldLogs(withoutAnomalies,StormshieldLogsAppSettings.logType,StormshieldLogsAppSettings.logDir,StormshieldLogsAppSettings.rebuildColumns)
      cleanedDF.show()

      cleanedDF.show(false)
      cleanedDF.write
               .mode("overwrite")
               .parquet(StormshieldLogsAppSettings.storageDir+StormshieldLogsAppSettings.logType+".parquet")
     // val usersCount: Int = cleanedDF.select("user").distinct().count().toInt
     // cleanedDF.select("user").distinct().sort("user").show(usersCount, false)
  }
}

