package stormshieldLogs

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.{call_udf, col}

import java.nio.file.{Files, Paths}

class Cleaner(spark: SparkSession,storageDir:String) {

  /**
   * Save column names to text file
   *
   * @param DF       DataFrame with extracted column names
   * @param logType  name of log file
   */
  private def saveUniqueCols(DF: Dataset[Row], logType: String): Unit = {
    DF.write.mode("overwrite").text(s"$storageDir$logType.txt")
  }

  /**
   * Read column names from text file
   *
   * @param logType    name of log file
   * @return           DataFrame with column names
   */
  private def readColsFromFile(logType: String):Dataset[Row]= {
   spark.read.text(s"$storageDir$logType.txt")

  }

  /**
   * convert DatafFame with column names to  Map[String,String]
   *
   * @param colDF    DataFrame with unique column names stored in "value" column
   * @return         Map[ColName,"NULL"] as type Map[String,String]
   */
  private def convertToMap(colDF: Dataset[Row]): Map[String, String] = {
    val headers: List[Row] = colDF.select("value")
                                  .collect
                                  .toList
    headers.map(t => t.getString(0) -> "NULL")
      .toMap
  }

  /**
   * get unique column names from DataFrame with text format like "ColName1=value1 ColName2=value2"
   *
   * @param DF               DataFrame with raw text format
   * @return                 DataFrame with unique column names stored in "value" column
   */
  private def getUniqueCols(DF: Dataset[Row]): Dataset[Row]= {
    import spark.implicits._
    DF
      .withColumn("value", regexp_replace(col("value"), "\"(.*?)\"", ""))
      .withColumn("value", regexp_replace(col("value"), "(?<==).*?(?=( ([a-z])|$| ))", ""))
      .withColumn("value", regexp_replace(col("value"), "=", ""))
      .select(split(col("value"), " ")
      .as("value"))
      .distinct()
      .withColumn("value", explode($"value"))
      .distinct()
  }

  /**
   * create DataFrame with multiple columns from rows with Map[String,String] (colname, value)
   *
   * @param DF  DataFrame with column "value" contains Map[String,String]  (colname, value)
   * @return    DataFrame with multiple columns extracted
   */
  private def convertMapToColumns(DF: Dataset[Row]): Dataset[Row] = {
      //extract column with Map (key, value ...) to dataframe
      import spark.implicits._
      val keysDF: Dataset[Row] = DF.select(explode(map_keys($"value")))
        .distinct() // extract keys to DF

      val keys: Array[Any] = keysDF.collect()
        .map(f => f.get(0)) // extract keys from DF to Map

      val keyCols: Array[Column] = keys.map(f => col("value")
        .getItem(f)
        .as(f.toString)) // create Array of columns from Map

      DF.select(col("value") +: keyCols: _*)
        .drop("value") // create new columns and drop old
  }

  /**
   * main cleaning function
   *
   * @param DF              DataFrame raw text format
   * @param logType         log type directory
   * @param logDir          main log store directory
   * @param rebuildColumns  force  column recalculating
   * @return                Cleaned DataFrame
   */

  def cleanStormshieldLogs(DF: Dataset[Row], logType: String, logDir: String, rebuildColumns: Boolean): Dataset[Row] = {

    val colsDF:Dataset[Row] =  if (!Files.exists(Paths.get(s"$logDir$logType.txt")) | rebuildColumns) {
      val colsTmp: Dataset[Row] = getUniqueCols(DF) // get list of all possible columns for logType
      saveUniqueCols(colsTmp,logType) // save to parquet
      colsTmp
    }
    else {
      readColsFromFile(logType)                   // read cached columns
    }

    val fullCols:Map[String,String] = convertToMap(colsDF)
    val result: Dataset[Row] = DF.withColumn("value", call_udf("mapColUDF", col("value"), typedLit(fullCols)))

    convertMapToColumns(result)

  }
}