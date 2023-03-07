package stormshieldLogs

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.{call_udf, col}
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.nio.file.{Files, Paths}

class Cleaner(spark: SparkSession,storageDir:String){
  private def getAndStoreUniqueCols(df: Dataset[Row], logType: String, rebuildColumns: Boolean): Map[String, String] = {
    //get all unique column sets from dataset within string  "field1=value1 field2=value2, ...."
    spark.sparkContext.broadcast()
    import spark.implicits._

    val result: Dataset[Row] =
      if (!Files.exists(Paths.get(s"$storageDir$logType.txt")) | rebuildColumns) { // recaclutating if file not exist or on rebuild flag
          val result: Dataset[Row] = df
            .withColumn("value", regexp_replace(col("value"), "\"(.*?)\"", ""))
            .withColumn("value", regexp_replace(col("value"), "(?<==).*?(?=( ([a-z])|$| ))", ""))
            .withColumn("value", regexp_replace(col("value"), "=", ""))
            .select(split(col("value"), " ").as("value")).distinct()
            .withColumn("value", explode($"value")).distinct()

            result.write.mode("overwrite").text(s"$storageDir$logType.txt") // cache results
            result
      }
    else
      {  //use col names from previous calculations
        spark.read.text(s"$storageDir$logType.txt")
      }
    //create colName -> NULL Map

    val headers:List[Row] = result.select("value").collect.toList
    headers.map(t => t.getString(0) -> "NULL").toMap // create empty columns map
  }

  private def convertMapToColumns(df: Dataset[Row]): Dataset[Row] = {
      //extract column with Map (key, value ...) to dataframe
      import spark.implicits._
      val keysDF: Dataset[Row] = df.select(explode(map_keys($"value")))
                                   .distinct() // extract keys to DF

      val keys:Array[Any] = keysDF.collect()
                                  .map(f => f.get(0)) // extract keys from DF to Map
      val keyCols:Array[Column] = keys.map(f => col("value")
                        .getItem(f)
                        .as(f.toString)) // create Array of columns from Map

      df.select(col("value") +: keyCols: _*)
        .drop("value") // create new columns and drop old
    }

  val mapCols = (row: String, cols: Map[String, String]) =>
      {  //udf function
        val pattern: String = "[ =]+(?=(?:[^\"]*[\"][^\"]*[\"])*[^\"]*$)"  // match ; and = outside double quotes ""
        val pairs: Iterator[Array[String]] = row.split(pattern).grouped(2) // split and group to (key, value)
        cols ++ pairs.map {case Array(k, v) => k -> v}.toMap             // insert values
      }: Map[String, String]

  def cleanStormshieldLogs(df:Dataset[Row],logType:String,logDir:String,rebuildColumns:Boolean):Dataset[Row] =   {
      //cleaning
      val fullCols:Map[String,String] = getAndStoreUniqueCols(df, logType, rebuildColumns) // get list of all possible columns for logType

      val mapColUDF: UserDefinedFunction = udf(mapCols)
      spark.udf.register("mapColUDF", mapColUDF)

      val result:Dataset[Row] = df.withColumn("value", call_udf("mapColUDF", col("value"),typedLit(fullCols)))

      convertMapToColumns(result)

    }

  }

