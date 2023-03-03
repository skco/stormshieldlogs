package stormshieldLogs

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.{call_udf, col, lit, when}

import java.nio.file.{Paths, Files}

class Cleaner(spark: SparkSession,storageDir:String){
  private def GetAndStoreUniqueCols(df: Dataset[Row], logType: String, rebuildColumns: Boolean): Map[String, String] = {
    //get all unique column sets from dataset within string  "field1=value1 field2=value2, ...."
    import spark.implicits._

    val result: Dataset[Row] =
      if (!Files.exists(Paths.get(s"${storageDir}${logType}.txt")) | rebuildColumns) { // recaclutating
          val result: Dataset[Row] = df
            //.withColumn("value", regexp_replace(col("value"), "=\"(.*?)\"", ""))
            .withColumn("value", regexp_replace(col("value"), "=(?<==).*?(?=( ([a-z])|$))", ""))
            //.withColumn("value", regexp_replace(col("value"), "=", ""))
            .select(split(col("value"), " ").as("value")).distinct()
            .withColumn("value", explode($"value")).distinct()

          result.write.mode("overwrite").text(s"${storageDir}${logType}.txt") // store results
          result
      }
    else
      {  //use result from previous calculations
        spark.read.text(s"${storageDir}${logType}.txt")
      }
    //create colName -> NULL Map

    val headers = result.select("value").collect.toList
    headers.map(t => t.getString(0) -> "NULL").toMap // create empty columns map
  }

  private def convertMapToColumns(df: Dataset[Row]): Dataset[Row] = {
      //extract column with Map (key, value ...) to dataframe
      import spark.implicits._
      val keysDF = df.select(explode(map_keys($"value"))).distinct() // extract keys to DF
      val keys = keysDF.collect().map(f => f.get(0)) // extract keys from DF to Map
      val keyCols = keys.map(f => col("value").getItem(f).as(f.toString)) // create Array of columns from Map

      df.select(col("value") +: keyCols: _*).drop("value") // create new columns and drop old
    }


  def cleanStormshieldLogs(df:Dataset[Row],logType:String,logDir:String):Dataset[Row] =   {
      //cleaing procedure
      addColumnsApp.setCols(GetAndStoreUniqueCols(df, logType, false))

      val mapColUDF = udf(addColumnsApp.mapCols)
      spark.udf.register("mapColUDF", mapColUDF)

      val result = df.withColumn("value", call_udf("mapColUDF", col("value")))

      convertMapToColumns(result)


    }

  }

