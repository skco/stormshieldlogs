import org.apache.spark.sql.expressions.UserDefinedFunction

object CustomUDFs {
    val mapCols = (row: String, cols: Map[String, String]) => { //udf function
      val pattern: String = "[ =]+(?=(?:[^\"]*[\"][^\"]*[\"])*[^\"]*$)" // match ; and = outside double quotes ""
      val pairs: Iterator[Array[String]] = row.split(pattern)
        .grouped(2) // split and group to (key, value)
      cols ++ pairs
        .map { case Array(k, v) => k -> v } // insert values
        .toMap
    }: Map[String, String]
  }
