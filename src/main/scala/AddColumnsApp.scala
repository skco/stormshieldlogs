package stormshieldLogs


object AddColumnsApp{

  private var fullCols: Map[String, String] = Map("" -> "") // empty Map for use in UDF mapColumns

  def setCols(cols:Map[String,String]): Unit = {
    fullCols = cols
  }
  def getCols():Map[String,String] = {
    fullCols
  }

  val mapCols = (row: String) => {
    val pattern: String = "[ =]+(?=(?:[^\"]*[\"][^\"]*[\"])*[^\"]*$)"       // match ;= outside double quotes ""
    val pairs = row.split(pattern).grouped(2)                               // split and group to (key, value)
    val result = fullCols ++ pairs.map { case Array(k, v) => k -> v }.toMap //insert values
    result
  }: Map[String, String]
}
