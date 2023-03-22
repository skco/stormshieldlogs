object StormshieldLogsAppSettings {
    val HDFSpath:String = "hdfs://hadoopMaster:9000/"
    val logsDir: String = HDFSpath+"SSLogs/"
    val storageDir: String = HDFSpath+"LogStore/"
    //"alarm", "auth", "connections", "filterstat", "plugin", "system", "web" available log types
    val rebuildColumns: Boolean = true
  }