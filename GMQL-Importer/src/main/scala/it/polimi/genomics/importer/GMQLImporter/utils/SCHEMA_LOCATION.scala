package it.polimi.genomics.importer.GMQLImporter.utils

/**
  * Created by nachon on 10/14/16.
  */
object SCHEMA_LOCATION extends Enumeration {
  type SCHEMA_LOCATION = Value
  val LOCAL = Value("local")
  val FTP = Value("ftp")
  val HTTP = Value("http")
}
