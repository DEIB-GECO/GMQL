package it.polimi.genomics.wsc.Knox

/**
  * Created by abdulrahman on 30/05/16.
  */
object KnoxOperation extends Enumeration{
  type OP = Value
  val LISTSTATUS = Value("LISTSTATUS")
  val DOWNLOAD = Value("OPEN")
  val MKDIRS = Value("MKDIRS")
  val UPLOAD = Value("CREATE")
  val DELETE = Value("DELETE")
}
