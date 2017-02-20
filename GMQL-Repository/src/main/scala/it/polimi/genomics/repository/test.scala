package it.polimi.genomics.repository

/**
  * Created by abdulrahman on 15/02/2017.
  */
object test {
  def main(args: Array[String]): Unit = {

    val LOCAL = ".*(LOCAL)".r
    val HDFS = ".*(HDFS)".r

    "lll_HDFS" match {
      case LOCAL(l) => println("Hio",l)
case HDFS(s) => println(s)
      case _ => println("not")
    }
  }
}
