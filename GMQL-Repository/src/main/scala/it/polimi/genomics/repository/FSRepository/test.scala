package it.polimi.genomics.repository.FSRepository

import java.nio.file.FileSystems

/**
  * Created by abdulrahman on 29/06/16.
  */
object test {
  def main(args: Array[String]) {
    val s = FileSystems.getDefault().getPath("")
    println(s.toAbsolutePath)
   println( Utilities.validate("/Users/abdulrahman/HG19_BED_ANNOTATION_V2.schema"))
  }

}
