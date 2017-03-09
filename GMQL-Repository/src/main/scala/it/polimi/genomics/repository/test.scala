package it.polimi.genomics.repository

import java.io.File

import it.polimi.genomics.repository.FSRepository.LFSRepository

/**
  * Created by abdulrahman on 15/02/2017.
  */
object test {
  def main(args: Array[String]): Unit = {

    println(new File(".").getAbsoluteFile.toString)
    val rep = new LFSRepository();
    println(rep.readSchemaFile("/Users/abdulrahman/Downloads/chr1_only/test.schema"))

  }
}
