package it.polimi.genomics.scidb.test

import it.polimi.genomics.scidb.repository.GmqlSciFileSystem

/**
  * Created by Cattani Simone on 03/03/16.
  * Email: simone.cattani@mail.polimi.it
  *
  */
object FileSystemTest
{
  def main(args: Array[String]): Unit =
  {
    println("\n-----------------------------------------------")
    println("-- FILE SYSTEM TEST ---------------------------")
    println("-----------------------------------------------")

    val local = "/Users/cattanisimone/Dropbox/projects/services/GenData2020/source/source"
    val path = "/home/scidb/test/real/np_exp"
    val file = "/_samples_2.csv"

    /*
    val res = GmqlSciFileSystem.ls(path)
    println(res)
    */

    /*
    val csv = GmqlSciFileSystem.cat(path+file)
    val bufferedSource = csv
    for (line <- bufferedSource.getLines)
      println(line)
    println(csv)
    */

    GmqlSciFileSystem.mkdir(local+path+"/test/test2")

  }
}
