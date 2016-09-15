package it.polimi.genomics.wsc.Knox

import java.io.File

import scala.concurrent.Await

/**
  * Created by abdulrahman on 30/05/16.
  */
object test_Knox {

  def main(args: Array[String]) {
    val inputPath = args(1)

    val outputPath = args(2)

    println(inputPath)
    println(outputPath)

    try {
      if (args(0) == "1") {
        println("Download File")
        KnoxClient.downloadFile(inputPath, new File(outputPath))
      } else if (args(0) == "2") {
        println("Download Folder")
        KnoxClient.downloadFolder(inputPath, outputPath)
      } else if (args(0) == "3") {
        println("Upload")
        KnoxClient.uploadFile(inputPath, outputPath)
      } else if (args(0) == "4") {
        import scala.concurrent.duration._
        println("LIST")
        val dd = Await.result(KnoxClient.listFiles(inputPath), 10.second)
        println(dd)
      } else if (args(0) == "5") {
        import scala.concurrent.duration._
        println("Delete")
        val dd = KnoxClient.delDIR(inputPath)
        println(dd)
      } else {
        println("MKDIRS")
        KnoxClient.mkdirs(inputPath)
      }

    } finally {
      KnoxClient.standaloneWSAPI.close()
    }
  }
}
