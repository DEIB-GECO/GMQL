package it.polimi.genomics.wsc.Knox

import java.io.File

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by abdulrahman on 30/05/16.
  */
object test_Knox {

  def main(args: Array[String]) {


    if( args.isEmpty ) {
      println("\n\n" +
        "arg0:\n"+
        "\t 1 - Download a file located at arg1 and store in arg2 (with file name) \n"+
        "\t 2 - Download a folder located at arg1 and store in arg2 (folder already existing)\n" +
        "\t 3 - Upload arg1 to arg2\n"+
        "\t 4 - List files in arg1\n"+
        "\t 5 - Delete file arg1\n"+
        "\t 6 - Mkdir arg1 \n\n" +
        "\t 7 - Upload dir arg1 to dir arg2 \n\n" +
        "hdfs path example 'user/akaitoua/example.txt' ");
      return ;

    }


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
        println("LIST")
        val dd = Await.result(KnoxClient.listFiles(inputPath), 10 second)
        println(dd)
      } else if (args(0) == "5") {
        println("Delete")
        val dd = KnoxClient.delDIR(inputPath)
        println(dd)
      } else if (args(0) == "6") {
        println("MKDIRS")
        KnoxClient.mkdirs(inputPath)
      } else if (args(0) == "7") {
        println("Upload dir "+inputPath+ " to "+outputPath)
        KnoxClient.uploadDir(inputPath, outputPath)
      } else {
        println("no operation corresponding to "+args(0))
      }

    } finally {
      KnoxClient.standaloneWSAPI.close()
    }
  }
}
