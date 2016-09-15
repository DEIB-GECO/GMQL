package it.polimi.genomics.spark.Run

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.CoverParameters.{CoverFlag, N}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.test3Parser
import org.apache.spark.{SparkContext, SparkConf}

/**
  * The entry point of the application
  * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
  */
object Cover {

  def main(args : Array[String]) {

    val conf = new SparkConf()
    val sc:SparkContext =new SparkContext(conf)

    val server = new GmqlServer(new GMQLSparkExecutor(sc=sc))

    val ex_data_path = "/home/abdulrahman/Desktop/datasets/coverData/"
    val output_path = "/home/abdulrahman/testCover/res/"


    val dataAsTheyAre = server READ ex_data_path USING test3Parser()

    val cover = dataAsTheyAre.COVER(CoverFlag.COVER, N(2), N(3), List(), None )

    server setOutputPath output_path MATERIALIZE cover

    server.run()

  }

}

////     val what = 0 // Cover
//    val what = 1 // Flat
//    // val what = 2 // Summit
//    // val what = 3 // Histogram
// val mainPath = "/home/abdulrahman/IDEA/GMQL_V2/GMQL-Flink/src/test/datasets/"
//    //COVER
//    val cover =
//      what match{
//        case 0 => {
//          dataAsTheyAre.COVER(CoverFlag.COVER, N(2), N(3), List(), None )
//        }
//
//        case 1 => {
//          dataAsTheyAre.COVER(CoverFlag.FLAT, N(2), N(3), List(), None )
//        }
//
//        case 2 => {
//          dataAsTheyAre.COVER(CoverFlag.SUMMIT, N(2), N(5), List(), None )
//        }
//
//        case 3 => {
//          dataAsTheyAre.COVER(CoverFlag.HISTOGRAM, ANY(), ANY(), List(), None )
//        }
//      }
//
//
//
//    server setOutputPath output_path MATERIALIZE cover
//
//    server.run()

