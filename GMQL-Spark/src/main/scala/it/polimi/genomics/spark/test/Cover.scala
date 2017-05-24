package it.polimi.genomics.spark.test

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.CoverParameters.{CoverFlag, N}
import it.polimi.genomics.core.DataStructures.IRSelectRD
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core._
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor.GMQL_DATASET
import it.polimi.genomics.spark.implementation.loaders.test3Parser
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The entry point of the application
  * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
  */
object Cover {

  def main(args : Array[String]) {

    val conf = new SparkConf().setAppName("test New API for inputing datasets").setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.sql.tungsten.enabled", "true")
    val sc:SparkContext =new SparkContext(conf)

    val server = new GmqlServer(new GMQLSparkExecutor(sc=sc,outputFormat = GMQLSchemaFormat.COLLECT))

    val metaDS = sc.parallelize((1 to 100).map(x=> (1,("test","Abdo"))))
    val regionDS = sc.parallelize((1 to 1000).map{x=>(new GRecordKey(1,"Chr"+(x%2),x,x+200,'*'),Array[GValue](GDouble(1)) )})

    val ex_data_path = "/home/abdulrahman/Desktop/datasets/coverData/"
    val output_path = "/Users/abdulrahman/Desktop/testCover/res111/"


    val dataAsTheyAre = server.READ(ex_data_path).USING(metaDS,regionDS,List[(String, PARSING_TYPE)](("score",ParsingType.DOUBLE)))

    val cover = dataAsTheyAre.COVER(CoverFlag.HISTOGRAM, N(1), it.polimi.genomics.core.DataStructures.CoverParameters.ANY(), List(), None )

    val output = server setOutputPath output_path COLLECT (cover)

    output.asInstanceOf[GMQL_DATASET]._1.foreach(println _)
//    server.run()

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

