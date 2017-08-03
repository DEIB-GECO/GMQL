package it.polimi.genomics.spark.test

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.CoverParameters.{CoverFlag, N}
import it.polimi.genomics.core.DataStructures.GroupMDParameters.{Direction, NoTop, TopP}
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{Default, MetaJoinCondition}
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core._
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor.GMQL_DATASET
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The entry point of the application
  * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
  */
object SemiJoin {

  def main(args : Array[String]) {

    val conf = new SparkConf().setAppName("test New API for inputing datasets").setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.sql.tungsten.enabled", "true")
    val sc:SparkContext =new SparkContext(conf)

    val server = new GmqlServer(new GMQLSparkExecutor(sc=sc,outputFormat = GMQLSchemaFormat.COLLECT))

    val metaDS = sc.parallelize((1 to 100).map(x=> (x%5l,("test","Abdo"))) :+ (1l,("hi","Khalil")) :+ (2l,("hi","sultan")):+ (3l,("hi","salem")))
    val regionDS = sc.parallelize((1 to 1000).map{x=>(new GRecordKey(x%2,"Chr"+(x%2),x,x+200,'*'),Array[GValue](GDouble(1)) )})

    val metaDS1 = sc.parallelize((1 to 100).map(x=> (x%5l,("test","Abdo"))):+ (1l,("hi","Khalil")) :+ (2l,("hi","sami")) :+ (3l,("hi","sultan")) :+ (4l,("hi","ahmad")))
    val regionDS1 = sc.parallelize((1 to 1000).map{x=>(new GRecordKey(x%2,"Chr"+(x%2),x,x+200,'*'),Array[GValue](GDouble(1)) )})

    val ex_data_path = "/home/abdulrahman/Desktop/datasets/coverData/"
    val output_path = "/Users/abdulrahman/Desktop/testCover/res111/"



    val dataAsTheyAre = server.READ(ex_data_path).USING(metaDS,regionDS,List[(String, PARSING_TYPE)](("score",ParsingType.DOUBLE)))

    val REFDS = server.READ(ex_data_path).USING(metaDS1,regionDS1,List[(String, PARSING_TYPE)](("score",ParsingType.DOUBLE)))

//    val semiJoinSelect = dataAsTheyAre.SELECT(
//      MetaJoinCondition(List(Default("hi")),true),
//      REFDS
//    )

    val semiJoinSelect = REFDS.SELECT(
      MetaJoinCondition(List(Default("hi")),true),
      dataAsTheyAre

    )

    val cover = dataAsTheyAre.COVER(CoverFlag.HISTOGRAM, new N{override val n=2}, new N{override val n=3}, List(), None )

    val res = dataAsTheyAre.ORDER(None, "group_name", NoTop(), Some(List((0, Direction.ASC))), TopP(1))

    val output = server setOutputPath output_path COLLECT (semiJoinSelect)

    println("The size is : "+output.asInstanceOf[GMQL_DATASET]._2.size)
    output.asInstanceOf[GMQL_DATASET]._2.foreach(x=>println((x._1,x._2/*.mkString("\t")*/)))
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

