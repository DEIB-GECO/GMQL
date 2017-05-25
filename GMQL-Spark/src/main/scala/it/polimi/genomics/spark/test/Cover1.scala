package it.polimi.genomics.spark.test

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.CoverParameters.{CoverFlag, N}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.BedScoreParser
import org.apache.spark.{SparkContext, SparkConf}


/**
 * The entry point of the application
 * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
 */
object Cover1 {

  def main(args : Array[String]) {
    val conf = new SparkConf()
      .setAppName("GMQL V2 Spark")
      //    .setSparkHome("/usr/local/Cellar/spark-1.5.2/")
      .setMaster("local[*]")
      //    .setMaster("yarn-client")
      //    .set("spark.executor.memory", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "64")
      .set("spark.driver.allowMultipleContexts","true")
      .set("spark.sql.tungsten.enabled", "true")
    val sc:SparkContext =new SparkContext(conf)
    val server = new GmqlServer(new GMQLSparkExecutor(testingIOFormats = true,sc=sc)/*, meta_implementation = Some(new FlinkImplementation(metaFirst = true))*/)

    val main = "/Users/abdulrahman/Desktop/summit/beds/"
    val ds1 = main + "annotations/"
    val ds2 = main + "beds/"
    val out = main + "resS1/"


    //val ds1 = "test3"

    //val ds1R = server READ ds1 USING RnaSeqParser
    val ds2R = server READ main USING BedScoreParser


    //val ds1S = ds1R.SELECT(NOT(Predicate("antibody", META_OP.EQ, "CTCF")))
    //val ds2S = ds2R.SELECT(Predicate("antibody", META_OP.EQ, "BRD4"))


    //val map = ds1R.MAP(None, List(), ds2R, None)
    //val map = ds1S.MAP(MetaJoinCondition(List("organism")), List(), ds1S)
    //val res = ds2S.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(1)))), RegionBuilder.CONTIG, ds1S)
    val cover = ds2R.COVER(CoverFlag.SUMMIT, new N{override val n=1;
      override val fun: (Int) => Int = (x) => x}, new N{override val n=4;
      override val fun: (Int) => Int = (x) => x/2}, List(), None)
/*
    val cR = server READ ds1 USING BedScoreParser
    val cS = cR SELECT(Predicate("antibody", META_OP.EQ, "CTCF"))
    val res = cS COVER(CoverFlag.COVER, N(1), N(2), List(), None)
*/

    val res = cover//.ORDER(None, "group_name", NoTop(), Some(List((0, Direction.ASC))), NoTop())
    server setOutputPath out MATERIALIZE res

/*

    println("-------------RESULT--------------")
    println(server.extractMetaFirst().mkString("\n"))
    println("-------------RESULT END--------------")
*/

    server.run()

  }


}
