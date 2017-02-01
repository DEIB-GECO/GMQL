package it.polimi.genomics.spark.test

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.test1Parser
import org.apache.spark.{SparkContext, SparkConf}

/**
 * The entry point of the application
 * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
 */
/**
  *
  */
object Join {

  /**
    *
    * @param args
    */
  def main(args : Array[String]) {

    val timestamp: Long = System.currentTimeMillis / 1000

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
    val server = new GmqlServer(new GMQLSparkExecutor(sc=sc))
    val mainPath = "/home/abdulrahman/IDEA/GMQL_V2/GMQL-Flink/src/test/datasets/"
    val ex_data_path = List(mainPath + "join2/ref/")
    val ex_data_path_optional = List(mainPath + "join2/exp/")
//    val ex_data_path = List(mainPath + "join/ref/")
//    val ex_data_path_optional = List(mainPath + "join/exp/")
    val output_path = mainPath + "res2/"+timestamp+"/"


    val dataAsTheyAre = server READ ex_data_path USING test1Parser() // as alternative // new DelimiterSeparatedValuesParser('\t',0,1,2,Some(5),Some(Array(4)),None,Some(Array(3,8)))
    val optionalDS = (server READ ex_data_path_optional USING test1Parser()) //.PROJECT(projected_meta = Some(List("test","antibody")), distinct = Some(List(1,2)))


    val what = 6 // Join distLess
//    val what = 1 // Join distLess minDist
//    val what = 2 // Join distLess upstream
//    val what = 3 // Join distGreat distLess
//    val what = 4 // Join distGreat minDist distless



    val difference = what match{

      case 0 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistLess(100)))), RegionBuilder.CONTIG, optionalDS)

      case 1 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)), Some(new MinDistance(1)))), RegionBuilder.CONTIG, optionalDS)

      case 2 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)), Some(new Upstream()))), RegionBuilder.CONTIG, optionalDS)

      case 3 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistGreater(4)), Some(new Upstream()), Some(new DistLess(20)))), RegionBuilder.CONTIG, optionalDS)

      case 4 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)), Some(new MinDistance(1)), Some(DistGreater(3)))), RegionBuilder.CONTIG, optionalDS)

      case 5 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new DistLess(0)))), RegionBuilder.CONTIG, optionalDS)

      case 6 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new DistGreater(0)))), RegionBuilder.CONTIG, optionalDS)

      case 7 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new Upstream()))), RegionBuilder.CONTIG, optionalDS)

      case 8 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new DownStream()))), RegionBuilder.CONTIG, optionalDS)

      case 9 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(1)))), RegionBuilder.CONTIG, optionalDS)

      case 10 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(2)))), RegionBuilder.CONTIG, optionalDS)

      case 11 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new DistGreater(0)), Some(new MinDistance(1)))), RegionBuilder.CONTIG, optionalDS)

      case 12 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new DistLess(0)), Some(new MinDistance(1)))), RegionBuilder.CONTIG, optionalDS)

      case 13 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new DistGreater(0)), Some(new Upstream()), Some(new MinDistance(1)))), RegionBuilder.CONTIG, optionalDS)

      case 14 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(10)))), RegionBuilder.CONTIG, optionalDS)

      case 15 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(10)), Some(DistLess(-1)))), RegionBuilder.CONTIG, optionalDS)

    }
    server setOutputPath output_path MATERIALIZE difference

    server.run()
  }

}

