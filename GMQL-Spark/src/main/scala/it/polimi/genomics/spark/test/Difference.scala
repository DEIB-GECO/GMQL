package it.polimi.genomics.spark.test

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{Default, MetaJoinCondition}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.test3Parser
import org.apache.spark.{SparkConf, SparkContext}

/**
 * The entry point of the application
 * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
 */
object Difference {

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
    val server = new GmqlServer(new GMQLSparkExecutor(sc=sc))
    val mainPath = "/home/abdulrahman/IDEA/GMQL_V2/GMQL-Flink/src/test/datasets/"
    val ex_data_path = List(mainPath + "difference/ref/")
    val ex_data_path_optional = List(mainPath + "difference/exp/")
    val output_path = mainPath + "resDif/"


    val dataAsTheyAre = server READ ex_data_path USING test3Parser()
    val optionalDS = server READ ex_data_path_optional USING test3Parser()

    //val what = 0 // difference
    val what = 1 // grouped difference


    val difference = what match{

      case 0 =>
        // MAP
        dataAsTheyAre.DIFFERENCE(condition = None, subtrahend = optionalDS)

      case 1 =>
        // MAP with aggregation
        dataAsTheyAre.DIFFERENCE(condition = Some(new MetaJoinCondition(List(Default("bert_value1")))), subtrahend = optionalDS)

    }
    server setOutputPath output_path MATERIALIZE difference

    server.run()
  }

}
