package it.polimi.genomics.spark.test

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.BedParser
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Abdulrahman Kaitoua on 25/06/15.
 * Email: abdulrahman.kaitoua@polimi.it
 *
 */
object TestSelect {
  def main(args: Array[String]) {
    import java.text.SimpleDateFormat
    import java.util.Date
    val formatter = new SimpleDateFormat("HH:mm:ss:SSS");

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

    val runner = new GMQLSparkExecutor(sc=sc)
    val server = new GmqlServer(runner)

    val output = "/home/abdulrahman/SparkOutput/V2DAG"+formatter.format(new Date(System.currentTimeMillis()))+"/"

    val meta_con =
      DataStructures.MetadataCondition.AND(
        DataStructures.MetadataCondition.Predicate("cell",DataStructures.MetadataCondition.META_OP.GTE, "11"),
        DataStructures.MetadataCondition.NOT(
          DataStructures.MetadataCondition.Predicate("provider", DataStructures.MetadataCondition.META_OP.NOTEQ, "UCSC")
        )
      )

    val   reg_con =
      //        DataStructures.RegionCondition.OR(
      //          DataStructures.RegionCondition.Predicate(3, DataStructures.RegionCondition.REG_OP.GT, 30),
      //DataStructures.RegionCondition.Predicate(0, DataStructures.RegionCondition.REG_OP.EQ, "400")
      DataStructures.RegionCondition.Predicate(2, DataStructures.RegionCondition.REG_OP.GT, DataStructures.RegionCondition.MetaAccessor("cell"))
    //        )

    //META SELECTION
    val input = "/home/abdulrahman/V2Spark_TestFiles/Samples/meta/exp.narrowPeak,/home/abdulrahman/V2Spark_TestFiles/Samples/meta/exp1.narrowPeak"
    val s = server READ input.split(",").toList USING BedParser
    val metaSelection = s.SELECT(meta_con,reg_con)

    server setOutputPath output MATERIALIZE metaSelection
    server.run()
  }
}
