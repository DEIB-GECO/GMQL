package it.polimi.genomics.spark.test

import it.polimi.genomics.GMQLServer.{DefaultRegionsToRegionFactory, GmqlServer}
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{Default, MetaJoinCondition}
import it.polimi.genomics.core.DataStructures.RegionAggregate.RegionsToRegion
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.core.{GDouble, GString, GValue, ParsingType}
import it.polimi.genomics.spark.implementation.GMQLSparkExecutor
import it.polimi.genomics.spark.implementation.loaders.test3Parser
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The entry point of the application
  * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
  */
object Map {

   def main(args : Array[String]) {


     val mainPath = "/home/abdulrahman/IDEA/GMQL_V2/GMQL-Flink/src/test/datasets/"
     val ex_data_path = List(mainPath + "map/exp/")
     val ex_data_path_optional = List(mainPath + "map/ref/")
     val output_path = mainPath + "res3/"



     val what = 0 // simple map
     //val what = 1 // map with aggregation
//     val what = 2 // map with grouping and aggregation

    val conf = new SparkConf()
      .setAppName("GMQL V2 Spark")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc:SparkContext =new SparkContext(conf)

    val server = new GmqlServer(new GMQLSparkExecutor(sc = sc))

    val dataAsTheyAre = server READ ex_data_path USING test3Parser()
    val optionalDS = server READ ex_data_path_optional USING test3Parser()

     val map = what match{
       case 0 =>
         // MAP
         optionalDS.MAP(None, List(), dataAsTheyAre)

       case 1 =>
         // MAP with aggregation
         dataAsTheyAre.MAP(None, List( DefaultRegionsToRegionFactory.get("MAX",0,Some("newScore")) ), optionalDS)

       case 2 =>
         //MAP with grouping and aggregation
         dataAsTheyAre.MAP(
           Some(new MetaJoinCondition(List(Default("bert_value1")))),
           List(
             new RegionsToRegion {
              override val resType = ParsingType.DOUBLE
              override val index: Int = 0
              override val associative: Boolean = true
              override val funOut: (GValue,(Int, Int)) => GValue = {(v1,v2)=>v1}
              override val fun: (List[GValue]) => GValue = {
               (line) =>{val ss = line.map(_.asInstanceOf[GDouble].v)
                if(!ss.isEmpty) GDouble(ss.reduce(_ + _))else GDouble (0)
               }
              }},
            new RegionsToRegion {
             override val resType: PARSING_TYPE = ParsingType.STRING
             override val index: Int = 1
             override val associative: Boolean = false
             override val funOut: (GValue, (Int, Int)) => GValue = { (v1, v2) => v1 }
             override val fun: (List[GValue]) => GValue = {
              (list) => GString(list.map((gvalue) => gvalue.asInstanceOf[GString].v).reduce((word1: String, word2: String) => word1 + " " + word2))
             }
            }
           ),
           optionalDS
         )
     }
     server setOutputPath output_path MATERIALIZE map

     server.run()
   }

 }
