package it.polimi.genomics.flink.examples

import it.polimi.genomics.core.DataStructures.GroupRDParameters.FIELD
import it.polimi.genomics.core.DataStructures.MetaGroupByCondition.MetaGroupByCondition
import it.polimi.genomics.core.DataStructures.RegionAggregate.{RegionsToMeta, RegionsToRegion}
import it.polimi.genomics.core.{GDouble, GValue}
import it.polimi.genomics.GMQLServer.{DefaultMetaAggregateFactory, GmqlServer}
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.Default
import it.polimi.genomics.core.ParsingType
import it.polimi.genomics.core.ParsingType.PARSING_TYPE
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.BedScoreParser

/**
  * The entry point of the application
  * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
  */
object Group {

   def main(args : Array[String]) {

     val server = new GmqlServer(new FlinkImplementation)
     val mainPath = "/Users/michelebertoni/Workspaces/IdeaProjects/genomic_computing/GMQLv2Scala/scalav2/trunk/GMQL-Flink/src/test/datasets/"
     val ex_data_path = List(mainPath + "group/")
     // val ex_data_path = mainPath + "grouprd/"
     val output_path = mainPath + "res/"


     val dataAsTheyAre = server READ ex_data_path USING BedScoreParser

     //GROUP

     val what = 0 // MD
     //val what = 1 // RD


     val group =
       what match{
         //GROUPMD
         case 0 => {
           val sum =
             (l : List[GValue]) => {
               l.reduce((a,b) => GDouble(a.asInstanceOf[GDouble].v + b.asInstanceOf[GDouble].v))
             }
           val regionsToMeta = new RegionsToMeta{
             override val newAttributeName = "Psum"
             override val inputIndex = 2
             override val associative : Boolean = true
             override val fun = sum
             override val funOut: (GValue,(Int, Int)) => GValue = {(v1,v2)=>v1}
           }
           dataAsTheyAre.GROUP(Some(MetaGroupByCondition(List(Default("bert_value1")))), Some(List(DefaultMetaAggregateFactory.get("SUM", "ID", Some("SUM_ID")),DefaultMetaAggregateFactory.get("MAX", "ID", Some("MAX_ID")))), "bert_value_1_group", None, None)
         }

         case 1 => {
           //GROUPRD
           val sum =
             (l : List[GValue]) => {
               l.reduce((a,b) => GDouble(a.asInstanceOf[GDouble].v + b.asInstanceOf[GDouble].v))
             }
           val regionsToRegion = new RegionsToRegion{
             override val index = 1
             override val fun = sum
             override val associative: Boolean = true
             override val funOut: (GValue,(Int, Int)) => GValue = {(v1,v2)=>v1}
             override val resType: PARSING_TYPE = ParsingType.DOUBLE
           }
           //dataAsTheyAre.GROUP(None, None, "pvalue", None, None)
           //dataAsTheyAre.GROUP(None, None, "pvalue", None, Some(List(regionsToRegion)))
           dataAsTheyAre.GROUP(None, None, "pvalue", Some(List(new FIELD(1))), Some(List(regionsToRegion)))
         }
       }

     server setOutputPath output_path MATERIALIZE group

     server.run()

   }

 }
