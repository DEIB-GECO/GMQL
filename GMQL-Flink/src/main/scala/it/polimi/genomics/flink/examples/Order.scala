package it.polimi.genomics.flink.examples

import it.polimi.genomics.core.DataStructures.GroupMDParameters.{Direction, NoTop, TopG}
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.BedScoreParser

/**
  * The entry point of the application
  * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
  */
object Order {

   def main(args : Array[String]) {

     val server = new GmqlServer(new FlinkImplementation)
     val mainPath = "/Users/michelebertoni/Workspaces/IdeaProjects/genomic_computing/GMQLv2Scala/scalav2/trunk/GMQL-Flink/src/test/datasets/"
     val ex_data_path = List(mainPath + "order/")
     val output_path = mainPath + "res/"


     val dataAsTheyAre = server READ ex_data_path USING BedScoreParser

     //ORDER

     val what = 0 // OrderMD
     // val what = 1 // OrderRD

     val order =
      what match{
        case 0 => {
          //ORDERMD
          // dataAsTheyAre.ORDER(Some(List(("bert_value2", Direction.DESC), ("bert_value1", Direction.ASC))), "group_ordering", NoTop(), None, NoTop())
          // dataAsTheyAre.ORDER(Some(List(("bert_value2", Direction.DESC), ("bert_value1", Direction.ASC))), "group_ordering", Top(2), None, NoTop())
          dataAsTheyAre.ORDER(Some(List(("bert_value2", Direction.DESC), ("bert_value1", Direction.ASC))), "group_ordering", TopG(1), None, NoTop())
        }

        case 1 => {
          //ORDERRD
          // dataAsTheyAre.ORDER(None, "groupName", NoTop(), Some(List((1, Direction.DESC))), NoTop())
          // dataAsTheyAre.ORDER(None, "groupName", NoTop(), Some(List((1, Direction.DESC))), Top(3))
          dataAsTheyAre.ORDER(None, "groupName", NoTop(), Some(List((0, Direction.ASC), (1, Direction.DESC))), TopG(3))
        }
      }

     server setOutputPath output_path MATERIALIZE order


     server.run()
   }

 }
