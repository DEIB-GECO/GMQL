package it.polimi.genomics.flink.examples


import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{Default, MetaJoinCondition}
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.BedScoreParser

/**
 * The entry point of the application
 * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
 */
object Difference {

  def main(args : Array[String]) {

    val server = new GmqlServer(new FlinkImplementation)
    val mainPath = "/Users/michelebertoni/Workspaces/IdeaProjects/genomic_computing/GMQLv2Scala/scalav2/trunk/GMQL-Flink/src/test/datasets/"
    val ex_data_path = List(mainPath + "difference/ref/")
    val ex_data_path_optional = List(mainPath + "difference/exp/")
    val output_path = mainPath + "res/"


    val dataAsTheyAre = server READ ex_data_path USING BedScoreParser
    val optionalDS = server READ ex_data_path_optional USING BedScoreParser

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
