package it.polimi.genomics.flink.examples

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.BedScoreParser

/**
  * The entry point of the application
  * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
  */
object Merge {

   def main(args : Array[String]) {

     val server = new GmqlServer(new FlinkImplementation)
     val mainPath = "/Users/michelebertoni/Workspaces/IdeaProjects/genomic_computing/GMQLv2Scala/scalav2/trunk/GMQL-Flink/src/test/datasets/"
     val ex_data_path = List(mainPath + "merge/left/")
     val ex_data_path_optional = List(mainPath + "merge/right/")
     val output_path = mainPath + "res/"


     val dataAsTheyAre = server READ ex_data_path USING BedScoreParser
     val optionalDS = server READ ex_data_path_optional USING BedScoreParser

     //MERGE
     val merge = dataAsTheyAre.UNION(optionalDS)

     server setOutputPath output_path MATERIALIZE merge


     server.run()
   }

 }
