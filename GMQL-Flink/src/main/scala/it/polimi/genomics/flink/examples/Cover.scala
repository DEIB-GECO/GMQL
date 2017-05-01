package it.polimi.genomics.flink.examples

import it.polimi.genomics.core.DataStructures.CoverParameters.{ANY, CoverFlag, N}
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.BedScoreParser

/**
  * The entry point of the application
  * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
  */
object   Cover {

  def main(args : Array[String]) {

    val server = new GmqlServer(new FlinkImplementation)
    val mainPath = "/Users/michelebertoni/Workspaces/IdeaProjects/genomic_computing/GMQLv2Scala/scalav2/trunk/GMQL-Flink/src/test/datasets/"
    val ex_data_path = List(mainPath + "cover2/")
    val output_path = mainPath + "res/"


    val DS1 = server READ ex_data_path USING BedScoreParser

     val what = 0 // Cover
    // val what = 1 // Flat
    // val what = 2 // Summit
    // val what = 3 // Histogram

    //COVER
    val cover =
      what match{
        case 0 => {
          DS1.COVER(CoverFlag.COVER, new N{override val n=4}, new N{override val n=9;}, List(), None )
        }

        case 1 => {
          DS1.COVER(CoverFlag.FLAT, new N{override val n=2;}, new N{override val n=3;}, List(), None )
        }

        case 2 => {
          DS1.COVER(CoverFlag.SUMMIT, new N{override val n=2;}, new N{override val n=5;}, List(), None )
        }

        case 3 => {
          DS1.COVER(CoverFlag.HISTOGRAM, new ANY{}, new ANY{}, List(), None )
        }
      }



    server setOutputPath output_path MATERIALIZE cover

    server.run()
  }

 }
