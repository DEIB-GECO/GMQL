package it.polimi.genomics.flink.examples

import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.BedScoreParser

/**
 * The entry point of the application
 * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
 */
object Join {

  def main(args : Array[String]) {

    val server = new GmqlServer(new FlinkImplementation(50000, 5))
    val mainPath = "/Users/michelebertoni/Workspaces/IdeaProjects/genomic_computing/GMQLv2Scala/scalav2/trunk/GMQL-Flink/src/test/datasets/"
    val ex_data_path = List(mainPath + "join/ref/")
    val ex_data_path_optional = List(mainPath + "join/exp/")
    val output_path = mainPath + "res/"


    val dataAsTheyAre = server READ ex_data_path_optional USING BedScoreParser
    val optionalDS = server READ ex_data_path USING BedScoreParser

    //val what = 0 // Join distLess
    //val what = 1 // Join distLess minDist
    //val what = 2 // Join distLess upstream
    //val what = 3 // Join distGreat
    val what = 4 // Join distGreat minDist distless



    val join = what match{

      case 0 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)))), RegionBuilder.CONTIG, optionalDS)

      case 1 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)), Some(new MinDistance(1)))), RegionBuilder.CONTIG, optionalDS)

      case 2 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)), Some(new Upstream()))), RegionBuilder.CONTIG, optionalDS)

      case 3 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistGreater(4)), Some(new Upstream()), Some(new DistLess(20)))), RegionBuilder.CONTIG, optionalDS)

      case 4 =>
        dataAsTheyAre.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)), Some(new MinDistance(1)), Some(DistGreater(3)))), RegionBuilder.CONTIG, optionalDS)

    }
    server setOutputPath output_path MATERIALIZE join

    server.run()
  }

}

