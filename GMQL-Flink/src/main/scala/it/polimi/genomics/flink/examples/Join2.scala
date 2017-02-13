package it.polimi.genomics.flink.examples

import it.polimi.genomics.core.DataStructures.JoinParametersRD._
import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.BedScoreParser2

/**
 * The entry point of the application
 * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
 */
object Join2 {

  def main(args : Array[String]) {

    val server = new GmqlServer(new FlinkImplementation(/*50000,*/ maxBinDistance =  20))
    val mainPath = "/home/abdo/"
    val ex_data_path = List(mainPath + "join3/ref/")
    val REF_DS_Path = List(mainPath + "join3/exp/")
    val ex_data_path_optional2 = List(mainPath + "join2/exp2/")
    val output_path = mainPath + "res/"


    val DS1 = server READ ex_data_path USING BedScoreParser2
    val REFDS = server READ REF_DS_Path USING BedScoreParser2


    val what = -1


    val join = what match{

      case -2 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(DistLess(200000)), Some(new MinDistance(1)), Some(new Upstream))), RegionBuilder.CONTIG, REFDS)

      case -1 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(1)))), RegionBuilder.CONTIG, REFDS)

      case 0 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(DistLess(100)))), RegionBuilder.CONTIG, REFDS)

      case 1 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)), Some(new MinDistance(1)))), RegionBuilder.CONTIG, REFDS)

      case 2 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)), Some(new Upstream()))), RegionBuilder.CONTIG, REFDS)

      case 3 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(DistGreater(4)), Some(new Upstream()), Some(new DistLess(20)))), RegionBuilder.CONTIG, REFDS)

      case 4 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(DistLess(20)), Some(new MinDistance(1)), Some(DistGreater(3)))), RegionBuilder.CONTIG, REFDS)

      case 5 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(new DistLess(0)))), RegionBuilder.CONTIG, REFDS)

      case 6 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(new DistGreater(0)))), RegionBuilder.CONTIG, REFDS)

      case 7 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(new Upstream()))), RegionBuilder.CONTIG, REFDS)

      case 8 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(new DownStream()))), RegionBuilder.CONTIG, REFDS)

      case 9 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(1)))), RegionBuilder.CONTIG, REFDS)

      case 10 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(2)))), RegionBuilder.CONTIG, REFDS)

      case 11 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(new DistGreater(0)), Some(new MinDistance(1)))), RegionBuilder.CONTIG, REFDS)

      case 12 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(new DistLess(0)), Some(new MinDistance(1)))), RegionBuilder.CONTIG, REFDS)

      case 13 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(new DistGreater(0)), Some(new Upstream()), Some(new MinDistance(1)))), RegionBuilder.CONTIG, REFDS)

      case 14 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(10)))), RegionBuilder.CONTIG, REFDS)

      case 15 =>
        DS1.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(10)), Some(DistLess(-1)))), RegionBuilder.CONTIG, REFDS)

    }
    server setOutputPath output_path MATERIALIZE join

    server.run()
  }

}

