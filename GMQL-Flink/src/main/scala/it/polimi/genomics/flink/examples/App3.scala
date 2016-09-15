package it.polimi.genomics.flink.examples

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.CoverParameters.{CoverFlag, N}
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.BedScoreParser


/**
 * The entry point of the application
 * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
 */
object App3 {

  def main(args : Array[String]) {
    val server = new GmqlServer(new FlinkImplementation(testingIOFormats = true, defaultBinSize = 50000)/*, meta_implementation = Some(new FlinkImplementation(metaFirst = true))*/)

    val main = "/Users/abdulrahman/Downloads/"
    val ds1 = main + "annotations/"
    val ds2 = main + "beds/"
    val out = main + "resF1/"


    //val ds1 = "test3"

    //val ds1R = server READ ds1 USING RnaSeqParser
    val ds2R = server READ ds2 USING BedScoreParser


    //val ds1S = ds1R.SELECT(NOT(Predicate("antibody", META_OP.EQ, "CTCF")))
    //val ds2S = ds2R.SELECT(Predicate("antibody", META_OP.EQ, "BRD4"))


    //val map = ds1R.MAP(None, List(), ds2R, None)
    //val map = ds1S.MAP(MetaJoinCondition(List("organism")), List(), ds1S)
    //val res = ds2S.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(1)))), RegionBuilder.CONTIG, ds1S)
    val cover = ds2R.COVER(CoverFlag.COVER, N(1), N(2), List(), Some(List("antibody")))
/*
    val cR = server READ ds1 USING BedScoreParser
    val cS = cR SELECT(Predicate("antibody", META_OP.EQ, "CTCF"))
    val res = cS COVER(CoverFlag.COVER, N(1), N(2), List(), None)
*/

    val res = cover//.ORDER(None, "group_name", NoTop(), Some(List((0, Direction.ASC))), NoTop())
    server setOutputPath out MATERIALIZE res

/*

    println("-------------RESULT--------------")
    println(server.extractMetaFirst().mkString("\n"))
    println("-------------RESULT END--------------")
*/

    server.run()

  }


}
