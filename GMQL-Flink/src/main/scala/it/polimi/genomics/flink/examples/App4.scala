package it.polimi.genomics.flink.examples

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.GroupMDParameters.{Direction, NoTop}
import it.polimi.genomics.core.DataStructures.JoinParametersRD.{DistLess, JoinQuadruple, MinDistance, RegionBuilder}
import it.polimi.genomics.core.DataStructures.MetaJoinCondition.{Default, MetaJoinCondition}
import it.polimi.genomics.core.DataStructures.MetadataCondition.{META_OP, Predicate}
import it.polimi.genomics.core.DataStructures.RegionCondition
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.BedScoreParser


/**
 * The entry point of the application
 * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
 */
object App4 {

  def main(args : Array[String]) {
    val server = new GmqlServer(new FlinkImplementation(testingIOFormats = true)/*, meta_implementation = Some(new FlinkImplementation(metaFirst = true))*/)
    val inPath1 = "/Users/michelebertoni/Workspaces/IdeaProjects/genomic_computing/GMQLv2Scala/scalav2/trunk/GMQL-Flink/src/test/datasets/"
    val ex_data_path = List(inPath1 + "pietro/")
    val output_path = inPath1 + "res/"

    val main = "/Datasets/"
    val ds1 = main + "EXP1/"
    val ds2 = main + "EXP2/"
    val out = main + "output/"


    //val ds1 = "test3"

    val ds1R = server READ ds1 USING BedScoreParser
    val ds2R = server READ ds2 USING BedScoreParser


    //val ds1S = ds1R.SELECT(reg_con = AND(RegionCondition.LeftEndCondition(REG_OP.GTE, 1000), RegionCondition.RightEndCondition(REG_OP.LTE, 10000)))
    val ds1S = ds1R.SELECT(reg_con = RegionCondition.ChrCondition("chr1"))
    //val ds1S = ds1R.SELECT(Predicate("antibody", META_OP.EQ, "CTCF"))
    val ds2S = ds2R.SELECT(Predicate("antibody", META_OP.EQ, "BRD4"))


    val join = ds1S.JOIN(Some(MetaJoinCondition(List(Default("organism")))), List(new JoinQuadruple(Some(DistLess(100000)),Some(new MinDistance(1)))), RegionBuilder.RIGHT, ds2S)

    val map = ds1S.MAP(Some(MetaJoinCondition(List(Default("organism")))), List(), join)

    //val cover = join.COVER(CoverFlag.COVER, N(1), N(2), List(), None)
/*
    val cR = server READ ds1 USING BedScoreParser
    val cS = cR SELECT(Predicate("antibody", META_OP.EQ, "CTCF"))
    val res = cS COVER(CoverFlag.COVER, N(1), N(2), List(), None)
*/

    val res = map.ORDER(None, "group_name", NoTop(), Some(List((0, Direction.ASC))), NoTop())
    server setOutputPath out MATERIALIZE res

/*

    println("-------------RESULT--------------")
    println(server.extractMetaFirst().mkString("\n"))
    println("-------------RESULT END--------------")
*/

    server.run(graph = true)

  }


}
