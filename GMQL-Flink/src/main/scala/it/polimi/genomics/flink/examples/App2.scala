package it.polimi.genomics.flink.examples

import it.polimi.genomics.GMQLServer.GmqlServer
import it.polimi.genomics.core.DataStructures.CoverParameters.{CoverFlag, N}
import it.polimi.genomics.core.DataStructures.MetadataCondition.{META_OP, Predicate}
import it.polimi.genomics.flink.FlinkImplementation.FlinkImplementation
import it.polimi.genomics.flink.FlinkImplementation.reader.parser.{BasicParser, BedScoreParser}


/**
 * The entry point of the application
 * It initialize the server, call server's methods to build the query and invoke the server's run method to start the execution.
 */
object App2 {

  def main(args : Array[String]) {
    //val server = new GmqlServer(new FlinkImplementation(testingOutputFormat = false)/*, meta_implementation = Some(new FlinkImplementation(metaFirst = true))*/)
    for (i <- args){
      println(i)
    }

    val server = new GmqlServer(new FlinkImplementation(/*defaultBinSize = args(3).toLong*/))

    val inPath1 = "/Users/michelebertoni/Workspaces/IdeaProjects/genomic_computing/GMQLv2Scala/scalav2/trunk/GMQL-Flink/src/test/datasets/"
    val ex_data_path = List(inPath1 + "pietro/")
    val output_path = inPath1 + "res/"

    val main = "/Users/michelebertoni/Desktop/error_datasets/test_1_1/"
    val ds1 = main + "EXP1/"
    val ds2 = main + "EXP2/"
    val out = main + "resF1/"



    val mainPath = /*"hdfs://172.31.21.223:9000/" +*/ args(0) + "/"
    //val ex_data_path = "hdfs://127.0.0.1:9000/user/Inputs/flinkInput/"
    val output_path2 = "hdfs://localhost:9000/user/bertoni/results/coverTest"+args(4)+"/"

    //val ds1 = "test3"

    val ds1R = server READ ds1 USING BedScoreParser
    val ds2R = server READ ds2 USING BedScoreParser
    val ds3R = server READ mainPath USING BasicParser //TODO


    val ds1S = ds1R.SELECT(Predicate("antibody", META_OP.EQ, "CTCF"))
    val ds2S = ds2R.SELECT(Predicate("antibody", META_OP.EQ, "BRD4"))
//Materialize data

    //val map = ds1S.MAP(MetaJoinCondition(List("organism")), List(), ds1S)
    //val res = ds2S.JOIN(None, List(new JoinQuadruple(Some(new MinDistance(1)))), RegionBuilder.CONTIG, ds1S)
    val cover = ds3R.COVER(CoverFlag.COVER, N(args(1).toInt), N(args(2).toInt), List(), None)
/*
    val cR = server READ ds1 USING BedScoreParser
    val cS = cR SELECT(Predicate("antibody", META_OP.EQ, "CTCF"))
    val res = cS COVER(CoverFlag.COVER, N(1), N(2), List(), None)
*/

    val res = cover//.ORDER(None, "group_name", NoTop(), Some(List((0, Direction.ASC))), NoTop())
    server setOutputPath output_path2 MATERIALIZE res

/*

    println("-------------RESULT--------------")
    println(server.extractMetaFirst().mkString("\n"))
    println("-------------RESULT END--------------")
*/

    server.run()

  }


}
