//package it.polimi.genomics.scidb.test.operator
//
//import it.polimi.genomics.core.DataStructures.ExecutionParameters.BinningParameter
//import it.polimi.genomics.core.DataStructures.JoinParametersRD._
//import it.polimi.genomics.core.DataStructures.MetaJoinCondition.MetaJoinCondition
//import it.polimi.genomics.core.DataStructures._
//import it.polimi.genomics.core.ParsingType
//import it.polimi.genomics.scidb.repository.GmqlSciExporter
//import it.polimi.genomics.scidb.{GmqlSciConfig, GmqlSciImplementation}
//
///**
//  * Created by Cattani Simone on 17/03/16.
//  * Email: simone.cattani@mail.polimi.it
//  *
//  */
//object UnionTest
//{
//  def main(args: Array[String]): Unit =
//  {
//    println("\n-----------------------------------------------")
//    println("-- Join TEST ----------------------------------")
//    println("-----------------------------------------------")
//
//    val LEFT = IRDataSet("BERT_JOIN_REF", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val RIGHT = IRDataSet("BERT_JOIN_EXP", List(
//      ("val1",ParsingType.INTEGER),
//      ("val2",ParsingType.STRING)
//    ))
//
//    val RES = IRDataSet("BERT_UNION2_REF_STORE", List(
//      ("left$val1",ParsingType.INTEGER),
//      ("left$val2",ParsingType.STRING),
//      ("right$val2",ParsingType.STRING)
//    ))
//
//    val ReadAncMD = IRReadMD(List(), new FakeParser, LEFT)
//    val ReadAncRD = IRReadRD(List(), new FakeParser, LEFT)
//    val ReadExpMD = IRReadMD(List(), new FakeParser, RIGHT)
//    val ReadExpRD = IRReadRD(List(), new FakeParser, RIGHT)
//
//    val UnionMD = IRUnionMD(ReadAncMD, ReadExpMD)
//    val UnionRD = IRUnionRD(List(0,2), ReadAncRD, ReadExpRD)
//
//    val StoreAncMD = IRStoreMD("", UnionMD, RES)
//    val StoreAncRD = IRStoreRD("", UnionRD, RES)
//
//    implicit val binS = BinningParameter(None)
//    var variable = IRVariable(StoreAncMD,StoreAncRD)
//
//    // ------------------------------------------------------------
//    // ------------------------------------------------------------
//
//    val implementation = new GmqlSciImplementation
//
//    implementation.addDAG(variable)
//    implementation.go()
//
//    if(GmqlSciConfig.scidb_server_on)
//    GmqlSciExporter.exportDS(
//      "BERT_UNION2_REF_STORE",
//      "/home/scidb/test/real/bertEXP/union/res",
//      List(
//        ("left$val1",ParsingType.INTEGER),
//        ("left$val2",ParsingType.STRING),
//        ("right$val2",ParsingType.STRING)
//      ), "tsv"
//    )
//
//  }
//}
